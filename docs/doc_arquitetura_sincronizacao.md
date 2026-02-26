# Documentação — Arquitetura de Tarefas: Celery + Redis + Sincronização Incremental

Este documento descreve como deve ser implementada a arquitetura de tarefas assíncronas e paralelas do script Python, utilizando Celery e Redis, com sincronização incremental de dados dos ERPs.

---

## Visão Geral

O sistema deve processar múltiplas empresas e múltiplos ERPs simultaneamente, sem que uma sincronização bloqueie ou atrase outra. Para isso, cada combinação de empresa + ERP + tipo de dado deve ser tratada como uma tarefa independente, enfileirada no Redis e executada em paralelo pelos workers do Celery.

```
Scheduler (a cada 30 minutos)
        ↓
Enfileira todas as tarefas no Redis de uma vez
        ↓
Workers Celery processam em paralelo
  worker_1 → empresa_1 : tiny : sales
  worker_2 → empresa_1 : tiny : stock
  worker_3 → empresa_2 : tiny : sales
  worker_4 → empresa_2 : tiny : stock
        ↓
Tempo total = tempo da tarefa mais lenta, não a soma de todas
```

---

## Dependências necessárias

```bash
pip install celery redis
```

O Redis deve rodar como serviço separado. Pode ser instalado na própria VPS ou utilizar o **Upstash Redis** como serviço externo gratuito para volumes pequenos. A URL de conexão do Redis deve estar no `.env`:

```bash
REDIS_URL=redis://localhost:6379/0
```

---

## Configuração do Celery

O Celery deve ser configurado com filas separadas por ERP para controlar o paralelismo e respeitar os rate limits de cada API:

```python
# celery_config.py

from kombu import Queue

CELERY_BROKER_URL = os.getenv('REDIS_URL')
CELERY_RESULT_BACKEND = os.getenv('REDIS_URL')

CELERY_TASK_QUEUES = (
    Queue('default'),
    Queue('tiny'),
    Queue('bling'),
    Queue('omie'),
    Queue('contaazul'),
)

CELERY_DEFAULT_QUEUE = 'default'

# Número máximo de tarefas simultâneas por fila
# Ajustar conforme rate limit de cada ERP
CELERYD_CONCURRENCY = 4         # workers gerais
```

Cada ERP deve ter seus workers iniciados com concorrência controlada:

```bash
# workers gerais
celery -A tasks worker --queues=default --concurrency=4

# workers do Tiny (respeita rate limit da API)
celery -A tasks worker --queues=tiny --concurrency=2

# workers do Bling
celery -A tasks worker --queues=bling --concurrency=2

# workers do Conta Azul
celery -A tasks worker --queues=contaazul --concurrency=2
```

---

## Tabela de checkpoints

Para a sincronização incremental funcionar, é necessário criar a seguinte tabela no banco antes de implementar as tarefas:

```sql
CREATE TABLE auth_integrations.sync_checkpoints (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id    UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,
  erp_type      TEXT NOT NULL,    -- 'tiny', 'bling', 'omie', etc.
  entity        TEXT NOT NULL,    -- 'sales', 'stock', 'customers', etc.
  last_sync_at  TIMESTAMPTZ,      -- última execução bem-sucedida
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (company_id, erp_type, entity)
);
```

Essa tabela é consultada antes de cada sincronização para saber o ponto de partida da busca na API, e atualizada ao final de cada sincronização bem-sucedida.

---

## Sincronização incremental

O script nunca deve buscar um período fixo (como "mês passado + mês atual") em todas as execuções. A lógica correta é:

```python
def get_sync_start(company_id, erp_type, entity):
    checkpoint = busca_checkpoint(company_id, erp_type, entity)

    if checkpoint and checkpoint.last_sync_at:
        return checkpoint.last_sync_at      # busca só o que mudou
    else:
        return agora - timedelta(days=30)   # primeira sync: últimos 30 dias

def update_checkpoint(company_id, erp_type, entity):
    # upsert no sync_checkpoints com last_sync_at = NOW()
    # atualizar APENAS após sincronização bem-sucedida
```

A primeira sincronização de uma empresa nova faz a carga do período inicial definido. As seguintes buscam apenas o delta desde a última execução bem-sucedida — muito mais rápido e leve para a API do ERP.

---

## Definição das tarefas

Cada tipo de sincronização deve ser uma tarefa Celery independente, com retry automático em caso de falha:

```python
# tasks.py

from celery import Celery

app = Celery('tasks')
app.config_from_object('celery_config')

@app.task(
    bind=True,
    queue='tiny',
    max_retries=3,
    default_retry_delay=60    # aguarda 60s antes de tentar novamente
)
def sync_tiny_sales(self, company_id):
    try:
        token = token_manager.get_valid_token(company_id, 'tiny')
        data_inicial = get_sync_start(company_id, 'tiny', 'sales')

        collector.fetch_sales(company_id, token, data_inicial)
        normalizer.process_sales(company_id)

        update_checkpoint(company_id, 'tiny', 'sales')

    except Exception as exc:
        raise self.retry(exc=exc)


@app.task(
    bind=True,
    queue='tiny',
    max_retries=3,
    default_retry_delay=60
)
def sync_tiny_stock(self, company_id):
    try:
        token = token_manager.get_valid_token(company_id, 'tiny')
        data_inicial = get_sync_start(company_id, 'tiny', 'stock')

        collector.fetch_stock(company_id, token, data_inicial)
        normalizer.process_stock(company_id)

        update_checkpoint(company_id, 'tiny', 'stock')

    except Exception as exc:
        raise self.retry(exc=exc)
```

---

## Scheduler

O scheduler deve rodar a cada 30 minutos, buscar todas as empresas ativas e enfileirar as tarefas de uma vez. O `.delay()` envia a tarefa para a fila e segue imediatamente — não bloqueia o scheduler esperando a execução terminar:

```python
# scheduler.py

from celery.schedules import crontab

app.conf.beat_schedule = {
    'sync-all-companies': {
        'task': 'tasks.dispatch_all',
        'schedule': crontab(minute='*/30'),   # a cada 30 minutos
    },
}


@app.task
def dispatch_all():
    companies = supabase \
        .schema('auth_integrations') \
        .table('companies') \
        .select('id') \
        .eq('is_active', True) \
        .execute()

    for company in companies.data:
        company_id = company['id']

        # busca conexões ativas da empresa
        connections = supabase \
            .schema('auth_integrations') \
            .table('erp_connections') \
            .select('erp_type') \
            .eq('company_id', company_id) \
            .eq('is_active', True) \
            .execute()

        for conn in connections.data:
            erp = conn['erp_type']

            if erp == 'tiny':
                sync_tiny_sales.delay(company_id)
                sync_tiny_stock.delay(company_id)

            elif erp == 'bling':
                sync_bling_sales.delay(company_id)
                sync_bling_stock.delay(company_id)

            elif erp == 'contaazul':
                sync_contaazul_sales.delay(company_id)
                sync_contaazul_stock.delay(company_id)
```

O Celery Beat deve ser iniciado separadamente para gerenciar o agendamento:

```bash
celery -A tasks beat --loglevel=info
```

---

## Proteção contra tarefas duplicadas

O scheduler não deve enfileirar uma nova tarefa para uma empresa que já tem a mesma tarefa em execução. Isso evita sobrecarga quando uma sincronização demora mais que o intervalo do scheduler:

```python
from celery.utils.log import get_task_logger

@app.task(bind=True, queue='tiny')
def sync_tiny_sales(self, company_id):
    task_id = f'sync_tiny_sales_{company_id}'

    # verifica se já existe uma tarefa igual rodando
    existing = redis_client.get(task_id)
    if existing:
        return  # ignora, já está em execução

    # registra no Redis com TTL de 25 minutos
    redis_client.setex(task_id, 1500, 'running')

    try:
        # lógica de sincronização
        ...
    finally:
        redis_client.delete(task_id)  # libera ao terminar
```

---

## Monitoramento com Flower

O Flower é uma interface visual para acompanhar as tarefas do Celery em tempo real — tarefas ativas, com falha, tempo de execução e fila de pendentes. Instalar e rodar na VPS:

```bash
pip install flower
celery -A tasks flower --port=5555
```

Acesse via `http://ip-da-vps:5555`. Recomenda-se proteger com senha em produção:

```bash
celery -A tasks flower --port=5555 --basic_auth=usuario:senha
```

---

## Regras importantes

- O scheduler nunca executa as tarefas diretamente — apenas as enfileira com `.delay()`.
- O checkpoint deve ser atualizado apenas após sincronização bem-sucedida. Nunca antes ou em caso de erro.
- Cada ERP deve ter sua própria fila e seus próprios workers para respeitar os rate limits de cada API independentemente.
- Em caso de falha, o Celery faz retry automático até `max_retries`. Se esgotar as tentativas, a tarefa vai para a fila de falhas e deve ser monitorada via Flower.
- Nunca usar `.apply()` ou `.run()` diretamente — sempre `.delay()` ou `.apply_async()` para garantir que a tarefa vai para a fila e não bloqueia o processo principal.
- O número de workers por fila deve ser ajustado conforme o rate limit documentado de cada ERP. Começar com 2 workers por ERP e ajustar conforme necessário.
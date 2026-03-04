# Documentação — Funcionamento Geral do Banco de Dados e Script Python

Este documento descreve como o script Python deve interagir com o banco de dados, o que é responsabilidade do script, o que é responsabilidade do banco, e o que nunca deve acontecer.

---

## Visão Geral

O banco de dados e o script Python têm responsabilidades bem definidas e complementares. O banco garante integridade, isolamento e segurança dos dados. O script cuida da lógica de negócio — coleta, transformação, normalização e orquestração.

Para detalhes específicos de cada fluxo:
- **Onboarding**: veja `docs/doc_fluxo_onboarding.md`.
- **Arquitetura de tarefas (Celery + Redis) e sync incremental**: veja `docs/doc_arquitetura_sincronizacao.md`.

```
Formulário / Agendador
        ↓
Script Python
  ├── token_manager       → garante tokens válidos antes de qualquer requisição
  ├── collector           → coleta dados da API e insere no staging
  └── normalizer          → lê o staging e normaliza para o core
        ↓
Banco de Dados (Supabase / PostgreSQL)
  ├── auth_integrations   → empresas e conexões
  ├── staging             → dados brutos
  └── core                → dados normalizados
```

---

## O que o banco faz automaticamente

O script não precisa se preocupar com as seguintes operações — o banco as executa sozinho:

- **Geração de UUIDs** — todos os campos `id` são gerados automaticamente pelo banco via `uuid_generate_v4()`. O script nunca deve gerar ou informar UUIDs manualmente.
- **Preenchimento do `created_at`** — definido automaticamente com `NOW()` no momento do INSERT. O script nunca deve informar esse campo.
- **Atualização do `updated_at`** — atualizado automaticamente pelo trigger `update_updated_at()` em qualquer UPDATE. O script nunca deve atualizar esse campo manualmente.
- **Cascata de deleção** — se uma empresa for removida de `companies`, o banco apaga automaticamente todos os registros relacionados nos demais schemas via `ON DELETE CASCADE`.
- **Rejeição de duplicatas** — as constraints `UNIQUE` em todas as tabelas garantem que o banco rejeite inserções duplicadas. O script deve usar sempre `upsert` em vez de `insert` simples para aproveitar esse comportamento.
- **Isolamento por empresa** — o RLS garante que cada empresa só acesse os próprios dados quando a requisição vem do frontend. O script usa a `service_role key` que bypassa o RLS, então a responsabilidade de filtrar por `company_id` corretamente é do próprio script.

---

## O que o script deve fazer

### Conexão com o banco

- Conectar sempre usando a `service_role key` e a `SUPABASE_URL`, ambas carregadas exclusivamente de variáveis de ambiente via arquivo `.env`.
- Nunca expor as credenciais em logs, código-fonte ou repositório.
- Especificar o schema em todas as operações: `supabase.schema('core').table('sales')`.

### Onboarding de empresas

- Validar todos os dados do formulário antes de qualquer inserção, incluindo **client_id**, **client_secret** e **redirect_uri** — necessários para a conexão com a API e para obter os tokens (troca do `code` por `access_token` e `refresh_token`).
- Criptografar o `erp_login`, o `erp_password` e o **client_secret** com AES usando a `ENCRYPTION_KEY` do `.env` antes de salvar no banco. O `client_id` e o `redirect_uri` podem ser armazenados em texto puro.
- Inserir a empresa e a conexão com o ERP (incluindo credenciais OAuth: `client_id`, `client_secret`, `redirect_uri`) em uma única operação atômica (CTE), garantindo que nunca haja uma empresa sem conexão ou uma conexão sem empresa.
- Acionar o Selenium automaticamente após a inserção para autenticar no ERP e obter os tokens iniciais (usando `client_id` e `client_secret` para trocar o `code` por tokens na API do ERP).

### Gerenciamento de tokens (token_manager)

- Verificar a validade do `access_token` antes de cada requisição à API do ERP comparando `access_token_expires_at` com `NOW()`.
- Se o `access_token` estiver expirado, usar o **client_id**, o **client_secret** (descriptografado do banco) e o `refresh_token` para renová-lo via API do ERP e atualizar no banco: `access_token`, `access_token_expires_at`, `last_token_refresh_at`.
- Se o `refresh_token` estiver expirado, descriptografar `erp_login`, `erp_password` e `client_secret` do banco e acionar o Selenium para reiniciar o ciclo de autenticação; ao obter o `code`, usar `client_id` e `client_secret` para trocá-lo por novos tokens na API do ERP.
- Se o Selenium falhar, marcar `is_active = false` na conexão e registrar o erro. Nunca tentar fazer requisições com token inválido.
- Nunca armazenar tokens em variáveis de ambiente ou logs.

### Coleta de dados — collector

- Buscar apenas conexões com `is_active = true` antes de iniciar qualquer sincronização.
- Passar sempre pelo `token_manager` antes de qualquer requisição à API do ERP.
- Inserir o payload bruto completo no staging exatamente como retornado pela API, sem nenhuma alteração. As tabelas de staging são por ERP: `staging.tiny_sales`, `staging.tiny_stock`, `staging.tiny_sale_items` (Tiny); `staging.contaazul_sales`, `staging.contaazul_stock`, `staging.contaazul_sale_items` (Conta Azul).
- Registrar o `fetched_at` com o momento exato da coleta.
- Atualizar `last_sync_at` em `erp_connections` ao final de cada coleta bem-sucedida.
- Tratar paginação da API — nunca assumir que todos os dados vieram em uma única resposta.

### Normalização de dados — normalizer

- Buscar apenas registros com `processed_at IS NULL` no staging.
- Processar sempre na ordem correta: primeiro `core.customers`, depois `core.sales`. Nunca inserir uma venda sem antes garantir que o cliente existe.
- Usar sempre `upsert` em todas as tabelas do core, nunca `insert` simples, para evitar duplicatas em sincronizações repetidas.
- Em caso de erro na normalização, registrar a mensagem em `process_error` e manter `processed_at` como nulo, deixando o registro na fila para reprocessamento.
- Nunca alterar o `raw_data` do staging após a inserção.
- Ao normalizar com sucesso, preencher `processed_at` com o horário atual.

### Agendamento — scheduler

- Buscar todas as empresas com `is_active = true` em `companies` antes de enfileirar tarefas.
- Enfileirar tarefas separadas por empresa e por tipo de dado (`sync_*_sales`, `sync_*_stock`).
- Garantir que o agendamento não bloqueie o processo principal (tarefa é sempre enfileirada, não executada inline).
- Para a arquitetura completa de filas, workers, retries e proteção contra duplicidade, consulte `docs/doc_arquitetura_sincronizacao.md`.

---

## O que o script nunca deve fazer

- **Nunca gerar UUIDs manualmente** — o banco gera automaticamente.
- **Nunca informar `created_at` ou `updated_at`** — o banco gerencia automaticamente.
- **Nunca usar `insert` simples nas tabelas do core** — sempre `upsert` para respeitar as constraints de unicidade.
- **Nunca fazer requisições à API do ERP sem passar pelo `token_manager`** — tokens expirados causam bloqueios.
- **Nunca salvar tokens, login, senha ou client_secret em texto puro** — sempre criptografado antes de qualquer persistência.
- **Nunca logar tokens, senhas ou a `service_role key`** em arquivos de log.
- **Nunca deletar registros do staging** — o histórico de payloads brutos é intencional.
- **Nunca deletar registros do core para "limpar" dados** — use filtros por `status`, `is_active` ou período nas queries.
- **Nunca alterar o `raw_data` após a inserção no staging** — ele deve ser uma cópia fiel do que a API retornou.
- **Nunca processar uma empresa com `is_active = false`** — respeitar o estado definido no banco.
- **Nunca expor a `service_role key` em qualquer camada que não seja o backend** — ela dá acesso irrestrito ao banco.
- **Nunca assumir que a API do ERP retornou todos os dados em uma única página** — sempre tratar paginação.

---

## Responsabilidades resumidas

| Responsabilidade                        | Banco | Script |
|-----------------------------------------|-------|--------|
| Gerar UUIDs                             | ✓     |        |
| Preencher created_at                    | ✓     |        |
| Atualizar updated_at                    | ✓     |        |
| Rejeitar duplicatas                     | ✓     |        |
| Isolar dados por empresa (RLS)          | ✓     |        |
| Cascata de deleção                      | ✓     |        |
| Validar dados do formulário             |       | ✓      |
| Criptografar credenciais                |       | ✓      |
| Gerenciar tokens                        |       | ✓      |
| Coletar dados das APIs                  |       | ✓      |
| Normalizar staging para core            |       | ✓      |
| Filtrar por company_id nas queries      |       | ✓      |
| Atualizar last_sync_at                  |       | ✓      |
| Atualizar last_token_refresh_at         |       | ✓      |
| Preencher processed_at após normalizar  |       | ✓      |
| Registrar erros em process_error        |       | ✓      |
| Agendar e paralelizar tarefas           |       | ✓      |
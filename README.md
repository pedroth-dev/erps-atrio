# Sistema de Integração com ERPs

Sistema modular para integração com ERPs (Tiny, Bling, Omie, etc.) usando Supabase como banco de dados centralizado. Todas as credenciais sensíveis são criptografadas antes de serem salvas no banco.

## 🏗️ Arquitetura

O sistema é dividido em módulos bem definidos:

```
erps_atrio/
├── src/                          # Código fonte modular
│   ├── config/                   # Configurações centralizadas
│   ├── database/                 # Cliente Supabase com criptografia
│   ├── auth/                     # Gerenciamento de tokens OAuth
│   ├── integrations/             # Clientes para APIs dos ERPs
│   └── sync/                     # Módulos de sincronização
│
├── scripts/                      # Scripts executáveis
│   ├── onboarding.py             # Cadastro de empresas
│   └── sync_company.py           # Sincronização de dados
│
├── database/                     # Schemas do banco de dados
│   └── schemas/                  # Arquivos SQL e documentação
│
└── [documentação]
```

## 📋 Pré-requisitos

- Python 3.8+
- Conta no Supabase com banco configurado
- Chrome instalado (para automação Selenium)
- Credenciais OAuth do ERP (Tiny, Bling, etc.)

## 🚀 Instalação

### 1. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 2. Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto:

```env
# Supabase Configuration
SUPABASE_URL=https://seu-projeto.supabase.co
SUPABASE_SERVICE_ROLE_KEY=sua-service-role-key-aqui
ENCRYPTION_KEY=sua-chave-aes-32-caracteres-aleatorios

# Redis (para Celery — tarefas assíncronas e scheduler; opcional para sync manual)
REDIS_URL=redis://localhost:6379/0
```

**Importante:**
- A `ENCRYPTION_KEY` deve ser uma string de 32 caracteres usada para criptografar credenciais no banco
- **Nunca** commite o arquivo `.env` no repositório
- Veja abaixo como gerar a `ENCRYPTION_KEY`

### 3. Gerar ENCRYPTION_KEY

**Opção 1 - Python:**
```python
import secrets
print(secrets.token_urlsafe(32))
```

**Opção 2 - OpenSSL:**
```bash
openssl rand -base64 32
```

**Opção 3 - PowerShell (Windows):**
```powershell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Minimum 0 -Maximum 256 }))
```

Cole o resultado no `.env` como:
```
ENCRYPTION_KEY=sua-chave-gerada-aqui
```

⚠️ **ATENÇÃO:** Guarde essa chave com segurança! Se você perdê-la, não será possível descriptografar as credenciais já salvas no banco.

### 4. Configurar Banco de Dados

Execute os arquivos SQL na ordem indicada em `database/README.md`:

1. `database/schemas/schemas.sql` - Cria os schemas
2. `database/schemas/auth_integrations/table_companies.sql`
3. `database/schemas/auth_integrations/table_erp_connections.sql`
4. `database/schemas/auth_integrations/table_sync_checkpoints.sql` - Checkpoints para sync incremental
5. `database/schemas/auth_integrations/triggers.sql`
6. `database/schemas/auth_integrations/rls.sql`
7. `database/schemas/staging/staging_tiny_sales.sql`
8. `database/schemas/staging/staging_tiny_stock.sql`
9. `database/schemas/core/table_customers.sql`
10. `database/schemas/core/table_sales.sql`
11. `database/schemas/core/table_stock.sql`

### 5. Configurar Extra Search Path no Supabase

No painel do Supabase:
- Acesse **Project Settings → API**
- Em **Exposed schemas** e em **Extra Search Path**, adicione:
```
auth_integrations, staging, core
```

Isso permite que o cliente Python acesse as tabelas dos schemas customizados.

## 📖 Uso

### Onboarding de Nova Empresa

Cadastra uma nova empresa e configura autenticação OAuth automaticamente:

```bash
python scripts/onboarding.py \
  "Nome da Empresa" \
  "12345678000190" \
  tiny \
  "user@email.com" \
  "senha123" \
  "client-id-da-aplicacao" \
  "client-secret-da-aplicacao" \
  "https://agregarnegocios.com.br/oauth/tiny"
```

**Parâmetros:**
- `nome`: Nome da empresa
- `cnpj`: CNPJ da empresa (apenas números)
- `erp_type`: Tipo do ERP (`tiny`, `bling`, `omie`)
- `erp_login`: Login do usuário no ERP
- `erp_password`: Senha do usuário no ERP
- `client_id`: Client ID da aplicação OAuth registrada no ERP
- `client_secret`: Client Secret da aplicação OAuth
- `redirect_uri`: URI de redirecionamento configurada na aplicação OAuth

**O que o script faz:**
1. Valida os dados (CNPJ, credenciais)
2. Cria a empresa no banco (`auth_integrations.companies`)
3. Cria a conexão ERP com credenciais criptografadas (`auth_integrations.erp_connections`)
4. Executa automação Selenium para obter tokens OAuth
5. Salva os tokens criptografados no banco

### Sincronização de Dados

Sincroniza vendas e estoque de uma empresa:

```bash
python scripts/sync_company.py <company_id> tiny
```

**Parâmetros:**
- `company_id`: ID da empresa no banco (UUID)
- `erp_type`: Tipo do ERP (padrão: `tiny`)
- `--no-sales`: Não sincroniza vendas
- `--no-stock`: Não sincroniza estoque

**Exemplos:**
```bash
# Sincroniza tudo
python scripts/sync_company.py abc123-456-def tiny

# Apenas vendas
python scripts/sync_company.py abc123-456-def tiny --no-stock

# Apenas estoque
python scripts/sync_company.py abc123-456-def tiny --no-sales
```

**O que o script faz:**
1. Lista empresas e ERPs disponíveis; você escolhe empresa, tipo de sync (vendas/estoque/ambos)
2. Busca conexão ERP e verifica se está ativa
3. Obtém token válido (renova automaticamente se necessário)
4. Busca vendas (período: mês atual + anterior) e/ou estoque do Tiny
5. Insere dados em lotes no `staging.tiny_sales` e `staging.tiny_stock`
6. Atualiza `last_sync_at` e o checkpoint de sync incremental (`auth_integrations.sync_checkpoints`)

### Sincronização automática (Celery + Redis)

Para rodar sincronização em background, com **requisições incrementais** (apenas delta desde a última execução) e filas por ERP, use Celery + Redis. Detalhes em `arquitetura_sincronizacao.md`.

**Pré-requisito:** Redis rodando (local ou Upstash). No `.env`: `REDIS_URL=redis://localhost:6379/0`.

```bash
# Worker para fila Tiny (sync vendas e estoque)
celery -A tasks worker --queues=tiny --concurrency=2

# Scheduler: enfileira todas as empresas a cada 30 min
celery -A tasks beat --loglevel=info
```

O scheduler chama `dispatch_all`, que enfileira uma tarefa por (empresa, ERP, tipo). As tarefas usam `get_sync_start()` para definir o período da API: primeira vez = últimos 30 dias; depois = desde o último checkpoint.

## 🔐 Autenticação e Segurança

### Gerenciamento Automático de Tokens

O sistema gerencia tokens OAuth automaticamente:

1. **Access Token** (4h): Usado para requisições à API
2. **Refresh Token** (24h): Usado para renovar o access token
3. **Reautenticação automática**: Quando o refresh token expira, o sistema usa Selenium para obter novos tokens automaticamente

### Fluxo de Autenticação

```
Requisição → Token válido?
    ├─ Sim → Descriptografa e usa token existente
    ├─ Não → Renova com refresh_token (descriptografado)
    └─ Refresh expirado → Reautentica via Selenium
```

### Criptografia de Credenciais

**Credenciais sempre criptografadas no banco:**
- ✅ `erp_login` — Login do ERP
- ✅ `erp_password` — Senha do ERP
- ✅ `client_secret` — Segredo da aplicação OAuth
- ✅ `access_token` — Token de acesso
- ✅ `refresh_token` — Token de renovação
- ✅ `api_key` — Chave de API (se presente)

**Credenciais não criptografadas (não são sensíveis):**
- `client_id` — ID público da aplicação
- `redirect_uri` — URI de redirecionamento

**Credenciais que permanecem no `.env` (nunca vão para o banco):**
- `SUPABASE_SERVICE_ROLE_KEY` — Chave de acesso ao Supabase
- `ENCRYPTION_KEY` — Chave de criptografia AES

### Boas Práticas de Segurança

- ✅ Credenciais sempre criptografadas antes de salvar no banco
- ✅ Chave de criptografia (`ENCRYPTION_KEY`) nunca é commitada
- ✅ Use sempre a `service_role_key` do Supabase no backend (nunca no frontend)
- ✅ Tokens nunca são logados ou expostos
- ✅ Nunca exponha a `service_role_key` em repositórios ou frontend

## 🗄️ Estrutura do Banco de Dados

O sistema utiliza três schemas no Supabase:

- **`auth_integrations`**: Empresas e conexões ERP
  - `companies` — Empresas cadastradas
  - `erp_connections` — Conexões ERP com credenciais criptografadas

- **`staging`**: Dados brutos das APIs (antes da normalização)
  - `tiny_sales` — Vendas brutas do Tiny
  - `tiny_stock` — Estoque bruto do Tiny

- **`core`**: Dados normalizados prontos para consumo
  - `customers` — Clientes normalizados
  - `sales` — Vendas normalizadas
  - `stock` — Estoque normalizado

Veja a documentação completa em `database/schemas/`.

## 📝 Scripts Disponíveis

### `scripts/onboarding.py`

Cadastra nova empresa e configura autenticação OAuth.

**Uso:**
```bash
python scripts/onboarding.py <nome> <cnpj> <erp_type> <erp_login> <erp_password> <client_id> <client_secret> <redirect_uri>
```

### `scripts/sync_company.py`

Sincroniza dados de uma empresa específica.

**Uso:**
```bash
python scripts/sync_company.py <company_id> [erp_type] [--no-sales] [--no-stock]
```

## 🔧 Desenvolvimento

### Estrutura de Módulos

- **`src/config/`**: Configurações centralizadas (carrega `.env`)
- **`src/database/`**: Cliente Supabase com criptografia AES
- **`src/auth/`**: Token manager e fluxo OAuth com Selenium
- **`src/integrations/`**: Clientes para APIs dos ERPs
- **`src/sync/`**: Lógica de sincronização de dados

### Adicionando Novo ERP

1. Crie um novo cliente em `src/integrations/` seguindo o padrão de `tiny_client.py`
2. Adicione suporte no `token_manager.py` se necessário
3. Crie módulos de sincronização em `src/sync/`
4. Adicione tabelas de staging em `database/schemas/staging/`
5. Adicione tabelas do core em `database/schemas/core/`

## 📚 Documentação Adicional

- **`database/README.md`** — Guia dos schemas do banco
- **`database/schemas/doc_fluxo_onboarding.md`** — Fluxo completo de onboarding
- **`database/schemas/doc_funcionamento_geral.md`** — Como o script interage com o banco
- **`database/schemas/auth_integrations/doc_auth_integrations.md`** — Documentação do schema de autenticação
- **`RESUMO_CRIPTOGRAFIA.md`** — Detalhes sobre criptografia de credenciais
- **`MUDANCAS_CREDENCIAIS.md`** — Resumo das mudanças de credenciais
- **`ESTRUTURA_PROJETO.md`** — Estrutura completa do projeto
- **`VERIFICACAO_CONFORMIDADE.md`** — Verificação de conformidade com especificações

## 🐛 Troubleshooting

### Erro: "ModuleNotFoundError: No module named 'src'"

Os scripts já estão configurados para adicionar o diretório raiz ao PYTHONPATH automaticamente. Se ainda ocorrer, execute a partir do diretório raiz:

```bash
cd c:\Users\Administrator\Desktop\erps_atrio
python scripts/onboarding.py ...
```

### Erro: "Token inválido ou expirado"

O sistema tentará renovar automaticamente. Se falhar, verifique:
- Se as credenciais OAuth estão corretas no banco (não mais no `.env`)
- Se o refresh token ainda é válido no banco
- Se a conexão está ativa (`is_active = true`)

### Erro: "Conexão ERP não encontrada"

Verifique se a empresa foi cadastrada corretamente via `onboarding.py` e se o `company_id` está correto.

### Erro no Selenium

Certifique-se de que:
- O Chrome está instalado e atualizado
- O sistema tem permissão para executar o ChromeDriver
- Não há bloqueios de firewall ou antivírus

### Erro: "ENCRYPTION_KEY deve estar configurada"

Certifique-se de que a `ENCRYPTION_KEY` está configurada no `.env` e tem 32 caracteres.

### Erro ao descriptografar credenciais

Se você perdeu a `ENCRYPTION_KEY`, não será possível descriptografar as credenciais já salvas. Você precisará:
1. Gerar uma nova `ENCRYPTION_KEY`
2. Recadastrar todas as empresas com novas credenciais

## 🔄 Migração de Credenciais

As credenciais OAuth (`client_id`, `client_secret`, `redirect_uri`) foram migradas do `.env` para o banco de dados, permitindo que cada empresa tenha suas próprias credenciais OAuth.

**Antes (no `.env`):**
```env
TINY_CLIENT_ID=...
TINY_CLIENT_SECRET=...
TINY_REDIRECT_URI=...
```

**Agora (no banco por empresa):**
- Armazenadas em `auth_integrations.erp_connections`
- `client_secret` é criptografado antes de salvar
- Cada empresa pode ter credenciais OAuth diferentes

Veja `MUDANCAS_CREDENCIAIS.md` para mais detalhes.

## 📄 Licença

Uso interno - Agregar Negócios
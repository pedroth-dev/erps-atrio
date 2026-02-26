# Tabelas e Schemas — Referência

As tabelas **não** estão no schema `public`. O código Python usa explicitamente os schemas abaixo.

## Mapeamento: tabela → schema

| Schema | Tabela | Uso no código |
|--------|--------|----------------|
| **auth_integrations** | **companies** | Cadastro de empresas (onboarding) |
| **auth_integrations** | **erp_connections** | Credenciais e tokens OAuth por empresa |
| **auth_integrations** | **sync_checkpoints** | Checkpoints de sync incremental (sales, stock) |
| **staging** | **tiny_sales** | Dados brutos de vendas do Tiny |
| **staging** | **tiny_stock** | Dados brutos de estoque do Tiny |
| **staging** | **tiny_sale_items** | Dados brutos de itens de vendas do Tiny |
| **staging** | **contaazul_sales** | Dados brutos de vendas do Conta Azul |
| **staging** | **contaazul_stock** | Dados brutos de estoque do Conta Azul |
| **staging** | **contaazul_sale_items** | Dados brutos de itens de vendas do Conta Azul |
| **core** | **customers** | Clientes normalizados (normalizer) |
| **core** | **sales** | Vendas normalizadas (normalizer) |
| **core** | **sale_items** | Itens de vendas normalizados (normalizer) |
| **core** | **stock** | Estoque normalizado (normalizer) |

## Onde cada coisa é usada

### Schema `auth_integrations`
- **companies**: `get_company_by_document()`, `create_company()`, `get_all_companies()` — onboarding e dispatch de tarefas.
- **erp_connections**: criação de conexão, leitura/atualização de tokens e credenciais — token_manager, oauth_flow, sync.
- **sync_checkpoints**: `get_checkpoint()`, `upsert_checkpoint()` — sincronização incremental (sales, stock).

### Schema `staging`
- **tiny_sales**: `insert_staging_sales_batch()` (upsert), `get_pending_staging_sales()`, `mark_staging_processed()` — sync de vendas e normalizer.
- **tiny_stock**: `insert_staging_stock_batch()` (upsert), `get_pending_staging_stock()`, `mark_staging_processed()` — sync de estoque e normalizer.
- **tiny_sale_items**: `insert_staging_sale_items_batch()` (upsert), `get_pending_staging_sale_items()`, `mark_staging_processed()` — coleta de itens e normalizer.

### Schema `core`
- **customers**, **sales**, **sale_items**, **stock**: preenchidos pelos normalizers (staging → core). Todas as operações são filtradas por `company_id`; o script usa `service_role` e garante isolamento por empresa.

## Configuração no Supabase

1. **Expor os schemas**  
   Em **Project Settings → API**, em **Exposed schemas**, inclua:
   - `auth_integrations`
   - `staging`
   - `core`

2. **Permissões (SQL)**  
   Execute para cada schema (`auth_integrations`, `staging`, `core`):

   ```sql
   GRANT USAGE ON SCHEMA auth_integrations TO anon, authenticated, service_role;
   GRANT ALL ON ALL TABLES IN SCHEMA auth_integrations TO anon, authenticated, service_role;
   GRANT ALL ON ALL ROUTINES IN SCHEMA auth_integrations TO anon, authenticated, service_role;
   GRANT ALL ON ALL SEQUENCES IN SCHEMA auth_integrations TO anon, authenticated, service_role;
   ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA auth_integrations
     GRANT ALL ON TABLES TO anon, authenticated, service_role;
   ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA auth_integrations
     GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
   ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA auth_integrations
     GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;
   ```

   Repita trocando `auth_integrations` por `staging` e por `core`.

## Código Python

O cliente usa `client.schema("nome_do_schema").table("nome_da_tabela")`:

- `auth_integrations` → `companies`, `erp_connections`, `sync_checkpoints`
- `staging` → `tiny_sales`, `tiny_stock`, `tiny_sale_items`
- `core` → `customers`, `sales`, `sale_items`, `stock`

Assim o PostgREST/Supabase acessa as tabelas nos schemas corretos e o erro “Could not find the table 'public.companies'” deixa de ocorrer.

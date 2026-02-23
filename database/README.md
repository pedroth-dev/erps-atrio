# Schemas do Banco de Dados

Esta pasta contém todos os arquivos SQL e documentação relacionados à estrutura do banco de dados no Supabase.

## 📁 Estrutura

```
schemas/
├── schemas.sql                    # Criação dos schemas principais
├── doc_schemas.md                 # Documentação geral dos schemas
│
├── auth_integrations/              # Schema de autenticação e integrações
│   ├── table_companies.sql
│   ├── table_erp_connections.sql
│   ├── table_sync_checkpoints.sql  # Checkpoints para sync incremental (Celery)
│   ├── add_last_full_refresh_at_sync_checkpoints.sql  # Migração: coluna last_full_refresh_at
│   ├── triggers.sql
│   ├── rls.sql
│   └── doc_auth_integrations.md
│
├── staging/                        # Schema de dados brutos
│   ├── staging_tiny_sales.sql
│   ├── add_sale_external_id_tiny_sales.sql      # Migração (se tabela já existir)
│   ├── staging_tiny_stock.sql
│   ├── add_product_external_id_tiny_stock.sql   # Migração (se tabela já existir)
│   ├── staging_tiny_sale_items.sql
│   ├── add_product_external_id_tiny_sale_items.sql  # Migração (se tabela já existir)
│   └── doc_staging.md
│
└── core/                          # Schema de dados normalizados
    ├── table_customers.sql
    ├── table_sales.sql
    ├── table_sale_items.sql       # Itens de vendas normalizados
    ├── table_stock.sql
    └── doc_core.md
```

## 🚀 Ordem de Execução

Execute os arquivos SQL na seguinte ordem:

1. `schemas.sql` - Cria os schemas e extensões
2. `auth_integrations/table_companies.sql`
3. `auth_integrations/table_erp_connections.sql`
4. `auth_integrations/table_sync_checkpoints.sql` - Checkpoints para sync incremental
5. `auth_integrations/add_last_full_refresh_at_sync_checkpoints.sql` - (se a tabela já existir sem a coluna)
6. `auth_integrations/triggers.sql`
7. `auth_integrations/rls.sql`
8. `staging/staging_tiny_sales.sql`
9. `staging/add_sale_external_id_tiny_sales.sql` - (apenas se a tabela tiny_sales já existir sem a coluna sale_external_id)
10. `staging/staging_tiny_stock.sql`
11. `staging/add_product_external_id_tiny_stock.sql` - (apenas se a tabela tiny_stock já existir sem a coluna product_external_id)
12. `staging/staging_tiny_sale_items.sql` - Itens de vendas (produtos vendidos)
13. `staging/add_product_external_id_tiny_sale_items.sql` - (apenas se a tabela tiny_sale_items já existir sem a coluna product_external_id)
14. `core/table_customers.sql`
15. `core/table_sales.sql`
16. `core/table_sale_items.sql` - Itens de vendas normalizados
17. `core/table_stock.sql`

## 📚 Documentação

- `schemas/doc_schemas.md`: Visão geral dos schemas
- `schemas/auth_integrations/doc_auth_integrations.md`: Empresas e conexões ERP
- `schemas/staging/doc_staging.md`: Dados brutos (staging)
- `schemas/core/doc_core.md`: Dados normalizados (core)
- Na pasta **`docs/`** do projeto (raiz): fluxo de onboarding, funcionamento geral e arquitetura de sincronização

## ⚙️ Configuração no Supabase

Após executar os scripts SQL, configure o **Extra Search Path**:

**Project Settings → API → Extra Search Path**
```
auth_integrations, staging, core
```

Isso permite que o cliente Python acesse as tabelas dos schemas customizados.
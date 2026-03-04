# Mapeamento: argumentos do onboarding → colunas em `erp_connections`

Este documento descreve a relação entre os argumentos do script `scripts/onboarding.py` e as colunas da tabela `auth_integrations.erp_connections`. Útil para rodar o onboarding pela linha de comando e para conferir valores no banco.

---

## Ordem dos argumentos

O script espera **8 parâmetros** nesta ordem:

| # | Argumento      | Variável        | Vai para                         |
|---|----------------|-----------------|-----------------------------------|
| 1 | Nome           | `name`          | `companies.name`                  |
| 2 | CNPJ           | `document`      | `companies.document`              |
| 3 | Tipo ERP       | `erp_type`      | `erp_connections.erp_type`        |
| 4 | Login ERP      | `erp_login`     | `erp_connections.erp_login` (criptografado) |
| 5 | Senha ERP      | `erp_password`  | `erp_connections.erp_password` (criptografado) |
| 6 | Client ID      | `client_id`     | `erp_connections.client_id`       |
| 7 | Client Secret  | `client_secret` | `erp_connections.client_secret` (criptografado) |
| 8 | Redirect URI   | `redirect_uri`  | `erp_connections.redirect_uri`    |

Se qualquer argumento for trocado de posição (ex.: 6º e 7º), a coluna errada será preenchida no banco.

---

## Preenchimento de `erp_connections`

**No INSERT** (criação da conexão, em `create_erp_connection`):

- Preenchidos: `company_id`, `erp_type`, `erp_login`, `erp_password`, `client_id`, `client_secret`, `redirect_uri`, `is_active`.
- Ficam NULL até o OAuth: `access_token`, `refresh_token`, `access_token_expires_at`, `refresh_token_expires_at`, `token_type`, `last_sync_at`, `last_token_refresh_at`.

**No UPDATE** (após OAuth, em `update_erp_tokens`):

- Preenchidos a partir da resposta da API do ERP: `access_token`, `refresh_token`, `access_token_expires_at`, `refresh_token_expires_at`, `token_type`, `last_token_refresh_at`, `is_active`.

---

## Causas comuns de valores errados

1. **Ordem** — Trocar dois argumentos (ex.: 6 e 7) inverte `client_id` e `client_secret` nas colunas.
2. **Aspas** — No PowerShell/CMD, valores com espaço devem estar entre aspas; caso contrário um único valor vira vários argumentos e desloca o restante.
3. **Redirect URI** — Com ou sem barra final (`/oauth/tiny` vs `/oauth/tiny/`) são valores diferentes; use exatamente o configurado no painel do ERP.

---

## Exemplo de chamada

```bash
# Tiny
python scripts/onboarding.py "Nome" "CNPJ" tiny "login" "senha" "client_id" "client_secret" "https://..../redirect"

# Conta Azul
python scripts/onboarding.py "Nome" "CNPJ" contaazul "login" "senha" "client_id" "client_secret" "https://..../redirect"

# Bling (OAuth 2.0 Authorization Code; troca de code por tokens usa autenticação HTTP Basic)
python scripts/onboarding.py "Nome" "CNPJ" bling "login" "senha" "client_id" "client_secret" "https://..../redirect"
```

Consulte `scripts/onboarding.py` e `src/database/supabase_client.py` (métodos `create_erp_connection` e `update_erp_tokens`) para detalhes do código.

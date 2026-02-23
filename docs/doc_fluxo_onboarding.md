# Documentação — Fluxo de Onboarding de Empresas

Este documento descreve o passo a passo que o script Python executará ao cadastrar uma nova empresa na plataforma, desde o recebimento dos dados do formulário até a primeira sincronização com o ERP.

---

## Visão Geral

O onboarding é o único momento em que um humano interage diretamente com o processo de autenticação. Após o formulário ser submetido, tudo ocorre automaticamente — sem intervenção manual para copiar tokens, UUIDs ou códigos.

O fluxo completo é dividido em 4 etapas:

```
Formulário submetido
        ↓
1. Validação dos dados
        ↓
2. Criação da empresa e conexão no banco
        ↓
3. Automação Selenium para autenticação no ERP
        ↓
4. Atualização dos tokens no banco
        ↓
Primeira sincronização agendada automaticamente
```

---

## Etapa 1 — Validação dos dados

Antes de qualquer inserção no banco, o script valida os dados recebidos do formulário:

- Nome da empresa não pode ser vazio
- CNPJ deve ser válido (formato e dígitos verificadores)
- CNPJ não pode já existir em `auth_integrations.companies`
- Login e senha do ERP não podem ser vazios
- **Credenciais OAuth da aplicação** são obrigatórias para a conexão com a API e para obter os tokens:
  - **Client ID** — ID da aplicação registrada no ERP (ex.: Tiny)
  - **Client Secret** — segredo da aplicação (será criptografado antes de salvar)
  - **Redirect URI** — URI de redirecionamento configurada na aplicação do ERP
- `erp_type` deve ser um valor conhecido (`tiny`, `bling`, `omie`, etc.)

Sem o Client ID e o Client Secret, não é possível trocar o `code` (obtido no fluxo OAuth) por `access_token` e `refresh_token`, nem renovar os tokens depois. Por isso essas credenciais são obrigatórias no onboarding.

Se qualquer validação falhar, o processo é interrompido e o erro é retornado ao formulário. Nenhuma inserção é feita no banco.

---

## Etapa 2 — Criação da empresa e conexão no banco

Após validação, o script insere a empresa e a conexão com o ERP em uma única operação atômica usando CTE. Se qualquer parte falhar, nada é inserido.

O login, a senha do ERP e o **Client Secret** são criptografados com AES antes de serem salvos, usando a chave de criptografia armazenada como variável de ambiente na VPS. Essas credenciais nunca trafegam ou são salvas em texto puro. O **Client ID** e a **Redirect URI** são armazenados em texto puro (não são considerados sensíveis).

Os campos de token (`access_token`, `refresh_token`, `access_token_expires_at`, `refresh_token_expires_at`) ficam temporariamente vazios neste momento — serão preenchidos na etapa seguinte, usando o Client ID e o Client Secret para trocar o `code` por tokens na API do ERP.

**O que é inserido:**

```
auth_integrations.companies
  → name, document, is_active = true

auth_integrations.erp_connections
  → company_id (gerado automaticamente pelo insert acima)
  → erp_type
  → erp_login (criptografado)
  → erp_password (criptografado)
  → client_id (texto puro — ID da aplicação OAuth)
  → client_secret (criptografado)
  → redirect_uri (texto puro)
  → is_active = true
  → tokens = NULL (ainda)
```

---

## Etapa 3 — Automação Selenium

Com a empresa criada no banco, o script aciona a automação Selenium em background. O usuário não precisa fazer nada nesse momento — o processo ocorre de forma invisível no servidor.

O Selenium executa em modo headless (sem interface gráfica) na VPS e segue os seguintes passos:

```
1. Descriptografa o login e senha do banco
2. Abre a URL de autorização do Tiny
3. Preenche login e senha automaticamente
4. Confirma a autorização
5. Captura o code gerado na URL de retorno
6. Usa client_id e client_secret (do banco) para trocar o code por access_token + refresh_token via API do Tiny
```

O **Client ID** e o **Client Secret** fornecidos no onboarding são necessários nesse passo 6: a API do ERP exige essas credenciais da aplicação para emitir os tokens. Sem elas, a conexão não pode ser concluída.

Se o Selenium falhar por qualquer motivo (credenciais incorretas, timeout, mudança na interface do ERP), o script registra o erro, marca `is_active = false` na conexão e notifica sobre a falha. A empresa continua cadastrada no banco e pode tentar novamente.

---

## Etapa 4 — Atualização dos tokens no banco

Com os tokens em mãos, o script atualiza a conexão criada na etapa 2:

```
auth_integrations.erp_connections
  → access_token
  → refresh_token
  → access_token_expires_at = NOW() + 4 horas   (Tiny)
  → refresh_token_expires_at = NOW() + 24 horas  (Tiny)
  → token_type = 'oauth2'
  → last_token_refresh_at = NOW()
  → is_active = true
```

A partir desse momento a conexão está ativa e pronta para uso.

---

## Etapa 5 — Primeira sincronização

Com a conexão ativa, o scheduler enfileira automaticamente as primeiras tarefas de sincronização para a nova empresa:

```
Celery recebe as tarefas:
  → sync_tiny_sales(company_id)
  → sync_tiny_stock(company_id)

Cada tarefa:
  1. token_manager verifica e garante token válido
  2. Coleta dados da API do Tiny
  3. Insere raw_data no staging
  4. Normaliza staging → core
  5. Atualiza last_sync_at na conexão
```

---

## Renovação automática após expiração

A partir do onboarding, o `token_manager` cuida de tudo automaticamente antes de cada sincronização, sem nenhuma ação humana necessária enquanto os tokens estiverem dentro do ciclo normal:

```
access_token expirou? (a cada 4h)
  → sim: chama API do Tiny com refresh_token
         atualiza access_token + access_token_expires_at
         atualiza last_token_refresh_at

refresh_token expirou? (após 24h sem sincronização)
  → sim: Selenium roda novamente usando erp_login e erp_password do banco
         reinicia o ciclo completo de tokens
         nenhuma ação humana necessária
```

---

## O que nunca deve acontecer

- O script nunca salva login, senha ou **client_secret** em texto puro — sempre criptografado antes do INSERT.
- A chave de criptografia nunca fica no banco ou no repositório — apenas como variável de ambiente na VPS.
- O `company_id` nunca é copiado ou informado manualmente — sempre gerado e propagado automaticamente pelo banco.
- Tokens nunca são logados em arquivos de log — apenas status de sucesso ou erro.
- Se o Selenium falhar, a empresa não deve ficar com `is_active = true` na conexão — o erro deve ser registrado e a flag corrigida.
- O onboarding nunca deve ser concluído sem **client_id** e **client_secret** — são obrigatórios para obter e renovar os tokens na API do ERP.
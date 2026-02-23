# Documentação — Schema auth_integrations

Este documento descreve os arquivos SQL que compõem o schema `auth_integrations`, sua finalidade, ordem de execução e cuidados necessários.

---

## Visão Geral

O schema `auth_integrations` é responsável por tudo que envolve empresas, credenciais e conexões com ERPs. É a base de toda a estrutura do projeto — todas as tabelas dos demais schemas referenciam as tabelas aqui definidas através do `company_id`.

Ele é composto por duas tabelas principais:

- **`companies`** — representa cada empresa (tenant) cadastrada na plataforma
- **`erp_connections`** — armazena as credenciais e configurações de conexão de cada empresa com cada ERP

Todos os arquivos devem ser executados **na ordem em que estão numerados**, pois há dependências entre eles.

---

## table_companies.sql

**O que faz:** Cria a tabela `auth_integrations.companies`, que representa cada empresa (tenant) cadastrada na plataforma. É a tabela raiz de toda a estrutura — todas as outras tabelas de todos os schemas referenciam ela.

**Quando executar:** Imediatamente após o `01_create_schemas.sql`. Nenhuma outra tabela pode ser criada antes desta, pois todas dependem de `company_id` referenciando esta tabela.

**Como usar:** Execute uma única vez. Ao cadastrar uma empresa no sistema, insira um registro aqui antes de qualquer outra operação relacionada a ela.

**Campos importantes:**
- `id` — UUID gerado automaticamente, é o `company_id` referenciado em todo o banco.
- `document` — armazena o CNPJ e possui constraint `UNIQUE`, impedindo empresas duplicadas.
- `is_active` — permite desativar uma empresa sem deletar seus dados, preservando todo o histórico.

**O que não pode acontecer:**
- Não delete registros desta tabela diretamente em produção. Como todas as outras tabelas usam `ON DELETE CASCADE`, deletar uma empresa aqui apagará em cascata todas as conexões, dados de staging e dados do core relacionados a ela.
- Não remova o campo `is_active` achando que basta deletar. A desativação lógica é intencional para preservar histórico.
- Não insira empresas duplicadas pelo CNPJ — o banco rejeitará, mas o código da aplicação deve tratar esse erro adequadamente.

---

## table_erp_connections.sql

**O que faz:** Cria a tabela `auth_integrations.erp_connections`, que armazena as credenciais e configurações de conexão de cada empresa com cada ERP integrado (Tiny, Bling, Omie, etc.).

**Quando executar:** Após o `02_create_table_companies.sql`, pois possui foreign key para `companies`.

**Como usar:** Cada vez que uma empresa configurar a integração com um ERP, um registro é inserido aqui com o token e as informações de expiração. O script de integração Python deve consultar esta tabela antes de cada requisição para verificar se o token ainda é válido.

**Campos importantes:**

**Credenciais de acesso ao ERP (criptografadas):**
- `erp_login` — login do usuário no ERP. **Sempre criptografado** antes de salvar no banco.
- `erp_password` — senha do usuário no ERP. **Sempre criptografado** antes de salvar no banco.

**Credenciais da aplicação OAuth (armazenadas por empresa):**
- `client_id` — ID da aplicação registrada no ERP (ex: Tiny). Armazenado em texto puro (não é sensível).
- `client_secret` — segredo da aplicação registrada no ERP. **Sempre criptografado** antes de salvar no banco.
- `redirect_uri` — URI de redirecionamento configurada na aplicação do ERP. Armazenado em texto puro.

**Tokens OAuth (criptografados):**
- `access_token` — token principal usado nas requisições à API do ERP. No Tiny, dura 4 horas. **Sempre criptografado** antes de salvar no banco.
- `refresh_token` — usado para renovar o `access_token` quando ele expira, sem precisar que o usuário reconecte manualmente. No Tiny, dura 24 horas. **Sempre criptografado** antes de salvar no banco.
- `api_key` — para ERPs que usam autenticação por chave fixa, sem expiração. **Sempre criptografado** se presente.

**Controle de expiração:**
- `access_token_expires_at` e `refresh_token_expires_at` — o script de integração deve comparar esses campos com `NOW()` antes de cada requisição.
- `token_type` — identifica o modelo de autenticação (`oauth2`, `apikey`, `basic`), permitindo que o script trate cada ERP de forma adequada.

**Controle de sincronização:**
- `last_sync_at` — atualizado após cada sincronização bem-sucedida, útil para saber quando os dados foram atualizados pela última vez.
- `last_token_refresh_at` — registra quando o token foi renovado pela última vez.
- `is_active` — quando `false`, indica que a conexão está inativa e o usuário precisa reconectar.

**Fluxo esperado pelo script de integração (token_manager):**
1. Buscar a conexão da empresa no banco.
2. Descriptografar `client_id`, `client_secret` e `redirect_uri` do banco (se necessário).
3. Verificar se `access_token_expires_at < NOW()`. Se sim:
   - Descriptografar `refresh_token` do banco
   - Usar `client_id`, `client_secret` e `refresh_token` para renovar via API
   - Criptografar novos tokens antes de salvar no banco
4. Verificar se `refresh_token_expires_at < NOW()`. Se sim:
   - Descriptografar `erp_login`, `erp_password`, `client_id`, `client_secret` e `redirect_uri`
   - Acionar a automação Selenium para gerar um novo `code`
   - Trocar o `code` por tokens usando `client_id` e `client_secret`
   - Criptografar novos tokens antes de salvar no banco
5. Descriptografar `access_token` do banco para usar nas requisições.
6. Realizar a requisição normalmente com o token válido.
7. Atualizar `last_sync_at` ao final.

**Migração do `.env` para o banco:**

Os campos `client_id`, `client_secret` e `redirect_uri` foram migrados do arquivo `.env` para o banco de dados, permitindo que cada empresa tenha suas próprias credenciais OAuth. Isso é necessário quando diferentes empresas usam aplicações OAuth diferentes no mesmo ERP.

**Credenciais que permanecem no `.env` (nunca vão para o banco):**
- `SUPABASE_SERVICE_ROLE_KEY` — chave de acesso ao Supabase (protegida como se fosse criptografada)
- `ENCRYPTION_KEY` — chave de criptografia AES (nunca armazenada no banco)

**Criptografia de credenciais:**

Todas as credenciais sensíveis são criptografadas usando AES (Fernet) antes de serem salvas no banco:

- ✅ **`erp_login`** — criptografado
- ✅ **`erp_password`** — criptografado
- ✅ **`client_secret`** — criptografado
- ✅ **`access_token`** — criptografado
- ✅ **`refresh_token`** — criptografado
- ✅ **`api_key`** — criptografado (se presente)

**Campos que NÃO são criptografados** (não são sensíveis):
- `client_id` — pode ser público, usado para identificar a aplicação
- `redirect_uri` — configuração pública da aplicação

A chave de criptografia (`ENCRYPTION_KEY`) fica apenas no arquivo `.env` da VPS e **nunca** é armazenada no banco de dados.

**O que não pode acontecer:**
- **Nunca** armazene credenciais sensíveis em texto puro no banco — sempre criptografadas.
- **Nunca** armazene tokens em texto puro em logs ou variáveis de ambiente expostas. Os tokens ficam no banco (criptografados) e devem ser acessados apenas via `service_role key` no backend.
- **Nunca** armazene a `ENCRYPTION_KEY` no banco — ela fica apenas no `.env` da VPS.
- **Nunca** ignore os campos de expiração. Requisitar um ERP com token expirado pode resultar em bloqueio temporário da conta.
- A constraint `UNIQUE (company_id, erp_type)` impede que a mesma empresa tenha duas conexões ativas com o mesmo ERP. Não tente removê-la — se precisar trocar credenciais, faça um `UPDATE` no registro existente, não um novo `INSERT`.
- Não delete conexões inativas — prefira manter `is_active = false` para preservar histórico de integrações.

---

## triggers.sql

**O que faz:** Cria a função `update_updated_at()` e os triggers que a utilizam nas tabelas `companies` e `erp_connections`. Sempre que um registro for atualizado, o campo `updated_at` é preenchido automaticamente com o horário atual.

**Quando executar:** Após a criação das tabelas (`02` e `03`). Os triggers precisam que as tabelas existam para serem associados a elas.

**Como usar:** Execute uma única vez. Após isso, nenhuma ação manual é necessária — o banco cuida da atualização do `updated_at` automaticamente em qualquer `UPDATE`. A função `update_updated_at()` criada aqui é reutilizável por triggers de qualquer outra tabela do projeto.

**O que não pode acontecer:**
- Não tente atualizar o campo `updated_at` manualmente no código. O trigger sobrescreverá o valor de qualquer forma, e manter essa lógica duplicada gera confusão.
- Não remova os triggers sem garantir que o código da aplicação passe a atualizar o `updated_at` manualmente — do contrário, o campo ficará desatualizado e inútil para rastreabilidade.
- Ao criar novas tabelas com o campo `updated_at` em qualquer schema, lembre-se de criar um trigger equivalente reutilizando a função `update_updated_at()`.

---

## rls.sql

**O que faz:** Ativa o Row Level Security (RLS) nas tabelas `companies` e `erp_connections` e define as políticas de acesso. Com o RLS ativo, cada empresa só consegue ler e modificar os próprios dados, mesmo que acesse o banco diretamente pela API do Supabase.

**Quando executar:** Por último, após todas as tabelas e triggers estarem criados.

**Como usar:** Execute uma única vez. As policies passam a valer imediatamente após a execução. Para que funcionem corretamente, o `company_id` de cada usuário deve estar salvo no `app_metadata` do JWT do Supabase Auth. Isso é feito ao criar ou atualizar o usuário via SDK Admin:

```js
supabase.auth.admin.updateUserById(user_id, {
  app_metadata: { company_id: "uuid-da-empresa" }
})
```

**Sobre o script Python de integração:** O script backend usa a `service_role key`, que bypassa o RLS por design. Isso é correto e esperado — a responsabilidade de filtrar por `company_id` nas queries fica no próprio script nesses casos. Nunca exponha a `service_role key` no frontend ou em repositórios.

**O que não pode acontecer:**
- Não deixe o RLS desativado em produção. Sem ele, qualquer usuário autenticado consegue ler dados de outras empresas via API do Supabase.
- Não use a `anon key` no script Python de integração. Ela está sujeita ao RLS e pode não ter permissão para realizar as operações necessárias.
- Ao criar novas tabelas no futuro dentro de qualquer schema, lembre-se de ativar o RLS e criar as policies correspondentes. Tabelas novas sem RLS ficam completamente abertas para qualquer usuário autenticado.
- Não remova as policies de `INSERT` sem um motivo claro — sem elas, um usuário poderia inserir dados com um `company_id` diferente do seu.

---

## Ordem de Execução

| Ordem | Arquivo                             |
|-------|-------------------------------------|
| 1º    | schemas.sql               |
| 2º    | table_companies.sql       |
| 3º    | table_erp_connections.sql |
| 4º    | triggers.sql              |
| 5º    | rls.sql                   |
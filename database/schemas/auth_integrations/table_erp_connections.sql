CREATE TABLE auth_integrations.erp_connections (
  id                        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id                UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,
  erp_type                  TEXT NOT NULL,        -- 'tiny', 'bling', 'omie', 'netsuite', etc.

  -- Credenciais de acesso ao ERP (criptografadas via AES no script Python)
  erp_login                 TEXT,                 -- login do ERP criptografado
  erp_password              TEXT,                 -- senha do ERP criptografada

  -- Credenciais da aplicação no ERP
  client_id                 TEXT,                 -- client_id da aplicação registrada no ERP
  client_secret             TEXT,                 -- client_secret criptografado
  redirect_uri              TEXT,                 -- redirect URI configurada na aplicação do ERP

  -- Tokens OAuth
  access_token              TEXT,                 -- token principal de acesso
  refresh_token             TEXT,                 -- usado para renovar o access_token
  api_key                   TEXT,                 -- ERPs que usam chave fixa (ex: Tiny v2)

  -- Controle de expiração
  access_token_expires_at   TIMESTAMPTZ,          -- quando o access_token expira
  refresh_token_expires_at  TIMESTAMPTZ,          -- quando o refresh_token expira (se aplicável)
  token_type                TEXT,                 -- 'oauth2', 'apikey', 'basic', etc.

  -- Controle de sincronização
  last_sync_at              TIMESTAMPTZ,
  last_token_refresh_at     TIMESTAMPTZ,
  is_active                 BOOLEAN NOT NULL DEFAULT true,

  created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Uma empresa não pode ter duas conexões ativas para o mesmo ERP
  UNIQUE (company_id, erp_type)
);

-- Índices
CREATE INDEX idx_erp_connections_company_id ON auth_integrations.erp_connections(company_id);
CREATE INDEX idx_erp_connections_erp_type   ON auth_integrations.erp_connections(erp_type);
CREATE INDEX idx_erp_connections_expires    ON auth_integrations.erp_connections(access_token_expires_at);
CREATE INDEX idx_erp_connections_is_active  ON auth_integrations.erp_connections(is_active);

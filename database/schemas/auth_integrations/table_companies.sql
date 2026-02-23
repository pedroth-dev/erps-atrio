CREATE TABLE auth_integrations.companies (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name        TEXT NOT NULL,
  document    TEXT UNIQUE,        -- CNPJ
  is_active   BOOLEAN NOT NULL DEFAULT true,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices
CREATE INDEX idx_companies_document  ON auth_integrations.companies(document);
CREATE INDEX idx_companies_is_active ON auth_integrations.companies(is_active);
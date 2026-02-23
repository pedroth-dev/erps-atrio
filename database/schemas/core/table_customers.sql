CREATE TABLE core.customers (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id        UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,
  erp_type          TEXT NOT NULL,              -- 'tiny', 'bling', 'omie', etc.

  -- Identificação no ERP
  external_id       TEXT NOT NULL,              -- ID do cliente no Tiny

  -- Dados pessoais
  name              TEXT,
  person_type       TEXT,                       -- 'fisica' ou 'juridica'
  document          TEXT,                       -- CPF ou CNPJ

  -- Contato
  phone             TEXT,                       -- telefone fixo (pode ser nulo)
  mobile            TEXT,                       -- celular (pode ser nulo)
  email             TEXT,                       -- email (pode ser nulo)

  -- Endereço
  neighborhood      TEXT,                       -- bairro
  city              TEXT,                       -- município
  zip_code          TEXT,                       -- CEP
  state             TEXT,                       -- UF
  country           TEXT,                       -- país

  -- Payload original
  raw_data          JSONB,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Um cliente não se repete por empresa e ERP
  UNIQUE (company_id, erp_type, external_id)
);

-- Índices
CREATE INDEX idx_customers_company_id   ON core.customers(company_id);
CREATE INDEX idx_customers_erp_type     ON core.customers(erp_type);
CREATE INDEX idx_customers_external_id  ON core.customers(external_id);
CREATE INDEX idx_customers_document     ON core.customers(document);
CREATE INDEX idx_customers_name         ON core.customers(name);

-- RLS
ALTER TABLE core.customers ENABLE ROW LEVEL SECURITY;

CREATE POLICY "customers: leitura da própria empresa"
  ON core.customers
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "customers: inserção da própria empresa"
  ON core.customers
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "customers: atualização da própria empresa"
  ON core.customers
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- Trigger updated_at
CREATE TRIGGER trg_customers_updated_at
  BEFORE UPDATE ON core.customers
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
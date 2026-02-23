CREATE TABLE core.stock (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id    UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,
  erp_type      TEXT NOT NULL,

  -- Identificação do produto
  external_id   TEXT NOT NULL,                  -- ID do produto no ERP
  sku           TEXT,                           -- código do produto
  product_name  TEXT,

  -- Posição atual
  quantity      NUMERIC(15, 4) NOT NULL DEFAULT 0,

  -- Payload original
  raw_data      JSONB,

  -- Controle de sincronização
  synced_at     TIMESTAMPTZ,                    -- momento exato da última sync com o ERP

  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Evita duplicatas na sincronização
  UNIQUE (company_id, erp_type, external_id)
);

-- Índices
CREATE INDEX idx_stock_company_id   ON core.stock(company_id);
CREATE INDEX idx_stock_erp_type     ON core.stock(erp_type);
CREATE INDEX idx_stock_external_id  ON core.stock(external_id);
CREATE INDEX idx_stock_sku          ON core.stock(sku);

-- RLS
ALTER TABLE core.stock ENABLE ROW LEVEL SECURITY;

CREATE POLICY "stock: leitura da própria empresa"
  ON core.stock
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "stock: inserção da própria empresa"
  ON core.stock
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "stock: atualização da própria empresa"
  ON core.stock
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- Trigger updated_at
CREATE TRIGGER trg_stock_updated_at
  BEFORE UPDATE ON core.stock
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
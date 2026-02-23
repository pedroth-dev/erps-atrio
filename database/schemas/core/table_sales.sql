CREATE TABLE core.sales (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id        UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,
  erp_type          TEXT NOT NULL,              -- 'tiny', 'bling', 'omie', etc.

  -- Identificação
  external_id       TEXT NOT NULL,              -- ID do pedido no Tiny
  order_number      TEXT,                       -- número do pedido no Tiny
  origin_order_id   TEXT,                       -- ID do pedido na origem (Mercado Livre, Shopee, etc.) — pode ser nulo
  origin_channel_id TEXT,                       -- ID do ecommerce no Tiny (ex: 1, 2, 3)
  origin_channel    TEXT,                       -- nome do ecommerce (ex: 'Mercado Livre', 'Shopee')

  -- Cliente
  customer_id       UUID REFERENCES core.customers(id) ON DELETE SET NULL,

  -- Valor
  total_amount      NUMERIC(15, 2),

  -- Status e datas
  status            TEXT,                       -- situação do pedido no Tiny
  issued_at         TIMESTAMPTZ,                -- data de criação do pedido no Tiny

  -- Payload original
  raw_data          JSONB,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Evita duplicatas na sincronização
  UNIQUE (company_id, erp_type, external_id)
);

-- Índices
CREATE INDEX idx_sales_company_id        ON core.sales(company_id);
CREATE INDEX idx_sales_erp_type          ON core.sales(erp_type);
CREATE INDEX idx_sales_external_id       ON core.sales(external_id);
CREATE INDEX idx_sales_order_number      ON core.sales(order_number);
CREATE INDEX idx_sales_origin_order_id   ON core.sales(origin_order_id);
CREATE INDEX idx_sales_origin_channel    ON core.sales(origin_channel);
CREATE INDEX idx_sales_customer_id       ON core.sales(customer_id);
CREATE INDEX idx_sales_status            ON core.sales(status);
CREATE INDEX idx_sales_issued_at         ON core.sales(issued_at);

-- RLS
ALTER TABLE core.sales ENABLE ROW LEVEL SECURITY;

CREATE POLICY "sales: leitura da própria empresa"
  ON core.sales
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "sales: inserção da própria empresa"
  ON core.sales
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "sales: atualização da própria empresa"
  ON core.sales
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- Trigger updated_at
CREATE TRIGGER trg_sales_updated_at
  BEFORE UPDATE ON core.sales
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
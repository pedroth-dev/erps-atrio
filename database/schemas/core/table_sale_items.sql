-- Tabela de itens de vendas normalizados (core).
-- Dados essenciais para análises de vendas por produtos.

CREATE TABLE core.sale_items (
  id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id          UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,
  erp_type            TEXT NOT NULL,              -- 'tiny', 'bling', 'omie', etc.

  -- Referência à venda
  sale_id             UUID REFERENCES core.sales(id) ON DELETE CASCADE,  -- FK para core.sales
  sale_external_id    TEXT NOT NULL,              -- ID da venda no Tiny (para consultas sem JOIN)

  -- Produto
  product_external_id TEXT NOT NULL,              -- ID do produto no Tiny
  product_sku         TEXT,                       -- SKU do produto
  product_description TEXT,                       -- descrição do produto
  product_type        TEXT,                       -- 'P' (Produto) ou 'S' (Serviço)

  -- Valores
  quantity            NUMERIC(15, 4),            -- quantidade vendida (suporta decimais)
  unit_price          NUMERIC(15, 2),            -- preço unitário
  total_price         NUMERIC(15, 2),            -- quantidade × unit_price

  -- Metadados da venda (para análises sem JOIN)
  sale_date           TIMESTAMPTZ,                -- data da venda (issued_at de core.sales)
  sale_status         TEXT,                       -- status da venda (Aprovada, Faturada, etc.)

  -- Payload original
  raw_data            JSONB,

  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Evita duplicatas: mesmo item da mesma venda não se repete
  UNIQUE (company_id, erp_type, sale_external_id, product_external_id)
);

-- Índices
CREATE INDEX idx_sale_items_company_id        ON core.sale_items(company_id);
CREATE INDEX idx_sale_items_erp_type          ON core.sale_items(erp_type);
CREATE INDEX idx_sale_items_sale_id           ON core.sale_items(sale_id);
CREATE INDEX idx_sale_items_sale_external_id ON core.sale_items(sale_external_id);
CREATE INDEX idx_sale_items_product_external  ON core.sale_items(product_external_id);
CREATE INDEX idx_sale_items_product_sku       ON core.sale_items(product_sku);
CREATE INDEX idx_sale_items_sale_date         ON core.sale_items(sale_date);
CREATE INDEX idx_sale_items_sale_status       ON core.sale_items(sale_status);

-- RLS
ALTER TABLE core.sale_items ENABLE ROW LEVEL SECURITY;

CREATE POLICY "sale_items: leitura da própria empresa"
  ON core.sale_items
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "sale_items: inserção da própria empresa"
  ON core.sale_items
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "sale_items: atualização da própria empresa"
  ON core.sale_items
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- Trigger updated_at
CREATE TRIGGER trg_sale_items_updated_at
  BEFORE UPDATE ON core.sale_items
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

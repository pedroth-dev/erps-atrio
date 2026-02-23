-- Tabela de itens de vendas no staging (dados brutos de cada produto vendido).
-- Um registro por item de cada venda. Coletado via GET /pedidos/{idPedido}.
-- UNIQUE (company_id, sale_external_id, product_external_id) evita duplicatas ao re-coletar a mesma venda.

CREATE TABLE staging.tiny_sale_items (
  id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id          UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,

  -- Referência à venda e ao produto (evita duplicatas)
  sale_external_id    TEXT NOT NULL,              -- ID da venda no Tiny (ex: 123)
  product_external_id TEXT NOT NULL,              -- ID do produto no Tiny (raw_data.produto.id)
  sale_staging_id     UUID REFERENCES staging.tiny_sales(id) ON DELETE CASCADE,  -- FK opcional

  -- Payload bruto do item (produto, quantidade, valorUnitario, etc.)
  raw_data            JSONB NOT NULL,

  -- Controle de processamento
  processed_at        TIMESTAMPTZ,                 -- nulo = ainda não normalizado para o core
  process_error       TEXT,                        -- mensagem de erro caso a normalização falhe

  -- Controle de sincronização
  fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- quando foi buscado na API

  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (company_id, sale_external_id, product_external_id)
);

-- Índices
CREATE INDEX idx_tiny_sale_items_company_id      ON staging.tiny_sale_items(company_id);
CREATE INDEX idx_tiny_sale_items_sale_external   ON staging.tiny_sale_items(sale_external_id);
CREATE INDEX idx_tiny_sale_items_sale_staging_id ON staging.tiny_sale_items(sale_staging_id);
CREATE INDEX idx_tiny_sale_items_processed_at    ON staging.tiny_sale_items(processed_at);
CREATE INDEX idx_tiny_sale_items_fetched_at      ON staging.tiny_sale_items(fetched_at);

-- Índice parcial: facilita buscar apenas os registros ainda não processados
CREATE INDEX idx_tiny_sale_items_pending ON staging.tiny_sale_items(company_id)
  WHERE processed_at IS NULL;

-- RLS
ALTER TABLE staging.tiny_sale_items ENABLE ROW LEVEL SECURITY;

CREATE POLICY "tiny_sale_items: leitura da própria empresa"
  ON staging.tiny_sale_items
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "tiny_sale_items: inserção da própria empresa"
  ON staging.tiny_sale_items
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "tiny_sale_items: atualização da própria empresa"
  ON staging.tiny_sale_items
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- Trigger updated_at (função em auth_integrations/triggers.sql)
CREATE TRIGGER trg_tiny_sale_items_updated_at
  BEFORE UPDATE ON staging.tiny_sale_items
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

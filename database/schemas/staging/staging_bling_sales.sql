-- Tabela de vendas (pedidos) no staging. Dados brutos da API do Bling.
-- UNIQUE (company_id, sale_external_id) evita duplicatas e mantém a tabela enxuta.

CREATE TABLE staging.bling_sales (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id        UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,

  -- Identificador da venda no ERP (evita duplicatas no staging)
  sale_external_id  TEXT NOT NULL,

  -- Payload bruto exatamente como veio da API do Bling
  raw_data          JSONB NOT NULL,

  -- Controle de processamento
  processed_at      TIMESTAMPTZ,                         -- nulo = ainda não normalizado para o core
  process_error     TEXT,                                -- mensagem de erro caso a normalização falhe

  -- Controle de sincronização
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- quando foi buscado na API

  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (company_id, sale_external_id)
);

-- Índices
CREATE INDEX idx_bling_sales_company_id       ON staging.bling_sales(company_id);
CREATE INDEX idx_bling_sales_sale_external_id ON staging.bling_sales(sale_external_id);
CREATE INDEX idx_bling_sales_processed_at     ON staging.bling_sales(processed_at);
CREATE INDEX idx_bling_sales_fetched_at       ON staging.bling_sales(fetched_at);

-- Índice parcial: registros ainda não processados
CREATE INDEX idx_bling_sales_pending ON staging.bling_sales(company_id)
  WHERE processed_at IS NULL;

-- RLS
ALTER TABLE staging.bling_sales ENABLE ROW LEVEL SECURITY;

CREATE POLICY "bling_sales: leitura da própria empresa"
  ON staging.bling_sales
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "bling_sales: inserção da própria empresa"
  ON staging.bling_sales
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "bling_sales: atualização da própria empresa"
  ON staging.bling_sales
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- Trigger updated_at (função update_updated_at() criada em auth_integrations/triggers.sql)
CREATE TRIGGER trg_bling_sales_updated_at
  BEFORE UPDATE ON staging.bling_sales
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Tabela de estoque no staging para o Conta Azul.
-- Armazena payloads brutos relacionados a estoque de produtos (consultas de produto/estoque).
-- UNIQUE (company_id, product_external_id) evita duplicatas quando várias execuções trazem os mesmos produtos.

CREATE TABLE staging.contaazul_stock (
  id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id          UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,

  -- Identificador do produto no ERP (evita duplicatas no staging)
  product_external_id TEXT NOT NULL,

  -- Payload bruto exatamente como veio da API do Conta Azul (produto/estoque)
  raw_data            JSONB NOT NULL,

  -- Controle de processamento
  processed_at        TIMESTAMPTZ,                         -- nulo = ainda não normalizado para o core
  process_error       TEXT,                                -- mensagem de erro caso a normalização falhe

  -- Controle de sincronização
  fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- quando foi buscado na API

  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (company_id, product_external_id)
);

-- Índices
CREATE INDEX idx_contaazul_stock_company_id          ON staging.contaazul_stock(company_id);
CREATE INDEX idx_contaazul_stock_product_external_id ON staging.contaazul_stock(product_external_id);
CREATE INDEX idx_contaazul_stock_processed_at        ON staging.contaazul_stock(processed_at);
CREATE INDEX idx_contaazul_stock_fetched_at          ON staging.contaazul_stock(fetched_at);

-- Índice parcial: facilita buscar apenas os registros ainda não processados
CREATE INDEX idx_contaazul_stock_pending ON staging.contaazul_stock(company_id)
  WHERE processed_at IS NULL;

-- RLS
ALTER TABLE staging.contaazul_stock ENABLE ROW LEVEL SECURITY;

CREATE POLICY "contaazul_stock: leitura da própria empresa"
  ON staging.contaazul_stock
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "contaazul_stock: inserção da própria empresa"
  ON staging.contaazul_stock
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "contaazul_stock: atualização da própria empresa"
  ON staging.contaazul_stock
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- Trigger updated_at (função em auth_integrations/triggers.sql)
CREATE TRIGGER trg_contaazul_stock_updated_at
  BEFORE UPDATE ON staging.contaazul_stock
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();


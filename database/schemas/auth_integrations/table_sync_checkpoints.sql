-- Tabela de checkpoints para sincronização incremental (arquitetura_sincronizacao.md).
-- Consultada antes de cada sync para saber o ponto de partida; atualizada apenas após sucesso.

CREATE TABLE auth_integrations.sync_checkpoints (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id    UUID NOT NULL REFERENCES auth_integrations.companies(id) ON DELETE CASCADE,
  erp_type      TEXT NOT NULL,    -- 'tiny', 'bling', 'omie', etc.
  entity        TEXT NOT NULL,   -- 'sales', 'stock', 'customers', etc.
  last_sync_at  TIMESTAMPTZ,     -- última execução bem-sucedida
  last_full_refresh_at TIMESTAMPTZ,  -- última vez que rodou sync dos últimos 30 dias (repõe mudanças de status, itens faltantes)
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (company_id, erp_type, entity)
);

CREATE INDEX idx_sync_checkpoints_company_erp_entity
  ON auth_integrations.sync_checkpoints(company_id, erp_type, entity);

-- RLS: acesso apenas aos checkpoints da própria empresa (app_metadata.company_id no JWT)
ALTER TABLE auth_integrations.sync_checkpoints ENABLE ROW LEVEL SECURITY;

CREATE POLICY "sync_checkpoints: leitura da própria empresa"
  ON auth_integrations.sync_checkpoints
  FOR SELECT
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "sync_checkpoints: inserção da própria empresa"
  ON auth_integrations.sync_checkpoints
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "sync_checkpoints: atualização da própria empresa"
  ON auth_integrations.sync_checkpoints
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "sync_checkpoints: exclusão da própria empresa"
  ON auth_integrations.sync_checkpoints
  FOR DELETE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

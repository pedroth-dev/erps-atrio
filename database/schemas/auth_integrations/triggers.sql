-- Função reutilizável para todos os triggers
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger: companies
CREATE TRIGGER trg_companies_updated_at
  BEFORE UPDATE ON auth_integrations.companies
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Trigger: erp_connections
CREATE TRIGGER trg_erp_connections_updated_at
  BEFORE UPDATE ON auth_integrations.erp_connections
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Trigger: sync_checkpoints
CREATE TRIGGER trg_sync_checkpoints_updated_at
  BEFORE UPDATE ON auth_integrations.sync_checkpoints
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
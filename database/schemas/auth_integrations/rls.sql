CREATE POLICY "erp_connections: inserção da própria empresa"
  ON auth_integrations.erp_connections
  FOR INSERT
  WITH CHECK (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "erp_connections: atualização da própria empresa"
  ON auth_integrations.erp_connections
  FOR UPDATE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

CREATE POLICY "erp_connections: exclusão da própria empresa"
  ON auth_integrations.erp_connections
  FOR DELETE
  USING (
    company_id = (auth.jwt() -> 'app_metadata' ->> 'company_id')::uuid
  );

-- ============================================================
-- IMPORTANTE:
-- As policies acima assumem que o company_id está salvo no
-- app_metadata do JWT do Supabase Auth.
-- Para isso, ao criar/logar um usuário, você deve setar:
--
-- supabase.auth.admin.updateUserById(user_id, {
--   app_metadata: { company_id: "uuid-da-empresa" }
-- })
--
-- O script Python de integração deve usar a service_role key,
-- que bypassa o RLS — a responsabilidade de filtrar por
-- company_id fica no próprio script nesses casos.
-- ============================================================
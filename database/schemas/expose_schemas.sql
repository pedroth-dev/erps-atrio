-- Execute este script no SQL Editor do Supabase para expor os schemas
-- customizados à API (PostgREST). Sem isso, a API só enxerga o schema public.

-- Schema auth_integrations
GRANT USAGE ON SCHEMA auth_integrations TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA auth_integrations TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA auth_integrations TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA auth_integrations TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA auth_integrations GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA auth_integrations GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA auth_integrations GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;

-- Schema staging
GRANT USAGE ON SCHEMA staging TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA staging TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA staging TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA staging GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA staging GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA staging GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;

-- Schema core
GRANT USAGE ON SCHEMA core TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA core TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA core TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA core TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA core GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA core GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA core GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;

-- Depois de executar este script, vá em:
-- Project Settings → API → Exposed schemas
-- e adicione: auth_integrations, staging, core

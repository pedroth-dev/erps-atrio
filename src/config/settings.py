"""
Configurações do sistema de integração com ERPs.
Carrega variáveis de ambiente e define constantes globais.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")  # Chave AES para criptografar credenciais

# URLs da API Tiny (fixas, não dependem de credenciais)
TINY_AUTH_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/auth"
TINY_TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"
TINY_API_BASE_URL = "https://api.tiny.com.br/public-api/v3"

# Validações
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem estar configurados no .env")

if not ENCRYPTION_KEY:
    raise ValueError("ENCRYPTION_KEY deve estar configurada no .env para criptografar credenciais")

# Redis (para Celery — tarefas assíncronas e scheduler)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Nota: TINY_CLIENT_ID, TINY_CLIENT_SECRET e TINY_REDIRECT_URI agora são armazenados
# no banco de dados por empresa, não mais no .env
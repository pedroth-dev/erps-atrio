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

# URLs da API Conta Azul
# Base da API pública: https://api-v2.contaazul.com
# Endpoint de token (renovação com refresh_token): https://auth.contaazul.com/oauth2/token
# Endpoint de login/autorização (para obter o code), conforme docs oficiais:
# https://developers.contaazul.com/requestingcode
CONTAZUL_AUTH_URL = "https://auth.contaazul.com/login"
CONTAZUL_TOKEN_URL = "https://auth.contaazul.com/oauth2/token"
CONTAZUL_API_BASE_URL = "https://api-v2.contaazul.com"
# Escopo recomendado na etapa de autorização inicial (code), conforme docs:
# scope=openid+profile+aws.cognito.signin.user.admin
CONTAZUL_AUTH_SCOPE = "openid+profile+aws.cognito.signin.user.admin"

# Credenciais OAuth globais do aplicativo Conta Azul.
# Essas credenciais são da aplicação (não do cliente final) e são compartilhadas
# entre todas as empresas; apenas login/senha e tokens são por empresa.
CONTAZUL_CLIENT_ID = os.getenv("CONTAZUL_CLIENT_ID")
CONTAZUL_CLIENT_SECRET = os.getenv("CONTAZUL_CLIENT_SECRET")
CONTAZUL_REDIRECT_URI = os.getenv("CONTAZUL_REDIRECT_URI")

# Validações
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem estar configurados no .env")

if not ENCRYPTION_KEY:
    raise ValueError("ENCRYPTION_KEY deve estar configurada no .env para criptografar credenciais")

# Redis (para Celery — tarefas assíncronas e scheduler)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Nota: TINY_CLIENT_ID, TINY_CLIENT_SECRET e TINY_REDIRECT_URI agora são armazenados
# no banco de dados por empresa, não mais no .env
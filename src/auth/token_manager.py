"""
Gerenciador de tokens OAuth para ERPs.
Verifica expiração, renova tokens e gerencia o ciclo de autenticação.
"""
import base64
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import requests
import time

from src.config.settings import TINY_TOKEN_URL, CONTAZUL_TOKEN_URL, BLING_TOKEN_URL
from src.database.supabase_client import SupabaseClient
class TokenManager:
    """Gerencia tokens OAuth para conexões ERP."""
    
    def __init__(self, db: SupabaseClient):
        self.db = db
        self._oauth_flow = None
    
    @property
    def oauth_flow(self):
        """Lazy load do OAuthFlow para evitar import circular."""
        if self._oauth_flow is None:
            from src.auth.oauth_flow import OAuthFlow
            self._oauth_flow = OAuthFlow(self.db)
        return self._oauth_flow
    
    def get_valid_token(self, connection_id: str, erp_type: str = "tiny") -> str:
        """
        Obtém um token válido para a conexão.
        Renova automaticamente se necessário.
        
        Args:
            connection_id: ID da conexão ERP
            erp_type: Tipo do ERP (padrão: 'tiny')
        
        Returns:
            Access token válido
        """
        # Busca conexão usando método do SupabaseClient
        conn = self.db.get_erp_connection_by_id(connection_id)
        
        if not conn:
            raise ValueError(f"Conexão {connection_id} não encontrada")
        
        # Garante que sempre temos um erp_type efetivo (prioriza argumento explícito)
        effective_erp_type = erp_type or conn.get("erp_type") or "tiny"
        
        # Verifica se o access_token ainda é válido (com margem de 5 minutos)
        access_expires_at = conn.get("access_token_expires_at")
        if access_expires_at:
            expires_dt = datetime.fromisoformat(access_expires_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            margin = timedelta(minutes=5)
            
            if now < (expires_dt - margin):
                # Token ainda válido - descriptografa e retorna
                return self.db.get_access_token(connection_id)
        
        # Token expirado ou próximo de expirar - tenta renovar com refresh_token
        refresh_expires_at = conn.get("refresh_token_expires_at")
        
        if refresh_expires_at:
            refresh_expires_dt = datetime.fromisoformat(refresh_expires_at.replace("Z", "+00:00"))
            if datetime.now(timezone.utc) < refresh_expires_dt:
                # Refresh token ainda válido - renova
                refresh_token = self.db.get_refresh_token(connection_id)
                new_tokens = self._refresh_token(connection_id, refresh_token, effective_erp_type)
                if new_tokens:
                    return new_tokens["access_token"]
        
        # Refresh token expirado ou inválido - precisa reautenticar via Selenium
        print(f"🔄 Refresh token expirado para conexão {connection_id}. Iniciando reautenticação ({effective_erp_type})...")
        new_tokens = self.oauth_flow.authenticate_connection(connection_id, effective_erp_type)
        return new_tokens["access_token"]
    
    def _refresh_token(self, connection_id: str, refresh_token: str, erp_type: str) -> Optional[Dict[str, Any]]:
        """
        Renova o access_token usando o refresh_token.
        Busca credenciais OAuth do banco de dados.
        
        Returns:
            Dicionário com novos tokens ou None em caso de erro
        """
        # Busca credenciais OAuth do banco (não mais do .env)
        oauth_creds = self.db.get_oauth_credentials(connection_id)
        
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        # Tiny e Conta Azul aceitam client_id/client_secret no body; Bling exige Basic no header.
        if erp_type != "bling":
            payload["client_id"] = oauth_creds["client_id"]
            payload["client_secret"] = oauth_creds["client_secret"]
        
        # Seleciona endpoint de token conforme ERP
        if erp_type == "tiny":
            token_url = TINY_TOKEN_URL
        elif erp_type == "contaazul":
            token_url = CONTAZUL_TOKEN_URL
        elif erp_type == "bling":
            token_url = BLING_TOKEN_URL
        else:
            raise ValueError(f"ERP não suportado para refresh de token: {erp_type}")
        
        if not token_url:
            raise ValueError(f"URL de token não configurada para o ERP {erp_type}")
        
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if erp_type == "bling":
            basic_creds = base64.b64encode(
                f"{oauth_creds['client_id']}:{oauth_creds['client_secret']}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {basic_creds}"
        
        try:
            response = requests.post(token_url, data=payload, headers=headers)
            response.raise_for_status()
            
            tokens = response.json()
            # Tokens são criptografados automaticamente pelo update_erp_tokens.
            # Para Conta Azul, o refresh_token segue uma janela deslizante de 30 dias:
            # a cada refresh empurramos o refresh_expires_at para agora + 30 dias,
            # evitando reautenticação enquanto houver uso periódico.
            expires_in = tokens.get("expires_in", 14400)
            if erp_type in ("contaazul", "bling"):
                refresh_expires_in = 30 * 24 * 3600  # 30 dias em segundos
            else:
                refresh_expires_in = tokens.get("refresh_expires_in", 86400)

            self.db.update_erp_tokens(
                connection_id=connection_id,
                access_token=tokens["access_token"],
                refresh_token=tokens["refresh_token"],
                expires_in=expires_in,
                refresh_expires_in=refresh_expires_in,
            )
            
            print(f"✅ Token renovado com sucesso para conexão {connection_id}")
            return tokens
            
        except Exception as e:
            print(f"❌ Erro ao renovar token: {e}")
            return None
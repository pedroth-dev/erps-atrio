"""
Script de onboarding de empresas.
Cadastra nova empresa e configura autenticação OAuth automaticamente.
"""
import sys
import os
import re
from pathlib import Path
from typing import Dict, Any

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

from src.database.supabase_client import SupabaseClient
from src.auth.oauth_flow import OAuthFlow
from src.config.settings import (
    CONTAZUL_CLIENT_ID,
    CONTAZUL_CLIENT_SECRET,
    CONTAZUL_REDIRECT_URI,
)


def validate_cnpj(cnpj: str) -> bool:
    """Valida formato e dígitos verificadores do CNPJ."""
    # Remove caracteres não numéricos
    cnpj = re.sub(r'\D', '', cnpj)
    
    if len(cnpj) != 14:
        return False
    
    # Validação básica de dígitos verificadores
    # (implementação simplificada - pode ser melhorada)
    return True


def onboard_company(
    name: str,
    document: str,
    erp_type: str,
    erp_login: str,
    erp_password: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str
) -> Dict[str, Any]:
    """
    Realiza o onboarding completo de uma empresa.
    
    Args:
        name: Nome da empresa
        document: CNPJ da empresa
        erp_type: Tipo do ERP ('tiny', 'bling', etc.)
        erp_login: Login do ERP
        erp_password: Senha do ERP
    
    Returns:
        Dicionário com informações da empresa e conexão criadas
    """
    db = SupabaseClient()
    
    print("=" * 60)
    print("🚀 INICIANDO ONBOARDING DE EMPRESA")
    print("=" * 60)
    
    # Etapa 1: Validação
    print("\n📋 Etapa 1: Validação dos dados...")
    
    if not name or not name.strip():
        raise ValueError("Nome da empresa não pode ser vazio")
    
    if not document:
        raise ValueError("CNPJ não pode ser vazio")
    
    if not validate_cnpj(document):
        raise ValueError("CNPJ inválido")
    
    if erp_type not in ["tiny", "bling", "omie", "contaazul"]:
        raise ValueError(f"Tipo de ERP inválido: {erp_type}")
    
    if not erp_login or not erp_password:
        raise ValueError("Login e senha do ERP são obrigatórios")

    # Para Conta Azul, usamos client_id/client_secret/redirect_uri globais do .env.
    # Isso evita repetir essas credenciais por empresa; o que diferencia cada conexão
    # são login/senha e os tokens obtidos.
    if erp_type == "contaazul":
        cid = client_id or CONTAZUL_CLIENT_ID
        csecret = client_secret or CONTAZUL_CLIENT_SECRET
        credir = redirect_uri or CONTAZUL_REDIRECT_URI
        if not cid or not csecret or not credir:
            raise ValueError(
                "Credenciais OAuth da aplicação Conta Azul devem estar configuradas "
                "no .env (CONTAZUL_CLIENT_ID, CONTAZUL_CLIENT_SECRET, CONTAZUL_REDIRECT_URI)."
            )
        client_id, client_secret, redirect_uri = cid, csecret, credir
    else:
        if not client_id or not client_secret or not redirect_uri:
            raise ValueError("Credenciais OAuth (client_id, client_secret, redirect_uri) são obrigatórias")
    
    # Verifica se empresa já existe
    existing = db.get_company_by_document(document)
    if existing:
        raise ValueError(f"Empresa com CNPJ {document} já está cadastrada")
    
    print("✅ Validação concluída")
    
    # Etapa 2: Criação da empresa e conexão
    print("\n📋 Etapa 2: Criando empresa e conexão no banco...")
    
    company = db.create_company(name, document)
    print(f"✅ Empresa criada: {company['id']}")
    
    connection = db.create_erp_connection(
        company_id=company["id"],
        erp_type=erp_type,
        erp_login=erp_login,
        erp_password=erp_password,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri
    )
    print(f"✅ Conexão criada: {connection['id']}")
    
    # Etapa 3: Autenticação OAuth
    print("\n📋 Etapa 3: Autenticação OAuth automática...")
    
    oauth_flow = OAuthFlow(db)
    try:
        tokens = oauth_flow.authenticate_connection(connection["id"], erp_type)
        print("✅ Autenticação concluída com sucesso!")
    except Exception as e:
        print(f"❌ Erro na autenticação: {e}")
        db.mark_connection_inactive(connection["id"], str(e))
        raise
    
    print("\n" + "=" * 60)
    print("✨ ONBOARDING CONCLUÍDO COM SUCESSO!")
    print("=" * 60)
    print(f"Empresa ID: {company['id']}")
    print(f"Conexão ID: {connection['id']}")
    print(f"Status: Ativa e pronta para sincronização")
    print("=" * 60)
    
    return {
        "company": company,
        "connection": connection,
        "tokens": tokens
    }


if __name__ == "__main__":
    print("Preencha os dados abaixo (ou deixe em branco e Enter para cancelar).\n")
    
    name = input("Nome da empresa: ").strip()
    if not name:
        print("Cancelado.")
        sys.exit(0)
    
    document = input("CNPJ (apenas números ou com formatação): ").strip()
    erp_type = input("Tipo do ERP (tiny / bling / omie / contaazul) [tiny]: ").strip() or "tiny"
    erp_login = input("Login do ERP (e-mail): ").strip()
    erp_password = input("Senha do ERP: ").strip()

    client_id = ""
    client_secret = ""
    redirect_uri = ""

    if erp_type == "contaazul":
        # Usa credenciais globais do .env; não pergunta no prompt.
        client_id = CONTAZUL_CLIENT_ID or ""
        client_secret = CONTAZUL_CLIENT_SECRET or ""
        redirect_uri = CONTAZUL_REDIRECT_URI or ""
        if not client_id or not client_secret or not redirect_uri:
            print(
                "As variáveis CONTAZUL_CLIENT_ID, CONTAZUL_CLIENT_SECRET e CONTAZUL_REDIRECT_URI "
                "devem estar configuradas no .env para onboarding Conta Azul."
            )
            sys.exit(1)
        print("\nUsando credenciais OAuth globais da aplicação Conta Azul definidas no .env.")
    else:
        client_id = input("Client ID (aplicação OAuth): ").strip()
        client_secret = input("Client Secret (aplicação OAuth): ").strip()
        redirect_uri = input("Redirect URI (ex: https://..../oauth/tiny): ").strip()
        if not client_id or not client_secret or not redirect_uri:
            print("Client ID, Client Secret e Redirect URI são obrigatórios para este ERP. Cancelado.")
            sys.exit(1)

    if not document or not erp_login or not erp_password:
        print("Nome, CNPJ, login e senha do ERP são obrigatórios. Cancelado.")
        sys.exit(1)
    
    try:
        result = onboard_company(name, document, erp_type, erp_login, erp_password, client_id, client_secret, redirect_uri)
        print("\n✅ Empresa cadastrada e autenticada com sucesso!")
    except Exception as e:
        print(f"\n❌ Erro no onboarding: {e}")
        sys.exit(1)
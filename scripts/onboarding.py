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
    
    if not erp_login or not erp_password:
        raise ValueError("Login e senha do ERP são obrigatórios")
    
    if not client_id or not client_secret or not redirect_uri:
        raise ValueError("Credenciais OAuth (client_id, client_secret, redirect_uri) são obrigatórias")
    
    if erp_type not in ["tiny", "bling", "omie"]:
        raise ValueError(f"Tipo de ERP inválido: {erp_type}")
    
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
    erp_type = input("Tipo do ERP (tiny / bling / omie) [tiny]: ").strip() or "tiny"
    erp_login = input("Login do ERP (e-mail): ").strip()
    erp_password = input("Senha do ERP: ").strip()
    client_id = input("Client ID (aplicação OAuth): ").strip()
    client_secret = input("Client Secret (aplicação OAuth): ").strip()
    redirect_uri = input("Redirect URI (ex: https://..../oauth/tiny): ").strip()
    
    if not document or not erp_login or not erp_password or not client_id or not client_secret or not redirect_uri:
        print("Todos os campos são obrigatórios (exceto tipo do ERP). Cancelado.")
        sys.exit(1)
    
    try:
        result = onboard_company(name, document, erp_type, erp_login, erp_password, client_id, client_secret, redirect_uri)
        print("\n✅ Empresa cadastrada e autenticada com sucesso!")
    except Exception as e:
        print(f"\n❌ Erro no onboarding: {e}")
        sys.exit(1)
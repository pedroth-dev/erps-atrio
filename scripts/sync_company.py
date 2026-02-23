"""
Script de sincronização de dados para uma empresa específica.
Sincroniza vendas e estoque do Tiny para o Supabase.
"""
import sys
import logging
from pathlib import Path
from typing import Optional

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

# Logging: resumido no console; libs HTTP em silêncio para não poluir
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
for _name in ("httpx", "httpcore", "urllib3"):
    logging.getLogger(_name).setLevel(logging.WARNING)

from src.database.supabase_client import SupabaseClient
from src.auth.token_manager import TokenManager
from src.sync.sales_sync import SalesSync
from src.sync.stock_sync import StockSync
from src.sync.checkpoints import get_sync_start
from src.sync.sales_normalizer import process_pending_sales
from src.sync.sale_items_collector import SaleItemsCollector
from src.sync.sale_items_normalizer import process_pending_sale_items
from src.sync.stock_normalizer import process_pending_stock


def sync_company(company_id: str, erp_type: str = "tiny", sync_sales: bool = True, sync_stock: bool = True):
    """
    Sincroniza dados de uma empresa.
    
    Args:
        company_id: ID da empresa
        erp_type: Tipo do ERP (padrão: 'tiny')
        sync_sales: Se deve sincronizar vendas
        sync_stock: Se deve sincronizar estoque
    """
    db = SupabaseClient()
    token_manager = TokenManager(db)
    
    print("=" * 60)
    print(f"🔄 SINCRONIZAÇÃO DE DADOS - Empresa {company_id}")
    print("=" * 60)
    
    # Busca conexão ERP e verifica se está ativa (conforme doc_funcionamento_geral.md)
    connection = db.get_erp_connection(company_id, erp_type)
    if not connection:
        raise ValueError(f"Conexão ERP não encontrada para empresa {company_id}")
    
    if not connection.get("is_active"):
        raise ValueError(f"Conexão ERP está inativa para empresa {company_id}. Não é possível sincronizar.")
    
    connection_id = connection["id"]
    
    # Sincroniza vendas: a cada 24h puxa últimos 30 dias (refresh); senão incremental
    if sync_sales:
        sales_sync = SalesSync(db, token_manager)
        data_inicial, data_final, is_full_refresh = get_sync_start(db, company_id, erp_type, "sales")
        if is_full_refresh:
            print(f"\n📅 Sincronizando vendas — refresh 30 dias ({data_inicial} a {data_final})...")
        else:
            print(f"\n📅 Sincronizando vendas — incremental ({data_inicial} a {data_final})...")
        sales_sync.sync_company_sales(
            company_id, connection_id,
            data_inicial=data_inicial,
            data_final=data_final,
            erp_type=erp_type,
            is_full_refresh=is_full_refresh,
        )
        # Normalizer: staging.tiny_sales → core.customers + core.sales (apenas Tiny por enquanto)
        # Processa TODAS as pendentes (não apenas 500)
        if erp_type == "tiny":
            n, sale_external_ids = process_pending_sales(db, company_id, erp_type, limit=500)
            if n > 0:
                print(f"📋 Normalizado: {n} vendas → core.customers / core.sales")

            # Coleta itens apenas das vendas recém-normalizadas (incremental = só essas; evita refazer todas)
            collector = SaleItemsCollector(db, token_manager)
            items_collected = collector.collect_sale_items(
                company_id, connection_id, erp_type, batch_size=100,
                sale_external_ids=sale_external_ids,
            )

            # Normaliza apenas itens das vendas recém-normalizadas (evita processar 222 quando só 28 são novos)
            items_normalized = process_pending_sale_items(
                db, company_id, erp_type, limit=500, sale_external_ids=sale_external_ids
            )
            if items_normalized > 0:
                print(f"📦 Normalizado: {items_normalized} itens → core.sale_items")
    
    # Sincroniza estoque
    if sync_stock:
        stock_sync = StockSync(db, token_manager)
        stock_sync.sync_company_stock(company_id, connection_id, erp_type=erp_type)
        if erp_type == "tiny":
            n_stock = process_pending_stock(db, company_id, erp_type, limit=500)
            if n_stock > 0:
                print(f"📦 Normalizado: {n_stock} estoques → core.stock")

    print("\n" + "=" * 60)
    print("✨ SINCRONIZAÇÃO CONCLUÍDA!")
    print("=" * 60)


if __name__ == "__main__":
    db = SupabaseClient()
    
    # Modo interativo: selecionar empresa e tipo de sincronização
    companies = db.get_all_companies()
    if not companies:
        print("Nenhuma empresa cadastrada. Execute primeiro o script de onboarding.")
        sys.exit(1)
    
    print("\nEmpresas disponíveis:\n")
    for i, c in enumerate(companies, 1):
        print(f"  {i}. {c['name']} (CNPJ: {c.get('document', 'N/A')})")
    
    while True:
        escolha = input("\nNúmero da empresa para sincronizar (ou 0 para cancelar): ").strip()
        if escolha == "0":
            print("Cancelado.")
            sys.exit(0)
        try:
            idx = int(escolha)
            if 1 <= idx <= len(companies):
                company_id = companies[idx - 1]["id"]
                company_name = companies[idx - 1]["name"]
                break
        except ValueError:
            pass
        print("Opção inválida. Digite o número da empresa (1 a {}) ou 0 para cancelar.".format(len(companies)))

    # Identifica quais ERPs estão disponíveis para esta empresa (conexões ativas)
    connections = db.get_erp_connections_by_company(company_id, active_only=True)
    if not connections:
        print(f"\n❌ Nenhuma conexão ERP ativa encontrada para a empresa '{company_name}'.")
        print("   Execute o onboarding desta empresa para configurar o ERP (Tiny, Bling, Omie, etc.).")
        sys.exit(1)

    erp_type = None
    if len(connections) == 1:
        erp_type = connections[0]["erp_type"]
        print(f"\nERP disponível: {erp_type}")
    else:
        print("\nERPs disponíveis para esta empresa:\n")
        for i, conn in enumerate(connections, 1):
            print(f"  {i}. {conn['erp_type']}")
        while True:
            erp_opt = input("\nNúmero do ERP a sincronizar (ou 0 para cancelar): ").strip()
            if erp_opt == "0":
                print("Cancelado.")
                sys.exit(0)
            try:
                erp_idx = int(erp_opt)
                if 1 <= erp_idx <= len(connections):
                    erp_type = connections[erp_idx - 1]["erp_type"]
                    break
            except ValueError:
                pass
            print("Opção inválida. Digite o número do ERP (1 a {}) ou 0 para cancelar.".format(len(connections)))

    print("\nO que deseja sincronizar?")
    print("  1. Apenas vendas")
    print("  2. Apenas estoque")
    print("  3. Vendas e estoque")
    while True:
        tipo = input("Opção (1, 2 ou 3) [3]: ").strip() or "3"
        if tipo == "1":
            sync_sales, sync_stock = True, False
            break
        if tipo == "2":
            sync_sales, sync_stock = False, True
            break
        if tipo == "3":
            sync_sales, sync_stock = True, True
            break
        print("Opção inválida. Digite 1, 2 ou 3.")

    print(f"\nEmpresa: {company_name}")
    print(f"ERP: {erp_type}")
    print(f"Sincronizar: {'Vendas' if sync_sales else ''} {'Estoque' if sync_stock else ''}")

    try:
        sync_company(company_id, erp_type, sync_sales, sync_stock)
    except Exception as e:
        print(f"\n❌ Erro na sincronização: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
"""
Sincronização de estoque do Tiny para o Supabase.
1) Lista produtos (full: todos ativos; incremental: por dataAlteracao).
2) Para cada produto, obtém GET /estoque/{idProduto} e grava no staging.
"""
import logging
import time
from typing import List, Dict, Any
from datetime import datetime, timezone

from src.database.supabase_client import SupabaseClient
from src.auth.token_manager import TokenManager
from src.integrations.tiny_client import TinyClient
from src.sync.checkpoints import get_sync_start, update_checkpoint

logger = logging.getLogger(__name__)

# Tamanho do lote para inserção no Supabase
STAGING_BATCH_SIZE = 100
# Pequena pausa entre requisições de estoque para não estourar rate limit
STOCK_REQUEST_DELAY_S = 0.3


class StockSync:
    """Gerencia a sincronização de estoque."""

    def __init__(self, db: SupabaseClient, token_manager: TokenManager):
        self.db = db
        self.token_manager = token_manager

    def sync_company_stock(
        self, company_id: str, connection_id: str, erp_type: str = None
    ) -> int:
        """
        Sincroniza estoque de uma empresa.
        - Se passou 24h desde last_full_refresh_at: busca todos os produtos ativos e estoque de cada um.
        - Senão (incremental): busca produtos por dataAlteracao e só desses busca o estoque.

        Args:
            company_id: ID da empresa
            connection_id: ID da conexão ERP
            erp_type: Tipo do ERP (para checkpoint); se None, obtido da conexão

        Returns:
            Número de registros de estoque inseridos no staging
        """
        print(f"\n📦 Iniciando sincronização de estoque...")

        connection = self.db.get_erp_connection_by_id(connection_id)
        if not connection or not connection.get("is_active"):
            raise ValueError(f"Conexão {connection_id} não está ativa")
        erp_type = erp_type or connection.get("erp_type") or "tiny"

        data_inicial, data_final, is_full_refresh = get_sync_start(
            self.db, company_id, erp_type, "stock"
        )
        if is_full_refresh:
            print(f"   Modo: refresh geral (última requisição geral há mais de 24h)")
        else:
            print(f"   Modo: incremental (produtos alterados desde {data_inicial})")

        access_token = self.token_manager.get_valid_token(connection_id)
        tiny_client = TinyClient(access_token)

        # 1) Listar produtos: ativos; em incremental filtrar por dataAlteracao
        if is_full_refresh:
            products = tiny_client.fetch_products(situacao="A")
        else:
            # API espera "YYYY-MM-DD HH:MM:SS"
            data_alteracao = f"{data_inicial} 00:00:00"
            products = tiny_client.fetch_products(
                situacao="A", data_alteracao=data_alteracao
            )

        if not products:
            print("⚠️  Nenhum produto encontrado para atualizar estoque")
            self.db.update_last_sync(connection_id)
            update_checkpoint(
                self.db, company_id, erp_type, "stock", set_full_refresh=is_full_refresh
            )
            return 0

        # 2) Para cada produto, GET /estoque/{id} e acumular payloads
        product_ids = []
        for p in products:
            pid = p.get("id")
            if pid is not None:
                product_ids.append(int(pid) if not isinstance(pid, int) else pid)

        if not product_ids:
            print("⚠️  Nenhum ID de produto válido")
            self.db.update_last_sync(connection_id)
            update_checkpoint(
                self.db, company_id, erp_type, "stock", set_full_refresh=is_full_refresh
            )
            return 0

        print(f"📥 Buscando estoque de {len(product_ids)} produto(s)...")
        stock_payloads: List[Dict[str, Any]] = []
        errors = 0
        t_start = time.perf_counter()
        for i, pid in enumerate(product_ids):
            payload = tiny_client.fetch_product_stock(pid)
            if payload is not None:
                stock_payloads.append(payload)
            else:
                errors += 1
            if (i + 1) % 50 == 0:
                print(f"   → {i + 1}/{len(product_ids)} estoques consultados...")
            time.sleep(STOCK_REQUEST_DELAY_S)

        elapsed = time.perf_counter() - t_start
        print(f"   → {len(stock_payloads)} estoques obtidos em {elapsed:.1f}s" + (f" ({errors} falhas)" if errors else ""))

        # 3) Inserir no staging em lotes (cada item = resposta GET /estoque/{id})
        fetched_at = datetime.now(timezone.utc)
        total = len(stock_payloads)
        inserted_count = 0
        num_batches = (total + STAGING_BATCH_SIZE - 1) // STAGING_BATCH_SIZE

        for i in range(0, total, STAGING_BATCH_SIZE):
            batch = stock_payloads[i : i + STAGING_BATCH_SIZE]
            batch_num = (i // STAGING_BATCH_SIZE) + 1
            try:
                n = self.db.insert_staging_stock_batch(company_id, batch, fetched_at)
                inserted_count += n
                print(f"   [{batch_num}/{num_batches}] {n} itens no staging ✓")
            except Exception as e:
                print(f"   [{batch_num}/{num_batches}] Erro: {e}")
                logger.exception("Erro lote %d estoque", batch_num)

        self.db.update_last_sync(connection_id)
        update_checkpoint(
            self.db, company_id, erp_type, "stock", set_full_refresh=is_full_refresh
        )

        print(f"✅ Estoque: {inserted_count} itens inseridos no staging")
        logger.info("Estoque: %d itens no staging (company=%s)", inserted_count, company_id)
        return inserted_count
"""
Sincronização de estoque (Tiny e Conta Azul) para o Supabase.
Tiny: lista produtos e GET /estoque/{id} por produto.
Conta Azul: lista produtos (GET /v1/produtos) e cada item já traz saldo; grava no staging.
"""
import logging
import time
from typing import List, Dict, Any
from datetime import datetime, timezone

from src.database.supabase_client import SupabaseClient
from src.auth.token_manager import TokenManager
from src.integrations.tiny_client import TinyClient
from src.integrations.contaazul_client import ContaAzulClient
from src.integrations.bling_client import BlingClient
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

        access_token = self.token_manager.get_valid_token(connection_id, erp_type=erp_type)

        if erp_type == "contaazul":
            # Conta Azul: GET /v1/produtos retorna itens com id, codigo, nome, saldo; cada item = 1 registro de staging
            api_client = ContaAzulClient(access_token)
            if is_full_refresh:
                products = api_client.fetch_products(status="ATIVO")
            else:
                # data_alteracao em ISO (ex: 2025-01-01T00:00:00)
                data_alteracao_de = f"{data_inicial}T00:00:00"
                data_alteracao_ate = f"{data_final}T23:59:59"
                products = api_client.fetch_products(
                    data_alteracao_de=data_alteracao_de,
                    data_alteracao_ate=data_alteracao_ate,
                    status="ATIVO",
                )
            stock_payloads = products
        elif erp_type == "bling":
            # Bling: GET /Api/v3/produtos retorna itens; cada item = 1 registro de staging
            api_client = BlingClient(access_token)
            if is_full_refresh:
                products = api_client.fetch_products(situacao="A")
            else:
                data_alteracao = f"{data_inicial} 00:00:00"
                products = api_client.fetch_products(
                    situacao="A", data_alteracao=data_alteracao
                )
            stock_payloads = products
        else:
            # Tiny: listar produtos e depois GET /estoque/{id} por produto
            tiny_client = TinyClient(access_token)
            if is_full_refresh:
                products = tiny_client.fetch_products(situacao="A")
            else:
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
            stock_payloads = []
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

        if not stock_payloads:
            print("⚠️  Nenhum registro de estoque para inserir")
            self.db.update_last_sync(connection_id)
            update_checkpoint(
                self.db, company_id, erp_type, "stock", set_full_refresh=is_full_refresh
            )
            return 0

        # Inserir no staging em lotes
        fetched_at = datetime.now(timezone.utc)
        total = len(stock_payloads)
        inserted_count = 0
        num_batches = (total + STAGING_BATCH_SIZE - 1) // STAGING_BATCH_SIZE

        for i in range(0, total, STAGING_BATCH_SIZE):
            batch = stock_payloads[i : i + STAGING_BATCH_SIZE]
            batch_num = (i // STAGING_BATCH_SIZE) + 1
            try:
                n = self.db.insert_staging_stock_batch(company_id, batch, fetched_at, erp_type=erp_type)
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
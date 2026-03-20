"""
Collector: busca itens de vendas já normalizadas (core.sales) e grava no staging.
- Tiny: GET /pedidos/{idPedido} e extrai o array 'itens'.
- Conta Azul: GET /v1/venda/{id}/itens.
Em sync incremental, deve receber apenas os external_id das vendas recém-normalizadas.
"""
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone

from src.database.postgres_client import PostgresClient
from src.auth.token_manager import TokenManager
from src.integrations.tiny_client import TinyClient
from src.integrations.contaazul_client import ContaAzulClient
from src.integrations.bling_client import BlingClient

logger = logging.getLogger(__name__)

# Tamanho do lote para inserção no Supabase
STAGING_BATCH_SIZE = 100


class SaleItemsCollector:
    """Coleta itens de vendas (produtos vendidos) para o staging."""

    def __init__(self, db: PostgresClient, token_manager: TokenManager):
        self.db = db
        self.token_manager = token_manager

    def collect_sale_items(
        self,
        company_id: str,
        connection_id: str,
        erp_type: str = "tiny",
        batch_size: int = 100,
        sale_external_ids: Optional[List[str]] = None,
    ) -> int:
        """
        Coleta itens de vendas via GET /pedidos/{idPedido} e grava no staging.

        Se sale_external_ids for informado, busca itens apenas dessas vendas (sync incremental).
        Se for None, busca de todas as vendas em core.sales (comportamento legado / full).

        Args:
            company_id: ID da empresa
            connection_id: ID da conexão ERP
            erp_type: Tipo do ERP (padrão: 'tiny')
            batch_size: Quantas vendas buscar por vez do banco quando sale_external_ids é None
            sale_external_ids: Lista de external_id das vendas para as quais buscar itens (apenas essas)

        Returns:
            Número total de itens coletados
        """
        print(f"\n🛍️  Coletando itens de vendas...")

        connection = self.db.get_erp_connection_by_id(connection_id)
        if not connection or not connection.get("is_active"):
            raise ValueError(f"Conexão {connection_id} não está ativa")

        access_token = self.token_manager.get_valid_token(connection_id, erp_type=erp_type)
        if erp_type == "contaazul":
            api_client = ContaAzulClient(access_token)
        elif erp_type == "bling":
            api_client = BlingClient(access_token)
        else:
            api_client = TinyClient(access_token)

        total_items = 0
        total_sales_processed = 0
        total_sales_failed = 0
        total_sales_no_items = 0
        fetched_at = datetime.now(timezone.utc)
        api_times: List[float] = []
        t_phase_start = time.perf_counter()
        # Listas para segunda tentativa ao final da fase:
        # - retry_api_sales: falhas ao chamar a API de itens/detalhes
        # - retry_db_payloads: falhas ao gravar no staging (Supabase indisponível, etc.)
        retry_api_sales: List[Tuple[str, Optional[str]]] = []
        retry_db_payloads: List[List[Dict[str, Any]]] = []

        if sale_external_ids is not None:
            # Incremental: apenas as vendas recém-normalizadas nesta execução
            if not sale_external_ids:
                print("⚠️  Nenhuma venda nova para coletar itens (lista vazia)")
                return 0
            sales = self.db.get_sales_from_core_by_external_ids(
                company_id, erp_type, sale_external_ids
            )
            if not sales:
                print("⚠️  Nenhuma venda encontrada no core para os external_ids informados")
                return 0
            n_sales = len(sales)
            print(f"📊 Coletando itens de {n_sales} venda(s) normalizada(s) nesta execução...")
            if n_sales > 100:
                print(f"   (cada venda = 1 requisição à API; ~{n_sales} req podem levar alguns minutos)")
            sales_batches = [sales]
        else:
            # Full: todas as vendas do core (paginação)
            sales_batches = []
            offset = 0
            while True:
                sales = self.db.get_sales_from_core(
                    company_id, erp_type, limit=batch_size, offset=offset
                )
                if not sales:
                    break
                sales_batches.append(sales)
                if len(sales) < batch_size:
                    break
                offset += batch_size
            if not sales_batches:
                print("⚠️  Nenhuma venda normalizada encontrada para coletar itens")
                return 0
            print(f"📊 Processando vendas em lotes de {batch_size}...")

        first_batch = True
        for sales in sales_batches:
            if first_batch:
                first_batch = False

            batch_failed = 0
            batch_no_items = 0
            batch_api_count = 0
            pending_payloads: List[Dict[str, Any]] = []

            sale_ext_ids = [str(s.get("external_id")) for s in sales if s.get("external_id")]
            staging_id_map = self.db.get_staging_sale_ids_by_external_ids(
                company_id, sale_ext_ids, erp_type=erp_type
            )

            total_in_batch = len(sales)
            for idx, sale in enumerate(sales):
                sale_external_id = sale.get("external_id")
                if not sale_external_id:
                    continue
                sale_staging_id = staging_id_map.get(str(sale_external_id))
                # 1) Chamada à API para buscar itens/detalhes
                try:
                    if erp_type == "contaazul":
                        items, elapsed = api_client.fetch_sale_items_timed(str(sale_external_id))
                        items = items or []
                    elif erp_type == "bling":
                        details, elapsed = api_client.fetch_sale_details_timed(str(sale_external_id))
                        if not details:
                            raise RuntimeError("Resposta vazia ao buscar detalhes da venda")
                        items = details.get("itens") or details.get("items") or []
                    else:
                        details, elapsed = api_client.fetch_sale_details_timed(str(sale_external_id))
                        if not details:
                            raise RuntimeError("Resposta vazia ao buscar detalhes da venda")
                        items = details.get("itens") or []
                    api_times.append(elapsed)
                    batch_api_count += 1
                    if not items:
                        batch_no_items += 1
                        total_sales_no_items += 1
                        continue
                except Exception as e:
                    logger.exception("Erro ao coletar itens da venda %s: %s", sale_external_id, e)
                    batch_failed += 1
                    total_sales_failed += 1
                    # Agenda segunda tentativa de chamada de API ao final
                    retry_api_sales.append((str(sale_external_id), sale_staging_id))
                    continue

                # 2) Acumula payload da venda para gravação em lote no staging
                pending_payloads.append(
                    {
                        "sale_external_id": str(sale_external_id),
                        "sale_staging_id": sale_staging_id,
                        "items": items,
                    }
                )
                total_sales_processed += 1

                # Progresso a cada 50 vendas (768 requisições levam vários minutos)
                if (idx + 1) % 50 == 0:
                    print(f"   → {idx + 1}/{total_in_batch} vendas consultadas | {total_items} itens até agora...")

                # Quando o buffer de payloads atingir o tamanho do lote, grava em bloco
                if len(pending_payloads) >= STAGING_BATCH_SIZE:
                    try:
                        n = self.db.insert_staging_sale_items_multi(
                            company_id=company_id,
                            payloads=pending_payloads,
                            fetched_at=fetched_at,
                            erp_type=erp_type,
                        )
                        total_items += n
                        pending_payloads = []
                    except Exception as e:
                        logger.exception(
                            "Erro ao salvar lote de itens no staging: %s",
                            e,
                        )
                        batch_failed += len(pending_payloads)
                        total_sales_failed += len(pending_payloads)
                        # Agenda segunda tentativa de gravação ao final (já temos os itens em memória)
                        retry_db_payloads.append(pending_payloads)
                        pending_payloads = []

            batch_time = sum(api_times[-batch_api_count:]) if batch_api_count else 0
            avg_req = (batch_time / batch_api_count) if batch_api_count else 0
            status_msg = f"   → {total_sales_processed} vendas ({total_items} itens) | API: {batch_time:.1f}s ({batch_api_count} req, ~{avg_req:.2f}s/req)"
            if batch_failed > 0 or batch_no_items > 0:
                status_msg += f" | {batch_failed} falharam | {batch_no_items} sem itens"
            print(status_msg)

            # Após terminar o grupo de vendas deste batch, grava o que sobrou no buffer
            if pending_payloads:
                try:
                    n = self.db.insert_staging_sale_items_multi(
                        company_id=company_id,
                        payloads=pending_payloads,
                        fetched_at=fetched_at,
                        erp_type=erp_type,
                    )
                    total_items += n
                    pending_payloads = []
                except Exception as e:
                    logger.exception(
                        "Erro ao salvar lote final de itens no staging: %s",
                        e,
                    )
                    batch_failed += len(pending_payloads)
                    total_sales_failed += len(pending_payloads)
                    retry_db_payloads.append(pending_payloads)
                    pending_payloads = []

        # Segunda tentativa: primeiro, re-tenta as chamadas de API que falharam,
        # depois re-tenta as gravações em staging que deram erro.
        if retry_api_sales:
            print(
                f"\n🔁 Re-tentando coleta de itens para {len(retry_api_sales)} venda(s) que falharam na primeira tentativa..."
            )
            for sale_ext_id, sale_staging_id in retry_api_sales:
                try:
                    if erp_type == "contaazul":
                        items, elapsed = api_client.fetch_sale_items_timed(str(sale_ext_id))
                        items = items or []
                    elif erp_type == "bling":
                        details, elapsed = api_client.fetch_sale_details_timed(str(sale_ext_id))
                        if not details:
                            continue
                        items = details.get("itens") or details.get("items") or []
                    else:
                        details, elapsed = api_client.fetch_sale_details_timed(str(sale_ext_id))
                        if not details:
                            continue
                        items = details.get("itens") or []
                    if not items:
                        continue
                    n = self.db.insert_staging_sale_items_batch(
                        company_id=company_id,
                        sale_external_id=sale_ext_id,
                        sale_staging_id=sale_staging_id,
                        items=items,
                        fetched_at=fetched_at,
                        erp_type=erp_type,
                    )
                    total_items += n
                    total_sales_processed += 1
                    # Compensa uma falha anterior
                    if total_sales_failed > 0:
                        total_sales_failed -= 1
                except Exception as e:
                    logger.exception(
                        "Segunda tentativa falhou para venda %s (API ou gravação): %s",
                        sale_ext_id,
                        e,
                    )

        if retry_db_payloads:
            print(
                f"\n🔁 Re-tentando gravação no staging para {len(retry_db_payloads)} lote(s) que falharam na primeira tentativa..."
            )
            for payload_batch in retry_db_payloads:
                try:
                    n = self.db.insert_staging_sale_items_multi(
                        company_id=company_id,
                        payloads=payload_batch,
                        fetched_at=fetched_at,
                        erp_type=erp_type,
                    )
                    total_items += n
                    # Compensa falhas anteriores de gravação para esse lote
                    failed_count = len(payload_batch)
                    if total_sales_failed >= failed_count:
                        total_sales_failed -= failed_count
                except Exception as e:
                    logger.exception(
                        "Segunda tentativa de salvar lote de itens no staging falhou: %s",
                        e,
                    )

        t_phase_elapsed = time.perf_counter() - t_phase_start
        print(f"\n✅ Resumo da coleta de itens:")
        print(f"   - {total_sales_processed} vendas processadas | {total_items} itens coletados")
        if api_times:
            total_api = sum(api_times)
            avg_api = total_api / len(api_times)
            min_api = min(api_times)
            max_api = max(api_times)
            print(f"   - ⏱️  API (GET /pedidos/{{id}}): {len(api_times)} requisições | total {total_api:.1f}s | média {avg_api:.2f}s/req | min {min_api:.2f}s | max {max_api:.2f}s")
            print(f"   - ⏱️  Fase completa: {t_phase_elapsed:.1f}s")
        if total_sales_failed > 0:
            print(f"   - ⚠️  {total_sales_failed} vendas falharam na coleta")
        if total_sales_no_items > 0:
            print(f"   - ℹ️  {total_sales_no_items} vendas não possuem itens")

        return total_items

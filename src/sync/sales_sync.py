"""
Sincronização de vendas (Tiny/Conta Azul/Bling) para o staging (fase 1).

Fluxo:
1) Chama endpoint geral de vendas e upserta no staging de pedidos (stg_erps.stg_*_pedidos)
2) Para cada venda do staging, chama endpoint detalhado e substitui o raw_json no mesmo registro
3) Atualiza checkpoints no fim (por integração)
"""
import logging
import os
from typing import List, Dict, Any
from datetime import datetime, timezone

from src.database.postgres_client import PostgresClient
from src.auth.token_manager import TokenManager
from src.integrations.tiny_client import TinyClient
from src.integrations.contaazul_client import ContaAzulClient
from src.integrations.bling_client import BlingClient
from src.sync.checkpoints import update_checkpoint

logger = logging.getLogger(__name__)

# Tamanho do lote para inserção no Supabase (reduz requisições e melhora tempo)
STAGING_BATCH_SIZE = 100
# A cada N pedidos detalhados, imprime progresso no console (ajustável via .env)
_DETAIL_LOG_EVERY = max(1, int(os.getenv("SALES_DETAIL_LOG_EVERY", "50")))
# Quantos erros de detalhe mostrar com numero_pedido (evita spam)
_DETAIL_ERR_LOG_MAX = max(0, int(os.getenv("SALES_DETAIL_ERR_LOG_MAX", "10")))


def _numero_pedido_from_sale_raw(db: PostgresClient, erp_type: str, raw: Dict[str, Any]) -> str:
    """
    Extrai o identificador lógico da venda para ser usado como `numero_pedido` no staging.
    """
    # Mantém compatibilidade com as chaves já utilizadas nos normalizadores legados.
    return db._sale_external_id_from_raw(raw)


class SalesSync:
    """Gerencia a sincronização de vendas."""
    
    def __init__(self, db: PostgresClient, token_manager: TokenManager):
        self.db = db
        self.token_manager = token_manager
    
    def sync_company_sales(
        self,
        company_id: str,
        connection_id: str,
        data_inicial: str = None,
        data_final: str = None,
        erp_type: str = None,
        is_full_refresh: bool = False,
    ) -> int:
        print("\nIniciando sincronização de vendas (fase 1 staging de pedidos)...")

        # Verifica se conexão está ativa
        connection = self.db.get_erp_connection_by_id(connection_id)
        if not connection or not connection.get("is_active"):
            raise ValueError(f"Conexão {connection_id} não está ativa")

        erp_type = erp_type or connection.get("erp_type") or "tiny"

        access_token = self.token_manager.get_valid_token(connection_id, erp_type=erp_type)

        if erp_type == "contaazul":
            api_client = ContaAzulClient(access_token)
        elif erp_type == "bling":
            api_client = BlingClient(access_token)
        else:
            api_client = TinyClient(access_token)

        # 1) Endpoint geral: lista resumos de vendas/pedidos
        sales = api_client.fetch_sales(data_inicial, data_final)
        if not sales:
            print("Nenhuma venda encontrada")
            return 0

        # 1.1) (Opcional) Bling: enriquecer situacao via endpoint /situacoes
        if erp_type == "bling" and sales:
            situacao_ids = set()
            for s in sales:
                situ = s.get("situacao")
                if isinstance(situ, dict):
                    sid = situ.get("id")
                    if isinstance(sid, int):
                        situacao_ids.add(sid)
            if situacao_ids:
                situacoes_map = api_client.fetch_situacoes(list(situacao_ids))
                for s in sales:
                    situ = s.get("situacao")
                    if not isinstance(situ, dict):
                        continue
                    sid = situ.get("id")
                    if not isinstance(sid, int):
                        continue
                    resolved = situacoes_map.get(sid)
                    if isinstance(resolved, dict):
                        s["situacao"] = {**situ, **resolved}

        # 2) Upsert do endpoint geral no staging de pedidos
        numero_pedidos: List[str] = []
        for s in sales:
            numero_pedido = _numero_pedido_from_sale_raw(self.db, erp_type, s)
            if numero_pedido:
                numero_pedidos.append(numero_pedido)
        numero_pedidos = list({str(n) for n in numero_pedidos if n})

        inserted_count = 0
        total = len(sales)
        num_batches = (total + STAGING_BATCH_SIZE - 1) // STAGING_BATCH_SIZE
        print(f"Pedidos gerais: {total} venda(s) em {num_batches} lote(s)")

        for i in range(0, total, STAGING_BATCH_SIZE):
            batch = sales[i : i + STAGING_BATCH_SIZE]
            batch_num = (i // STAGING_BATCH_SIZE) + 1
            try:
                n = self.db.upsert_staging_pedidos_batch(company_id, erp_type, batch)
                inserted_count += n
                print(f"   [{batch_num}/{num_batches}] {n} pedido(s)")
            except Exception as e:
                print(f"   [{batch_num}/{num_batches}] Erro: {e}")
                logger.exception("Erro lote %d vendas", batch_num)

        if not numero_pedidos:
            print("Nenhum numero_pedido foi extraído; abortando fase detalhada.")
            return inserted_count

        # 3) Busca no staging e substitui pelo detalhado
        staging_rows = self.db.get_staging_pedidos_by_numero_pedido(company_id, erp_type, numero_pedidos)

        total_detalhar = len(staging_rows)
        print(f"Pedidos para detalhar: {total_detalhar}")
        print(
            f"   (progresso a cada {_DETAIL_LOG_EVERY} pedidos; "
            f"erros com numero_pedido: até {_DETAIL_ERR_LOG_MAX}; "
            f"env SALES_DETAIL_LOG_EVERY / SALES_DETAIL_ERR_LOG_MAX)"
        )

        detailed_buffer: List[Dict[str, Any]] = []
        processed_details = 0
        erro_details = 0

        def _flush_details():
            nonlocal detailed_buffer, processed_details
            if not detailed_buffer:
                return
            n = len(detailed_buffer)
            self.db.upsert_staging_pedidos_details_batch(company_id, erp_type, detailed_buffer)
            processed_details += n
            print(
                f"   [detalhe lote] +{n} substituições no staging "
                f"(total detalhados gravados={processed_details})"
            )
            detailed_buffer = []

        for idx, row in enumerate(staging_rows, start=1):
            numero_pedido = str(row.get("numero_pedido") or "")
            if not numero_pedido:
                continue

            if erp_type == "tiny":
                details, _elapsed = api_client.fetch_sale_details_timed(numero_pedido)
            elif erp_type == "contaazul":
                details, _elapsed = api_client.fetch_sale_details_timed(numero_pedido)
            else:
                details, _elapsed = api_client.fetch_sale_details_timed(numero_pedido)

            if not details:
                self.db.mark_staging_pedido_erro(company_id, erp_type, numero_pedido)
                erro_details += 1
                if erro_details <= _DETAIL_ERR_LOG_MAX:
                    print(f"   [detalhe ERRO] numero_pedido={numero_pedido} (sem payload)")
                continue

            detailed_buffer.append(details)
            if len(detailed_buffer) >= STAGING_BATCH_SIZE:
                _flush_details()

            # Progresso: a cada N itens e no último
            if idx % _DETAIL_LOG_EVERY == 0 or idx == total_detalhar:
                ok_ate_agora = idx - erro_details
                pendente_buffer = len(detailed_buffer)
                print(
                    f"   [detalhe] {idx}/{total_detalhar} "
                    f"| ok={ok_ate_agora} | erros={erro_details} "
                    f"| buffer_pendente_gravar={pendente_buffer}"
                )

        # flush do que sobrou
        _flush_details()

        # 4) Atualiza checkpoints ao final
        update_checkpoint(self.db, company_id, erp_type, "sales", set_full_refresh=is_full_refresh)

        print(
            f"Fase1 vendas: upsert gerais={inserted_count}, detalhados_substituídos={processed_details}, erros={erro_details}"
        )
        logger.info(
            "Fase1 vendas (company=%s, erp=%s): gerais=%d detalhados=%d erros=%d",
            company_id,
            erp_type,
            inserted_count,
            processed_details,
            erro_details,
        )

        return inserted_count
"""
Fase 1: staging de itens de pedidos (stg_erps.stg_*_itens_pedidos).

Fonte: `stg_erps.stg_*_pedidos.raw_json` que já foi substituído pelo "detalhado"
durante `SalesSync` (endpoint geral -> staging -> detalhado -> substituição).

Objetivo:
  - Ler pedidos detalhados pendentes que ainda não possuem linha de itens
  - Validar que o raw_json contém `itens` (ou `items`) como array não vazio
  - Gravar no staging de itens com `raw_json` como o chunk completo do detalhado
  - Se falhar, gravar a linha e marcar `stg_status='erro'`
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.auth.token_manager import TokenManager
from src.database.postgres_client import PostgresClient
from src.integrations.contaazul_client import ContaAzulClient

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = max(1, int(os.getenv("ITEMS_PEDIDOS_STAGE_BATCH_SIZE", "200")))
DEFAULT_PROGRESS_EVERY = max(1, int(os.getenv("ITEMS_PEDIDOS_STAGE_PROGRESS_EVERY", "50")))


def _extract_itens_array_from_pedido_raw(raw_json: Any) -> List[Any] | None:
    """
    Normaliza a extração de itens do payload detalhado.
    Aceita tanto `itens` quanto `items` como chave.
    """
    if not isinstance(raw_json, dict):
        return None
    itens = raw_json.get("itens") if "itens" in raw_json else raw_json.get("items")
    if not isinstance(itens, list) or not itens:
        return None
    return itens


def stage_items_pedidos_fase1(
    db: PostgresClient,
    company_id: str,
    erp_type: str,
) -> Dict[str, int]:
    """
    Executa o staging de itens (fase 1) para um ERP.
    Retorna um resumo com contagens.
    """
    if erp_type not in ("tiny", "contaazul", "bling"):
        raise ValueError(f"Fase 1 itens_pedidos suporta apenas tiny/contaazul/bling. Recebido: {erp_type}")

    inserted_rows = 0
    erro_count = 0
    considered_count = 0

    started_at = datetime.now(timezone.utc)
    print(f"Iniciando staging de itens de pedidos (fase 1) empresa={company_id} erp={erp_type} ...")

    contaazul_client: ContaAzulClient | None = None
    if erp_type == "contaazul":
        token_manager = TokenManager(db)
        connection = db.get_erp_connection(company_id, "contaazul")
        if not connection or not connection.get("is_active"):
            raise ValueError(f"Conexão Conta Azul não encontrada/ativa para empresa {company_id}")
        access_token = token_manager.get_valid_token(connection["id"], erp_type="contaazul")
        contaazul_client = ContaAzulClient(access_token)

    while True:
        pending_pedidos = db.get_staging_pedidos_detailed_pending_without_itens(
            company_id=company_id,
            erp_type=erp_type,
            limit=DEFAULT_BATCH_SIZE,
        )
        if not pending_pedidos:
            break

        # Prepara linhas para upsert (inclui erros, pois precisamos garantir a existência da linha)
        upsert_rows: List[Dict[str, Any]] = []
        error_numbers: List[str] = []

        for p in pending_pedidos:
            considered_count += 1
            numero_pedido = str(p.get("numero_pedido") or "")
            raw_json = p.get("raw_json")

            if not numero_pedido or raw_json is None:
                error_numbers.append(numero_pedido or "")
                continue

            # Conta Azul: itens vêm do endpoint dedicado /v1/venda/{id}/itens.
            if erp_type == "contaazul":
                assert contaazul_client is not None
                try:
                    items = contaazul_client.fetch_sale_items_paginated(numero_pedido)
                except Exception:
                    items = None
                wrapped_raw = {"itens": items or []}
                has_itens = wrapped_raw["itens"] if wrapped_raw["itens"] else None
                raw_to_store = wrapped_raw
            else:
                # Tiny/Bling: extrai itens do payload detalhado já armazenado no staging de pedidos.
                extracted = _extract_itens_array_from_pedido_raw(raw_json)
                has_itens = extracted if extracted else None
                # Regra do projeto: staging de itens deve conter apenas os itens vendidos.
                raw_to_store = {"itens": extracted or []}

            upsert_rows.append(
                {
                    "numero_pedido": numero_pedido,
                    "raw_json": raw_to_store,
                }
            )
            if has_itens is None:
                error_numbers.append(numero_pedido)

            if considered_count % DEFAULT_PROGRESS_EVERY == 0:
                print(
                    f"   Progresso: {considered_count} pedidos considerados | "
                    f"erros={erro_count} | buffer={len(upsert_rows)}"
                )

        if upsert_rows:
            inserted_rows += db.upsert_staging_itens_pedidos_batch(
                company_id=company_id,
                erp_type=erp_type,
                rows=upsert_rows,
            )

        # Marca erros (se houver)
        for num in error_numbers:
            if not num:
                continue
            erro_count += 1
            db.mark_staging_itens_pedido_erro(company_id=company_id, erp_type=erp_type, numero_pedido=num)

        print(
            f"   Lote concluido: pedidos_considerados={considered_count} "
            f"| itens_gravados_ate_agora={inserted_rows} | erros={erro_count}"
        )

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    print(
        f"Concluido staging itens (fase 1) empresa={company_id} erp={erp_type} "
        f"em {elapsed:.1f}s | linhas_upsertadas={inserted_rows} | erros={erro_count}"
    )

    return {"upserted": inserted_rows, "erro": erro_count, "considered": considered_count}


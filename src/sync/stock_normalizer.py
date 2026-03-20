"""
Normalizer: lê staging (tiny_stock ou contaazul_stock) e grava em core.stock.
Tiny: payload = GET /estoque/{id}. Conta Azul: payload = item de GET /v1/produtos (id, codigo, nome, saldo).
"""
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from src.database.postgres_client import PostgresClient
from src.sync.contaazul_normalizer import contaazul_raw_to_core_stock_row as contaazul_stock_row
from src.sync.bling_normalizer import bling_raw_to_core_stock_row as bling_stock_row

logger = logging.getLogger(__name__)

NORMALIZER_BATCH_SIZE = 100


def _tiny_raw_to_core_stock_row(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Converte payload Tiny GET /estoque/{id} para core.stock."""
    external_id = raw_data.get("id")
    if external_id is None:
        raise ValueError("raw_data sem 'id' do produto")
    quantity = raw_data.get("disponivel")
    if quantity is None:
        quantity = raw_data.get("saldo", 0)
    try:
        quantity = float(quantity)
    except (TypeError, ValueError):
        quantity = 0.0
    return {
        "external_id": str(external_id),
        "sku": raw_data.get("codigo"),
        "product_name": raw_data.get("nome"),
        "quantity": quantity,
        "raw_data": raw_data,
    }


def process_pending_stock(
    db: PostgresClient,
    company_id: str,
    erp_type: str = "tiny",
    limit: int = 500,
) -> int:
    """
    Processa registros pendentes de staging (tiny_stock ou contaazul_stock) para core.stock em lotes.

    Returns:
        Número de registros normalizados (inseridos/atualizados no core).
    """
    if erp_type == "bling":
        staging_table = "bling_stock"
        raw_to_row = bling_stock_row
    elif erp_type == "contaazul":
        staging_table = "contaazul_stock"
        raw_to_row = contaazul_stock_row
    else:
        staging_table = "tiny_stock"
        raw_to_row = _tiny_raw_to_core_stock_row

    total_processed = 0
    total_failed = 0
    fetch_limit = limit
    synced_at = datetime.now(timezone.utc)

    while True:
        pending = db.get_pending_staging_stock(company_id, limit=fetch_limit, erp_type=erp_type)
        if not pending:
            break

        batch_total = len(pending)
        if total_processed == 0:
            print(f"📋 Normalizando estoque pendente [{erp_type}] em lotes de {NORMALIZER_BATCH_SIZE}...")

        for start in range(0, batch_total, NORMALIZER_BATCH_SIZE):
            batch = pending[start : start + NORMALIZER_BATCH_SIZE]
            record_ids: List[str] = []
            core_rows: List[Dict[str, Any]] = []
            ok_ids: List[str] = []

            for row in batch:
                record_id = row.get("id")
                raw_data = row.get("raw_data")
                if not record_id or raw_data is None:
                    if record_id:
                        db.mark_staging_processed(staging_table, str(record_id), error="raw_data ausente")
                    total_failed += 1
                    continue
                try:
                    core_row = raw_to_row(raw_data)
                    core_rows.append(core_row)
                    record_ids.append(str(record_id))
                    ok_ids.append(str(record_id))
                except Exception as ex:
                    logger.warning("Estoque id=%s falhou: %s", raw_data.get("id"), ex)
                    db.mark_staging_processed(staging_table, str(record_id), error=str(ex)[:500])
                    total_failed += 1

            if not core_rows:
                continue

            try:
                db.upsert_core_stock_batch(company_id, erp_type, core_rows, synced_at=synced_at)
                db.mark_staging_stock_processed_batch(ok_ids, error=None, erp_type=erp_type)
                total_processed += len(ok_ids)
                print(f"   → {total_processed} estoques normalizados")
            except Exception as e:
                msg = str(e)[:500]
                logger.exception("Erro no lote de estoque: %s", msg)
                total_failed += len(record_ids)
                try:
                    db.mark_staging_stock_processed_batch(record_ids, error=msg, erp_type=erp_type)
                except Exception:
                    for rid in record_ids:
                        try:
                            db.mark_staging_processed(staging_table, rid, error=msg)
                        except Exception:
                            pass

    return total_processed

"""
Normalizer: lê staging.tiny_stock (processed_at IS NULL), grava em core.stock.
Payload bruto = resposta GET /estoque/{idProduto} (id, nome, codigo, saldo, reservado, disponivel, etc.).
"""
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from src.database.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

NORMALIZER_BATCH_SIZE = 100


def _raw_to_core_stock_row(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte o payload do GET /estoque/{idProduto} para uma linha de core.stock.
    Ref: ObterEstoqueProdutoModelResponse (id, nome, codigo, saldo, reservado, disponivel, ...).
    """
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
    db: SupabaseClient,
    company_id: str,
    erp_type: str = "tiny",
    limit: int = 500,
) -> int:
    """
    Processa registros pendentes de staging.tiny_stock para core.stock em lotes.

    Returns:
        Número de registros normalizados (inseridos/atualizados no core).
    """
    total_processed = 0
    total_failed = 0
    fetch_limit = limit
    synced_at = datetime.now(timezone.utc)

    while True:
        pending = db.get_pending_staging_stock(company_id, limit=fetch_limit)
        if not pending:
            break

        batch_total = len(pending)
        if total_processed == 0:
            print(f"📋 Normalizando estoque pendente em lotes de {NORMALIZER_BATCH_SIZE}...")

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
                        db.mark_staging_processed("tiny_stock", str(record_id), error="raw_data ausente")
                    total_failed += 1
                    continue
                try:
                    core_row = _raw_to_core_stock_row(raw_data)
                    core_rows.append(core_row)
                    record_ids.append(str(record_id))
                    ok_ids.append(str(record_id))
                except Exception as ex:
                    logger.warning("Estoque id=%s falhou: %s", raw_data.get("id"), ex)
                    db.mark_staging_processed("tiny_stock", str(record_id), error=str(ex)[:500])
                    total_failed += 1

            if not core_rows:
                continue

            try:
                db.upsert_core_stock_batch(company_id, erp_type, core_rows, synced_at=synced_at)
                db.mark_staging_stock_processed_batch(ok_ids, error=None)
                total_processed += len(ok_ids)
                print(f"   → {total_processed} estoques normalizados")
            except Exception as e:
                msg = str(e)[:500]
                logger.exception("Erro no lote de estoque: %s", msg)
                total_failed += len(record_ids)
                try:
                    db.mark_staging_stock_processed_batch(record_ids, error=msg)
                except Exception:
                    for rid in record_ids:
                        try:
                            db.mark_staging_processed("tiny_stock", rid, error=msg)
                        except Exception:
                            pass

    return total_processed

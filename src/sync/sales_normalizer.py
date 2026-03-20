"""
Normalizer: lê staging (tiny_sales ou contaazul_sales) e grava em core.customers e core.sales.
Processamento em lote para ser bem mais rápido (poucas requisições ao banco).
"""
import logging
from typing import Optional, List, Dict, Any

from src.database.postgres_client import PostgresClient
from src.sync.tiny_normalizer import tiny_raw_to_customer, tiny_raw_to_sale
from src.sync.contaazul_normalizer import contaazul_raw_to_customer, contaazul_raw_to_sale
from src.sync.bling_normalizer import bling_raw_to_customer, bling_raw_to_sale

logger = logging.getLogger(__name__)

# Processar em lotes: 1 upsert de customers + 1 upsert de sales + 1 update staging por lote
NORMALIZER_BATCH_SIZE = 100


def process_pending_sales(
    db: PostgresClient,
    company_id: str,
    erp_type: str = "tiny",
    limit: int = 500,
) -> tuple:
    """
    Processa TODAS as vendas pendentes do staging para o core em lotes.
    Continua processando até não haver mais registros pendentes.

    Returns:
        (total_processed, sale_external_ids): quantidade normalizada e lista de external_id
        das vendas que foram para o core (para a coleta de itens buscar só essas).
    """
    if erp_type == "bling":
        staging_table = "bling_sales"
        raw_to_customer = bling_raw_to_customer
        raw_to_sale = bling_raw_to_sale
    elif erp_type == "contaazul":
        staging_table = "contaazul_sales"
        raw_to_customer = contaazul_raw_to_customer
        raw_to_sale = contaazul_raw_to_sale
    else:
        staging_table = "tiny_sales"
        raw_to_customer = tiny_raw_to_customer
        raw_to_sale = tiny_raw_to_sale

    total_processed = 0
    total_failed = 0
    sale_external_ids: List[str] = []
    fetch_limit = limit
    all_failed_reasons: Dict[str, int] = {}

    while True:
        pending = db.get_pending_staging_sales(company_id, limit=fetch_limit, erp_type=erp_type)
        if not pending:
            break

        batch_total = len(pending)
        if total_processed == 0:
            print(f"📋 Normalizando vendas pendentes [{erp_type}] em lotes de {NORMALIZER_BATCH_SIZE}...")

        for start in range(0, batch_total, NORMALIZER_BATCH_SIZE):
            batch = pending[start : start + NORMALIZER_BATCH_SIZE]
            record_ids: List[str] = []
            valid_rows: List[Dict[str, Any]] = []

            for row in batch:
                record_id = row.get("id")
                raw_data = row.get("raw_data")
                if not record_id or raw_data is None:
                    if record_id:
                        db.mark_staging_processed(staging_table, str(record_id), error="raw_data ausente")
                    continue
                record_ids.append(str(record_id))
                valid_rows.append({"id": record_id, "raw_data": raw_data})

            if not record_ids:
                continue

            try:
                customers_by_ext: Dict[str, Dict] = {}
                for item in valid_rows:
                    c = raw_to_customer(item["raw_data"])
                    if c:
                        customers_by_ext[str(c["external_id"])] = c

                if customers_by_ext:
                    id_map = db.upsert_core_customers_batch(
                        company_id, erp_type, list(customers_by_ext.values())
                    )
                else:
                    id_map = {}

                sale_rows: List[Dict[str, Any]] = []
                ok_ids: List[str] = []
                failed_count = 0
                failed_reasons: Dict[str, int] = {}

                for item in valid_rows:
                    try:
                        raw = item["raw_data"]
                        c = raw_to_customer(raw)
                        ext = str(c["external_id"]) if c else None
                        customer_id = id_map.get(ext) if ext else None
                        sale_payload = raw_to_sale(raw, customer_id)
                        sale_payload["customer_id"] = customer_id
                        sale_rows.append(sale_payload)
                        ok_ids.append(str(item["id"]))
                    except Exception as ex:
                        failed_count += 1
                        error_msg = str(ex)[:500]
                        sale_id = raw.get("id") or raw.get("numero") or "N/A"
                        logger.warning("Venda %s falhou na normalização: %s", sale_id, error_msg)
                        error_type = type(ex).__name__
                        failed_reasons[error_type] = failed_reasons.get(error_type, 0) + 1
                        db.mark_staging_processed(staging_table, str(item["id"]), error=error_msg)

                if sale_rows:
                    db.upsert_core_sales_batch(company_id, erp_type, sale_rows)
                    db.mark_staging_sales_processed_batch(ok_ids, error=None, erp_type=erp_type)
                    for row in sale_rows:
                        ext_id = row.get("external_id")
                        if ext_id is not None:
                            sale_external_ids.append(str(ext_id))
                total_processed += len(ok_ids)

                status_msg = f"   → {total_processed} normalizados"
                if failed_count > 0:
                    total_failed += failed_count
                    status_msg += f" | {failed_count} falharam neste lote"
                    for reason, count in failed_reasons.items():
                        all_failed_reasons[reason] = all_failed_reasons.get(reason, 0) + count
                print(status_msg)
            except Exception as e:
                msg = str(e)[:500]
                logger.exception("Normalizer erro no lote: %s", msg)
                total_failed += len(record_ids)
                try:
                    db.mark_staging_sales_processed_batch(record_ids, error=msg, erp_type=erp_type)
                except Exception:
                    for rid in record_ids:
                        try:
                            db.mark_staging_processed(staging_table, rid, error=msg)
                        except Exception:
                            pass

    # Resumo final com detalhes de falhas
    if total_failed > 0:
        print(f"\n⚠️  Resumo de falhas: {total_failed} vendas não foram normalizadas")
        if all_failed_reasons:
            print("   Motivos:")
            for reason, count in sorted(all_failed_reasons.items(), key=lambda x: x[1], reverse=True):
                print(f"     - {reason}: {count} venda(s)")

    return total_processed, sale_external_ids

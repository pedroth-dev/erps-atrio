"""
Normalizer: lê staging.tiny_sale_items (processed_at IS NULL), grava em core.sale_items.
Processamento em lote para ser bem mais rápido.
"""
import logging
from typing import Optional, List, Dict, Any

from src.database.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

NORMALIZER_BATCH_SIZE = 100


def _extract_sale_item(raw_item: Dict[str, Any], sale_external_id: str) -> Dict[str, Any]:
    """Extrai dados do item para core.sale_items."""
    produto = raw_item.get("produto") or {}
    if not isinstance(produto, dict):
        raise ValueError("Item sem objeto 'produto' no raw_data")

    product_external_id = produto.get("id")
    if product_external_id is None:
        raise ValueError("Item sem product.id no raw_data")

    quantidade = raw_item.get("quantidade")
    valor_unitario = raw_item.get("valorUnitario") or raw_item.get("valor_unitario")
    
    if quantidade is None or valor_unitario is None:
        raise ValueError("Item sem quantidade ou valorUnitario")

    try:
        qty = float(str(quantidade).replace(",", "."))
        unit_price = float(str(valor_unitario).replace(",", "."))
        total_price = qty * unit_price
    except (TypeError, ValueError) as e:
        raise ValueError(f"Erro ao converter quantidade/valor: {e}")

    return {
        "sale_external_id": str(sale_external_id),
        "product_external_id": str(product_external_id),
        "product_sku": produto.get("sku"),
        "product_description": produto.get("descricao"),
        "product_type": produto.get("tipo"),  # 'P' ou 'S'
        "quantity": qty,
        "unit_price": unit_price,
        "total_price": total_price,
        "raw_data": raw_item,
    }


def process_pending_sale_items(
    db: SupabaseClient,
    company_id: str,
    erp_type: str = "tiny",
    limit: int = 500,
    sale_external_ids: Optional[List[str]] = None,
) -> int:
    """
    Processa itens de vendas pendentes do staging para o core em lotes.
    Se sale_external_ids for informado, processa apenas itens dessas vendas (sync incremental).
    """
    total_processed = 0
    fetch_limit = limit

    while True:
        pending = db.get_pending_staging_sale_items(
            company_id, limit=fetch_limit, sale_external_ids=sale_external_ids
        )
        if not pending:
            break

        batch_total = len(pending)
        if total_processed == 0:
            print(f"📋 Normalizando itens de vendas em lotes de {NORMALIZER_BATCH_SIZE}...")

        processed_in_batch = 0

        for start in range(0, batch_total, NORMALIZER_BATCH_SIZE):
            batch = pending[start : start + NORMALIZER_BATCH_SIZE]
            record_ids: List[str] = []
            valid_rows: List[Dict[str, Any]] = []

            for row in batch:
                record_id = row.get("id")
                raw_data = row.get("raw_data")
                sale_external_id = row.get("sale_external_id")
                if not record_id or raw_data is None or not sale_external_id:
                    if record_id:
                        db.mark_staging_processed("tiny_sale_items", str(record_id), error="dados ausentes")
                    continue
                record_ids.append(str(record_id))
                valid_rows.append({"id": record_id, "raw_data": raw_data, "sale_external_id": sale_external_id})

            if not record_ids:
                continue

            try:
                # Buscar sale_id e metadata para todas as vendas de uma vez (otimização)
                unique_sale_ids = list(set(item["sale_external_id"] for item in valid_rows))
                sales_result = (
                    db._core_sales()
                    .select("id, external_id, issued_at, status")
                    .eq("company_id", company_id)
                    .eq("erp_type", erp_type)
                    .in_("external_id", unique_sale_ids)
                    .execute()
                )
                
                sale_id_map: Dict[str, Optional[str]] = {}
                sale_metadata: Dict[str, Dict] = {}
                for sale in sales_result.data:
                    ext_id = str(sale["external_id"])
                    sale_id_map[ext_id] = sale["id"]
                    sale_metadata[ext_id] = {
                        "issued_at": sale.get("issued_at"),
                        "status": sale.get("status"),
                    }

                # Montar itens normalizados
                item_rows: List[Dict[str, Any]] = []
                ok_ids: List[str] = []
                for item in valid_rows:
                    try:
                        ext_id = item["sale_external_id"]
                        item_data = _extract_sale_item(item["raw_data"], ext_id)
                        item_data["sale_id"] = sale_id_map.get(ext_id)
                        meta = sale_metadata.get(ext_id, {})
                        item_data["sale_date"] = meta.get("issued_at")
                        item_data["sale_status"] = meta.get("status")
                        item_rows.append(item_data)
                        ok_ids.append(str(item["id"]))
                    except Exception as ex:
                        db.mark_staging_processed("tiny_sale_items", str(item["id"]), error=str(ex)[:500])

                # Deduplicar por (sale_external_id, product_external_id): mesmo produto na mesma venda
                # agrega quantidade e total_price (evita erro "cannot affect row a second time" no upsert)
                if item_rows:
                    by_key: Dict[tuple, Dict[str, Any]] = {}
                    for r in item_rows:
                        key = (str(r["sale_external_id"]), str(r["product_external_id"]))
                        if key not in by_key:
                            by_key[key] = {**r}
                        else:
                            agg = by_key[key]
                            agg["quantity"] = (agg.get("quantity") or 0) + (r.get("quantity") or 0)
                            agg["total_price"] = (agg.get("total_price") or 0) + (r.get("total_price") or 0)
                    for agg in by_key.values():
                        qty = agg.get("quantity") or 0
                        if qty:
                            agg["unit_price"] = (agg.get("total_price") or 0) / qty
                    item_rows = list(by_key.values())

                if item_rows:
                    db.upsert_core_sale_items_batch(company_id, erp_type, item_rows)
                    db.mark_staging_sale_items_processed_batch(ok_ids, error=None)
                processed_in_batch += len(ok_ids)
                total_processed += len(ok_ids)
                print(f"   → {total_processed} itens normalizados")
            except Exception as e:
                msg = str(e)[:500]
                logger.exception("Normalizer erro no lote de itens: %s", msg)
                try:
                    db.mark_staging_sale_items_processed_batch(record_ids, error=msg)
                except Exception:
                    for rid in record_ids:
                        try:
                            db.mark_staging_processed("tiny_sale_items", rid, error=msg)
                        except Exception:
                            pass

    return total_processed

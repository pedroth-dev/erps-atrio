"""
Mapeamento do payload Conta Azul (staging raw_data) para core.customers, core.sales e core.stock.
Baseado na estrutura da API v1 (venda/busca, venda/{id}/itens, produtos).
"""
from typing import Dict, Any, Optional
from datetime import datetime

# Situações de venda na API: "em andamento", "aprovado", "faturado", "cancelado".
# A API pode retornar situacao.nome, situacao.descricao, situacao.codigo ou situacao como string;
# às vezes vem algo genérico como "VENDA".
CONTAZUL_SITUACAO_NORMALIZE: Dict[str, str] = {
    "em andamento": "em_andamento",
    "em_andamento": "em_andamento",
    "aprovado": "aprovado",
    "approved": "aprovado",
    "venda": "aprovado",  # genérico quando a API não envia situação detalhada
    "faturado": "faturado",
    "cancelado": "cancelado",
    "canceled": "cancelado",
    "cancelada": "cancelado",
    "recusado": "recusado",
    "refused": "recusado",
    "waiting_approved": "em_andamento",
    "aguardando aprovação": "em_andamento",
    "aguardando aprovacao": "em_andamento",
}

# Mapeamento unificado Conta Azul → status canônico do core
CONTAZUL_STATUS_UNIFIED_MAP: Dict[str, str] = {
    "em_andamento": "pending",
    "aprovado": "paid",
    "faturado": "paid",
    "cancelado": "canceled",
    "recusado": "canceled",
}


def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _parse_date(value: Any) -> Optional[str]:
    """Retorna data em ISO para issued_at ou None."""
    if value is None:
        return None
    if isinstance(value, str) and len(value) >= 10:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00")[:19])
            return dt.isoformat()
        except Exception:
            return value[:10] if value else None
    return None


def contaazul_raw_to_customer(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extrai dados do cliente do raw_data de uma venda Conta Azul para upsert em core.customers.
    Ref: itens.cliente (id, nome, email, telefone, endereco, cidade, estado, cep, pais).
    """
    cliente = raw.get("cliente")
    if not isinstance(cliente, dict):
        return None

    external_id = cliente.get("id")
    if external_id is None:
        return None

    return {
        "external_id": str(external_id),
        "name": _safe_str(cliente.get("nome")),
        "person_type": "juridica",  # Conta Azul não envia tipo no resumo; default
        "document": _safe_str(cliente.get("documento")) or _safe_str(cliente.get("cpf_cnpj")),
        "phone": _safe_str(cliente.get("telefone")),
        "mobile": _safe_str(cliente.get("celular")) or _safe_str(cliente.get("telefone")),
        "email": _safe_str(cliente.get("email")),
        "neighborhood": _safe_str(cliente.get("bairro")),
        "city": _safe_str(cliente.get("cidade")),
        "zip_code": _safe_str(cliente.get("cep")),
        "state": _safe_str(cliente.get("estado")),
        "country": _safe_str(cliente.get("pais")),
        "raw_data": cliente,
    }


def contaazul_raw_to_sale(raw: Dict[str, Any], customer_id: Optional[str]) -> Dict[str, Any]:
    """
    Extrai dados da venda do raw_data Conta Azul para upsert em core.sales.
    Ref: itens (id, numero, total, data, criado_em, situacao.nome).
    """
    external_id = raw.get("id") or raw.get("numero")
    if external_id is None:
        raise ValueError("Venda sem id/numero no raw_data")

    valor = raw.get("total")
    if valor is not None and not isinstance(valor, (int, float)):
        try:
            valor = float(str(valor).replace(",", "."))
        except (TypeError, ValueError):
            valor = None

    situacao = raw.get("situacao")
    if isinstance(situacao, dict):
        # API pode enviar nome, descricao ou codigo (ex.: APPROVED, CANCELED)
        status_raw = _safe_str(
            situacao.get("nome") or situacao.get("descricao") or situacao.get("codigo")
        )
    else:
        status_raw = _safe_str(situacao)

    status_key = status_raw.strip().lower() if status_raw else None
    normalized_code = CONTAZUL_SITUACAO_NORMALIZE.get(status_key) if status_key else None
    unified_status = CONTAZUL_STATUS_UNIFIED_MAP.get(normalized_code) if normalized_code else None
    status = unified_status or "pending"

    issued_at = _parse_date(raw.get("data") or raw.get("criado_em") or raw.get("data_alteracao"))

    return {
        "external_id": str(external_id),
        "order_number": _safe_str(raw.get("numero")),
        "origin_order_id": None,
        "origin_channel_id": None,
        "origin_channel": _safe_str(raw.get("origem")),
        "total_amount": valor,
        "status": status,
        "issued_at": issued_at,
        "raw_data": raw,
    }


def contaazul_raw_to_core_stock_row(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte o payload de produto Conta Azul (GET /v1/produtos) para uma linha de core.stock.
    Campos: id, codigo, nome, saldo.
    """
    external_id = raw_data.get("id")
    if external_id is None:
        raise ValueError("raw_data sem 'id' do produto")

    quantity = raw_data.get("saldo")
    try:
        quantity = float(quantity) if quantity is not None else 0.0
    except (TypeError, ValueError):
        quantity = 0.0

    return {
        "external_id": str(external_id),
        "sku": raw_data.get("codigo"),
        "product_name": raw_data.get("nome"),
        "quantity": quantity,
        "raw_data": raw_data,
    }


def contaazul_extract_sale_item(raw_item: Dict[str, Any], sale_external_id: str) -> Dict[str, Any]:
    """
    Extrai dados do item de venda Conta Azul para core.sale_items.
    Estrutura esperada (GET /v1/venda/{id_venda}/itens):
      - id:       id do registro de item
      - id_item:  id do produto
      - nome / descricao: identificação do produto
      - tipo:     'PRODUTO' ou 'SERVICO'
      - quantidade: número
      - valor:    valor unitário
      - custo:    opcional
    """
    # Para Conta Azul, o id do produto no endpoint de itens é id_item (uuid do produto);
    # se não vier, usamos id (id do item) como último recurso.
    product_external_id = raw_item.get("id_item") or raw_item.get("id")

    if product_external_id is None:
        raise ValueError("Item sem id do produto no raw_data")

    quantidade = raw_item.get("quantidade") or raw_item.get("qty")
    # Na API de itens Conta Azul, 'valor' é o valor unitário do item.
    valor_unitario = raw_item.get("valor") or raw_item.get("valor_unitario") or raw_item.get("valorUnitario")

    if quantidade is None or valor_unitario is None:
        raise ValueError("Item sem quantidade ou valor unitário")

    try:
        qty = float(str(quantidade).replace(",", "."))
        unit_price = float(str(valor_unitario).replace(",", "."))
        total_price = qty * unit_price
    except (TypeError, ValueError) as e:
        raise ValueError(f"Erro ao converter quantidade/valor: {e}") from e

    return {
        "sale_external_id": str(sale_external_id),
        "product_external_id": str(product_external_id),
        "product_sku": None,
        "product_description": _safe_str(raw_item.get("nome") or raw_item.get("descricao")),
        "product_type": raw_item.get("tipo"),
        "quantity": qty,
        "unit_price": unit_price,
        "total_price": total_price,
        "raw_data": raw_item,
    }

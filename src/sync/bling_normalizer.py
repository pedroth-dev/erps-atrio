"""
Mapeamento do payload Bling (staging.bling_sales, bling_stock, bling_sale_items raw_data) para core.
Compatível com a estrutura da API v3 do Bling (pedidos, produtos, itens).
Ref: https://developer.bling.com.br/bling-api
"""
from typing import Dict, Any, Optional, List
from datetime import datetime


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


# Situações/status comuns na API Bling (pedidos)
BLING_SITUACAO_NORMALIZE: Dict[str, str] = {
    "em aberto": "aberto",
    "aberto": "aberto",
    "aberta": "aberto",
    "em andamento": "em_andamento",
    "em digitação": "em_andamento",
    "venda agenciada": "venda_agenciada",
    "atendido": "atendido",
    "cancelado": "cancelado",
    "verificado": "verificado",
}

BLING_STATUS_UNIFIED_MAP: Dict[str, str] = {
    # pending
    "aberto": "pending",
    "em_andamento": "pending",
    # paid
    "atendido": "paid",
    "venda_agenciada": "paid",
    "verificado": "paid",
    # canceled
    "cancelado": "canceled",
}

# Ordem de prioridade para casar nomes de situações personalizadas.
# Em casos raros de múltiplos matches, o primeiro da lista vence.
BLING_SITUACAO_PRIORITY: List[str] = [
    "cancelado",         # sempre priorizar cancelado se aparecer junto
    "atendido",
    "venda agenciada",
    "verificado",
    "em andamento",
    "em digitação",
    "em aberto",
    "aberto",
    "aberta",
]


def bling_raw_to_customer(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extrai dados do cliente do raw_data de um pedido Bling para upsert em core.customers.
    Retorna None se não houver cliente.
    """
    cliente = raw.get("contato") or raw.get("cliente")
    if not isinstance(cliente, dict):
        return None

    external_id = cliente.get("id") or cliente.get("idContato")
    if external_id is None:
        return None

    # tipoPessoa: "J" (jurídica) ou "F" (física)
    tipo_pessoa = (cliente.get("tipoPessoa") or "").upper()
    person_type = "juridica" if tipo_pessoa == "J" else "fisica"

    return {
        "external_id": str(external_id),
        "name": _safe_str(cliente.get("nome") or cliente.get("nomeContato")),
        "person_type": person_type,
        "document": _safe_str(cliente.get("numeroDocumento") or cliente.get("cpfCnpj")),
        "phone": _safe_str(cliente.get("telefone") or cliente.get("fone")),
        "mobile": _safe_str(cliente.get("celular") or cliente.get("telefone")),
        "email": _safe_str(cliente.get("email")),
        "neighborhood": _safe_str(cliente.get("bairro")),
        "city": _safe_str(cliente.get("cidade")),
        "zip_code": _safe_str(cliente.get("cep")),
        "state": _safe_str(cliente.get("uf")),
        "country": _safe_str(cliente.get("pais")),
        "raw_data": cliente,
    }


def bling_raw_to_sale(raw: Dict[str, Any], customer_id: Optional[str]) -> Dict[str, Any]:
    """
    Extrai dados da venda do raw_data Bling para upsert em core.sales.
    """
    external_id = raw.get("id") or raw.get("numero") or raw.get("idPedidoVenda")
    if external_id is None:
        raise ValueError("Pedido Bling sem id/numero no raw_data")

    # Na API v3 do Bling, o valor total do pedido vem em "total".
    # Campos como "valor" podem aparecer em outros contextos (ex.: situacao.valor),
    # então aqui usamos exclusivamente "total" para o total_amount.
    valor = raw.get("total")
    if valor is not None and not isinstance(valor, (int, float)):
        try:
            valor = float(str(valor).replace(",", "."))
        except (TypeError, ValueError):
            valor = None

    situacao = raw.get("situacao") or raw.get("status")
    status = "pending"
    # Após SalesSync, situacao deve ter sido enriquecida via /situacoes/{idSituacao},
    # contendo pelo menos o campo "nome" (ex.: "Em aberto", "Atendido - Full ML", etc.).
    if isinstance(situacao, dict):
        status_raw = _safe_str(situacao.get("nome") or situacao.get("descricao"))
    else:
        status_raw = _safe_str(situacao)

    base_key: Optional[str] = None
    if status_raw:
        name_l = status_raw.strip().lower()

        # 1) Match exato
        if name_l in BLING_SITUACAO_NORMALIZE:
            base_key = BLING_SITUACAO_NORMALIZE[name_l]
        else:
            # 2) Match parcial para situações personalizadas:
            # se o nome contiver o texto de uma situação nativa,
            # herdamos o status dessa situação.
            for pattern in BLING_SITUACAO_PRIORITY:
                if pattern in name_l:
                    normalized = BLING_SITUACAO_NORMALIZE.get(pattern)
                    if normalized:
                        base_key = normalized
                        break

    if base_key:
        status = BLING_STATUS_UNIFIED_MAP.get(base_key, "pending")

    issued_at = _parse_date(
        raw.get("data") or raw.get("dataCriacao") or raw.get("dataPedido")
    )

    return {
        "external_id": str(external_id),
        "order_number": _safe_str(raw.get("numero")),
        "origin_order_id": None,
        "origin_channel_id": None,
        "origin_channel": _safe_str(raw.get("origem") or raw.get("canal")),
        "total_amount": valor,
        "status": status,
        "issued_at": issued_at,
        "raw_data": raw,
    }


def bling_raw_to_core_stock_row(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte o payload de produto Bling (GET /Api/v3/produtos) para uma linha de core.stock.
    """
    external_id = raw_data.get("id")
    if external_id is None:
        raise ValueError("raw_data Bling sem 'id' do produto")

    # Endpoint /produtos e /produtos/{idProduto} trazem um objeto "estoque"
    # com vários campos; no exemplo oficial, o saldo vem em estoque.saldoVirtualTotal.
    estoque = raw_data.get("estoque") or {}
    if isinstance(estoque, dict):
        quantity = (
            estoque.get("saldoVirtualTotal")
            or estoque.get("saldoFisicoTotal")
            or estoque.get("saldo")  # fallback genérico
        )
    else:
        quantity = None
    try:
        quantity = float(quantity) if quantity is not None else 0.0
    except (TypeError, ValueError):
        quantity = 0.0

    return {
        "external_id": str(external_id),
        "sku": raw_data.get("codigo") or raw_data.get("sku"),
        "product_name": raw_data.get("nome"),
        "quantity": quantity,
        "raw_data": raw_data,
    }


def bling_extract_sale_item(raw_item: Dict[str, Any], sale_external_id: str) -> Dict[str, Any]:
    """
    Extrai dados do item de venda Bling para core.sale_items.
    Estrutura esperada (itens do pedido): produto (id, codigo, nome), quantidade, valorUnitario.
    """
    produto = raw_item.get("produto") or raw_item.get("product") or {}
    if isinstance(produto, dict):
        product_external_id = produto.get("id") or raw_item.get("idProduto") or raw_item.get("id")
    else:
        product_external_id = raw_item.get("idProduto") or raw_item.get("id")

    if product_external_id is None:
        raise ValueError("Item Bling sem id do produto no raw_data")

    quantidade = raw_item.get("quantidade") or raw_item.get("qty")
    valor_unitario = (
        raw_item.get("valorUnitario")
        or raw_item.get("valor_unitario")
        or raw_item.get("valor")
    )

    if quantidade is None or valor_unitario is None:
        raise ValueError("Item Bling sem quantidade ou valor unitário")

    try:
        qty = float(str(quantidade).replace(",", "."))
        unit_price = float(str(valor_unitario).replace(",", "."))
        total_price = qty * unit_price
    except (TypeError, ValueError) as e:
        raise ValueError(f"Erro ao converter quantidade/valor: {e}") from e

    descricao = None
    if isinstance(produto, dict):
        descricao = _safe_str(produto.get("nome") or produto.get("descricao"))

    return {
        "sale_external_id": str(sale_external_id),
        "product_external_id": str(product_external_id),
        "product_sku": produto.get("codigo") or produto.get("sku") if isinstance(produto, dict) else None,
        "product_description": descricao or _safe_str(raw_item.get("descricao")),
        "product_type": raw_item.get("tipo"),
        "quantity": qty,
        "unit_price": unit_price,
        "total_price": total_price,
        "raw_data": raw_item,
    }

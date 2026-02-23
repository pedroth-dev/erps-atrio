"""
Mapeamento do payload Tiny (staging.tiny_sales raw_data) para core.customers e core.sales.
Compatível com a estrutura retornada pela API de pedidos do Tiny (v2/v3).
"""
from typing import Dict, Any, Optional
from datetime import datetime


def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _parse_tiny_date(value: Any) -> Optional[str]:
    """Retorna data em ISO para issued_at ou None."""
    if value is None:
        return None
    if isinstance(value, str) and len(value) >= 10:
        try:
            # "2024-01-15" ou "2024-01-15T10:00:00"
            dt = datetime.fromisoformat(value.replace("Z", "+00:00")[:19])
            return dt.isoformat()
        except Exception:
            return value[:10] if value else None
    return None


# Mapeamento de situação Tiny (número) para status texto (core.sales)
TINY_SITUACAO_MAP = {
    8: "Dados Incompletos",
    0: "Aberta",
    3: "Aprovada",
    4: "Preparando Envio",
    1: "Faturada",
    7: "Pronto Envio",
    5: "Enviada",
    6: "Entregue",
    2: "Cancelada",
    9: "Nao Entregue",
}


def _map_tiny_situacao(value: Any, raw_values: Optional[Dict[str, Any]] = None) -> str:
    """
    Mapeia situação numérica do Tiny para texto.
    Se a situação não estiver no mapeamento válido, lança ValueError com detalhamento.
    raw_values: opcional, para mensagem de erro mostrando o que veio no raw (situacao, situacaoPedido, status).
    """
    valores_aceitos = ", ".join(f"{k}={v}" for k, v in sorted(TINY_SITUACAO_MAP.items()))

    if value is None:
        detalhe = "Campo(s) situacao/situacaoPedido/status ausentes ou nulos no raw_data"
        if raw_values is not None:
            detalhe += f" (situacao={raw_values.get('situacao')!r}, situacaoPedido={raw_values.get('situacaoPedido')!r}, status={raw_values.get('status')!r})"
        detalhe += f". Valores aceitos: {valores_aceitos}"
        raise ValueError(detalhe)

    # Tenta converter para número
    num = None
    if isinstance(value, (int, float)):
        num = int(value)
    elif isinstance(value, str):
        try:
            num = int(value.strip())
        except (ValueError, AttributeError):
            # Se já vier como texto válido, verifica se está no mapeamento
            texto_lower = value.strip().lower()
            for k, v in TINY_SITUACAO_MAP.items():
                if v.lower() == texto_lower:
                    return v
            raise ValueError(
                f"Situação presente mas não mapeada: valor recebido {value!r} (tipo: {type(value).__name__}). "
                f"Valores aceitos: {valores_aceitos}"
            )

    if num not in TINY_SITUACAO_MAP:
        raise ValueError(
            f"Situação presente mas não mapeada: valor numérico {num} (tipo: {type(value).__name__}). "
            f"Valores aceitos: {valores_aceitos}"
        )

    return TINY_SITUACAO_MAP[num]


def tiny_raw_to_customer(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extrai dados do cliente do raw_data de um pedido Tiny para upsert em core.customers.
    Retorna None se não houver cliente (ex.: pedido sem cliente).
    """
    cliente = raw.get("cliente") or raw.get("cliente_principal")
    if not isinstance(cliente, dict):
        return None

    external_id = cliente.get("id") or cliente.get("codigo") or cliente.get("idCliente")
    if external_id is None:
        return None

    tipo = (cliente.get("tipo_pessoa") or cliente.get("tipoPessoa") or "").upper()
    person_type = "juridica" if tipo in ("J", "JURIDICA") else "fisica"

    end = cliente.get("endereco") if isinstance(cliente.get("endereco"), dict) else {}
    return {
        "external_id": str(external_id),
        "name": _safe_str(cliente.get("nome") or cliente.get("nomeCliente")),
        "person_type": person_type,
        "document": _safe_str(cliente.get("cpf_cnpj") or cliente.get("cpfCnpj")),
        "phone": _safe_str(cliente.get("fone") or cliente.get("telefone")),
        "mobile": _safe_str(cliente.get("celular") or cliente.get("mobile")),
        "email": _safe_str(cliente.get("email")),
        "neighborhood": _safe_str(cliente.get("bairro") or end.get("bairro")),
        "city": _safe_str(cliente.get("cidade") or end.get("cidade")),
        "zip_code": _safe_str(cliente.get("cep") or end.get("cep")),
        "state": _safe_str(cliente.get("uf") or end.get("uf")),
        "country": _safe_str(cliente.get("pais") or end.get("pais")),
        "raw_data": cliente,
    }


def tiny_raw_to_sale(raw: Dict[str, Any], customer_id: Optional[str]) -> Dict[str, Any]:
    """
    Extrai dados da venda do raw_data de um pedido Tiny para upsert em core.sales.
    """
    external_id = raw.get("id") or raw.get("idPedido") or raw.get("numero")
    if external_id is None:
        raise ValueError("Pedido sem id/external_id no raw_data")

    ecommerce = raw.get("ecommerce") or {}
    if not isinstance(ecommerce, dict):
        ecommerce = {}

    valor = raw.get("valor") or raw.get("valorTotal") or raw.get("total")
    if valor is not None and not isinstance(valor, (int, float)):
        try:
            valor = float(str(valor).replace(",", "."))
        except (TypeError, ValueError):
            valor = None

    # Valida e mapeia situação: se inválida, lança ValueError (pedido não será normalizado).
    # Usar "is None" para não tratar 0 (Aberta) como ausente.
    situacao_raw = raw.get("situacao")
    if situacao_raw is None:
        situacao_raw = raw.get("situacaoPedido")
    if situacao_raw is None:
        situacao_raw = raw.get("status")
    raw_campos = {"situacao": raw.get("situacao"), "situacaoPedido": raw.get("situacaoPedido"), "status": raw.get("status")}
    status = _map_tiny_situacao(situacao_raw, raw_values=raw_campos)

    return {
        "external_id": str(external_id),
        "order_number": _safe_str(raw.get("numero") or raw.get("numeroPedido")),
        "origin_order_id": _safe_str(ecommerce.get("numeroPedidoEcommerce") or ecommerce.get("numero_pedido_ecommerce")),
        "origin_channel_id": _safe_str(ecommerce.get("id")) if ecommerce.get("id") is not None else None,
        "origin_channel": _safe_str(ecommerce.get("nome") or ecommerce.get("nomeCanal")),
        "total_amount": valor,
        "status": status,
        "issued_at": _parse_tiny_date(raw.get("dataCriacao") or raw.get("data_pedido") or raw.get("dataPedido")),
        "raw_data": raw,
    }

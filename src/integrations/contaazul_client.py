"""
Cliente para interagir com a API v1 da Conta Azul.
Focado em operações de leitura para vendas e produtos/estoque.
"""
from typing import List, Dict, Any, Optional, Tuple
import requests
import time

from src.config.settings import CONTAZUL_API_BASE_URL


class ContaAzulClient:
    """Cliente para a API da Conta Azul (v1)."""

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = CONTAZUL_API_BASE_URL.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Faz uma requisição à API da Conta Azul.

        A API usa Bearer JWT no header Authorization e retorna erros HTTP padrão.
        """
        url = f"{self.base_url}{endpoint}"
        response = requests.request(method, url, headers=self.headers, params=params)

        if response.status_code == 401:
            raise ValueError("Token Conta Azul inválido ou expirado")

        response.raise_for_status()
        return response.json()

    # ========== VENDAS ==========

    def fetch_sales(
        self,
        data_inicial: Optional[str] = None,
        data_final: Optional[str] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Busca vendas da Conta Azul com paginação automática usando GET /v1/venda/busca.

        Args:
            data_inicial: Data de início da emissão da venda (YYYY-MM-DD) → data_inicio.
            data_final: Data final da emissão da venda (YYYY-MM-DD) → data_fim.
            page_size: tamanho_pagina (default 100).

        Returns:
            Lista com todos os itens de venda retornados em 'itens'.
        """
        all_sales: List[Dict[str, Any]] = []
        page = 1

        params: Dict[str, Any] = {
            "pagina": page,
            "tamanho_pagina": page_size,
        }
        if data_inicial:
            params["data_inicio"] = data_inicial
        if data_final:
            params["data_fim"] = data_final

        periodo = (
            f" ({data_inicial} a {data_final})"
            if (data_inicial and data_final)
            else ""
        )
        print(f"📊 API Conta Azul: buscando vendas{periodo}...")

        t_start = time.perf_counter()
        num_requests = 0

        while True:
            params["pagina"] = page
            data = self._make_request("GET", "/v1/venda/busca", params=params)
            num_requests += 1

            items = data.get("itens") or []
            if not items:
                break

            all_sales.extend(items)

            total_itens = data.get("total_itens")
            if total_itens is None:
                # Sem metadado de total, para por falta de página cheia
                if len(items) < page_size:
                    break
                page += 1
            else:
                # Se já trouxemos todos, para; caso contrário, próxima página
                if len(all_sales) >= int(total_itens):
                    break
                page += 1

        elapsed = time.perf_counter() - t_start
        if num_requests > 0:
            avg = elapsed / num_requests
            print(
                f"   → {len(all_sales)} vendas em {elapsed:.1f}s | "
                f"{num_requests} requisição(ões) | ~{avg:.2f}s/requisição"
            )
        else:
            print(f"   → {len(all_sales)} vendas obtidas")

        return all_sales

    def fetch_sale_items(self, sale_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Retorna os itens de uma venda específica via GET /v1/venda/{id_venda}/itens.

        Args:
            sale_id: ID da venda no Conta Azul.

        Returns:
            Lista de itens ou None se a venda não existir.
        """
        try:
            data = self._make_request("GET", f"/v1/venda/{sale_id}/itens")
            items = data.get("itens") or data.get("items") or data
            if isinstance(items, list):
                return items
            return None
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise

    def fetch_sale_items_timed(self, sale_id: str) -> Tuple[Optional[List[Dict[str, Any]]], float]:
        """
        Versão da busca de itens que também retorna o tempo de requisição.
        """
        t0 = time.perf_counter()
        try:
            items = self.fetch_sale_items(sale_id)
            return items, time.perf_counter() - t0
        except Exception:
            return None, time.perf_counter() - t0

    # ========== PRODUTOS / ESTOQUE ==========

    def fetch_products(
        self,
        data_alteracao_de: Optional[str] = None,
        data_alteracao_ate: Optional[str] = None,
        page_size: int = 100,
        status: Optional[str] = "ATIVO",
    ) -> List[Dict[str, Any]]:
        """
        Lista produtos com suporte a filtro por data de alteração (sync incremental).

        Usa GET /v1/produtos com paginação (pagina, tamanho_pagina).

        Args:
            data_alteracao_de: Data inicial de alteração (ISO 8601, GMT-3).
            data_alteracao_ate: Data final de alteração (ISO 8601, GMT-3).
            page_size: tamanho_pagina.
            status: status do produto (ex: 'ATIVO', 'INATIVO') ou None para todos.

        Returns:
            Lista com todos os produtos retornados em 'items'.
        """
        all_products: List[Dict[str, Any]] = []
        page = 1

        print("📦 API Conta Azul: buscando produtos...")

        t_start = time.perf_counter()
        num_requests = 0

        while True:
            params: Dict[str, Any] = {
                "pagina": page,
                "tamanho_pagina": page_size,
            }
            if data_alteracao_de:
                params["data_alteracao_de"] = data_alteracao_de
            if data_alteracao_ate:
                params["data_alteracao_ate"] = data_alteracao_ate
            if status:
                params["status"] = status

            data = self._make_request("GET", "/v1/produtos", params=params)
            num_requests += 1

            items = data.get("items") or data.get("itens") or []
            if not items:
                break

            all_products.extend(items)

            total_items = data.get("totalItems")
            if total_items is None:
                if len(items) < page_size:
                    break
                page += 1
            else:
                if len(all_products) >= int(total_items):
                    break
                page += 1

        elapsed = time.perf_counter() - t_start
        if num_requests > 0:
            avg = elapsed / num_requests
            print(
                f"   → {len(all_products)} produtos em {elapsed:.1f}s | "
                f"{num_requests} requisição(ões) | ~{avg:.2f}s/requisição"
            )
        else:
            print(f"   → {len(all_products)} produtos obtidos")

        return all_products


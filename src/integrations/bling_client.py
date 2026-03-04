"""
Cliente para interagir com a API v3 do Bling ERP.
Gerencia requisições paginadas e tratamento de erros.
Ref: https://developer.bling.com.br/bling-api
"""
from typing import List, Dict, Any, Optional, Tuple
import requests
import time

from src.config.settings import BLING_API_BASE_URL


class BlingClient:
    """Cliente para a API do Bling ERP (v3)."""

    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = BLING_API_BASE_URL.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        # Controle simples de rate limit: Bling permite 3 req/s.
        # Garantimos um intervalo mínimo de ~0,35s entre requisições.
        self._last_request_ts: Optional[float] = None

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Faz uma requisição à API do Bling v3 respeitando limites de 3 req/s.
        """
        now = time.perf_counter()
        if self._last_request_ts is not None:
            elapsed = now - self._last_request_ts
            min_interval = 0.35  # ~3 requisições/segundo com folga
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        self._last_request_ts = time.perf_counter()

        if response.status_code == 401:
            raise ValueError("Token Bling inválido ou expirado")

        response.raise_for_status()
        return response.json()

    def fetch_sales(
        self,
        data_inicial: Optional[str] = None,
        data_final: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Busca vendas (pedidos) do Bling com paginação automática.
        GET /Api/v3/pedidos.

        Args:
            data_inicial: Data inicial no formato YYYY-MM-DD
            data_final: Data final no formato YYYY-MM-DD
            limit: Quantidade de registros por página

        Returns:
            Lista com todas as vendas encontradas
        """
        all_sales: List[Dict[str, Any]] = []
        page = 1

        params: Dict[str, Any] = {"pagina": page, "limite": limit}
        if data_inicial:
            params["dataInicial"] = data_inicial
        if data_final:
            params["dataFinal"] = data_final

        periodo = (
            f" ({data_inicial} a {data_final})"
            if (data_inicial and data_final)
            else ""
        )
        print(f"📊 API Bling: buscando vendas{periodo}...")

        t_start = time.perf_counter()
        num_requests = 0

        while True:
            params["pagina"] = page
            try:
                # Endpoint oficial: /pedidos/vendas (lista pedidos de venda)
                # Base: https://api.bling.com.br/Api/v3/pedidos/vendas
                data = self._make_request("pedidos/vendas", params)
                num_requests += 1
                # Resposta: {"data": [ {pedido}, ... ]}
                items = data.get("data") or []
                if not items:
                    break
                all_sales.extend(items)
                if len(items) < limit:
                    break
                page += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"❌ API Bling vendas: {e}")
                break

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

    def fetch_products(
        self,
        situacao: str = "A",
        data_alteracao: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Busca produtos do Bling com paginação automática.
        GET /Api/v3/produtos.

        Args:
            situacao: A=Ativo, I=Inativo (default A)
            data_alteracao: Filtro por data de alteração (YYYY-MM-DD) para sync incremental
            limit: Quantidade de registros por página

        Returns:
            Lista com todos os produtos encontrados
        """
        all_products: List[Dict[str, Any]] = []
        page = 1

        params: Dict[str, Any] = {"pagina": page, "limite": limit}
        if situacao:
            params["situacao"] = situacao
        if data_alteracao:
            params["dataAlteracao"] = data_alteracao

        msg = "produtos ativos" if not data_alteracao else f"produtos alterados desde {data_alteracao}"
        print(f"📦 API Bling: buscando {msg}...")

        t_start = time.perf_counter()
        num_requests = 0

        while True:
            params["pagina"] = page
            try:
                data = self._make_request("produtos", params)
                num_requests += 1
                # Resposta: {"data": [ {produto}, ... ]}
                items = data.get("data") or []
                if not items:
                    break
                all_products.extend(items)
                if len(items) < limit:
                    break
                page += 1
                time.sleep(0.3)
            except Exception as e:
                print(f"❌ API Bling produtos: {e}")
                break

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

    def fetch_sale_details(self, sale_id: str) -> Optional[Dict[str, Any]]:
        """
        Busca detalhes completos de uma venda (pedido) específica.
        GET /Api/v3/pedidos/vendas/{idPedidoVenda}.

        Args:
            sale_id: ID do pedido no Bling (external_id)

        Returns:
            Payload completo da venda ou None se não encontrada
        """
        try:
            data = self._make_request(f"pedidos/vendas/{sale_id}")
            return data.get("data") or data
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"❌ Erro ao buscar detalhes da venda Bling {sale_id}: {e}")
            return None

    def fetch_sale_details_timed(
        self, sale_id: str
    ) -> Tuple[Optional[Dict[str, Any]], float]:
        """
        Como fetch_sale_details, mas retorna também o tempo da requisição em segundos.
        """
        t0 = time.perf_counter()
        try:
            data = self.fetch_sale_details(sale_id)
            return data, time.perf_counter() - t0
        except Exception:
            return None, time.perf_counter() - t0

    # ========== SITUAÇÕES ==========

    def fetch_situacoes(self, ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """
        Busca as situações (status) de pedidos por id, usando o endpoint:
        GET /Api/v3/situacoes/{idSituacao}

        Args:
            ids: Lista de IDs de situação (inteiros).

        Returns:
            Dict id_situacao -> payload completo da situação (objeto 'data').
        """
        result: Dict[int, Dict[str, Any]] = {}
        unique_ids = {int(i) for i in ids if i is not None}
        if not unique_ids:
            return result

        for sid in sorted(unique_ids):
            try:
                data = self._make_request(f"situacoes/{sid}")
                situacao = data.get("data") or data
                if isinstance(situacao, dict):
                    result[sid] = situacao
            except Exception as e:
                print(f"❌ API Bling situacao {sid}: {e}")
                continue

        return result

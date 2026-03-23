"""
Cliente para interagir com a API v3 do Tiny ERP.
Gerencia requisições paginadas e tratamento de erros.
"""
from typing import List, Dict, Any, Optional, Tuple
import os
import requests
import time
from datetime import datetime

from src.config.settings import TINY_API_BASE_URL


def _tiny_integration_debug() -> bool:
    """Logs extras para depuração (desligar em produção). Ver TINY_INTEGRATION_DEBUG no .env."""
    return os.getenv("TINY_INTEGRATION_DEBUG", "0").strip().lower() in ("1", "true", "yes")


def _mask_token(token: Optional[str]) -> str:
    if not token:
        return "(vazio)"
    t = str(token).strip()
    if len(t) <= 12:
        return f"(len={len(t)})"
    return f"{t[:8]}...{t[-4:]} (len={len(t)})"


class TinyClient:
    """Cliente para a API do Tiny ERP."""
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = TINY_API_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self._rate_limit_remaining = None
        self._rate_limit_reset_at = None
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Faz uma requisição à API do Tiny com controle de rate limit.
        Respeita X-RateLimit-Remaining e X-RateLimit-Reset dos headers.
        """
        # Verifica rate limit ANTES da requisição
        if self._rate_limit_remaining is not None and self._rate_limit_remaining <= 5:
            if self._rate_limit_reset_at:
                wait = max(0, self._rate_limit_reset_at - time.time())
                if wait > 0:
                    wait_seconds = min(wait, 60)  # máximo 60s de espera
                    time.sleep(wait_seconds)
                    # Reset após espera
                    self._rate_limit_remaining = None
                    self._rate_limit_reset_at = None
        
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)

        if _tiny_integration_debug():
            # Não logar o token completo — apenas preview mascarado.
            print(
                f"[TINY_DEBUG] GET {url} "
                f"status={response.status_code} "
                f"bearer={_mask_token(self.access_token)} "
                f"params={params!r}"
            )
        
        if response.status_code == 401:
            if _tiny_integration_debug():
                body_preview = (response.text or "")[:1200].replace("\n", " ")
                www = response.headers.get("WWW-Authenticate") or ""
                print(
                    f"[TINY_DEBUG] 401 Unauthorized — "
                    f"www_authenticate={www!r} "
                    f"body_preview={body_preview!r}"
                )
            raise ValueError("Token inválido ou expirado")
        
        # Atualiza rate limit a partir dos headers APÓS a requisição
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        if remaining is not None:
            try:
                self._rate_limit_remaining = int(remaining)
            except (ValueError, TypeError):
                pass
        if reset is not None:
            try:
                self._rate_limit_reset_at = time.time() + int(reset)
            except (ValueError, TypeError):
                pass
        
        try:
            response.raise_for_status()
        except requests.HTTPError:
            if _tiny_integration_debug():
                body_preview = (response.text or "")[:1200].replace("\n", " ")
                print(
                    f"[TINY_DEBUG] HTTP erro {response.status_code} em {url} — "
                    f"body_preview={body_preview!r}"
                )
            raise

        return response.json()
    
    def fetch_sales(
        self,
        data_inicial: Optional[str] = None,
        data_final: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Busca vendas (pedidos) do Tiny com paginação automática.
        
        Args:
            data_inicial: Data inicial no formato YYYY-MM-DD
            data_final: Data final no formato YYYY-MM-DD
            limit: Quantidade de registros por página (máx: 100)
        
        Returns:
            Lista com todas as vendas encontradas
        """
        all_sales = []
        offset = 0
        
        params = {"offset": offset, "limit": limit}
        if data_inicial:
            params["dataInicial"] = data_inicial
        if data_final:
            params["dataFinal"] = data_final
        
        periodo = f" ({data_inicial} a {data_final})" if (data_inicial and data_final) else ""
        print(f"API Tiny: buscando vendas{periodo}...")
        
        t_start = time.perf_counter()
        num_requests = 0
        
        while True:
            params["offset"] = offset
            try:
                t_req = time.perf_counter()
                data = self._make_request("pedidos", params)
                num_requests += 1
                items = data.get("itens", []) or data.get("items", []) or data.get("pedidos", [])
                if not items:
                    break
                all_sales.extend(items)
                if len(items) < limit:
                    break
                offset += limit
                time.sleep(0.5)
            except Exception as e:
                print(f"Erro API vendas: {e}")
                break
        
        elapsed = time.perf_counter() - t_start
        if num_requests > 0:
            avg = elapsed / num_requests
            print(f"   -> {len(all_sales)} vendas em {elapsed:.1f}s | {num_requests} requisição(ões) | ~{avg:.2f}s/requisição")
        else:
            print(f"   -> {len(all_sales)} vendas obtidas")
        return all_sales
    
    def fetch_products(
        self,
        situacao: str = "A",
        data_alteracao: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Busca produtos do Tiny com paginação automática.
        Ref: https://api-docs.erp.olist.com/api-reference/produtos/listar-produtos

        Args:
            situacao: A=Ativo, I=Inativo, E=Excluído (default A)
            data_alteracao: Filtro por data de alteração (formato "YYYY-MM-DD HH:MM:SS") para sync incremental
            limit: Quantidade de registros por página (máx: 100)

        Returns:
            Lista com todos os produtos encontrados (cada item tem "id", "sku", etc.)
        """
        all_products = []
        offset = 0

        msg = "produtos ativos" if not data_alteracao else f"produtos alterados desde {data_alteracao}"
        print(f"API Tiny: buscando {msg}...")

        t_start = time.perf_counter()
        num_requests = 0

        while True:
            params = {"offset": offset, "limit": limit, "situacao": situacao}
            if data_alteracao:
                params["dataAlteracao"] = data_alteracao
            try:
                data = self._make_request("produtos", params)
                num_requests += 1
                items = data.get("itens", []) or data.get("items", []) or data.get("produtos", [])
                if not items:
                    break
                all_products.extend(items)
                if len(items) < limit:
                    break
                offset += limit
                time.sleep(0.5)
            except Exception as e:
                print(f"Erro API produtos: {e}")
                break

        elapsed = time.perf_counter() - t_start
        if num_requests > 0:
            avg = elapsed / num_requests
            print(f"   -> {len(all_products)} produtos em {elapsed:.1f}s | {num_requests} requisição(ões) | ~{avg:.2f}s/requisição")
        else:
            print(f"   -> {len(all_products)} produtos obtidos")
        return all_products

    def fetch_product_stock(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Obtém o estoque de um produto (GET /estoque/{idProduto}).
        Ref: https://api-docs.erp.olist.com/api-reference/estoque/obter-o-estoque-de-um-produto

        Args:
            product_id: ID do produto no ERP

        Returns:
            Payload do estoque (id, nome, codigo, saldo, reservado, disponivel, depositos, etc.) ou None
        """
        try:
            data = self._make_request(f"estoque/{product_id}")
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Erro ao buscar estoque do produto {product_id}: {e}")
            return None

    def fetch_sale_details(self, sale_id: str) -> Optional[Dict[str, Any]]:
        """
        Busca detalhes completos de uma venda específica (GET /pedidos/{idPedido}).
        Retorna o payload completo com itens, cliente detalhado, etc.
        
        Args:
            sale_id: ID do pedido no Tiny (external_id)
        
        Returns:
            Payload completo da venda ou None se não encontrada
        """
        try:
            data = self._make_request(f"pedidos/{sale_id}")
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as e:
            print(f"Erro ao buscar detalhes da venda {sale_id}: {e}")
            return None

    def fetch_sale_details_timed(self, sale_id: str) -> Tuple[Optional[Dict[str, Any]], float]:
        """
        Como fetch_sale_details, mas retorna também o tempo da requisição em segundos.
        Returns:
            (payload ou None, tempo_em_segundos)
        """
        t0 = time.perf_counter()
        try:
            data = self._make_request(f"pedidos/{sale_id}")
            return data, time.perf_counter() - t0
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None, time.perf_counter() - t0
            raise
        except Exception as e:
            elapsed = time.perf_counter() - t0
            print(f"Erro ao buscar detalhes da venda {sale_id}: {e}")
            return None, elapsed
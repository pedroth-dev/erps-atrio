"""
Cliente Supabase para interação com o banco de dados.
Gerencia conexões, criptografia de credenciais e operações CRUD.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
import logging
from cryptography.fernet import Fernet
import base64
import hashlib

from src.config.settings import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, ENCRYPTION_KEY


# Schemas do banco (não usar public)
SCHEMA_AUTH = "auth_integrations"
SCHEMA_STAGING = "staging"
SCHEMA_CORE = "core"


class SupabaseClient:
    """Cliente para interagir com o Supabase."""
    
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        self._cipher = self._init_cipher()
    
    def _companies(self):
        """Tabela companies no schema auth_integrations."""
        return self.client.schema(SCHEMA_AUTH).table("companies")
    
    def _erp_connections(self):
        """Tabela erp_connections no schema auth_integrations."""
        return self.client.schema(SCHEMA_AUTH).table("erp_connections")

    def _sync_checkpoints(self):
        """Tabela sync_checkpoints no schema auth_integrations (sincronização incremental)."""
        return self.client.schema(SCHEMA_AUTH).table("sync_checkpoints")
    
    def _tiny_sales(self):
        """Tabela tiny_sales no schema staging."""
        return self.client.schema(SCHEMA_STAGING).table("tiny_sales")
    
    def _tiny_stock(self):
        """Tabela tiny_stock no schema staging."""
        return self.client.schema(SCHEMA_STAGING).table("tiny_stock")

    def _tiny_sale_items(self):
        """Tabela tiny_sale_items no schema staging."""
        return self.client.schema(SCHEMA_STAGING).table("tiny_sale_items")

    def _contaazul_sales(self):
        """Tabela contaazul_sales no schema staging."""
        return self.client.schema(SCHEMA_STAGING).table("contaazul_sales")

    def _contaazul_stock(self):
        """Tabela contaazul_stock no schema staging."""
        return self.client.schema(SCHEMA_STAGING).table("contaazul_stock")

    def _contaazul_sale_items(self):
        """Tabela contaazul_sale_items no schema staging."""
        return self.client.schema(SCHEMA_STAGING).table("contaazul_sale_items")

    def _customers(self):
        """Tabela customers no schema core."""
        return self.client.schema(SCHEMA_CORE).table("customers")

    def _core_sales(self):
        """Tabela sales no schema core."""
        return self.client.schema(SCHEMA_CORE).table("sales")

    def _core_sale_items(self):
        """Tabela sale_items no schema core."""
        return self.client.schema(SCHEMA_CORE).table("sale_items")

    def _core_stock(self):
        """Tabela stock no schema core."""
        return self.client.schema(SCHEMA_CORE).table("stock")

    def _init_cipher(self) -> Fernet:
        """Inicializa o cipher Fernet para criptografia AES."""
        # Gera uma chave Fernet a partir da ENCRYPTION_KEY
        key = hashlib.sha256(ENCRYPTION_KEY.encode()).digest()
        key_b64 = base64.urlsafe_b64encode(key)
        return Fernet(key_b64)
    
    def encrypt_credential(self, value: str) -> str:
        """Criptografa uma credencial usando AES."""
        if not value:
            return value
        return self._cipher.encrypt(value.encode()).decode()
    
    def decrypt_credential(self, encrypted_value: str) -> str:
        """Descriptografa uma credencial."""
        if not encrypted_value:
            return encrypted_value
        return self._cipher.decrypt(encrypted_value.encode()).decode()
    
    # ========== COMPANIES ==========
    
    def get_company_by_document(self, document: str) -> Optional[Dict[str, Any]]:
        """Busca uma empresa pelo CNPJ (schema auth_integrations)."""
        result = self._companies().select("*").eq("document", document).execute()
        return result.data[0] if result.data else None
    
    def create_company(self, name: str, document: str) -> Dict[str, Any]:
        """Cria uma nova empresa (schema auth_integrations)."""
        data = {
            "name": name,
            "document": document,
            "is_active": True
        }
        result = self._companies().insert(data).execute()
        return result.data[0]
    
    def get_all_companies(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Lista empresas (id, name, document) para seleção. Ordenado por nome."""
        query = self._companies().select("id, name, document").order("name")
        if active_only:
            query = query.eq("is_active", True)
        result = query.execute()
        return result.data or []
    
    # ========== ERP CONNECTIONS ==========
    
    def get_erp_connections_by_company(self, company_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """Lista conexões ERP de uma empresa (id, erp_type, is_active). Ordenado por erp_type."""
        query = (
            self._erp_connections()
            .select("id, erp_type, is_active")
            .eq("company_id", company_id)
            .order("erp_type")
        )
        if active_only:
            query = query.eq("is_active", True)
        result = query.execute()
        return result.data or []

    def get_erp_connection(self, company_id: str, erp_type: str) -> Optional[Dict[str, Any]]:
        """Busca a conexão ERP de uma empresa (schema auth_integrations)."""
        result = (
            self._erp_connections()
            .select("*")
            .eq("company_id", company_id)
            .eq("erp_type", erp_type)
            .execute()
        )
        return result.data[0] if result.data else None
    
    def get_erp_connection_by_id(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Busca uma conexão ERP pelo ID (schema auth_integrations)."""
        result = (
            self._erp_connections()
            .select("*")
            .eq("id", connection_id)
            .execute()
        )
        return result.data[0] if result.data else None
    
    def create_erp_connection(
        self,
        company_id: str,
        erp_type: str,
        erp_login: str,
        erp_password: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str
    ) -> Dict[str, Any]:
        """
        Cria uma nova conexão ERP com credenciais criptografadas.
        
        Args:
            company_id: ID da empresa
            erp_type: Tipo do ERP ('tiny', 'bling', etc.)
            erp_login: Login do ERP (será criptografado)
            erp_password: Senha do ERP (será criptografado)
            client_id: Client ID da aplicação OAuth (texto puro)
            client_secret: Client Secret da aplicação OAuth (será criptografado)
            redirect_uri: URI de redirecionamento OAuth (texto puro)
        """
        data = {
            "company_id": company_id,
            "erp_type": erp_type,
            "erp_login": self.encrypt_credential(erp_login),
            "erp_password": self.encrypt_credential(erp_password),
            "client_id": client_id,  # Não criptografado (não é sensível)
            "client_secret": self.encrypt_credential(client_secret),  # Criptografado
            "redirect_uri": redirect_uri,  # Não criptografado (não é sensível)
            "is_active": True
        }
        result = self._erp_connections().insert(data).execute()
        return result.data[0]
    
    def update_erp_tokens(
        self,
        connection_id: str,
        access_token: str,
        refresh_token: str,
        expires_in: int = 14400,  # 4 horas padrão Tiny
        refresh_expires_in: int = 86400  # 24 horas padrão Tiny
    ) -> Dict[str, Any]:
        """
        Atualiza os tokens OAuth de uma conexão.
        Tokens são criptografados antes de serem salvos no banco.
        """
        now = datetime.utcnow()
        data = {
            "access_token": self.encrypt_credential(access_token),  # Criptografado
            "refresh_token": self.encrypt_credential(refresh_token),  # Criptografado
            "access_token_expires_at": (now + timedelta(seconds=expires_in)).isoformat(),
            "refresh_token_expires_at": (now + timedelta(seconds=refresh_expires_in)).isoformat(),
            "token_type": "oauth2",
            "last_token_refresh_at": now.isoformat(),
            "is_active": True
        }
        result = (
            self._erp_connections()
            .update(data)
            .eq("id", connection_id)
            .execute()
        )
        return result.data[0]
    
    def get_erp_credentials(self, connection_id: str) -> Dict[str, str]:
        """
        Obtém e descriptografa as credenciais de acesso ao ERP de uma conexão.
        Retorna login e senha descriptografados.
        """
        result = (
            self._erp_connections()
            .select("erp_login, erp_password")
            .eq("id", connection_id)
            .execute()
        )
        
        if not result.data:
            raise ValueError(f"Conexão {connection_id} não encontrada")
        
        conn = result.data[0]
        return {
            "login": self.decrypt_credential(conn["erp_login"]),
            "password": self.decrypt_credential(conn["erp_password"])
        }
    
    def get_oauth_credentials(self, connection_id: str) -> Dict[str, str]:
        """
        Obtém e descriptografa as credenciais OAuth de uma conexão.
        Retorna client_id, client_secret e redirect_uri.
        """
        result = (
            self._erp_connections()
            .select("client_id, client_secret, redirect_uri")
            .eq("id", connection_id)
            .execute()
        )
        
        if not result.data:
            raise ValueError(f"Conexão {connection_id} não encontrada")
        
        conn = result.data[0]
        return {
            "client_id": conn["client_id"],  # Não precisa descriptografar
            "client_secret": self.decrypt_credential(conn["client_secret"]),
            "redirect_uri": conn["redirect_uri"]  # Não precisa descriptografar
        }
    
    def get_access_token(self, connection_id: str) -> str:
        """
        Obtém e descriptografa o access_token de uma conexão.
        """
        result = (
            self._erp_connections()
            .select("access_token")
            .eq("id", connection_id)
            .execute()
        )
        
        if not result.data or not result.data[0].get("access_token"):
            raise ValueError(f"Access token não encontrado para conexão {connection_id}")
        
        return self.decrypt_credential(result.data[0]["access_token"])
    
    def get_refresh_token(self, connection_id: str) -> str:
        """
        Obtém e descriptografa o refresh_token de uma conexão.
        """
        result = (
            self._erp_connections()
            .select("refresh_token")
            .eq("id", connection_id)
            .execute()
        )
        
        if not result.data or not result.data[0].get("refresh_token"):
            raise ValueError(f"Refresh token não encontrado para conexão {connection_id}")
        
        return self.decrypt_credential(result.data[0]["refresh_token"])
    
    def mark_connection_inactive(self, connection_id: str, error_message: Optional[str] = None):
        """Marca uma conexão como inativa."""
        data = {"is_active": False}
        if error_message:
            # Se houver campo para erro, adicionar aqui
            pass
        self._erp_connections().update(data).eq("id", connection_id).execute()
    
    def update_last_sync(self, connection_id: str):
        """Atualiza o timestamp da última sincronização."""
        self._erp_connections().update({
            "last_sync_at": datetime.utcnow().isoformat()
        }).eq("id", connection_id).execute()

    # ========== SYNC CHECKPOINTS (sincronização incremental) ==========

    def get_checkpoint(
        self, company_id: str, erp_type: str, entity: str
    ) -> Optional[Dict[str, Any]]:
        """Busca checkpoint de sincronização (company_id, erp_type, entity)."""
        result = (
            self._sync_checkpoints()
            .select("*")
            .eq("company_id", company_id)
            .eq("erp_type", erp_type)
            .eq("entity", entity)
            .execute()
        )
        return result.data[0] if result.data else None

    def upsert_checkpoint(
        self,
        company_id: str,
        erp_type: str,
        entity: str,
        set_full_refresh: bool = False,
    ) -> None:
        """
        Atualiza checkpoint após sincronização bem-sucedida.
        set_full_refresh=True: preenche last_full_refresh_at (sync dos últimos 30 dias).
        """
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "company_id": company_id,
            "erp_type": erp_type,
            "entity": entity,
            "last_sync_at": now,
            "updated_at": now,
        }
        if set_full_refresh:
            row["last_full_refresh_at"] = now
        self._sync_checkpoints().upsert(row, on_conflict="company_id,erp_type,entity").execute()
    
    # ========== STAGING ==========
    
    @staticmethod
    def _sale_external_id_from_raw(raw_data: Dict[str, Any]) -> str:
        """Extrai o ID da venda no ERP a partir do raw_data (mesma lógica do normalizer)."""
        ext_id = raw_data.get("id") or raw_data.get("idPedido") or raw_data.get("numero")
        return str(ext_id) if ext_id is not None else ""

    def insert_staging_sales(self, company_id: str, raw_data: Dict[str, Any], fetched_at: datetime):
        """Insere ou atualiza um registro de venda no staging (upsert por company_id + sale_external_id)."""
        sale_external_id = self._sale_external_id_from_raw(raw_data)
        data = {
            "company_id": company_id,
            "sale_external_id": sale_external_id,
            "raw_data": raw_data,
            "fetched_at": fetched_at.isoformat(),
        }
        self._tiny_sales().upsert(data, on_conflict="company_id,sale_external_id").execute()

    def insert_staging_sales_batch(
        self,
        company_id: str,
        raw_data_list: List[Dict[str, Any]],
        fetched_at: datetime,
        erp_type: str = "tiny",
    ) -> int:
        """Insere ou atualiza vendas no staging em lote (upsert por company_id + sale_external_id). Evita duplicatas."""
        if not raw_data_list:
            return 0
        table = self._contaazul_sales() if erp_type == "contaazul" else self._tiny_sales()
        fetched_at_str = fetched_at.isoformat()
        rows = []
        for item in raw_data_list:
            sale_external_id = self._sale_external_id_from_raw(item)
            rows.append({
                "company_id": company_id,
                "sale_external_id": sale_external_id,
                "raw_data": item,
                "fetched_at": fetched_at_str,
            })
        table.upsert(rows, on_conflict="company_id,sale_external_id").execute()
        logging.getLogger(__name__).debug("staging: %d vendas (upsert) [%s]", len(rows), erp_type)
        return len(rows)
    
    @staticmethod
    def _stock_product_external_id_from_raw(raw_data: Dict[str, Any]) -> str:
        """Extrai o ID do produto do payload (Tiny: GET /estoque/{id}; Conta Azul: item de GET /v1/produtos)."""
        pid = raw_data.get("id")
        return str(pid) if pid is not None else ""

    def insert_staging_stock(self, company_id: str, raw_data: Dict[str, Any], fetched_at: datetime, erp_type: str = "tiny"):
        """Insere ou atualiza um registro de estoque no staging (upsert por company_id + product_external_id)."""
        table = self._contaazul_stock() if erp_type == "contaazul" else self._tiny_stock()
        product_external_id = self._stock_product_external_id_from_raw(raw_data)
        data = {
            "company_id": company_id,
            "product_external_id": product_external_id,
            "raw_data": raw_data,
            "fetched_at": fetched_at.isoformat(),
        }
        table.upsert(data, on_conflict="company_id,product_external_id").execute()

    def insert_staging_stock_batch(
        self,
        company_id: str,
        raw_data_list: List[Dict[str, Any]],
        fetched_at: datetime,
        erp_type: str = "tiny",
    ) -> int:
        """Insere ou atualiza registros de estoque no staging (upsert por company_id + product_external_id). Evita duplicatas."""
        if not raw_data_list:
            return 0
        table = self._contaazul_stock() if erp_type == "contaazul" else self._tiny_stock()
        fetched_at_str = fetched_at.isoformat()
        rows = []
        for item in raw_data_list:
            product_external_id = self._stock_product_external_id_from_raw(item)
            rows.append({
                "company_id": company_id,
                "product_external_id": product_external_id,
                "raw_data": item,
                "fetched_at": fetched_at_str,
            })
        table.upsert(rows, on_conflict="company_id,product_external_id").execute()
        logging.getLogger(__name__).debug("staging: %d estoque (upsert) [%s]", len(rows), erp_type)
        return len(rows)
    
    def get_pending_staging_sales(
        self, company_id: str, limit: int = 100, erp_type: str = "tiny"
    ) -> List[Dict[str, Any]]:
        """Busca vendas pendentes de processamento no staging."""
        table = self._contaazul_sales() if erp_type == "contaazul" else self._tiny_sales()
        result = (
            table.select("*")
            .eq("company_id", company_id)
            .is_("processed_at", "null")
            .limit(limit)
            .execute()
        )
        return result.data

    def get_staging_sale_ids_by_external_ids(
        self, company_id: str, external_ids: List[str], erp_type: str = "tiny"
    ) -> Dict[str, str]:
        """
        Busca os IDs (UUID) dos registros em staging (tiny_sales ou contaazul_sales) a partir dos external_ids.
        Retorna um dicionário: external_id -> staging_id (UUID).
        """
        if not external_ids:
            return {}
        table = self._contaazul_sales() if erp_type == "contaazul" else self._tiny_sales()
        external_ids_set = set(str(eid) for eid in external_ids)
        result_map = {}
        result = table.select("id, raw_data").eq("company_id", company_id).execute()
        for row in result.data:
            raw_data = row.get("raw_data") or {}
            ext_id = str(
                raw_data.get("id")
                or raw_data.get("idPedido")
                or raw_data.get("numero")
                or ""
            )
            if ext_id in external_ids_set:
                result_map[ext_id] = row["id"]
        return result_map
    
    def get_pending_staging_stock(
        self, company_id: str, limit: int = 100, erp_type: str = "tiny"
    ) -> List[Dict[str, Any]]:
        """Busca estoque pendente de processamento no staging."""
        table = self._contaazul_stock() if erp_type == "contaazul" else self._tiny_stock()
        result = (
            table.select("*")
            .eq("company_id", company_id)
            .is_("processed_at", "null")
            .limit(limit)
            .execute()
        )
        return result.data

    def mark_staging_processed(self, table_name: str, record_id: str, error: Optional[str] = None):
        """Marca um registro do staging como processado (schema staging)."""
        data = {"processed_at": datetime.utcnow().isoformat()}
        if error:
            data["process_error"] = error
        table_name = table_name.replace("staging.", "")
        if table_name == "tiny_sales":
            self._tiny_sales().update(data).eq("id", record_id).execute()
        elif table_name == "tiny_stock":
            self._tiny_stock().update(data).eq("id", record_id).execute()
        elif table_name == "tiny_sale_items":
            self._tiny_sale_items().update(data).eq("id", record_id).execute()
        elif table_name == "contaazul_sales":
            self._contaazul_sales().update(data).eq("id", record_id).execute()
        elif table_name == "contaazul_stock":
            self._contaazul_stock().update(data).eq("id", record_id).execute()
        elif table_name == "contaazul_sale_items":
            self._contaazul_sale_items().update(data).eq("id", record_id).execute()

    @staticmethod
    def _product_external_id_from_item(item: Dict[str, Any]) -> str:
        """
        Extrai o ID do produto no ERP a partir do item (raw_data do item).
        Tiny:   item['produto']['id'].
        Conta Azul (GET /v1/venda/{id}/itens): usa id_item (id do produto) ou, em último caso, id do item.
        """
        produto = item.get("produto") or item.get("product") or {}
        if isinstance(produto, dict):
            pid = produto.get("id")
        else:
            pid = item.get("product_id") or item.get("id_item") or item.get("id")
        return str(pid) if pid is not None else ""

    def insert_staging_sale_items_batch(
        self,
        company_id: str,
        sale_external_id: str,
        sale_staging_id: Optional[str],
        items: List[Dict[str, Any]],
        fetched_at: datetime,
        erp_type: str = "tiny",
    ) -> int:
        """
        Insere ou atualiza itens de uma venda no staging (upsert por company_id + sale_external_id + product_external_id).
        Evita duplicatas quando a mesma venda é coletada mais de uma vez.
        Se o mesmo produto aparecer em mais de uma linha na mesma venda, as quantidades são agrupadas
        (o PostgreSQL não permite dois rows com a mesma chave no mesmo comando ON CONFLICT).
        """
        if not items:
            return 0
        fetched_at_str = fetched_at.isoformat()
        sale_ext = str(sale_external_id)

        # Agrupar por product_external_id: mesma venda pode ter o mesmo produto em várias linhas
        by_product: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            pid = self._product_external_id_from_item(item)
            by_product.setdefault(pid, []).append(item)

        rows = []
        for product_external_id, group in by_product.items():
            if len(group) == 1:
                raw_data = group[0]
            else:
                # Mesclar várias linhas do mesmo produto: soma quantidade, mantém preço unitário do primeiro
                raw_data = dict(group[0])
                qty_total = 0
                for it in group:
                    q = it.get("quantidade")
                    try:
                        qty_total += float(str(q).replace(",", ".")) if q is not None else 0
                    except (TypeError, ValueError):
                        pass
                raw_data["quantidade"] = qty_total
                # valorUnitario permanece do primeiro item (normalizer calcula total = qty * unit_price)
            rows.append({
                "company_id": company_id,
                "sale_external_id": sale_ext,
                "product_external_id": product_external_id,
                "sale_staging_id": sale_staging_id,
                "raw_data": raw_data,
                "fetched_at": fetched_at_str,
            })
        table = self._contaazul_sale_items() if erp_type == "contaazul" else self._tiny_sale_items()
        table.upsert(rows, on_conflict="company_id,sale_external_id,product_external_id").execute()
        logging.getLogger(__name__).debug("staging: %d itens de venda %s (upsert) [%s]", len(rows), sale_external_id, erp_type)
        return len(rows)

    def get_pending_staging_sale_items(
        self,
        company_id: str,
        limit: int = 100,
        sale_external_ids: Optional[List[str]] = None,
        erp_type: str = "tiny",
    ) -> List[Dict[str, Any]]:
        """
        Busca itens de vendas pendentes de processamento no staging.
        Se sale_external_ids for informado, retorna apenas itens dessas vendas (sync incremental).
        """
        if sale_external_ids is not None and len(sale_external_ids) == 0:
            return []
        table = self._contaazul_sale_items() if erp_type == "contaazul" else self._tiny_sale_items()
        q = (
            table.select("*")
            .eq("company_id", company_id)
            .is_("processed_at", "null")
            .limit(limit)
        )
        if sale_external_ids:
            q = q.in_("sale_external_id", sale_external_ids)
        result = q.execute()
        return result.data

    def mark_staging_sale_items_processed_batch(
        self, record_ids: List[str], error: Optional[str] = None, erp_type: str = "tiny"
    ) -> None:
        """Marca vários itens de staging como processados de uma vez."""
        if not record_ids:
            return
        table = self._contaazul_sale_items() if erp_type == "contaazul" else self._tiny_sale_items()
        data = {"processed_at": datetime.utcnow().isoformat()}
        if error:
            data["process_error"] = error
        table.update(data).in_("id", record_ids).execute()

    def get_sale_id_by_external_id(
        self, company_id: str, erp_type: str, sale_external_id: str
    ) -> Optional[str]:
        """Retorna o id (UUID) da venda em core.sales ou None."""
        result = (
            self._core_sales()
            .select("id")
            .eq("company_id", company_id)
            .eq("erp_type", erp_type)
            .eq("external_id", str(sale_external_id))
            .execute()
        )
        return result.data[0]["id"] if result.data else None

    def get_sales_from_core(
        self, company_id: str, erp_type: str, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Busca vendas normalizadas do core para coletar itens.
        Suporta paginação via offset.
        """
        result = (
            self._core_sales()
            .select("id, external_id, issued_at, status")
            .eq("company_id", company_id)
            .eq("erp_type", erp_type)
            .order("issued_at", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        return result.data

    def get_sales_from_core_by_external_ids(
        self, company_id: str, erp_type: str, external_ids: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Busca vendas do core cujo external_id está na lista.
        Usado para coletar itens apenas das vendas recém-normalizadas (sync incremental).
        """
        if not external_ids:
            return []
        result = (
            self._core_sales()
            .select("id, external_id, issued_at, status")
            .eq("company_id", company_id)
            .eq("erp_type", erp_type)
            .in_("external_id", external_ids)
            .execute()
        )
        return result.data or []

    # ========== CORE (normalizer: staging → core) ==========

    def upsert_core_customer(
        self, company_id: str, erp_type: str, data: Dict[str, Any]
    ) -> str:
        """Faz upsert em core.customers. Retorna o id do cliente (criado ou existente)."""
        row = {
            "company_id": company_id,
            "erp_type": erp_type,
            "external_id": str(data["external_id"]),
            "name": data.get("name"),
            "person_type": data.get("person_type"),
            "document": data.get("document"),
            "phone": data.get("phone"),
            "mobile": data.get("mobile"),
            "email": data.get("email"),
            "neighborhood": data.get("neighborhood"),
            "city": data.get("city"),
            "zip_code": data.get("zip_code"),
            "state": data.get("state"),
            "country": data.get("country"),
            "raw_data": data.get("raw_data"),
        }
        result = (
            self._customers()
            .upsert(row, on_conflict="company_id,erp_type,external_id")
            .execute()
        )
        return result.data[0]["id"]

    def upsert_core_sale(
        self,
        company_id: str,
        erp_type: str,
        data: Dict[str, Any],
        customer_id: Optional[str] = None,
    ) -> None:
        """Faz upsert em core.sales. customer_id pode ser nulo (venda sem cliente)."""
        row = {
            "company_id": company_id,
            "erp_type": erp_type,
            "external_id": str(data["external_id"]),
            "order_number": data.get("order_number"),
            "origin_order_id": data.get("origin_order_id"),
            "origin_channel_id": data.get("origin_channel_id"),
            "origin_channel": data.get("origin_channel"),
            "customer_id": customer_id,
            "total_amount": data.get("total_amount"),
            "status": data.get("status"),
            "issued_at": data.get("issued_at"),
            "raw_data": data.get("raw_data"),
        }
        self._core_sales().upsert(row, on_conflict="company_id,erp_type,external_id").execute()

    def get_customer_id_by_external_id(
        self, company_id: str, erp_type: str, external_id: str
    ) -> Optional[str]:
        """Retorna o id (UUID) do cliente em core.customers ou None."""
        result = (
            self._customers()
            .select("id")
            .eq("company_id", company_id)
            .eq("erp_type", erp_type)
            .eq("external_id", str(external_id))
            .execute()
        )
        return result.data[0]["id"] if result.data else None

    def upsert_core_customers_batch(
        self, company_id: str, erp_type: str, payloads: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Upsert em lote em core.customers. Retorna mapa external_id -> id."""
        if not payloads:
            return {}
        rows = [
            {
                "company_id": company_id,
                "erp_type": erp_type,
                "external_id": str(p["external_id"]),
                "name": p.get("name"),
                "person_type": p.get("person_type"),
                "document": p.get("document"),
                "phone": p.get("phone"),
                "mobile": p.get("mobile"),
                "email": p.get("email"),
                "neighborhood": p.get("neighborhood"),
                "city": p.get("city"),
                "zip_code": p.get("zip_code"),
                "state": p.get("state"),
                "country": p.get("country"),
                "raw_data": p.get("raw_data"),
            }
            for p in payloads
        ]
        result = (
            self._customers()
            .upsert(rows, on_conflict="company_id,erp_type,external_id")
            .execute()
        )
        return {str(r["external_id"]): r["id"] for r in (result.data or [])}

    def upsert_core_sales_batch(
        self,
        company_id: str,
        erp_type: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        """Upsert em lote em core.sales. Cada item de rows deve ter os campos da venda + customer_id."""
        if not rows:
            return
        data = [
            {
                "company_id": company_id,
                "erp_type": erp_type,
                "external_id": str(r["external_id"]),
                "order_number": r.get("order_number"),
                "origin_order_id": r.get("origin_order_id"),
                "origin_channel_id": r.get("origin_channel_id"),
                "origin_channel": r.get("origin_channel"),
                "customer_id": r.get("customer_id"),
                "total_amount": r.get("total_amount"),
                "status": r.get("status"),
                "issued_at": r.get("issued_at"),
                "raw_data": r.get("raw_data"),
            }
            for r in rows
        ]
        self._core_sales().upsert(data, on_conflict="company_id,erp_type,external_id").execute()

    def mark_staging_sales_processed_batch(
        self, record_ids: List[str], error: Optional[str] = None, erp_type: str = "tiny"
    ) -> None:
        """Marca vários registros de staging (tiny_sales ou contaazul_sales) como processados de uma vez."""
        if not record_ids:
            return
        table = self._contaazul_sales() if erp_type == "contaazul" else self._tiny_sales()
        data = {"processed_at": datetime.utcnow().isoformat()}
        if error:
            data["process_error"] = error
        table.update(data).in_("id", record_ids).execute()

    def mark_staging_stock_processed_batch(
        self, record_ids: List[str], error: Optional[str] = None, erp_type: str = "tiny"
    ) -> None:
        """Marca vários registros de staging (tiny_stock ou contaazul_stock) como processados de uma vez."""
        if not record_ids:
            return
        table = self._contaazul_stock() if erp_type == "contaazul" else self._tiny_stock()
        data = {"processed_at": datetime.utcnow().isoformat()}
        if error:
            data["process_error"] = error
        table.update(data).in_("id", record_ids).execute()

    def upsert_core_sale_items_batch(
        self, company_id: str, erp_type: str, rows: List[Dict[str, Any]]
    ) -> None:
        """Upsert em lote em core.sale_items."""
        if not rows:
            return
        data = [
            {
                "company_id": company_id,
                "erp_type": erp_type,
                "sale_id": r.get("sale_id"),
                "sale_external_id": str(r["sale_external_id"]),
                "product_external_id": str(r["product_external_id"]),
                "product_sku": r.get("product_sku"),
                "product_description": r.get("product_description"),
                "product_type": r.get("product_type"),
                "quantity": r.get("quantity"),
                "unit_price": r.get("unit_price"),
                "total_price": r.get("total_price"),
                "sale_date": r.get("sale_date"),
                "sale_status": r.get("sale_status"),
                "raw_data": r.get("raw_data"),
            }
            for r in rows
        ]
        self._core_sale_items().upsert(
            data, on_conflict="company_id,erp_type,sale_external_id,product_external_id"
        ).execute()

    def upsert_core_stock_batch(
        self, company_id: str, erp_type: str, rows: List[Dict[str, Any]], synced_at: Optional[datetime] = None
    ) -> None:
        """Upsert em lote em core.stock. Cada row: external_id, sku, product_name, quantity, raw_data."""
        if not rows:
            return
        synced_at_str = (synced_at or datetime.utcnow()).isoformat()
        data = [
            {
                "company_id": company_id,
                "erp_type": erp_type,
                "external_id": str(r["external_id"]),
                "sku": r.get("sku"),
                "product_name": r.get("product_name"),
                "quantity": r.get("quantity", 0),
                "raw_data": r.get("raw_data"),
                "synced_at": synced_at_str,
            }
            for r in rows
        ]
        self._core_stock().upsert(data, on_conflict="company_id,erp_type,external_id").execute()
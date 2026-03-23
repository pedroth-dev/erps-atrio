"""
Cliente Postgres para interação com o banco de dados.

Este arquivo substitui a camada antes feita via Supabase SDK.
Mantemos as mesmas responsabilidades (cripto de credenciais e operações CRUD)
para que o restante da aplicação não precise de alterações de lógica.
"""

import base64
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

import psycopg2
from cryptography.fernet import Fernet
from psycopg2.extras import Json, RealDictCursor

from src.config.settings import ENCRYPTION_KEY, POSTGRES_URL

logger = logging.getLogger(__name__)


SCHEMA_AUTH = "auth_integrations"
SCHEMA_STAGING = "staging"
SCHEMA_CORE = "core"
DEFAULT_SEGMENT_ID = "ebae1de9-a28a-4ffe-bff3-0ad1d9405372"


def _jsonify_params(v: Any) -> Any:
    """
    Helper para garantir que raw_data(JSONB) vai corretamente ao Postgres.
    psycopg2 aceita Json(obj) para serializar com segurança.
    """
    if isinstance(v, (dict, list)):
        return Json(v)
    return v


class PostgresClient:
    """Cliente Postgres para operações do domínio do sistema."""

    def __init__(self) -> None:
        if not POSTGRES_URL:
            raise ValueError("POSTGRES_URL não configurada (verifique o .env)")

        self._conn = psycopg2.connect(POSTGRES_URL, connect_timeout=10)
        self._conn.autocommit = True
        self._cipher = self._init_cipher()

    def _init_cipher(self) -> Fernet:
        # Mantém a mesma lógica do projeto original (derivação a partir de ENCRYPTION_KEY).
        key = hashlib.sha256(ENCRYPTION_KEY.encode()).digest()
        key_b64 = base64.urlsafe_b64encode(key)
        return Fernet(key_b64)

    def encrypt_credential(self, value: str) -> str:
        """Criptografa uma credencial usando Fernet (criptografia simétrica)."""
        if not value:
            return value
        return self._cipher.encrypt(value.encode()).decode()

    def decrypt_credential(self, encrypted_value: str) -> str:
        """Descriptografa uma credencial."""
        if not encrypted_value:
            return encrypted_value
        return self._cipher.decrypt(encrypted_value.encode()).decode()

    # ========= Serialização (compatibilidade com o código legado) =========

    @staticmethod
    def _serialize_value(v: Any) -> Any:
        if isinstance(v, datetime):
            dt = v
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, UUID):
            return str(v)
        return v

    @classmethod
    def _serialize_row(cls, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        return {k: cls._serialize_value(v) for k, v in row.items()}

    def _fetchone(self, query: str, params: tuple[Any, ...]) -> Optional[Dict[str, Any]]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params if params else None)
            row = cur.fetchone()
            return self._serialize_row(dict(row) if row else None)

    def _fetchall(self, query: str, params: tuple[Any, ...]) -> List[Dict[str, Any]]:
        with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params if params else None)
            rows = cur.fetchall() or []
            return [self._serialize_row(dict(r)) for r in rows]  # type: ignore[arg-type]

    def _execute(self, query: str, params: tuple[Any, ...]) -> None:
        with self._conn.cursor() as cur:
            cur.execute(query, params if params else None)

    @staticmethod
    def _to_iso_or_none(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return str(value)

    def _connection_row_to_legacy(self, row: Dict[str, Any]) -> Dict[str, Any]:
        creds = row.get("erp_credenciais") or {}
        if not isinstance(creds, dict):
            creds = {}
        return {
            "id": row.get("integracao_erp_id"),
            "company_id": row.get("empresa_id"),
            "erp_type": row.get("erp_nome"),
            "is_active": row.get("erp_status_ativo", True),
            "erp_login": creds.get("erp_login"),
            "erp_password": creds.get("erp_password"),
            "client_id": creds.get("client_id"),
            "client_secret": creds.get("client_secret"),
            "redirect_uri": creds.get("redirect_uri"),
            "access_token": creds.get("access_token"),
            "refresh_token": creds.get("refresh_token"),
            "token_type": creds.get("token_type"),
            "access_token_expires_at": self._to_iso_or_none(creds.get("access_token_expires_at")),
            "refresh_token_expires_at": self._to_iso_or_none(creds.get("refresh_token_expires_at")),
            "last_token_refresh_at": self._to_iso_or_none(creds.get("last_token_refresh_at")),
            "last_sync_at": self._to_iso_or_none(row.get("ultima_req_incremental")),
            "last_full_refresh_at": self._to_iso_or_none(row.get("ultima_req_completa")),
            "created_at": self._to_iso_or_none(row.get("criado_em")),
            "updated_at": self._to_iso_or_none(row.get("atualizado_em")),
        }

    def _upsert_many(
        self,
        table_fqn: str,
        insert_cols: List[str],
        rows: List[Dict[str, Any]],
        conflict_cols: List[str],
        update_cols: List[str],
    ) -> None:
        """
        Upsert em lote (executa uma vez por linha via executemany).

        Observação: para manter a mudança simples no início, usamos executemany
        com um INSERT ON CONFLICT por linha.
        """
        if not rows:
            return

        cols_sql = ", ".join(insert_cols)
        placeholders = ", ".join(["%s"] * len(insert_cols))
        conflict_sql = ", ".join(conflict_cols)
        update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

        if not update_sql:
            # Caso raro; mas mantém consistência.
            query = f"INSERT INTO {table_fqn} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT ({conflict_sql}) DO NOTHING"
        else:
            query = (
                f"INSERT INTO {table_fqn} ({cols_sql}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}"
            )

        params = []
        for r in rows:
            params.append(tuple(_jsonify_params(r.get(c)) for c in insert_cols))

        with self._conn.cursor() as cur:
            cur.executemany(query, params)

    # ========= COMPANIES =========

    def get_company_by_document(self, document: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT empresa_id, empresa_nome, empresa_cnpj, criado_em, atualizado_em
            FROM infraestrutura.empresas
            WHERE empresa_cnpj = %s
            LIMIT 1
        """
        row = self._fetchone(query, (document,))
        if not row:
            return None
        return {
            "id": row["empresa_id"],
            "name": row["empresa_nome"],
            "document": row["empresa_cnpj"],
            "is_active": True,
            "created_at": row.get("criado_em"),
            "updated_at": row.get("atualizado_em"),
        }

    def create_company(self, name: str, document: str) -> Dict[str, Any]:
        query = """
            INSERT INTO infraestrutura.empresas (empresa_nome, empresa_cnpj)
            VALUES (%s, %s)
            RETURNING empresa_id, empresa_nome, empresa_cnpj, criado_em, atualizado_em
        """
        row = self._fetchone(query, (name, document))
        if not row:
            raise RuntimeError("Falha ao criar company no Postgres")
        return {
            "id": row["empresa_id"],
            "name": row["empresa_nome"],
            "document": row["empresa_cnpj"],
            "is_active": True,
            "created_at": row.get("criado_em"),
            "updated_at": row.get("atualizado_em"),
        }

    def get_all_companies(self, active_only: bool = True) -> List[Dict[str, Any]]:
        query = """
            SELECT empresa_id, empresa_nome, empresa_cnpj
            FROM infraestrutura.empresas
        """
        query += " ORDER BY empresa_nome"
        rows = self._fetchall(query, ())
        return [
            {
                "id": r["empresa_id"],
                "name": r["empresa_nome"],
                "document": r["empresa_cnpj"],
            }
            for r in rows
        ]

    # ========= ERP CONNECTIONS =========

    def get_erp_connections_by_company(
        self, company_id: str, active_only: bool = True
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT integracao_erp_id, erp_nome, erp_status_ativo
            FROM infraestrutura.integracoes_erps
            WHERE empresa_id = %s
        """
        params: List[Any] = [company_id]
        if active_only:
            query += " AND erp_status_ativo = TRUE"
        query += " ORDER BY erp_nome"
        rows = self._fetchall(query, tuple(params))
        return [
            {
                "id": r["integracao_erp_id"],
                "erp_type": r["erp_nome"],
                "is_active": r["erp_status_ativo"],
            }
            for r in rows
        ]

    def get_erp_connection(self, company_id: str, erp_type: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT *
            FROM infraestrutura.integracoes_erps
            WHERE empresa_id = %s AND erp_nome = %s
            LIMIT 1
        """
        row = self._fetchone(query, (company_id, erp_type))
        return self._connection_row_to_legacy(row) if row else None

    def get_erp_connection_by_id(self, connection_id: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT *
            FROM infraestrutura.integracoes_erps
            WHERE integracao_erp_id = %s
            LIMIT 1
        """
        row = self._fetchone(query, (connection_id,))
        return self._connection_row_to_legacy(row) if row else None

    def create_erp_connection(
        self,
        company_id: str,
        erp_type: str,
        erp_login: str,
        erp_password: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        credentials = {
            "erp_login": self.encrypt_credential(erp_login),
            "erp_password": self.encrypt_credential(erp_password),
            "client_id": client_id,
            "client_secret": self.encrypt_credential(client_secret),
            "redirect_uri": redirect_uri,
            "token_type": "oauth2",
            "created_at": now.isoformat(),
        }
        query = """
            INSERT INTO infraestrutura.integracoes_erps (
                empresa_id,
                segmento_id,
                erp_nome,
                erp_credenciais,
                erp_status_ativo
            )
            VALUES (%s,%s,%s,%s,%s)
            RETURNING *
        """
        row = self._fetchone(
            query,
            (
                company_id,
                DEFAULT_SEGMENT_ID,
                erp_type,
                Json(credentials),
                True,
            ),
        )
        if not row:
            raise RuntimeError("Falha ao criar erp_connection no Postgres")
        return self._connection_row_to_legacy(row)

    def update_erp_tokens(
        self,
        connection_id: str,
        access_token: str,
        refresh_token: str,
        expires_in: int = 14400,  # 4 horas padrão Tiny
        refresh_expires_in: int = 86400,  # 24 horas padrão Tiny
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        current = self._fetchone(
            """
            SELECT erp_credenciais
            FROM infraestrutura.integracoes_erps
            WHERE integracao_erp_id = %s
            LIMIT 1
            """,
            (connection_id,),
        )
        if not current:
            raise ValueError(f"Conexão {connection_id} não encontrada para update_erp_tokens")
        credentials = current.get("erp_credenciais") or {}
        if not isinstance(credentials, dict):
            credentials = {}
        credentials.update(
            {
                "access_token": self.encrypt_credential(access_token),
                "refresh_token": self.encrypt_credential(refresh_token),
                "access_token_expires_at": (now + timedelta(seconds=expires_in)).isoformat(),
                "refresh_token_expires_at": (now + timedelta(seconds=refresh_expires_in)).isoformat(),
                "token_type": "oauth2",
                "last_token_refresh_at": now.isoformat(),
            }
        )
        row = self._fetchone(
            """
            UPDATE infraestrutura.integracoes_erps
            SET erp_credenciais = %s,
                erp_status_ativo = TRUE
            WHERE integracao_erp_id = %s
            RETURNING *
            """,
            (Json(credentials), connection_id),
        )
        if not row:
            raise ValueError(f"Conexão {connection_id} não encontrada para update_erp_tokens")
        return self._connection_row_to_legacy(row)

    def get_erp_credentials(self, connection_id: str) -> Dict[str, str]:
        row = self.get_erp_connection_by_id(connection_id)
        if not row:
            raise ValueError(f"Conexão {connection_id} não encontrada")
        if not row.get("erp_login") or not row.get("erp_password"):
            raise ValueError(f"Credenciais ERP ausentes no JSON de {connection_id}")
        return {
            "login": self.decrypt_credential(row.get("erp_login")),
            "password": self.decrypt_credential(row.get("erp_password")),
        }

    def get_oauth_credentials(self, connection_id: str) -> Dict[str, str]:
        row = self.get_erp_connection_by_id(connection_id)
        if not row:
            raise ValueError(f"Conexão {connection_id} não encontrada")
        if not row.get("client_id") or not row.get("client_secret") or not row.get("redirect_uri"):
            raise ValueError(f"Credenciais OAuth ausentes no JSON de {connection_id}")
        return {
            "client_id": row["client_id"],
            "client_secret": self.decrypt_credential(row.get("client_secret")),
            "redirect_uri": row["redirect_uri"],
        }

    def get_access_token(self, connection_id: str) -> str:
        row = self.get_erp_connection_by_id(connection_id)
        if not row or not row.get("access_token"):
            raise ValueError(f"Access token não encontrado para conexão {connection_id}")
        return self.decrypt_credential(row["access_token"])

    def get_refresh_token(self, connection_id: str) -> str:
        row = self.get_erp_connection_by_id(connection_id)
        if not row or not row.get("refresh_token"):
            raise ValueError(f"Refresh token não encontrado para conexão {connection_id}")
        return self.decrypt_credential(row["refresh_token"])

    def mark_connection_inactive(self, connection_id: str, error_message: Optional[str] = None) -> None:
        query = """
            UPDATE infraestrutura.integracoes_erps
            SET erp_status_ativo = FALSE
            WHERE integracao_erp_id = %s
        """
        self._execute(query, (connection_id,))

    def update_last_sync(self, connection_id: str) -> None:
        query = """
            UPDATE infraestrutura.integracoes_erps
            SET ultima_req_incremental = %s
            WHERE integracao_erp_id = %s
        """
        self._execute(query, (datetime.now(timezone.utc), connection_id))

    # ========= SYNC CHECKPOINTS =========

    def get_checkpoint(self, company_id: str, erp_type: str, entity: str) -> Optional[Dict[str, Any]]:
        query = """
            SELECT ultima_req_incremental, ultima_req_completa
            FROM infraestrutura.integracoes_erps
            WHERE empresa_id = %s AND erp_nome = %s
            LIMIT 1
        """
        row = self._fetchone(query, (company_id, erp_type))
        if not row:
            return None
        return {
            "company_id": company_id,
            "erp_type": erp_type,
            "entity": entity,
            "last_sync_at": self._to_iso_or_none(row.get("ultima_req_incremental")),
            "last_full_refresh_at": self._to_iso_or_none(row.get("ultima_req_completa")),
        }

    def upsert_checkpoint(
        self,
        company_id: str,
        erp_type: str,
        entity: str,
        set_full_refresh: bool = False,
    ) -> None:
        now = datetime.now(timezone.utc)
        if set_full_refresh:
            query = """
                UPDATE infraestrutura.integracoes_erps
                SET ultima_req_incremental = %s,
                    ultima_req_completa = %s
                WHERE empresa_id = %s AND erp_nome = %s
            """
            self._execute(query, (now, now, company_id, erp_type))
        else:
            query = """
                UPDATE infraestrutura.integracoes_erps
                SET ultima_req_incremental = %s
                WHERE empresa_id = %s AND erp_nome = %s
            """
            self._execute(query, (now, company_id, erp_type))

    # ========= STAGING =========

    @staticmethod
    def _sale_external_id_from_raw(raw_data: Dict[str, Any]) -> str:
        ext_id = (
            raw_data.get("id")
            or raw_data.get("idPedido")
            or raw_data.get("idPedidoVenda")
            or raw_data.get("numero")
        )
        return str(ext_id) if ext_id is not None else ""

    # ========= STAGING (Fase 1) — PEDIDOS =========

    @staticmethod
    def _staging_pedidos_table(erp_type: str) -> str:
        if erp_type == "tiny":
            return "stg_erps.stg_tiny_pedidos"
        if erp_type == "contaazul":
            return "stg_erps.stg_contaazul_pedidos"
        if erp_type == "bling":
            return "stg_erps.stg_bling_pedidos"
        raise ValueError(f"erp_type inválido para staging de pedidos: {erp_type}")

    def upsert_staging_pedidos_batch(
        self,
        company_id: str,
        erp_type: str,
        vendas_raw_list: List[Dict[str, Any]],
    ) -> int:
        """
        Upsert em lote no staging de pedidos (stg_erps.stg_*_pedidos).

        Insere/substitui o payload "geral" da venda em `raw_json`
        mantendo `stg_status='pendente'`.
        """
        if not vendas_raw_list:
            return 0

        table_fqn = self._staging_pedidos_table(erp_type)

        rows: List[Dict[str, Any]] = []
        for sale in vendas_raw_list:
            numero_pedido = self._sale_external_id_from_raw(sale)
            if not numero_pedido:
                continue
            rows.append(
                {
                    "empresa_id": company_id,
                    "numero_pedido": numero_pedido,
                    "raw_json": sale,
                    "stg_status": "pendente",
                    "processado_em": None,
                }
            )

        if not rows:
            return 0

        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=[
                "empresa_id",
                "numero_pedido",
                "raw_json",
                "stg_status",
                "processado_em",
            ],
            rows=rows,
            conflict_cols=["empresa_id", "numero_pedido"],
            update_cols=["raw_json", "stg_status", "processado_em"],
        )

        return len(rows)

    def get_staging_pedidos_by_numero_pedido(
        self,
        company_id: str,
        erp_type: str,
        numero_pedidos: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Busca registros do staging de pedidos pelo `numero_pedido`.
        """
        if not numero_pedidos:
            return []

        table_fqn = self._staging_pedidos_table(erp_type)
        unique_ids = list({str(n) for n in numero_pedidos if n})
        placeholders = ", ".join(["%s"] * len(unique_ids))

        query = f"""
            SELECT stg_id, empresa_id, numero_pedido, raw_json, stg_status, processado_em
            FROM {table_fqn}
            WHERE empresa_id = %s
              AND numero_pedido IN ({placeholders})
        """
        params: tuple[Any, ...] = tuple([company_id] + unique_ids)
        return self._fetchall(query, params)

    def upsert_staging_pedidos_details_batch(
        self,
        company_id: str,
        erp_type: str,
        detalhes_raw_list: List[Dict[str, Any]],
    ) -> int:
        """
        Substitui `raw_json` do staging pelos payloads "detalhados".

        Mantém `stg_status='pendente'` para o ETL futuro operar com o registro atualizado.
        """
        if not detalhes_raw_list:
            return 0

        table_fqn = self._staging_pedidos_table(erp_type)

        rows: List[Dict[str, Any]] = []
        for sale_details in detalhes_raw_list:
            numero_pedido = self._sale_external_id_from_raw(sale_details)
            if not numero_pedido:
                continue
            rows.append(
                {
                    "empresa_id": company_id,
                    "numero_pedido": numero_pedido,
                    "raw_json": sale_details,
                    "stg_status": "pendente",
                    "processado_em": None,
                }
            )

        if not rows:
            return 0

        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=[
                "empresa_id",
                "numero_pedido",
                "raw_json",
                "stg_status",
                "processado_em",
            ],
            rows=rows,
            conflict_cols=["empresa_id", "numero_pedido"],
            update_cols=["raw_json", "stg_status", "processado_em"],
        )
        return len(rows)

    def mark_staging_pedido_erro(
        self,
        company_id: str,
        erp_type: str,
        numero_pedido: str,
        error_msg: Optional[str] = None,
    ) -> None:
        """
        Marca um pedido no staging como erro (`stg_status='erro'`).
        Observação: o schema atual não possui coluna para mensagem de erro.
        """
        table_fqn = self._staging_pedidos_table(erp_type)
        now = datetime.now(timezone.utc)
        query = f"""
            UPDATE {table_fqn}
            SET stg_status = 'erro',
                processado_em = %s
            WHERE empresa_id = %s
              AND numero_pedido = %s
        """
        self._execute(query, (now, company_id, numero_pedido))

    # ========= STAGING (Fase 1) — ITENS PEDIDOS =========

    @staticmethod
    def _staging_itens_pedidos_table(erp_type: str) -> str:
        if erp_type == "tiny":
            return "stg_erps.stg_tiny_itens_pedidos"
        if erp_type == "contaazul":
            return "stg_erps.stg_contaazul_itens_pedidos"
        if erp_type == "bling":
            return "stg_erps.stg_bling_itens_pedidos"
        raise ValueError(f"erp_type inválido para staging de itens pedidos: {erp_type}")

    def get_staging_pedidos_detailed_pending_without_itens(
        self,
        company_id: str,
        erp_type: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Seleciona pedidos detalhados pendentes (stg_status='pendente') que ainda
        não possuem linha correspondente em stg_*_itens_pedidos.
        """
        pedidos_table = self._staging_pedidos_table(erp_type)
        itens_table = self._staging_itens_pedidos_table(erp_type)

        query = f"""
            SELECT
                p.stg_id,
                p.empresa_id,
                p.numero_pedido,
                p.raw_json,
                p.stg_status
            FROM {pedidos_table} p
            LEFT JOIN {itens_table} i
              ON i.empresa_id = p.empresa_id
             AND i.numero_pedido = p.numero_pedido
            WHERE p.empresa_id = %s
              AND p.stg_status = 'pendente'
              AND i.numero_pedido IS NULL
            ORDER BY p.criado_em DESC
            LIMIT %s
        """
        return self._fetchall(query, (company_id, limit))

    def upsert_staging_itens_pedidos_batch(
        self,
        company_id: str,
        erp_type: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        """
        Upsert em lote no staging de itens pedidos (stg_erps.stg_*_itens_pedidos).

        Espera rows com chaves:
          - numero_pedido: str
          - raw_json: dict (chunk completo do detalhado)
        """
        if not rows:
            return 0

        table_fqn = self._staging_itens_pedidos_table(erp_type)

        prepared: List[Dict[str, Any]] = []
        for r in rows:
            numero_pedido = r.get("numero_pedido")
            raw_json = r.get("raw_json")
            if not numero_pedido or raw_json is None:
                continue
            prepared.append(
                {
                    "empresa_id": company_id,
                    "numero_pedido": str(numero_pedido),
                    "raw_json": raw_json,
                    "stg_status": "pendente",
                    "processado_em": None,
                }
            )

        if not prepared:
            return 0

        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=["empresa_id", "numero_pedido", "raw_json", "stg_status", "processado_em"],
            rows=prepared,
            conflict_cols=["empresa_id", "numero_pedido"],
            update_cols=["raw_json", "stg_status", "processado_em"],
        )
        return len(prepared)

    def mark_staging_itens_pedido_erro(
        self,
        company_id: str,
        erp_type: str,
        numero_pedido: str,
    ) -> None:
        """
        Marca um pedido no staging de itens como erro (`stg_status='erro'`).
        """
        table_fqn = self._staging_itens_pedidos_table(erp_type)
        now = datetime.now(timezone.utc)
        query = f"""
            UPDATE {table_fqn}
            SET stg_status = 'erro',
                processado_em = %s
            WHERE empresa_id = %s
              AND numero_pedido = %s
        """
        self._execute(query, (now, company_id, numero_pedido))

    def _staging_sales_table(self, erp_type: str) -> str:
        if erp_type == "bling":
            return "staging.bling_sales"
        if erp_type == "contaazul":
            return "staging.contaazul_sales"
        return "staging.tiny_sales"

    def _staging_stock_table(self, erp_type: str) -> str:
        if erp_type == "bling":
            return "stg_erps.stg_bling_estoque"
        if erp_type == "contaazul":
            return "stg_erps.stg_contaazul_estoque"
        return "stg_erps.stg_tiny_estoque"

    def _staging_sale_items_table(self, erp_type: str) -> str:
        if erp_type == "bling":
            return "staging.bling_sale_items"
        if erp_type == "contaazul":
            return "staging.contaazul_sale_items"
        return "staging.tiny_sale_items"

    def insert_staging_sales(
        self,
        company_id: str,
        raw_data: Dict[str, Any],
        fetched_at: datetime,
    ) -> None:
        """
        Inserção legada apenas para Tiny (mantida por compatibilidade).
        Para múltiplas vendas, use `insert_staging_sales_batch`.
        """
        sale_external_id = self._sale_external_id_from_raw(raw_data)
        table_fqn = "staging.tiny_sales"
        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=["company_id", "sale_external_id", "raw_data", "fetched_at"],
            rows=[
                {
                    "company_id": company_id,
                    "sale_external_id": sale_external_id,
                    "raw_data": raw_data,
                    "fetched_at": fetched_at,
                }
            ],
            conflict_cols=["company_id", "sale_external_id"],
            update_cols=["raw_data", "fetched_at"],
        )

    def insert_staging_sales_batch(
        self,
        company_id: str,
        raw_data_list: List[Dict[str, Any]],
        fetched_at: datetime,
        erp_type: str = "tiny",
    ) -> int:
        if not raw_data_list:
            return 0

        table_fqn = self._staging_sales_table(erp_type)
        rows: List[Dict[str, Any]] = []
        for item in raw_data_list:
            sale_external_id = self._sale_external_id_from_raw(item)
            rows.append(
                {
                    "company_id": company_id,
                    "sale_external_id": sale_external_id,
                    "raw_data": item,
                    "fetched_at": fetched_at,
                }
            )

        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=["company_id", "sale_external_id", "raw_data", "fetched_at"],
            rows=rows,
            conflict_cols=["company_id", "sale_external_id"],
            update_cols=["raw_data", "fetched_at"],
        )
        logger.debug("staging: %d vendas (upsert) [%s]", len(rows), erp_type)
        return len(rows)

    @staticmethod
    def _stock_product_external_id_from_raw(raw_data: Dict[str, Any]) -> str:
        pid = (
            raw_data.get("id")
            or raw_data.get("idProduto")
            or raw_data.get("produto_id")
            or raw_data.get("codigo")
            or raw_data.get("sku")
        )
        return str(pid) if pid is not None else ""

    def insert_staging_stock(
        self,
        company_id: str,
        raw_data: Dict[str, Any],
        fetched_at: datetime,
        erp_type: str = "tiny",
    ) -> None:
        if erp_type not in ("tiny", "contaazul", "bling"):
            raise ValueError(f"erp_type inválido: {erp_type}")

        table_fqn = self._staging_stock_table(erp_type)

        numero_produto = self._stock_product_external_id_from_raw(raw_data)

        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=["empresa_id", "numero_produto", "raw_json", "stg_status", "processado_em"],
            rows=[
                {
                    "empresa_id": company_id,
                    "numero_produto": numero_produto,
                    "raw_json": raw_data,
                    "stg_status": "pendente",
                    "processado_em": None,
                }
            ],
            conflict_cols=["empresa_id", "numero_produto"],
            update_cols=["raw_json", "stg_status", "processado_em"],
        )

    def insert_staging_stock_batch(
        self,
        company_id: str,
        raw_data_list: List[Dict[str, Any]],
        fetched_at: datetime,
        erp_type: str = "tiny",
    ) -> int:
        if not raw_data_list:
            return 0

        table_fqn = self._staging_stock_table(erp_type)
        rows: List[Dict[str, Any]] = []
        for item in raw_data_list:
            numero_produto = self._stock_product_external_id_from_raw(item)
            if not numero_produto:
                continue
            rows.append(
                {
                    "empresa_id": company_id,
                    "numero_produto": numero_produto,
                    "raw_json": item,
                    "stg_status": "pendente",
                    "processado_em": None,
                }
            )
        if not rows:
            return 0

        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=["empresa_id", "numero_produto", "raw_json", "stg_status", "processado_em"],
            rows=rows,
            conflict_cols=["empresa_id", "numero_produto"],
            update_cols=["raw_json", "stg_status", "processado_em"],
        )
        logger.debug("staging: %d estoque (upsert) [%s]", len(rows), erp_type)
        return len(rows)

    def get_pending_staging_sales(
        self,
        company_id: str,
        limit: int = 100,
        erp_type: str = "tiny",
    ) -> List[Dict[str, Any]]:
        table_fqn = self._staging_sales_table(erp_type)
        query = f"""
            SELECT *
            FROM {table_fqn}
            WHERE company_id = %s
              AND processed_at IS NULL
            LIMIT %s
        """
        return self._fetchall(query, (company_id, limit))

    def get_staging_sale_ids_by_external_ids(
        self,
        company_id: str,
        external_ids: List[str],
        erp_type: str = "tiny",
    ) -> Dict[str, str]:
        """
        Busca os IDs (UUID) dos registros em staging a partir dos external_ids.
        Mantém a mesma estratégia do código legado: busca dados e faz o filtro em Python.
        """
        if not external_ids:
            return {}
        table_fqn = self._staging_sales_table(erp_type)
        external_ids_set = set(str(eid) for eid in external_ids)

        query = f"""
            SELECT id, raw_data
            FROM {table_fqn}
            WHERE company_id = %s
        """
        rows = self._fetchall(query, (company_id,))

        result_map: Dict[str, str] = {}
        for row in rows:
            raw_data = row.get("raw_data") or {}
            ext_id = str(
                raw_data.get("id")
                or raw_data.get("idPedido")
                or raw_data.get("idPedidoVenda")
                or raw_data.get("numero")
                or ""
            )
            if ext_id in external_ids_set and row.get("id"):
                result_map[ext_id] = str(row["id"])
        return result_map

    def get_pending_staging_stock(
        self,
        company_id: str,
        limit: int = 100,
        erp_type: str = "tiny",
    ) -> List[Dict[str, Any]]:
        table_fqn = self._staging_stock_table(erp_type)
        query = f"""
            SELECT *
            FROM {table_fqn}
            WHERE empresa_id = %s
              AND stg_status = 'pendente'
            LIMIT %s
        """
        return self._fetchall(query, (company_id, limit))

    def mark_staging_processed(
        self,
        table_name: str,
        record_id: str,
        error: Optional[str] = None,
    ) -> None:
        """Marca um registro do staging como processado (schema staging)."""
        table_name = table_name.replace("staging.", "")

        mapping = {
            "tiny_sales": "staging.tiny_sales",
            "tiny_stock": "staging.tiny_stock",
            "tiny_sale_items": "staging.tiny_sale_items",
            "contaazul_sales": "staging.contaazul_sales",
            "contaazul_stock": "staging.contaazul_stock",
            "contaazul_sale_items": "staging.contaazul_sale_items",
            "bling_sales": "staging.bling_sales",
            "bling_stock": "staging.bling_stock",
            "bling_sale_items": "staging.bling_sale_items",
        }
        if table_name not in mapping:
            raise ValueError(f"table_name de staging inválido: {table_name}")

        table_fqn = mapping[table_name]
        now = datetime.now(timezone.utc)

        if error:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s, process_error = %s
                WHERE id = %s
            """
            self._execute(query, (now, error, record_id))
        else:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s
                WHERE id = %s
            """
            self._execute(query, (now, record_id))

    @staticmethod
    def _product_external_id_from_item(item: Dict[str, Any]) -> str:
        produto = item.get("produto") or item.get("product") or {}
        if isinstance(produto, dict):
            pid = produto.get("id")
        else:
            pid = item.get("product_id") or item.get("id_item") or item.get("id")
        return str(pid) if pid is not None else ""

    def _build_sale_item_rows(
        self,
        company_id: str,
        sale_external_id: str,
        sale_staging_id: Optional[str],
        items: List[Dict[str, Any]],
        fetched_at: datetime,
    ) -> List[Dict[str, Any]]:
        if not items:
            return []

        sale_ext = str(sale_external_id)

        by_product: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            pid = self._product_external_id_from_item(item)
            by_product.setdefault(pid, []).append(item)

        rows: List[Dict[str, Any]] = []
        for product_external_id, group in by_product.items():
            if len(group) == 1:
                raw_data = group[0]
            else:
                raw_data = dict(group[0])
                qty_total = 0.0
                for it in group:
                    q = it.get("quantidade")
                    try:
                        qty_total += float(str(q).replace(",", ".")) if q is not None else 0.0
                    except (TypeError, ValueError):
                        pass
                raw_data["quantidade"] = qty_total

            rows.append(
                {
                    "company_id": company_id,
                    "sale_external_id": sale_ext,
                    "product_external_id": product_external_id,
                    "sale_staging_id": sale_staging_id,
                    "raw_data": raw_data,
                    "fetched_at": fetched_at,
                }
            )
        return rows

    def insert_staging_sale_items_batch(
        self,
        company_id: str,
        sale_external_id: str,
        sale_staging_id: Optional[str],
        items: List[Dict[str, Any]],
        fetched_at: datetime,
        erp_type: str = "tiny",
    ) -> int:
        if not items:
            return 0

        rows = self._build_sale_item_rows(
            company_id=company_id,
            sale_external_id=sale_external_id,
            sale_staging_id=sale_staging_id,
            items=items,
            fetched_at=fetched_at,
        )
        if not rows:
            return 0

        table_fqn = self._staging_sale_items_table(erp_type)
        self._upsert_many(
            table_fqn=table_fqn,
            insert_cols=[
                "company_id",
                "sale_external_id",
                "product_external_id",
                "sale_staging_id",
                "raw_data",
                "fetched_at",
            ],
            rows=rows,
            conflict_cols=["company_id", "sale_external_id", "product_external_id"],
            update_cols=["sale_staging_id", "raw_data", "fetched_at"],
        )
        logger.debug(
            "staging: %d itens de venda %s (upsert) [%s]",
            len(rows),
            sale_external_id,
            erp_type,
        )
        return len(rows)

    def insert_staging_sale_items_multi(
        self,
        company_id: str,
        payloads: List[Dict[str, Any]],
        fetched_at: datetime,
        erp_type: str = "tiny",
        batch_size: int = 1000,
    ) -> int:
        if not payloads:
            return 0

        if batch_size <= 0:
            raise ValueError("batch_size deve ser > 0")

        table_fqn = self._staging_sale_items_table(erp_type)

        rows_buffer: List[Dict[str, Any]] = []
        total_rows = 0

        for payload in payloads:
            sale_external_id = payload.get("sale_external_id")
            sale_staging_id = payload.get("sale_staging_id")
            items = payload.get("items") or []
            if not sale_external_id or not items:
                continue

            rows = self._build_sale_item_rows(
                company_id=company_id,
                sale_external_id=str(sale_external_id),
                sale_staging_id=sale_staging_id,
                items=items,
                fetched_at=fetched_at,
            )
            if not rows:
                continue

            rows_buffer.extend(rows)
            if len(rows_buffer) >= batch_size:
                self._upsert_many(
                    table_fqn=table_fqn,
                    insert_cols=[
                        "company_id",
                        "sale_external_id",
                        "product_external_id",
                        "sale_staging_id",
                        "raw_data",
                        "fetched_at",
                    ],
                    rows=rows_buffer,
                    conflict_cols=["company_id", "sale_external_id", "product_external_id"],
                    update_cols=["sale_staging_id", "raw_data", "fetched_at"],
                )
                total_rows += len(rows_buffer)
                rows_buffer = []

        if rows_buffer:
            self._upsert_many(
                table_fqn=table_fqn,
                insert_cols=[
                    "company_id",
                    "sale_external_id",
                    "product_external_id",
                    "sale_staging_id",
                    "raw_data",
                    "fetched_at",
                ],
                rows=rows_buffer,
                conflict_cols=["company_id", "sale_external_id", "product_external_id"],
                update_cols=["sale_staging_id", "raw_data", "fetched_at"],
            )
            total_rows += len(rows_buffer)

        logger.debug("staging: %d itens de venda (multi upsert) [%s]", total_rows, erp_type)
        return total_rows

    def get_pending_staging_sale_items(
        self,
        company_id: str,
        limit: int = 100,
        sale_external_ids: Optional[List[str]] = None,
        erp_type: str = "tiny",
    ) -> List[Dict[str, Any]]:
        table_fqn = self._staging_sale_items_table(erp_type)
        query = f"""
            SELECT *
            FROM {table_fqn}
            WHERE company_id = %s
              AND processed_at IS NULL
        """
        params: List[Any] = [company_id]
        if sale_external_ids:
            # Mantém comportamento: se lista for vazia, retorna [].
            query += " AND sale_external_id = ANY(%s)"
            params.append(sale_external_ids)
        query += " LIMIT %s"
        params.append(limit)
        return self._fetchall(query, tuple(params))

    def mark_staging_sale_items_processed_batch(
        self,
        record_ids: List[str],
        error: Optional[str] = None,
        erp_type: str = "tiny",
    ) -> None:
        if not record_ids:
            return

        table_fqn = self._staging_sale_items_table(erp_type)
        now = datetime.now(timezone.utc)

        if error:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s,
                    process_error = %s
                WHERE id = ANY(%s)
            """
            self._execute(query, (now, error, record_ids))
        else:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s
                WHERE id = ANY(%s)
            """
            self._execute(query, (now, record_ids))

    # ========= CORE (normalizer: staging → core) =========

    def get_sale_id_by_external_id(
        self,
        company_id: str,
        erp_type: str,
        sale_external_id: str,
    ) -> Optional[str]:
        query = """
            SELECT id
            FROM core.sales
            WHERE company_id = %s AND erp_type = %s AND external_id = %s
            LIMIT 1
        """
        row = self._fetchone(query, (company_id, erp_type, str(sale_external_id)))
        if not row:
            return None
        return row.get("id")

    def get_sales_from_core(
        self,
        company_id: str,
        erp_type: str,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query = """
            SELECT id, external_id, issued_at, status
            FROM core.sales
            WHERE company_id = %s AND erp_type = %s
            ORDER BY issued_at DESC
            LIMIT %s OFFSET %s
        """
        return self._fetchall(query, (company_id, erp_type, limit, offset))

    def get_sales_from_core_by_external_ids(
        self,
        company_id: str,
        erp_type: str,
        external_ids: List[str],
    ) -> List[Dict[str, Any]]:
        if not external_ids:
            return []

        query = """
            SELECT id, external_id, issued_at, status
            FROM core.sales
            WHERE company_id = %s
              AND erp_type = %s
              AND external_id = ANY(%s)
        """
        return self._fetchall(query, (company_id, erp_type, external_ids))

    def upsert_core_customer(
        self,
        company_id: str,
        erp_type: str,
        data: Dict[str, Any],
    ) -> str:
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

        insert_cols = [
            "company_id",
            "erp_type",
            "external_id",
            "name",
            "person_type",
            "document",
            "phone",
            "mobile",
            "email",
            "neighborhood",
            "city",
            "zip_code",
            "state",
            "country",
            "raw_data",
        ]
        conflict_cols = ["company_id", "erp_type", "external_id"]
        update_cols = [c for c in insert_cols if c not in conflict_cols]

        cols_sql = ", ".join(insert_cols)
        placeholders = ", ".join(["%s"] * len(insert_cols))
        conflict_sql = ", ".join(conflict_cols)
        update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

        query = f"""
            INSERT INTO core.customers ({cols_sql})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_sql})
            DO UPDATE SET {update_sql}
            RETURNING id
        """

        params = tuple(_jsonify_params(row.get(c)) for c in insert_cols)
        found = self._fetchone(query, params)
        if not found or not found.get("id"):
            raise RuntimeError("Falha ao upsert_core_customer (sem id retornado)")
        return str(found["id"])

    def upsert_core_sale(
        self,
        company_id: str,
        erp_type: str,
        data: Dict[str, Any],
        customer_id: Optional[str] = None,
    ) -> None:
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

        insert_cols = [
            "company_id",
            "erp_type",
            "external_id",
            "order_number",
            "origin_order_id",
            "origin_channel_id",
            "origin_channel",
            "customer_id",
            "total_amount",
            "status",
            "issued_at",
            "raw_data",
        ]
        conflict_cols = ["company_id", "erp_type", "external_id"]
        update_cols = [c for c in insert_cols if c not in conflict_cols]

        self._upsert_many(
            table_fqn="core.sales",
            insert_cols=insert_cols,
            rows=[row],
            conflict_cols=conflict_cols,
            update_cols=update_cols,
        )

    def get_customer_id_by_external_id(
        self,
        company_id: str,
        erp_type: str,
        external_id: str,
    ) -> Optional[str]:
        query = """
            SELECT id
            FROM core.customers
            WHERE company_id = %s AND erp_type = %s AND external_id = %s
            LIMIT 1
        """
        row = self._fetchone(query, (company_id, erp_type, str(external_id)))
        if not row:
            return None
        return row.get("id")

    def upsert_core_customers_batch(
        self,
        company_id: str,
        erp_type: str,
        payloads: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        if not payloads:
            return {}
        mapping: Dict[str, str] = {}
        for p in payloads:
            ext = str(p["external_id"])
            mapping[ext] = self.upsert_core_customer(company_id, erp_type, p)
        return mapping

    def upsert_core_sales_batch(
        self,
        company_id: str,
        erp_type: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        if not rows:
            return

        insert_cols = [
            "company_id",
            "erp_type",
            "external_id",
            "order_number",
            "origin_order_id",
            "origin_channel_id",
            "origin_channel",
            "customer_id",
            "total_amount",
            "status",
            "issued_at",
            "raw_data",
        ]
        conflict_cols = ["company_id", "erp_type", "external_id"]
        update_cols = [c for c in insert_cols if c not in conflict_cols]

        data_rows: List[Dict[str, Any]] = []
        for r in rows:
            data_rows.append(
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
            )

        self._upsert_many(
            table_fqn="core.sales",
            insert_cols=insert_cols,
            rows=data_rows,
            conflict_cols=conflict_cols,
            update_cols=update_cols,
        )

    def mark_staging_sales_processed_batch(
        self,
        record_ids: List[str],
        error: Optional[str] = None,
        erp_type: str = "tiny",
    ) -> None:
        if not record_ids:
            return
        table_fqn = self._staging_sales_table(erp_type)
        now = datetime.now(timezone.utc)

        if error:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s,
                    process_error = %s
                WHERE id = ANY(%s)
            """
            self._execute(query, (now, error, record_ids))
        else:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s
                WHERE id = ANY(%s)
            """
            self._execute(query, (now, record_ids))

    def mark_staging_stock_processed_batch(
        self,
        record_ids: List[str],
        error: Optional[str] = None,
        erp_type: str = "tiny",
    ) -> None:
        if not record_ids:
            return
        table_fqn = self._staging_stock_table(erp_type)
        now = datetime.now(timezone.utc)

        if error:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s,
                    process_error = %s
                WHERE id = ANY(%s)
            """
            self._execute(query, (now, error, record_ids))
        else:
            query = f"""
                UPDATE {table_fqn}
                SET processed_at = %s
                WHERE id = ANY(%s)
            """
            self._execute(query, (now, record_ids))

    def upsert_core_sale_items_batch(
        self,
        company_id: str,
        erp_type: str,
        rows: List[Dict[str, Any]],
    ) -> None:
        if not rows:
            return

        insert_cols = [
            "company_id",
            "erp_type",
            "sale_id",
            "sale_external_id",
            "product_external_id",
            "product_sku",
            "product_description",
            "product_type",
            "quantity",
            "unit_price",
            "total_price",
            "sale_date",
            "sale_status",
            "raw_data",
        ]
        conflict_cols = ["company_id", "erp_type", "sale_external_id", "product_external_id"]
        update_cols = [c for c in insert_cols if c not in conflict_cols]

        data_rows: List[Dict[str, Any]] = []
        for r in rows:
            data_rows.append(
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
            )

        self._upsert_many(
            table_fqn="core.sale_items",
            insert_cols=insert_cols,
            rows=data_rows,
            conflict_cols=conflict_cols,
            update_cols=update_cols,
        )

    def upsert_core_stock_batch(
        self,
        company_id: str,
        erp_type: str,
        rows: List[Dict[str, Any]],
        synced_at: Optional[datetime] = None,
    ) -> None:
        if not rows:
            return

        synced_at_ts = synced_at or datetime.now(timezone.utc)

        insert_cols = [
            "company_id",
            "erp_type",
            "external_id",
            "sku",
            "product_name",
            "quantity",
            "raw_data",
            "synced_at",
        ]
        conflict_cols = ["company_id", "erp_type", "external_id"]
        update_cols = [c for c in insert_cols if c not in conflict_cols]

        data_rows: List[Dict[str, Any]] = []
        for r in rows:
            data_rows.append(
                {
                    "company_id": company_id,
                    "erp_type": erp_type,
                    "external_id": str(r["external_id"]),
                    "sku": r.get("sku"),
                    "product_name": r.get("product_name"),
                    "quantity": r.get("quantity", 0),
                    "raw_data": r.get("raw_data"),
                    "synced_at": synced_at_ts,
                }
            )

        self._upsert_many(
            table_fqn="core.stock",
            insert_cols=insert_cols,
            rows=data_rows,
            conflict_cols=conflict_cols,
            update_cols=update_cols,
        )


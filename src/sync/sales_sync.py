"""
Sincronização de vendas (Tiny e Conta Azul) para o Supabase.
Coleta dados da API e insere no staging em lotes.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

from src.database.postgres_client import PostgresClient
from src.auth.token_manager import TokenManager
from src.integrations.tiny_client import TinyClient
from src.integrations.contaazul_client import ContaAzulClient
from src.integrations.bling_client import BlingClient
from src.sync.checkpoints import update_checkpoint

logger = logging.getLogger(__name__)

# Tamanho do lote para inserção no Supabase (reduz requisições e melhora tempo)
STAGING_BATCH_SIZE = 100


class SalesSync:
    """Gerencia a sincronização de vendas."""
    
    def __init__(self, db: PostgresClient, token_manager: TokenManager):
        self.db = db
        self.token_manager = token_manager
    
    def sync_company_sales(
        self,
        company_id: str,
        connection_id: str,
        data_inicial: str = None,
        data_final: str = None,
        erp_type: str = None,
        is_full_refresh: bool = False,
    ) -> int:
        """
        Sincroniza vendas de uma empresa.
        
        Args:
            company_id: ID da empresa
            connection_id: ID da conexão ERP
            data_inicial: Data inicial (YYYY-MM-DD) - opcional; use get_sync_start() para incremental/30 dias
            data_final: Data final (YYYY-MM-DD) - opcional
            erp_type: Tipo do ERP (tiny, bling, etc.); se None, obtido da conexão (para checkpoint)
            is_full_refresh: Se True, atualiza last_full_refresh_at no checkpoint (sync dos últimos 30 dias)
        
        Returns:
            Número de vendas sincronizadas
        """
        print(f"\n🛒 Iniciando sincronização de vendas...")
        
        # Verifica se conexão está ativa (conforme doc_funcionamento_geral.md)
        connection = self.db.get_erp_connection_by_id(connection_id)
        if not connection or not connection.get("is_active"):
            raise ValueError(f"Conexão {connection_id} não está ativa")
        erp_type = erp_type or connection.get("erp_type") or "tiny"

        # Obtém token válido (passa pelo token_manager conforme doc)
        access_token = self.token_manager.get_valid_token(connection_id, erp_type=erp_type)

        # Cliente de API conforme ERP
        if erp_type == "contaazul":
            api_client = ContaAzulClient(access_token)
        elif erp_type == "bling":
            api_client = BlingClient(access_token)
        else:
            api_client = TinyClient(access_token)

        # Busca vendas
        sales = api_client.fetch_sales(data_inicial, data_final)

        # Para Bling, após obter todas as vendas, resolvemos as situações (status)
        # via endpoint /situacoes/{idSituacao}, uma vez por ID distinto,
        # e mesclamos essa informação no campo "situacao" de cada venda.
        if erp_type == "bling" and sales:
            situacao_ids = set()
            for s in sales:
                situ = s.get("situacao")
                if isinstance(situ, dict):
                    sid = situ.get("id")
                    if isinstance(sid, int):
                        situacao_ids.add(sid)
            if situacao_ids:
                situacoes_map = api_client.fetch_situacoes(list(situacao_ids))
                for s in sales:
                    situ = s.get("situacao")
                    if not isinstance(situ, dict):
                        continue
                    sid = situ.get("id")
                    if not isinstance(sid, int):
                        continue
                    resolved = situacoes_map.get(sid)
                    if isinstance(resolved, dict):
                        # Mescla para manter id/valor originais e adicionar nome/cor/etc.
                        merged = {**situ, **resolved}
                        s["situacao"] = merged

        if not sales:
            print("⚠️  Nenhuma venda encontrada")
            return 0

        # Insere no staging em lotes
        fetched_at = datetime.now(timezone.utc)
        total = len(sales)
        inserted_count = 0
        num_batches = (total + STAGING_BATCH_SIZE - 1) // STAGING_BATCH_SIZE

        print(f"📥 Vendas: {total} registros em {num_batches} lote(s)")

        for i in range(0, total, STAGING_BATCH_SIZE):
            batch = sales[i : i + STAGING_BATCH_SIZE]
            batch_num = (i // STAGING_BATCH_SIZE) + 1
            try:
                n = self.db.insert_staging_sales_batch(company_id, batch, fetched_at, erp_type=erp_type)
                inserted_count += n
                print(f"   [{batch_num}/{num_batches}] {n} vendas ✓")
            except Exception as e:
                print(f"   [{batch_num}/{num_batches}] Erro: {e}")
                logger.exception("Erro lote %d vendas", batch_num)

        self.db.update_last_sync(connection_id)
        update_checkpoint(self.db, company_id, erp_type, "sales", set_full_refresh=is_full_refresh)

        print(f"✅ Vendas: {inserted_count} inseridas no staging")
        logger.info("Vendas: %d inseridas no staging (company=%s)", inserted_count, company_id)
        return inserted_count
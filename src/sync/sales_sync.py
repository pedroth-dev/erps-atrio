"""
Sincronização de vendas do Tiny para o Supabase.
Coleta dados da API e insere no staging em lotes.
"""
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

from src.database.supabase_client import SupabaseClient
from src.auth.token_manager import TokenManager
from src.integrations.tiny_client import TinyClient
from src.sync.checkpoints import update_checkpoint

logger = logging.getLogger(__name__)

# Tamanho do lote para inserção no Supabase (reduz requisições e melhora tempo)
STAGING_BATCH_SIZE = 100


class SalesSync:
    """Gerencia a sincronização de vendas."""
    
    def __init__(self, db: SupabaseClient, token_manager: TokenManager):
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
        access_token = self.token_manager.get_valid_token(connection_id)
        
        # Cria cliente Tiny
        tiny_client = TinyClient(access_token)
        
        # Busca vendas
        sales = tiny_client.fetch_sales(data_inicial, data_final)
        
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
                n = self.db.insert_staging_sales_batch(company_id, batch, fetched_at)
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
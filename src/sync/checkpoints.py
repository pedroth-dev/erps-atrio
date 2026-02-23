"""
Sincronização incremental: ponto de partida e atualização de checkpoint.
A cada 24h executa um refresh dos últimos 30 dias (repõe mudanças de status, itens faltantes).
"""
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from src.database.supabase_client import SupabaseClient


# Primeira sincronização ou refresh completo: buscar últimos N dias
INITIAL_SYNC_DAYS = 30
# Intervalo em horas para forçar sync dos últimos 30 dias (refresh completo)
FULL_REFRESH_INTERVAL_HOURS = 24


def get_sync_start(
    db: SupabaseClient,
    company_id: str,
    erp_type: str,
    entity: str,
) -> Tuple[str, str, bool]:
    """
    Retorna (data_inicial, data_final, is_full_refresh) em YYYY-MM-DD para a próxima requisição.

    - Se passou 24h ou mais desde last_full_refresh_at (ou nunca rodou): is_full_refresh=True,
      retorna últimos 30 dias. Repõe pedidos que mudaram (ex.: status) e itens que faltaram.
    - Caso contrário: is_full_refresh=False, sync incremental desde last_sync_at (ou 30 dias na primeira vez).
    - data_final é sempre hoje.
    """
    now = datetime.now(timezone.utc)
    data_final = now.strftime("%Y-%m-%d")
    thirty_days_ago = (now - timedelta(days=INITIAL_SYNC_DAYS)).strftime("%Y-%m-%d")

    checkpoint = db.get_checkpoint(company_id, erp_type, entity)
    last_full_refresh_at = checkpoint.get("last_full_refresh_at") if checkpoint else None

    # Decidir se faz refresh completo (últimos 30 dias) ou incremental
    if last_full_refresh_at is None:
        is_full_refresh = True
        data_inicial = thirty_days_ago
    else:
        try:
            if isinstance(last_full_refresh_at, str):
                dt = datetime.fromisoformat(last_full_refresh_at.replace("Z", "+00:00"))
            else:
                dt = last_full_refresh_at
            if hasattr(dt, "tzinfo") and dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            hours_since = (now - dt).total_seconds() / 3600
            is_full_refresh = hours_since >= FULL_REFRESH_INTERVAL_HOURS
        except Exception:
            is_full_refresh = True

        if is_full_refresh:
            data_inicial = thirty_days_ago
        else:
            # Incremental: a partir de last_sync_at
            if checkpoint and checkpoint.get("last_sync_at"):
                try:
                    last = checkpoint["last_sync_at"]
                    if isinstance(last, str):
                        dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
                    else:
                        dt = last
                    if hasattr(dt, "tzinfo") and dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    data_inicial = dt.strftime("%Y-%m-%d")
                except Exception:
                    data_inicial = thirty_days_ago
            else:
                data_inicial = thirty_days_ago

    return data_inicial, data_final, is_full_refresh


def update_checkpoint(
    db: SupabaseClient,
    company_id: str,
    erp_type: str,
    entity: str,
    set_full_refresh: bool = False,
) -> None:
    """
    Atualiza o checkpoint após sincronização bem-sucedida.
    set_full_refresh=True: preenche last_full_refresh_at (para que nas próximas 24h seja usado sync incremental).
    """
    db.upsert_checkpoint(company_id, erp_type, entity, set_full_refresh=set_full_refresh)

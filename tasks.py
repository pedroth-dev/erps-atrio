"""
Tarefas Celery para sincronização assíncrona (arquitetura_sincronizacao.md).
Enfileira por empresa + ERP + tipo de dado; workers processam em paralelo.
"""
import sys
from pathlib import Path

# Garante que o projeto raiz está no path
root_dir = Path(__file__).parent
sys.path.insert(0, str(root_dir))

import os
import redis
from celery import Celery
from celery.schedules import crontab

# Configuração do Celery
app = Celery("tasks")
app.config_from_object("celery_config")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# TTL do lock anti-duplicata (25 min), em segundos
SYNC_LOCK_TTL = 25 * 60


def _get_redis():
    """Cliente Redis para proteção contra tarefas duplicadas."""
    return redis.from_url(REDIS_URL)


def _sync_tiny_sales_impl(company_id: str) -> int:
    """Lógica de sync vendas Tiny (fase 1 staging de pedidos)."""
    from src.database.postgres_client import PostgresClient
    from src.auth.token_manager import TokenManager
    from src.sync.sales_sync import SalesSync
    from src.sync.checkpoints import get_sync_start

    db = PostgresClient()
    token_manager = TokenManager(db)
    connection = db.get_erp_connection(company_id, "tiny")
    if not connection or not connection.get("is_active"):
        raise ValueError(f"Conexão Tiny não encontrada ou inativa para empresa {company_id}")
    connection_id = connection["id"]

    data_inicial, data_final, is_full_refresh = get_sync_start(db, company_id, "tiny", "sales")
    sales_sync = SalesSync(db, token_manager)
    count = sales_sync.sync_company_sales(
        company_id, connection_id,
        data_inicial=data_inicial,
        data_final=data_final,
        erp_type="tiny",
        is_full_refresh=is_full_refresh,
    )
    return count


def _sync_tiny_stock_impl(company_id: str) -> int:
    """Lógica de sync estoque Tiny."""
    from src.database.postgres_client import PostgresClient
    from src.auth.token_manager import TokenManager
    from src.sync.stock_sync import StockSync

    db = PostgresClient()
    token_manager = TokenManager(db)
    connection = db.get_erp_connection(company_id, "tiny")
    if not connection or not connection.get("is_active"):
        raise ValueError(f"Conexão Tiny não encontrada ou inativa para empresa {company_id}")
    connection_id = connection["id"]

    stock_sync = StockSync(db, token_manager)
    return stock_sync.sync_company_stock(company_id, connection_id, erp_type="tiny")


def _sync_contaazul_sales_impl(company_id: str) -> int:
    """Lógica de sync vendas Conta Azul (fase 1 staging de pedidos)."""
    from src.database.postgres_client import PostgresClient
    from src.auth.token_manager import TokenManager
    from src.sync.sales_sync import SalesSync
    from src.sync.checkpoints import get_sync_start

    db = PostgresClient()
    token_manager = TokenManager(db)
    connection = db.get_erp_connection(company_id, "contaazul")
    if not connection or not connection.get("is_active"):
        raise ValueError(f"Conexão Conta Azul não encontrada ou inativa para empresa {company_id}")
    connection_id = connection["id"]

    data_inicial, data_final, is_full_refresh = get_sync_start(db, company_id, "contaazul", "sales")
    sales_sync = SalesSync(db, token_manager)
    count = sales_sync.sync_company_sales(
        company_id, connection_id,
        data_inicial=data_inicial,
        data_final=data_final,
        erp_type="contaazul",
        is_full_refresh=is_full_refresh,
    )
    return count


def _sync_contaazul_stock_impl(company_id: str) -> int:
    """Lógica de sync estoque Conta Azul."""
    from src.database.postgres_client import PostgresClient
    from src.auth.token_manager import TokenManager
    from src.sync.stock_sync import StockSync

    db = PostgresClient()
    token_manager = TokenManager(db)
    connection = db.get_erp_connection(company_id, "contaazul")
    if not connection or not connection.get("is_active"):
        raise ValueError(f"Conexão Conta Azul não encontrada ou inativa para empresa {company_id}")
    connection_id = connection["id"]

    stock_sync = StockSync(db, token_manager)
    return stock_sync.sync_company_stock(company_id, connection_id, erp_type="contaazul")


def _sync_bling_sales_impl(company_id: str) -> int:
    """Lógica de sync vendas Bling (fase 1 staging de pedidos)."""
    from src.database.postgres_client import PostgresClient
    from src.auth.token_manager import TokenManager
    from src.sync.sales_sync import SalesSync
    from src.sync.checkpoints import get_sync_start

    db = PostgresClient()
    token_manager = TokenManager(db)
    connection = db.get_erp_connection(company_id, "bling")
    if not connection or not connection.get("is_active"):
        raise ValueError(f"Conexão Bling não encontrada ou inativa para empresa {company_id}")
    connection_id = connection["id"]

    data_inicial, data_final, is_full_refresh = get_sync_start(db, company_id, "bling", "sales")
    sales_sync = SalesSync(db, token_manager)
    return sales_sync.sync_company_sales(
        company_id,
        connection_id,
        data_inicial=data_inicial,
        data_final=data_final,
        erp_type="bling",
        is_full_refresh=is_full_refresh,
    )


@app.task(bind=True, queue="tiny", max_retries=3, default_retry_delay=60)
def sync_tiny_sales(self, company_id: str):
    """Sincroniza vendas Tiny para uma empresa (incremental)."""
    task_id = f"sync_tiny_sales_{company_id}"
    r = _get_redis()
    if r.get(task_id):
        return  # já em execução
    r.setex(task_id, SYNC_LOCK_TTL, "running")
    try:
        _sync_tiny_sales_impl(company_id)
    except Exception as exc:
        raise self.retry(exc=exc)
    finally:
        try:
            r.delete(task_id)
        except Exception:
            pass


@app.task(bind=True, queue="tiny", max_retries=3, default_retry_delay=60)
def sync_tiny_stock(self, company_id: str):
    """Sincroniza estoque Tiny para uma empresa."""
    task_id = f"sync_tiny_stock_{company_id}"
    r = _get_redis()
    if r.get(task_id):
        return
    r.setex(task_id, SYNC_LOCK_TTL, "running")
    try:
        _sync_tiny_stock_impl(company_id)
    except Exception as exc:
        raise self.retry(exc=exc)
    finally:
        try:
            r.delete(task_id)
        except Exception:
            pass


@app.task(bind=True, queue="contaazul", max_retries=3, default_retry_delay=60)
def sync_contaazul_sales(self, company_id: str):
    """Sincroniza vendas Conta Azul para uma empresa (incremental + itens)."""
    task_id = f"sync_contaazul_sales_{company_id}"
    r = _get_redis()
    if r.get(task_id):
        return
    r.setex(task_id, SYNC_LOCK_TTL, "running")
    try:
        _sync_contaazul_sales_impl(company_id)
    except Exception as exc:
        raise self.retry(exc=exc)
    finally:
        try:
            r.delete(task_id)
        except Exception:
            pass


@app.task(bind=True, queue="contaazul", max_retries=3, default_retry_delay=60)
def sync_contaazul_stock(self, company_id: str):
    """Sincroniza estoque Conta Azul para uma empresa."""
    task_id = f"sync_contaazul_stock_{company_id}"
    r = _get_redis()
    if r.get(task_id):
        return
    r.setex(task_id, SYNC_LOCK_TTL, "running")
    try:
        _sync_contaazul_stock_impl(company_id)
    except Exception as exc:
        raise self.retry(exc=exc)
    finally:
        try:
            r.delete(task_id)
        except Exception:
            pass


@app.task(bind=True, queue="bling", max_retries=3, default_retry_delay=60)
def sync_bling_sales(self, company_id: str):
    """Sincroniza vendas Bling (fase 1 staging de pedidos)."""
    task_id = f"sync_bling_sales_{company_id}"
    r = _get_redis()
    if r.get(task_id):
        return
    r.setex(task_id, SYNC_LOCK_TTL, "running")
    try:
        _sync_bling_sales_impl(company_id)
    except Exception as exc:
        raise self.retry(exc=exc)
    finally:
        try:
            r.delete(task_id)
        except Exception:
            pass


@app.task(queue="default")
def dispatch_all():
    """Scheduler: enfileira uma tarefa por (empresa, ERP, tipo) para todas as empresas ativas."""
    from src.database.postgres_client import PostgresClient

    db = PostgresClient()
    companies = db.get_all_companies(active_only=True)
    if not companies:
        return

    for company in companies:
        company_id = company["id"]
        connections = db.get_erp_connections_by_company(company_id, active_only=True)
        for conn in connections:
            erp = conn["erp_type"]
            if erp == "tiny":
                sync_tiny_sales.delay(company_id)
            elif erp == "contaazul":
                sync_contaazul_sales.delay(company_id)
            elif erp == "bling":
                sync_bling_sales.delay(company_id)


# Celery Beat: executa dispatch_all a cada 30 minutos
app.conf.beat_schedule = {
    "sync-all-companies-every-30min": {
        "task": "tasks.dispatch_all",
        "schedule": crontab(minute="*/30"),
    },
}

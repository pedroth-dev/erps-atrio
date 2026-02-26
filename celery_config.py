"""
Configuração do Celery para tarefas assíncronas (arquitetura_sincronizacao.md).
Filas separadas por ERP para controlar paralelismo e rate limits.
"""
import os

from kombu import Queue

# Broker e backend usam o mesmo Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

broker_url = REDIS_URL
result_backend = REDIS_URL

# Filas por ERP
task_queues = (
    Queue("default"),
    Queue("tiny"),
    Queue("bling"),
    Queue("omie"),
    Queue("contaazul"),
)
task_default_queue = "default"

# Serialização
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]

# Concorrência (ajustar conforme rate limit de cada ERP)
worker_concurrency = 4

# Retry
task_acks_late = True
task_reject_on_worker_lost = True

# Timezone
timezone = "America/Sao_Paulo"
enable_utc = True

# Documentação — Schema staging

Este documento descreve os arquivos SQL que compõem o schema `staging`, sua finalidade, ordem de execução e cuidados necessários.

---

## Visão Geral

O schema `staging` é a camada de entrada de dados no banco. Ele armazena os payloads brutos exatamente como chegaram das APIs dos ERPs, sem nenhum tratamento ou normalização.

A decisão de ter um staging separado do `core` traz três vantagens principais:

- **Reprocessamento** — se a normalização tiver um bug, os dados originais estão preservados e podem ser reprocessados sem precisar buscar na API novamente.
- **Rastreabilidade** — você consegue comparar o dado bruto com o que foi normalizado para o `core`, facilitando depuração.
- **Separação de responsabilidades** — o script Python que coleta dados da API não precisa saber nada sobre a estrutura do `core`. Ele só insere o raw e segue em frente.

Atualmente o staging contém tabelas para o ERP **Tiny**:
- `tiny_sales` — vendas (pedidos)
- `tiny_stock` — estoque de produtos
- `tiny_sale_items` — itens de vendas (produtos vendidos em cada pedido)

Conforme novos ERPs forem integrados, novas tabelas serão adicionadas aqui seguindo o mesmo padrão.

---

## Fluxo do staging

```
Script Python coleta da API do Tiny
        ↓
staging.tiny_sales / staging.tiny_stock
(raw_data completo, processed_at = NULL)
        ↓
Script Python lê os registros pendentes
normaliza os campos
faz upsert no core
        ↓
processed_at preenchido com o horário da normalização

Para sale_items:
1. Vendas são normalizadas primeiro (staging.tiny_sales → core.sales)
2. Script busca vendas já normalizadas em core.sales
3. Para cada venda, faz GET /pedidos/{idPedido} para obter detalhes
4. Extrai array 'itens' e insere em staging.tiny_sale_items
5. Normaliza itens: staging.tiny_sale_items → core.sale_items
```

Registros com `processed_at = NULL` estão pendentes de normalização. Registros com `process_error` preenchido falharam na normalização e precisam de atenção.

---

## staging_tiny_sales.sql

**O que faz:** Cria o schema `staging` e a tabela `staging.tiny_sales`, que armazena os payloads brutos do endpoint de vendas do Tiny.

**Quando executar:** Após os arquivos do schema `auth_integrations` (01 a 05), pois referencia `auth_integrations.companies`.

**Como usar:** O script Python de coleta insere um registro aqui a cada venda retornada pela API do Tiny, com o payload completo no campo `raw_data`. O script de normalização lê os registros onde `processed_at IS NULL`, extrai os campos necessários e os envia para `core.customers` e `core.sales`.

**Campos importantes:**
- `raw_data` — payload completo retornado pela API do Tiny, em formato JSONB. Contém todos os dados da venda incluindo cliente, ecommerce de origem e valores.
- `processed_at` — nulo enquanto o registro não foi normalizado para o `core`. Preenchido automaticamente pelo script após a normalização bem-sucedida.
- `process_error` — quando a normalização falha, o script registra aqui a mensagem de erro e mantém `processed_at` como nulo, deixando o registro na fila para reprocessamento.
- `fetched_at` — momento exato em que o dado foi coletado da API, independente de quando foi inserido no banco.

**Índice parcial:** A tabela possui um índice `WHERE processed_at IS NULL` que indexa apenas registros pendentes. Isso garante que a query de busca por registros a processar seja rápida mesmo com milhões de registros históricos já normalizados.

**O que não pode acontecer:**
- Não delete registros do staging após a normalização — o histórico dos payloads brutos é intencional e valioso para depuração e reprocessamento.
- Não altere o `raw_data` após a inserção. Ele deve ser uma cópia fiel do que a API retornou.
- Não marque `processed_at` manualmente sem ter de fato normalizado o registro para o `core` — isso faria o registro sumir da fila sem ter sido processado.
- Não ignore registros com `process_error` preenchido. Eles indicam falhas na normalização que precisam ser investigadas e corrigidas.

---

## staging_tiny_stock.sql

**O que faz:** Cria a tabela `staging.tiny_stock`, que armazena os payloads brutos do endpoint de estoque do Tiny.

**Quando executar:** Após o `staging_tiny_sales.sql`, pois o schema `staging` já deve existir.

**Como usar:** Segue o mesmo padrão da `tiny_sales`. O script Python de coleta insere o payload bruto aqui, e o script de normalização processa os registros pendentes e faz upsert em `core.stock`.

**Campos importantes:**
- `raw_data` — payload completo do produto retornado pelo endpoint de estoque do Tiny.
- `processed_at` — nulo enquanto pendente, preenchido após normalização bem-sucedida para `core.stock`.
- `process_error` — registra erros de normalização, mantendo o registro na fila para reprocessamento.
- `fetched_at` — momento da coleta na API.
- `synced_at` em `core.stock` — campo correspondente no core que registra especificamente quando o saldo foi sincronizado, diferente do `updated_at` que registra qualquer alteração no registro.

**Índice parcial:** Assim como na `tiny_sales`, possui índice `WHERE processed_at IS NULL` para consultas eficientes de registros pendentes.

**O que não pode acontecer:**
- Não delete registros históricos — o estoque muda frequentemente e o histórico de sincronizações pode ser útil para auditar variações de quantidade.
- Não altere o `raw_data` após a inserção.
- Não ignore registros com `process_error` — podem indicar mudanças na estrutura da API do Tiny que precisam de ajuste no script de normalização.

---

## staging_tiny_sale_items.sql

**O que faz:** Cria a tabela `staging.tiny_sale_items`, que armazena os itens (produtos vendidos) de cada venda coletados via `GET /pedidos/{idPedido}`.

**Quando executar:** Após o `staging_tiny_stock.sql`, pois o schema `staging` já deve existir.

**Como usar:** O script Python primeiro normaliza as vendas (`staging.tiny_sales` → `core.sales`). Em seguida, para cada venda já normalizada em `core.sales`, faz uma requisição individual `GET /pedidos/{idPedido}` para obter os detalhes completos da venda, incluindo o array `itens`. Cada item do array é inserido como um registro separado nesta tabela. O script de normalização então processa os registros pendentes e faz upsert em `core.sale_items`.

**Campos importantes:**
- `sale_external_id` — ID da venda no Tiny (ex: `123`). Usado para relacionar o item com a venda em `core.sales`.
- `sale_staging_id` — FK opcional para `staging.tiny_sales.id`. Pode ser nulo se a venda já foi normalizada e removida do staging.
- `raw_data` — payload completo do item retornado pela API do Tiny. Contém dados do produto (id, sku, descrição, tipo), quantidade, valores unitário e total, etc.
- `processed_at` — nulo enquanto pendente, preenchido após normalização bem-sucedida para `core.sale_items`.
- `process_error` — registra erros de normalização, mantendo o registro na fila para reprocessamento.
- `fetched_at` — momento da coleta na API via `GET /pedidos/{idPedido}`.

**Índice parcial:** Possui índice `WHERE processed_at IS NULL` para consultas eficientes de registros pendentes.

**Sobre rate limit:** Como cada venda requer uma requisição individual à API (`GET /pedidos/{idPedido}`), o script implementa controle de rate limit baseado nos headers `X-RateLimit-Remaining` e `X-RateLimit-Reset` da API do Tiny. Quando o limite está próximo de esgotar (≤ 5 requisições restantes), o script aguarda automaticamente o reset antes de continuar.

**O que não pode acontecer:**
- Não delete registros históricos — o histórico de itens vendidos é valioso para análises e auditoria.
- Não altere o `raw_data` após a inserção.
- Não ignore registros com `process_error` — podem indicar problemas na estrutura dos dados do item ou na API do Tiny.
- Não tente coletar itens antes de normalizar as vendas — o collector depende de vendas já existentes em `core.sales`.

---

## Ordem de Execução

| Ordem | Arquivo                          |
|-------|----------------------------------|
| 6º    | staging_tiny_sales.sql |
| 7º    | staging_tiny_stock.sql |
| 8º    | staging_tiny_sale_items.sql |

Os arquivos do staging dependem do schema `auth_integrations` estar completamente criado (arquivos 01 a 05) antes de serem executados.

---

## Adicionando novos ERPs no futuro

Quando um novo ERP for integrado, basta criar novas tabelas dentro do schema `staging` seguindo o mesmo padrão:

```
staging.bling_sales
staging.bling_stock
staging.bling_sale_items
staging.omie_sales
...
```

Cada tabela terá sua própria estrutura de `raw_data` correspondente ao payload daquele ERP específico, e seu próprio script de normalização que saberá como extrair e transformar os campos para o `core`.
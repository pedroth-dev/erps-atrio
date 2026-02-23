# Documentação — Schema core

Este documento descreve os arquivos SQL que compõem o schema `core`, sua finalidade, ordem de execução e cuidados necessários.

---

## Visão Geral

O schema `core` é a camada de dados normalizados do projeto. Aqui vivem os dados já tratados, padronizados e prontos para consumo pela aplicação, dashboards e relatórios.

Diferente do `staging`, que armazena payloads brutos por ERP, o `core` é agnóstico à origem — uma venda do Tiny e uma venda do Bling ocupam a mesma tabela `core.sales`, identificadas apenas pelo campo `erp_type`. Isso permite que a aplicação consulte dados de todos os ERPs de forma unificada, sem precisar saber de onde cada dado veio.

O schema é composto por quatro tabelas:

- **`customers`** — clientes das empresas, extraídos das vendas
- **`sales`** — vendas normalizadas, referenciando o cliente correspondente
- **`sale_items`** — itens de vendas (produtos vendidos em cada pedido), referenciando a venda correspondente
- **`stock`** — posição atual do estoque por produto

---

## Relacionamento entre as tabelas

```
auth_integrations.companies
        ↓ (company_id)
core.customers ←── core.sales ←── core.sale_items
                         ↑
                    core.stock (independente, mas também referencia companies)
```

A `sales` depende de `customers` — antes de inserir uma venda, o cliente já deve existir em `core.customers`. O script Python garante essa ordem durante a normalização.

A `sale_items` depende de `sales` — antes de inserir um item de venda, a venda já deve existir em `core.sales`. O collector de itens busca apenas vendas já normalizadas em `core.sales`.

---

## table_customers.sql

**O que faz:** Cria a tabela `core.customers`, que armazena os clientes de cada empresa, extraídos dos payloads de venda do staging.

**Quando executar:** Antes da `core.sales`, pois a tabela de vendas referencia `customers`.

**Como usar:** O script de normalização extrai os dados do cliente do `raw_data` em `staging.tiny_sales` e faz upsert aqui antes de processar a venda. Se o cliente já existe, os dados são atualizados; se não existe, é criado.

**Campos importantes:**
- `external_id` — ID do cliente no ERP de origem (Tiny, Bling, etc.). Usado em conjunto com `company_id` e `erp_type` para identificar o cliente de forma única e evitar duplicatas.
- `person_type` — distingue pessoa física (`fisica`) de pessoa jurídica (`juridica`).
- `document` — CPF ou CNPJ, dependendo do `person_type`.
- `phone`, `mobile`, `email` — campos de contato que podem ser nulos, pois nem todos os clientes os preenchem no ERP.
- `neighborhood`, `city`, `zip_code`, `state`, `country` — dados de endereço extraídos do payload. Correspondem aos campos `cliente.endereco.*` da API do Tiny.
- `raw_data` — payload original do cliente para consulta futura ou reprocessamento.

**Sobre duplicatas:** A constraint `UNIQUE (company_id, erp_type, external_id)` garante que o mesmo cliente não seja inserido duas vezes para a mesma empresa e ERP. O script deve sempre usar `upsert` em vez de `insert` simples.

**O que não pode acontecer:**
- Não insira uma venda em `core.sales` antes de garantir que o cliente já existe em `core.customers`. O banco rejeitará a inserção por violação de foreign key.
- Não use `ON DELETE CASCADE` na relação entre `customers` e `sales` — se um cliente for deletado, as vendas devem ser preservadas com `customer_id = NULL`, não apagadas.
- Não tente criar clientes duplicados manualmente — sempre use upsert.

---

## table_sales.sql

**O que faz:** Cria a tabela `core.sales`, que armazena as vendas normalizadas de todos os ERPs.

**Quando executar:** Após `core.customers`, pois possui foreign key para essa tabela.

**Como usar:** O script de normalização, após garantir que o cliente existe em `core.customers`, extrai os dados da venda do `raw_data` em `staging.tiny_sales` e faz upsert aqui. O `customer_id` é obtido do registro criado ou atualizado em `core.customers`.

**Campos importantes:**
- `external_id` — ID do pedido no Tiny. Sempre presente, nunca nulo. Usado para upsert e para evitar duplicatas.
- `order_number` — número sequencial do pedido no Tiny, diferente do `external_id`.
- `origin_order_id` — ID do pedido na plataforma de origem (Mercado Livre, Shopee, etc.). Nulo para vendas presenciais ou sem origem de ecommerce. Corresponde ao campo `ecommerce.numeroPedidoEcommerce` da API do Tiny.
- `origin_channel_id` — ID numérico do ecommerce no Tiny (ex: 1, 2, 3). Corresponde ao campo `ecommerce.id`.
- `origin_channel` — nome do ecommerce (ex: `Mercado Livre`, `Shopee`). Varia por empresa — cada empresa tem seus próprios canais configurados no Tiny. Corresponde ao campo `ecommerce.nome`.
- `customer_id` — referência para `core.customers`. Definido como `ON DELETE SET NULL`, ou seja, se o cliente for deletado, a venda é preservada com esse campo nulo.
- `total_amount` — valor total da venda. Corresponde ao campo `valor` da API do Tiny.
- `status` — situação do pedido no Tiny (ex: `aprovado`, `cancelado`, `pendente`). Corresponde ao campo `situacao`.
- `issued_at` — data de criação do pedido no Tiny. Corresponde ao campo `dataCriacao`.
- `raw_data` — payload original completo da venda para consulta futura ou reprocessamento.

**O que não pode acontecer:**
- Não insira vendas sem antes garantir que o cliente existe em `core.customers`.
- Não use `insert` simples — sempre `upsert` usando a constraint `UNIQUE (company_id, erp_type, external_id)` para evitar duplicatas em sincronizações repetidas.
- Não delete vendas para "limpar" dados — prefira filtrar por `status` ou por período nas queries.
- Não altere `origin_channel` para um valor padronizado global. Cada empresa tem seus próprios canais no Tiny e o valor deve refletir exatamente o que vem da API.

---

## table_sale_items.sql

**O que faz:** Cria a tabela `core.sale_items`, que armazena os itens (produtos vendidos) de cada venda normalizada, extraídos do array `itens` retornado por `GET /pedidos/{idPedido}`.

**Quando executar:** Após `core.sales`, pois possui foreign key para essa tabela.

**Como usar:** O script de normalização primeiro garante que a venda existe em `core.sales`. Em seguida, extrai os dados do item do `raw_data` em `staging.tiny_sale_items` e faz upsert aqui. O `sale_id` é obtido do registro correspondente em `core.sales` através do `sale_external_id`. Campos como `sale_date` e `sale_status` são enriquecidos diretamente da venda para permitir análises sem necessidade de JOIN.

**Campos importantes:**
- `sale_id` — referência para `core.sales.id` via foreign key. Definido como `ON DELETE CASCADE`, ou seja, se a venda for deletada, os itens também são removidos.
- `sale_external_id` — ID da venda no Tiny. Mantido mesmo com a FK para permitir consultas diretas sem JOIN e para garantir a constraint de unicidade.
- `product_external_id` — ID do produto no Tiny. Usado em conjunto com `sale_external_id` para identificar o item de forma única.
- `product_sku` — código SKU do produto. Útil para cruzar dados com outros sistemas e análises por produto.
- `product_description` — descrição do produto no momento da venda. Pode diferir da descrição atual do produto se o catálogo foi atualizado.
- `product_type` — tipo do produto: `'P'` (Produto) ou `'S'` (Serviço).
- `quantity` — quantidade vendida. Usa `NUMERIC(15, 4)` para suportar produtos vendidos por peso ou fração (ex: `2,5000 kg`).
- `unit_price` — preço unitário do item no momento da venda.
- `total_price` — valor total do item (quantidade × preço unitário).
- `sale_date` — data da venda (`issued_at` de `core.sales`). Enriquecido diretamente para análises sem JOIN.
- `sale_status` — status da venda (`status` de `core.sales`). Enriquecido diretamente para análises sem JOIN.
- `raw_data` — payload original completo do item para consulta futura ou reprocessamento.

**Sobre duplicatas:** A constraint `UNIQUE (company_id, erp_type, sale_external_id, product_external_id)` garante que o mesmo item da mesma venda não seja inserido duas vezes. O script deve sempre usar `upsert` em vez de `insert` simples.

**Sobre análises:** Os campos `sale_date` e `sale_status` são denormalizados (copiados da venda) para permitir análises eficientes de vendas por produto sem necessidade de JOIN com `core.sales`. Isso é especialmente útil para dashboards e relatórios que precisam filtrar itens por data ou status da venda.

**O que não pode acontecer:**
- Não insira itens sem antes garantir que a venda existe em `core.sales`.
- Não use `insert` simples — sempre `upsert` usando a constraint `UNIQUE` para evitar duplicatas em sincronizações repetidas.
- Não delete itens para "limpar" dados — prefira filtrar por `sale_status` ou por período nas queries.
- Não altere `product_description` para refletir a descrição atual do produto — o campo deve manter a descrição no momento da venda para histórico fiel.

---

## table_stock.sql

**O que faz:** Cria a tabela `core.stock`, que armazena a posição atual do estoque por produto de cada empresa.

**Quando executar:** Pode ser executado em qualquer ordem em relação a `customers` e `sales`, pois não depende delas. Depende apenas do schema `auth_integrations`.

**Como usar:** O script de normalização extrai os dados do produto do `raw_data` em `staging.tiny_stock` e faz upsert aqui. Como o estoque representa sempre a posição atual, cada sincronização sobrescreve o valor anterior da `quantity`.

**Campos importantes:**
- `external_id` — ID do produto no Tiny. Usado em conjunto com `company_id` e `erp_type` para identificar o produto de forma única.
- `sku` — código do produto. Útil para cruzar dados com outros sistemas.
- `quantity` — posição atual do estoque. Usa `NUMERIC(15, 4)` para suportar produtos vendidos por peso ou fração (ex: `2,5000 kg`).
- `synced_at` — momento exato em que o saldo foi coletado da API. Diferente do `updated_at`, que registra qualquer alteração no registro — o `synced_at` indica especificamente quando o dado veio do ERP.
- `raw_data` — payload original do produto para consulta futura.

**O que não pode acontecer:**
- Não use `insert` simples — sempre `upsert` para que cada sincronização atualize o saldo existente em vez de criar duplicatas.
- Não interprete o `updated_at` como o momento da sincronização com o ERP. Use sempre o `synced_at` para isso.
- Não delete registros de estoque zerado — produto com quantidade zero é diferente de produto inexistente no banco.

---

## Ordem de Execução

| Ordem | Arquivo                       |
|-------|-------------------------------|
| 9º    | table_customers.sql |
| 10º   | table_sales.sql     |
| 11º   | table_sale_items.sql |
| 12º   | table_stock.sql     |

Todos os arquivos do `core` dependem do schema `auth_integrations` estar completamente criado (arquivos 01 a 05) e do schema `staging` criado (arquivo 09) antes de serem executados.

---

## Adicionando novos dados no futuro

Conforme novos endpoints dos ERPs forem integrados, novas tabelas serão adicionadas ao `core` seguindo o mesmo padrão. Exemplos futuros:

```
core.products       ← catálogo de produtos
core.orders         ← pedidos de compra
core.suppliers      ← fornecedores
```

Cada nova tabela deve sempre ter `company_id`, `erp_type`, `external_id`, `raw_data`, `created_at`, `updated_at`, RLS ativado com suas policies, e trigger de `updated_at`.
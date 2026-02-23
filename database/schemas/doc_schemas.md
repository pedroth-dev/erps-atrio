# Documentação — Schemas do Projeto

Este documento descreve a organização geral dos schemas do banco de dados, sua finalidade e como se relacionam entre si. O banco utiliza o **Supabase (PostgreSQL)**.

---

## Visão Geral

O projeto utiliza três schemas para separar as responsabilidades de forma clara e escalável:

- **`auth_integrations`** — empresas, credenciais e conexões com ERPs
- **`staging`** — dados brutos coletados das APIs dos ERPs, antes de qualquer normalização
- **`core`** — dados normalizados e prontos para consumo pela aplicação

Essa separação garante que cada camada do sistema tenha um propósito bem definido, facilitando manutenção, depuração e crescimento do projeto sem que as responsabilidades se misturem.

---

## schemas.sql

**O que faz:** Cria a extensão UUID e os três schemas do projeto no banco de dados. A extensão UUID é necessária para a geração automática dos IDs em todas as tabelas.

**Quando executar:** Deve ser o primeiro arquivo executado em qualquer ambiente — desenvolvimento, homologação ou produção. Sem os schemas criados, todos os demais scripts falharão.

**Como usar:** Execute uma única vez no SQL Editor do Supabase ou via cliente PostgreSQL conectado ao projeto.

**O que não pode acontecer:**
- Não execute este arquivo mais de uma vez sem verificar se os schemas já existem. Se quiser proteger a execução, use `CREATE SCHEMA IF NOT EXISTS`, mas saiba que o arquivo original não tem essa proteção para evitar mascarar erros acidentais.
- Não renomeie schemas depois de criados sem atualizar todas as referências nas tabelas, triggers, policies e no código da aplicação — o impacto é amplo e difícil de rastrear.
- Não pule este arquivo achando que os schemas serão criados automaticamente pelos arquivos seguintes.
- Não crie tabelas diretamente no schema `public`. Ele deve permanecer vazio — toda a estrutura do projeto vive nos três schemas definidos aqui.

---

## Como os schemas se relacionam

O fluxo de dados sempre segue a mesma direção:

```
APIs dos ERPs
     ↓
staging         ← dados brutos, sem tratamento
     ↓
core            ← dados normalizados, prontos para uso
```

O schema `auth_integrations` é transversal — ele não faz parte do fluxo de dados, mas é referenciado por todos os outros schemas através do `company_id`, que identifica a qual empresa cada registro pertence.

---

## Configuração no Supabase

Após executar o arquivo, acesse **Project Settings → API → Extra Search Path** e adicione os três schemas:

```
auth_integrations, staging, core
```

Isso é necessário para que a API REST do Supabase consiga enxergar as tabelas fora do schema `public`. Sem essa configuração, as tabelas existem no banco mas ficam invisíveis para a API.
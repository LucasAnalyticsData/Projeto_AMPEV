# Databricks notebook source
# MAGIC %md
# MAGIC #  Pipeline Silver — Pedidos (Delta Lake)
# MAGIC
# MAGIC 👨‍💻 **Autor:** Lucas Sousa Santos Oliveira**  
# MAGIC 🎯 **Objetivo:** Refino, padronização, governança e **carga incremental robusta** de dados de *Pedidos* para consumo analítico e downstream (Camada Gold).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🌟 O que você vai encontrar neste README
# MAGIC
# MAGIC - **Por que Silver?**: o papel da camada no Data Lakehouse  
# MAGIC - **UPSERT com `MERGE INTO`** (destaque): por que é mais robusto que `APPEND + deduplicação`  
# MAGIC - **CTEs + LEFT JOIN**: legibilidade, organização e preservação total de pedidos  
# MAGIC - **Documentação (governança)**: comentários em tabela/colunas para usuários finais  
# MAGIC - **Boas práticas**: particionamento, Z-ORDER, VACUUM, idempotência, schema evolution  
# MAGIC - **Exemplos de código**: SQL e PySpark prontos para uso
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Missão da Camada Silver
# MAGIC
# MAGIC A **Silver** transforma o *raw* da Bronze em dados **limpos, padronizados e governados**, prontos para consumo.  
# MAGIC No caso de *Pedidos*, as entregas-chave são:
# MAGIC
# MAGIC - 🧹 **Qualidade**: deduplicação por `PedidoID`, filtragem de chaves nulas, padronização de nomes/tipos.  
# MAGIC - 🛡 **Governança**: documentação (comentários), metadados e *lineage* explícitos.  
# MAGIC - 🔁 **Incrementalidade robusta**: **UPSERT com `MERGE INTO`** para atualizar/insertar sem retrabalho.  
# MAGIC - ⚡ **Performance**: Repartition + `OPTIMIZE ZORDER` + `VACUUM` no Delta Lake.  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧭 Visão de Arquitetura
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TD
# MAGIC     A[🥉 Bronze.Pedidos] --> B[🧹 Limpeza & Padronização via CTEs]
# MAGIC     B --> C[🔗 LEFT JOIN com Estabelecimentos]
# MAGIC     C --> D[🧠 Enriquecimento & Metadados]
# MAGIC     D --> E[🔁 MERGE INTO Silver (UPSERT)]
# MAGIC     E --> F[🛡 Comentários e Catálogo / Metastore]
# MAGIC     F --> G[⚙️ OPTIMIZE + Z-ORDER + VACUUM]
# MAGIC     G --> H[🥇 Pronto para Gold & BI]
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔁 Por que **MERGE INTO (UPSERT)** é superior ao `APPEND + deduplicação`?
# MAGIC
# MAGIC | Critério                     | `APPEND + deduplicação posterior`                 | **MERGE INTO (UPSERT)** 🚀 |
# MAGIC |-----------------------------|---------------------------------------------------|----------------------------|
# MAGIC | **Consistência**            | Risco de *races* e duplicatas temporárias         | **ACID**: insere/atualiza atômico |
# MAGIC | **Custo de processamento**  | Dedup a cada execução aumenta custo                | Processa somente mudanças   |
# MAGIC | **Simplicidade**            | Fluxo com múltiplas etapas de correção            | Uma única operação declarativa |
# MAGIC | **Idempotência**            | Difícil de garantir em reprocessos                | Nativamente idempotente     |
# MAGIC | **Escalabilidade**          | Degrada com volume                                | Escala melhor               |
# MAGIC
# MAGIC > **Conclusão**: `MERGE INTO` é a abordagem **profissional** para cargas incrementais no Delta Lake.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧩 CTEs + LEFT JOIN (legibilidade e preservação de pedidos)
# MAGIC
# MAGIC **CTEs** (Common Table Expressions) tornam o pipeline **mais legível e modular**.  
# MAGIC O **LEFT JOIN** garante que **todos os pedidos da Bronze** sejam preservados **mesmo sem correspondência** na dimensão/estabelecimentos (campos do estabelecimento ficam nulos).
# MAGIC
# MAGIC ```sql
# MAGIC -- 👇 Exemplo SQL completo usando CTEs + LEFT JOIN e MERGE INTO (UPSERT)
# MAGIC
# MAGIC WITH bronze_pedidos AS (
# MAGIC   SELECT
# MAGIC     PedidoID,
# MAGIC     EstabelecimentoID,
# MAGIC     Produto,
# MAGIC     CAST(quantidade_vendida AS INT) AS QuantidadeVendida,
# MAGIC     CAST(Preco_Unitario AS DECIMAL(18,2)) AS PrecoUnitario,
# MAGIC     CAST(data_venda AS DATE)            AS DataVenda,
# MAGIC     data_ingestao
# MAGIC   FROM delta.`dbfs:/FileStore/Ampev/tables/bronze/pedidos`
# MAGIC   WHERE PedidoID IS NOT NULL AND TRIM(PedidoID) != ''
# MAGIC ),
# MAGIC
# MAGIC bronze_estabelecimentos AS (
# MAGIC   SELECT
# MAGIC     EstabelecimentoID,
# MAGIC     Nome   AS NomeEstabelecimento,
# MAGIC     Local  AS LocalEstabelecimento
# MAGIC   FROM delta.`dbfs:/FileStore/Ampev/tables/bronze/estabelecimentos`
# MAGIC ),
# MAGIC
# MAGIC pedidos_enriquecidos AS (
# MAGIC   SELECT
# MAGIC     p.PedidoID,
# MAGIC     p.EstabelecimentoID,
# MAGIC     p.Produto,
# MAGIC     p.QuantidadeVendida,
# MAGIC     p.PrecoUnitario,
# MAGIC     p.DataVenda,
# MAGIC     e.NomeEstabelecimento,
# MAGIC     e.LocalEstabelecimento,
# MAGIC     current_timestamp() AS data_processamento
# MAGIC   FROM bronze_pedidos p
# MAGIC   LEFT JOIN bronze_estabelecimentos e
# MAGIC     ON p.EstabelecimentoID = e.EstabelecimentoID     -- LEFT JOIN preserva todos os pedidos
# MAGIC )
# MAGIC
# MAGIC MERGE INTO silver.pedidos AS tgt
# MAGIC USING pedidos_enriquecidos AS src
# MAGIC ON tgt.PedidoID = src.PedidoID
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   tgt.EstabelecimentoID   = src.EstabelecimentoID,
# MAGIC   tgt.Produto             = src.Produto,
# MAGIC   tgt.QuantidadeVendida   = src.QuantidadeVendida,
# MAGIC   tgt.PrecoUnitario       = src.PrecoUnitario,
# MAGIC   tgt.DataVenda           = src.DataVenda,
# MAGIC   tgt.NomeEstabelecimento = src.NomeEstabelecimento,
# MAGIC   tgt.LocalEstabelecimento= src.LocalEstabelecimento,
# MAGIC   tgt.data_processamento  = src.data_processamento
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ;
# MAGIC ```
# MAGIC
# MAGIC > ✅ *Boas práticas aplicadas*: CTEs nomeadas, *typing* explícito, enriquecimento controlado, **UPSERT transacional**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🗒️ Governança: **Documentação com Comentários** (Tabela & Colunas)
# MAGIC
# MAGIC Documentar a tabela e suas colunas é **crucial** para analistas e cientistas de dados entenderem **origem, propósito e estrutura**.  
# MAGIC Use **comentários** diretamente no catálogo (Metastore).
# MAGIC
# MAGIC ```sql
# MAGIC -- Criação (ou garantia) da tabela Silver com comentários completos
# MAGIC CREATE TABLE IF NOT EXISTS silver.pedidos (
# MAGIC   PedidoID              STRING  COMMENT 'Identificador único do pedido (chave de negócio)',
# MAGIC   EstabelecimentoID     STRING  COMMENT 'Chave do estabelecimento de origem do pedido',
# MAGIC   Produto               STRING  COMMENT 'Descrição do produto vendido',
# MAGIC   QuantidadeVendida     INT     COMMENT 'Quantidade de itens vendidos no pedido',
# MAGIC   PrecoUnitario         DECIMAL(18,2) COMMENT 'Preço unitário do produto (BRL)',
# MAGIC   DataVenda             DATE    COMMENT 'Data da venda no fuso padrão do Lakehouse',
# MAGIC   NomeEstabelecimento   STRING  COMMENT 'Nome do estabelecimento associado (pode ser nulo)',
# MAGIC   LocalEstabelecimento  STRING  COMMENT 'Localização do estabelecimento (pode ser nulo)',
# MAGIC   data_processamento    TIMESTAMP COMMENT 'Timestamp do processamento na Silver (auditoria)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/Ampev/tables/silver/pedidos'
# MAGIC COMMENT 'Fato de Pedidos (Silver): dados limpos, padronizados, documentados e prontos para consumo analítico.'
# MAGIC TBLPROPERTIES (
# MAGIC   'quality' = 'silver',
# MAGIC   'owner'   = 'dados@empresa.com',
# MAGIC   'pii'     = 'false'
# MAGIC );
# MAGIC
# MAGIC -- Exemplo de documentação adicional (quando a tabela já existe)
# MAGIC COMMENT ON TABLE silver.pedidos IS 'Fato de Pedidos (Silver) — refinado a partir da Bronze, com UPSERT via MERGE.';
# MAGIC COMMENT ON COLUMN silver.pedidos.QuantidadeVendida IS 'Quantidade de itens; sempre inteiro >= 0';
# MAGIC ```
# MAGIC
# MAGIC > 🧭 **Dica**: `DESCRIBE EXTENDED silver.pedidos` exibe os comentários no Metastore.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧪 PySpark (equivalente) — leitura, enriquecimento e UPSERT
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import current_timestamp, col
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC bronze_pedidos = spark.read.format("delta").load("dbfs:/FileStore/Ampev/tables/bronze/pedidos")
# MAGIC bronze_estabs  = spark.read.format("delta").load("dbfs:/FileStore/Ampev/tables/bronze/estabelecimentos")
# MAGIC
# MAGIC # Limpeza/typing básico
# MAGIC bronze_pedidos = (
# MAGIC     bronze_pedidos
# MAGIC       .dropDuplicates(["PedidoID"])
# MAGIC       .filter("PedidoID IS NOT NULL AND TRIM(PedidoID) != ''")
# MAGIC       .withColumn("QuantidadeVendida", col("quantidade_vendida").cast("int"))
# MAGIC       .withColumn("PrecoUnitario", col("Preco_Unitario").cast("decimal(18,2)"))
# MAGIC       .withColumnRenamed("data_venda", "DataVenda")
# MAGIC       .select("PedidoID","EstabelecimentoID","Produto","QuantidadeVendida","PrecoUnitario","DataVenda")
# MAGIC )
# MAGIC
# MAGIC # LEFT JOIN para preservar todos os pedidos
# MAGIC enriquecido = (
# MAGIC     bronze_pedidos.alias("p")
# MAGIC     .join(bronze_estabs.selectExpr(
# MAGIC         "EstabelecimentoID",
# MAGIC         "Nome as NomeEstabelecimento",
# MAGIC         "Local as LocalEstabelecimento"
# MAGIC     ).alias("e"), on=col("p.EstabelecimentoID")==col("e.EstabelecimentoID"), how="left")
# MAGIC     .withColumn("data_processamento", current_timestamp())
# MAGIC )
# MAGIC
# MAGIC silver_path = "dbfs:/FileStore/Ampev/tables/silver/pedidos"
# MAGIC if DeltaTable.isDeltaTable(spark, silver_path):
# MAGIC     tgt = DeltaTable.forPath(spark, silver_path)
# MAGIC     (tgt.alias("tgt")
# MAGIC         .merge(enriquecido.alias("src"), "tgt.PedidoID = src.PedidoID")
# MAGIC         .whenMatchedUpdateAll()
# MAGIC         .whenNotMatchedInsertAll()
# MAGIC         .execute())
# MAGIC else:
# MAGIC     (enriquecido.write.format("delta")
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .mode("overwrite")
# MAGIC         .save(silver_path))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⚙️ Otimização & Manutenção (Delta Lake)
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE silver.pedidos ZORDER BY (PedidoID);
# MAGIC VACUUM silver.pedidos RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC - `OPTIMIZE + ZORDER`: melhora leitura por `PedidoID` (arquivos compactados & localizar rápido).  
# MAGIC - `VACUUM`: remove arquivos órfãos; reduz custo; mantém histórico controlado.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Checklist de *Production-Readiness*
# MAGIC
# MAGIC - [x] **UPSERT transacional** com `MERGE INTO` (idempotência e ACID)  
# MAGIC - [x] **CTEs + LEFT JOIN** para legibilidade e preservação total de pedidos  
# MAGIC - [x] **Documentação**: comentários em tabela/colunas no catálogo  
# MAGIC - [x] **Schema evolution** habilitado (`mergeSchema`)  
# MAGIC - [x] **Particionamento & Repartition** para escrita/consulta eficientes  
# MAGIC - [x] **OPTIMIZE + ZORDER + VACUUM** aplicados  
# MAGIC - [x] **Pronto para Gold/BI** com semântica clara e governança
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Conclusão
# MAGIC
# MAGIC Este pipeline Silver evidencia **senioridade de engenharia de dados**:  
# MAGIC - Utiliza **MERGE INTO (UPSERT)** como estratégia **profissional** de incrementalidade.  
# MAGIC - Aplica **CTEs e LEFT JOIN** para modularidade e **preservação total** da base de pedidos.  
# MAGIC - Implementa **governança de dados** com **comentários** e metadados úteis a usuários finais.  
# MAGIC - Garante **performance** e **custo-eficiência** com *Delta Lake best practices*.  
# MAGIC
# MAGIC > **Resultado**: dados **limpos, documentados e prontos** para análises de alto impacto na camada Gold.
# Databricks notebook source
# MAGIC %md
# MAGIC # 📦 Pipeline Bronze - Ingestão e Governança de Dados de Pedidos (CSV ➝ Delta Lake)
# MAGIC
# MAGIC 👨‍💻 **Autor:** Lucas Sousa Santos Oliveira  
# MAGIC 🎯 **Especialista em Finanças em transição para Engenharia de Dados** | Pós-graduação em Big Data e Cloud Computing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objetivo do Projeto
# MAGIC
# MAGIC Este notebook implementa um **pipeline de ingestão robusto e escalável** para carregar dados brutos de **pedidos** (CSV) na **Camada Bronze** de um Data Lakehouse utilizando **Delta Lake** no Databricks.
# MAGIC
# MAGIC O pipeline garante:
# MAGIC
# MAGIC - ✅ **Qualidade na origem** com schema enforcement
# MAGIC - 🔁 **Upsert incremental** usando `MERGE INTO`
# MAGIC - 🧠 **Rastreabilidade completa** com metadados técnicos
# MAGIC - ⚙️ **Otimização de performance** com particionamento, `OPTIMIZE ZORDER` e `VACUUM`
# MAGIC - 🛡 **Governança e confiabilidade** com registro no metastore
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧱 Arquitetura do Pipeline
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart TD
# MAGIC     A[📄 CSV - Pedidos] --> B[📥 Leitura com Schema Enforcement]
# MAGIC     B --> C[🧹 Limpeza e Validação Inicial]
# MAGIC     C --> D[🧠 Enriquecimento com data_ingestao]
# MAGIC     D --> E[🧭 Reparticionamento Estratégico]
# MAGIC     E --> F{Tabela Delta existe?}
# MAGIC     F -- Sim --> G[🔁 MERGE INTO para UPSERT]
# MAGIC     F -- Não --> H[🆕 Criação Inicial da Tabela Delta]
# MAGIC     G & H --> I[🛡 Registro no Metastore]
# MAGIC     I --> J[⚙️ OPTIMIZE ZORDER BY (PedidoID)]
# MAGIC     J --> K[🧼 VACUUM 168 HOURS]
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Técnicas e Boas Práticas Aplicadas
# MAGIC
# MAGIC | Técnica / Prática                       | Objetivo / Benefício |
# MAGIC |-----------------------------------------|----------------------|
# MAGIC | **Schema Enforcement (StructType)**    | Garante tipagem correta e evita inferência inconsistente |
# MAGIC | **Tratamento de registros inválidos**   | Remove dados malformados prevenindo falhas no MERGE |
# MAGIC | **Deduplicação e filtro de nulos**      | Assegura integridade da chave primária |
# MAGIC | **Coluna `data_ingestao`**              | Rastreabilidade e suporte a Time Travel |
# MAGIC | **Reparticionamento por chave**         | Melhora performance em escrita e consultas futuras |
# MAGIC | **`CREATE TABLE IF NOT EXISTS`**        | Garante visibilidade no catálogo e previne erros |
# MAGIC | **`MERGE INTO` (Upsert)**               | Atualiza e insere registros de forma incremental e ACID |
# MAGIC | **`OPTIMIZE ZORDER`**                   | Melhora filtragem e leitura |
# MAGIC | **`VACUUM`**                            | Reduz custo de armazenamento removendo arquivos obsoletos |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🛠 Fluxo Detalhado
# MAGIC
# MAGIC ### 1️⃣ Leitura do CSV com Schema Enforcement
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("PedidoID", StringType(), True),
# MAGIC     StructField("EstabelecimentoID", StringType(), True),
# MAGIC     StructField("Produto", StringType(), True),
# MAGIC     StructField("quantidade_vendida", IntegerType(), True),
# MAGIC     StructField("Preco_Unitario", DoubleType(), True),
# MAGIC     StructField("data_venda", DateType(), True)
# MAGIC ])
# MAGIC
# MAGIC df_raw = (
# MAGIC     spark.read.format("csv")
# MAGIC     .option("header", "true")
# MAGIC     .option("mode", "DROPMALFORMED")
# MAGIC     .schema(schema)
# MAGIC     .load("dbfs:/FileStore/Ampev/pedidos.csv")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 📌 *Por que?* — Evita inferência automática e garante consistência entre execuções.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2️⃣ Limpeza e Validação
# MAGIC
# MAGIC ```python
# MAGIC df_clean = (
# MAGIC     df_raw.dropDuplicates()
# MAGIC           .na.drop()
# MAGIC           .filter("PedidoID IS NOT NULL AND TRIM(PedidoID) != ''")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 📌 *Por que?* — Remove duplicatas, nulos e registros sem chave para evitar falhas no upsert.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3️⃣ Enriquecimento e Reparticionamento
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import current_timestamp
# MAGIC
# MAGIC df_partitioned = (
# MAGIC     df_clean.withColumn("data_ingestao", current_timestamp())
# MAGIC             .repartition("PedidoID")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 📌 *Por que?* — Adiciona rastreabilidade e melhora performance de escrita.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4️⃣ Escrita com MERGE INTO
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC if DeltaTable.isDeltaTable(spark, "dbfs:/FileStore/Ampev/tables/bronze/pedidos"):
# MAGIC     delta_table = DeltaTable.forPath(spark, "dbfs:/FileStore/Ampev/tables/bronze/pedidos")
# MAGIC     (
# MAGIC         delta_table.alias("tgt")
# MAGIC         .merge(df_partitioned.alias("src"), "tgt.PedidoID = src.PedidoID")
# MAGIC         .whenMatchedUpdateAll()
# MAGIC         .whenNotMatchedInsertAll()
# MAGIC         .execute()
# MAGIC     )
# MAGIC else:
# MAGIC     df_partitioned.write.format("delta")         .partitionBy("data_ingestao")         .option("mergeSchema", "true")         .mode("overwrite")         .save("dbfs:/FileStore/Ampev/tables/bronze/pedidos")
# MAGIC ```
# MAGIC
# MAGIC 📌 *Por que?* — Garante ingestão incremental sem sobrescrever dados íntegros.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5️⃣ Otimização e Manutenção
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE bronze.pedidos ZORDER BY (PedidoID);
# MAGIC VACUUM bronze.pedidos RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC 📌 *Por que?* — Compacta arquivos, melhora filtragem e libera espaço.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Resultado Esperado
# MAGIC
# MAGIC | Métrica                     | Valor                          |
# MAGIC |-----------------------------|--------------------------------|
# MAGIC | 📌 Tabela Delta Criada       | `bronze.pedidos`               |
# MAGIC | 🔑 Chave de Negócio         | `PedidoID`                     |
# MAGIC | 🧾 Particionamento Físico   | `data_ingestao`                 |
# MAGIC | ⚙️ Otimizações Aplicadas     | `OPTIMIZE ZORDER + VACUUM`     |
# MAGIC | 🔁 Tipo de Ingestão         | `Batch` + `MERGE INTO`         |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Conclusão
# MAGIC
# MAGIC Este pipeline é **produção-ready** e incorpora princípios modernos de engenharia de dados:
# MAGIC
# MAGIC - 📊 Qualidade desde a origem
# MAGIC - 🔁 Idempotência e resiliência
# MAGIC - ⚡ Performance otimizada para custo e velocidade
# MAGIC - 🛡 Governança e rastreabilidade completas
# MAGIC - 🚀 Pronto para evolução futura com streaming, camada Silver e Gold
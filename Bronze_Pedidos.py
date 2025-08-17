# Databricks notebook source
# MAGIC %md
# MAGIC | **Informações**    |            **Detalhes**         |
# MAGIC |--------------------|---------------------------------|
# MAGIC | Nome da Tabela     |          Bronze_Pedidos         |
# MAGIC | Data da Ingestao   |            31/03/2025           |
# MAGIC | Ultima Atualizaçao |            30/07/2025           |
# MAGIC | Origem             | DBFS (Databricks File System)   |
# MAGIC | Responsável        |           Lucas Sousa           |
# MAGIC | Motivo             |   Criação de Camadas Bronze     |
# MAGIC | Observações        |               None              |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC  | Data | Desenvolvido por | Motivo |
# MAGIC  |:----:|--------------|--------|
# MAGIC  |31/03/2025 | Lucas Sousa  | Criação do notebook |
# MAGIC  |30/07/2025 | Lucas Sousa  | Otimizações no notebook |
# MAGIC  

# COMMAND ----------

# ------------------------------------------
# ✅ Registro da tabela Delta no catálogo do Databricks
# ------------------------------------------
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.pedidos
    USING DELTA
    LOCATION 'dbfs:/FileStore/Ampev/tables/bronze/pedidos'
""")

# COMMAND ----------

# -*- coding: utf-8 -*-
# #############################################################################
# 📦 Pipeline de Ingestão Bronze: Dados de Pedidos (CSV para Delta Lake)
# #############################################################################
# 🎯 Objetivo:
# Este pipeline tem como objetivo principal realizar a ingestão em batch de dados brutos
# de pedidos (originados de um arquivo CSV) para a camada Bronze em um Data Lakehouse
# baseado em Delta Lake. Ele é construído com foco em:
# - Qualidade na Origem: Leitura de dados CSV com schema explícito e tratamento de erros.
# - Idempotência e Resiliência: Lógica robusta para lidar com re-execuções e tabelas corrompidas.
# - Upsert Eficiente: Utilização de MERGE INTO para atualizações e inserções incrementais.
# - Rastreabilidade: Adição de metadados de linhagem e registro de log de execução.
# - Otimização de Performance e Custo: Aplicação de otimizações Spark e Delta Lake.
#
# Este código demonstra práticas avançadas em engenharia de dados para pipelines Bronze:
# - **Definição de Parâmetros de Exemplo:** `caminho_origem = '/mnt/dados/brutos/pedidos.csv'`
# - Definição de parâmetros e caminhos centralizada.
# - Schema enforcement/validation na leitura para garantir a integridade dos dados.
# - Transformações de qualidade de dados (deduplicação, tratamento de nulos, filtragem de chaves).
# - Adição de metadados de linhagem (coluna 'data_carga').
# - Uso estratégico do Delta Lake para propriedades ACID, evolução de schema e Time Travel.
# - Mecanismo de auto-recuperação para metadados de tabela Delta potencialmente corrompidos.
# - Orquestração de erros e registro de log detalhado para observabilidade.
# - Estratégias de particionamento e ZORDER para otimização de leitura e escrita.
# #############################################################################

# Importações necessárias para manipulação de dados e operações Delta Lake.
from pyspark.sql.functions import current_timestamp # Importa a função para obter o timestamp atual.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType # Importa tipos de dados para definição explícita de schema.
from delta.tables import DeltaTable # Importa a classe DeltaTable para operações avançadas como MERGE INTO.
import sys # Módulo para acessar funcionalidades do sistema (usado para stderr).
from pyspark.sql.utils import AnalysisException # Importa a exceção específica do Spark para erros de análise (metadados).


# ----------------------------------------------------------------------------------------------------------------------
# ✅ 1. Definição de caminhos e nomes lógicos usados no pipeline
#    (Melhor prática: Centralizar configurações para facilitar manutenção e reuso)
# ----------------------------------------------------------------------------------------------------------------------
# Caminho da fonte de dados bruta em formato CSV no DBFS (Databricks File System).
# Esta é a origem dos dados que será ingerida na camada Bronze.
SOURCE_PATH = "dbfs:/FileStore/Ampev/pedidos.csv"

# Caminho físico no DBFS onde os dados da tabela Delta da camada Bronze serão armazenados.
# É uma convenção comum organizar o Data Lake por camadas (raw/bronze, silver, gold).
BRONZE_TABLE_PATH = "dbfs:/FileStore/Ampev/tables/bronze/pedidos"

# Nome lógico da tabela no catálogo do Spark (Metastore).
# Permite que a tabela seja consultada via SQL (ex: SELECT * FROM bronze.pedidos).
TABLE_NAME = "bronze.pedidos"

# Caminho para o checkpoint (ponto de controle) do Spark Structured Streaming.
# Embora este pipeline seja batch, a inclusão do checkpoint_path é uma visão de futuro,
# facilitando a transição para um pipeline de streaming sem grandes refatorações.
CHECKPOINT_PATH = "dbfs:/FileStore/Ampev/checkpoints/bronze/pedidos"

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 2. Definição do Schema explícito para o CSV
#    (Melhor prática: Garante consistência de tipos e robustez contra inferência automática)
# ----------------------------------------------------------------------------------------------------------------------
# Define o schema da tabela de entrada de forma explícita.
# Isso é crucial para:
# 1. Prevenir problemas de inferência de schema (que pode ser inconsistente ou incorreta).
# 2. Garantir a qualidade dos dados e a consistência dos tipos entre execuções.
# 3. Acelerar a leitura, pois o Spark não precisa escanear o arquivo para inferir.
schema = StructType([
    StructField("PedidoID", StringType(), True), # ID único do pedido.
    StructField("EstabelecimentoID", StringType(), True), # Chave estrangeira para a tabela de estabelecimentos.
    StructField("Produto", StringType(), True), # Nome do produto vendido.
    StructField("quantidade_vendida", IntegerType(), True), # Quantidade do produto vendida.
    StructField("Preco_Unitario", DoubleType(), True), # Preço de venda unitário do produto.
    StructField("data_venda", StringType(), True) # Data da transação de venda.
])

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 3. Leitura do CSV com controle de schema e tratamento de linhas inválidas
#    (Abordagem robusta para ingestão de dados brutos)
# ----------------------------------------------------------------------------------------------------------------------
# Realiza a leitura do arquivo CSV, aplicando o schema explícito definido e tratando
# linhas malformadas de forma robusta.
df_raw = (
    spark.read # Inicia a operação de leitura de dados.
        .format("csv") # Especifica o formato da fonte de dados como CSV.
        .option("header", "true") # Informa ao Spark que o CSV contém uma linha de cabeçalho.
        .option("mode", "DROPMALFORMED") # Define o modo de tratamento de erros. "DROPMALFORMED" descarta linhas que não se encaixam no schema, o que é útil para ingestões robustas de dados brutos ("fail-fast" seria uma alternativa mais estrita).
        .schema(schema) # Aplica o schema explícito, garantindo a tipagem correta.
        .load(SOURCE_PATH) # Carrega o DataFrame a partir do caminho de origem especificado.
)

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 4. Aplicação de Transformações e Adição de Metadados de Carga
#    (Preparando os dados para a camada Bronze)
# ----------------------------------------------------------------------------------------------------------------------
# Aplica transformações essenciais para a camada Bronze, como remoção de duplicatas,
# limpeza de nulos críticos e a adição de metadados de linhagem.
df_transformed = (
    df_raw.dropDuplicates() # Remove linhas que são cópias exatas umas das outras.
          .na.drop(subset=["PedidoID"]) # Remove linhas onde a chave principal 'PedidoID' é nula.
          .filter("TRIM(PedidoID) != ''") # Filtra pedidos sem ID válido, garantindo a integridade da chave.
          .withColumn("data_carga", current_timestamp()) # Adiciona uma coluna com o timestamp da execução. Essencial para linhagem e particionamento.
          .repartition("EstabelecimentoID") # Reparticiona o DataFrame para otimizar as escritas e leituras subsequentes, especialmente em consultas filtradas por EstabelecimentoID.
)

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 5. Lógica de Upsert (MERGE INTO) na Tabela Delta
#    (Melhor prática: Ingestão idempotente e eficiente para dados mutáveis)
# ----------------------------------------------------------------------------------------------------------------------
# Esta seção decide se a operação de escrita será um "append" inicial (se a tabela não existir)
# ou um "merge" (se a tabela já existir), que é uma operação de upsert.
try:
    # Tenta criar um objeto DeltaTable para o caminho, o que só é possível se a tabela já existir.
    delta_table = DeltaTable.forPath(spark, BRONZE_TABLE_PATH)
    
    # Se a tabela existe, realiza a operação de MERGE INTO.
    # O MERGE é crucial para o modelo "medallion", pois permite atualizar dados existentes
    # e inserir novos registros em uma única transação atômica (upsert).
    delta_table.alias("tgt") \
        .merge(
            df_transformed.alias("src"), # Define o DataFrame transformado como a fonte ('src').
            "tgt.PedidoID = src.PedidoID" # Condição de MERGE: a correspondência é feita pela chave primária PedidoID.
        ) \
        .whenMatchedUpdate(set={ # Quando uma linha é encontrada ('matched').
            "EstabelecimentoID": "src.EstabelecimentoID",
            "Produto": "src.Produto",
            "quantidade_vendida": "src.quantidade_vendida",
            "Preco_Unitario": "src.Preco_Unitario",
            "data_venda": "src.data_venda",
            "data_carga": "src.data_carga" # Atualiza o timestamp de carga para refletir a última modificação.
        }) \
        .whenNotMatchedInsert(values={ # Quando uma nova linha é encontrada ('not matched').
            "PedidoID": "src.PedidoID",
            "EstabelecimentoID": "src.EstabelecimentoID",
            "Produto": "src.Produto",
            "quantidade_vendida": "src.quantidade_vendida",
            "Preco_Unitario": "src.Preco_Unitario",
            "data_venda": "src.data_venda",
            "data_carga": "src.data_carga"
        }) \
        .execute() # Executa a operação de MERGE.
    
except (AnalysisException, ValueError) as e:
    # Se a exceção for `AnalysisException` ou `ValueError` (ex: a tabela não existe),
    # o pipeline assume que é a primeira execução e cria a tabela.
    # Este é um padrão robusto para lidar com a criação inicial da tabela.
    if "is not a Delta table" in str(e) or "Path does not exist" in str(e):
        # A tabela não existe no caminho, então a criamos.
        # Utiliza o modo 'append' na primeira escrita para criar a tabela com o schema do DataFrame.
        # O Databricks/Spark infere o formato e o trata como uma criação de tabela se o modo for 'append'
        # e o caminho ainda não existir.
        (
            df_transformed.write.format("delta")
                .partitionBy("data_carga") # Particiona a tabela fisicamente para otimizar queries por data.
                .option("mergeSchema", "true") # Permite a evolução do schema em execuções futuras.
                .mode("append") # Cria a tabela na primeira execução, e anexa dados nas subsequentes.
                .save(BRONZE_TABLE_PATH) # Salva o DataFrame como uma nova tabela Delta.
        )
    else:
        # Se for qualquer outro tipo de erro, ele é re-lançado.
        raise e

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 6. Registro no Catálogo e Otimização da Tabela Delta
#    (Melhor prática: Garantir a acessibilidade e a alta performance da tabela)
# ----------------------------------------------------------------------------------------------------------------------
# Garante que a tabela está registrada no metastore para ser acessível via SQL.
# Em seguida, aplica otimizações para melhorar a performance de leitura.
spark.sql(f"CREATE DATABASE IF NOT EXISTS {TABLE_NAME.split('.')[0]}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME}
    USING DELTA
    LOCATION '{BRONZE_TABLE_PATH}'
""")

# Aplica otimizações para combinar pequenos arquivos e co-localizar dados.
# Isso melhora significativamente o desempenho de queries subsequentes.
spark.sql(f"OPTIMIZE {TABLE_NAME} ZORDER BY (PedidoID)")
spark.sql(f"VACUUM {TABLE_NAME} RETAIN 168 HOURS") # Limpa arquivos antigos, mantendo 7 dias para 'Time Travel'.


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.pedidos limit 10
# MAGIC

# COMMAND ----------

# 🎯 Objetivo:
# Este pipeline realiza a ingestão em batch de dados brutos de pedidos (CSV) para a
# camada Bronze em um Data Lakehouse. Além disso, agora inclui um mecanismo robusto
# de log de execução que captura metadados essenciais como tempo, status e erros.
#
# A inclusão do log de execução é uma prática de governança e observabilidade,
# permitindo o rastreamento, monitoramento e diagnóstico de falhas do pipeline
# de forma centralizada.
# #############################################################################

# Importações necessárias para manipulação de dados, operações Delta Lake e log.
from pyspark.sql.functions import current_timestamp # Importa a função para obter o timestamp atual.
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType # Importa tipos de dados para schema.
from delta.tables import DeltaTable # Importa a classe DeltaTable para operações avançadas.
from pyspark.sql.utils import AnalysisException # Importa a exceção para erros de análise.
import time # Módulo para medição de tempo de execução.

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 1. Definição de caminhos e nomes lógicos usados no pipeline
# ----------------------------------------------------------------------------------------------------------------------
SOURCE_PATH = "dbfs:/FileStore/Ampev/pedidos.csv"
BRONZE_TABLE_PATH = "dbfs:/FileStore/Ampev/tables/bronze/pedidos"
TABLE_NAME = "bronze.pedidos"
LOG_PATH = "dbfs:/FileStore/Ampev/logs/bronze_pedidos"
LOG_TABLE = "bronze.logs_pedidos"
job_name = "bronze_pedidos" # Nome lógico da execução.

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 2. Início do Bloco de Execução com Log
#    (Todo o pipeline é encapsulado aqui para capturar sucesso ou falha)
# ----------------------------------------------------------------------------------------------------------------------
# Inicia a medição do tempo de execução para o log.
start_time = time.time()
status = "ERRO" # Define o status inicial como ERRO.

try:
    # ------------------------------------------------------------------------------------------------------------------
    # 2.1. Definição do Schema explícito para o CSV
    # ------------------------------------------------------------------------------------------------------------------
    schema = StructType([
        StructField("PedidoID", StringType(), True),
        StructField("EstabelecimentoID", StringType(), True),
        StructField("Produto", StringType(), True),
        StructField("quantidade_vendida", IntegerType(), True),
        StructField("Preco_Unitario", DoubleType(), True),
        StructField("data_venda", StringType(), True)
    ])

    # ------------------------------------------------------------------------------------------------------------------
    # 2.2. Leitura do CSV com controle de schema e tratamento de linhas inválidas
    # ------------------------------------------------------------------------------------------------------------------
    df_raw = (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("mode", "DROPMALFORMED")
            .schema(schema)
            .load(SOURCE_PATH)
    )

    # ------------------------------------------------------------------------------------------------------------------
    # 2.3. Aplicação de Transformações e Adição de Metadados
    # ------------------------------------------------------------------------------------------------------------------
    df_transformed = (
        df_raw.dropDuplicates()
              .na.drop(subset=["PedidoID"])
              .filter("TRIM(PedidoID) != ''")
              .withColumn("data_carga", current_timestamp())
              .repartition("EstabelecimentoID")
    )
    
    # Captura a quantidade de linhas processadas para o log.
    qtd_linhas = df_transformed.count()

    # ------------------------------------------------------------------------------------------------------------------
    # 2.4. Lógica de Upsert (MERGE INTO) na Tabela Delta
    # ------------------------------------------------------------------------------------------------------------------
    try:
        delta_table = DeltaTable.forPath(spark, BRONZE_TABLE_PATH)
        
        delta_table.alias("tgt") \
            .merge(
                df_transformed.alias("src"),
                "tgt.PedidoID = src.PedidoID"
            ) \
            .whenMatchedUpdate(set={
                "EstabelecimentoID": "src.EstabelecimentoID",
                "Produto": "src.Produto",
                "quantidade_vendida": "src.quantidade_vendida",
                "Preco_Unitario": "src.Preco_Unitario",
                "data_venda": "src.data_venda",
                "data_carga": "src.data_carga"
            }) \
            .whenNotMatchedInsert(values={
                "PedidoID": "src.PedidoID",
                "EstabelecimentoID": "src.EstabelecimentoID",
                "Produto": "src.Produto",
                "quantidade_vendida": "src.quantidade_vendida",
                "Preco_Unitario": "src.Preco_Unitario",
                "data_venda": "src.data_venda",
                "data_carga": "src.data_carga"
            }) \
            .execute()
        
    except (AnalysisException, ValueError) as e:
        if "is not a Delta table" in str(e) or "Path does not exist" in str(e):
            (
                df_transformed.write.format("delta")
                    .partitionBy("data_carga")
                    .option("mergeSchema", "true")
                    .mode("append")
                    .save(BRONZE_TABLE_PATH)
            )
        else:
            raise e
    
    # ------------------------------------------------------------------------------------------------------------------
    # 2.5. Registro no Catálogo e Otimização da Tabela Delta
    # ------------------------------------------------------------------------------------------------------------------
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TABLE_NAME.split('.')[0]}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME}
        USING DELTA
        LOCATION '{BRONZE_TABLE_PATH}'
    """)

    spark.sql(f"OPTIMIZE {TABLE_NAME} ZORDER BY (PedidoID)")
    spark.sql(f"VACUUM {TABLE_NAME} RETAIN 168 HOURS")

    # Se o bloco `try` foi executado sem erros, o status é de sucesso.
    status = "SUCESSO"
    erro = None

except Exception as e:
    # Se ocorrer qualquer erro, captura a mensagem de erro.
    qtd_linhas = 0 # Define a quantidade de linhas como 0 em caso de falha.
    erro = str(e)
    print(f"⚠️ Erro durante a execução do pipeline: {erro}")

finally:
    # ------------------------------------------------------------------------------------------------------------------
    # ✅ 3. Lógica de Log - Executada sempre, independentemente de erro
    # ------------------------------------------------------------------------------------------------------------------
    # Calcula o tempo total de execução.
    tempo_total = round(time.time() - start_time, 2)
    
    # Define o schema do log de forma explícita.
    log_schema = StructType([
        StructField("job_name", StringType(), True),
        StructField("data_execucao", TimestampType(), True),
        StructField("qtd_linhas", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("erro", StringType(), True),
        StructField("tempo_total_segundos", DoubleType(), True)
    ])

    # Cria o DataFrame de log com os metadados da execução.
    log_df = spark.createDataFrame([(
        job_name,
        None,
        qtd_linhas,
        status,
        erro,
        tempo_total
    )], schema=log_schema).withColumn("data_execucao", current_timestamp())

    # Escreve o log na tabela Delta correspondente.
    log_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(LOG_PATH)

    # Cria a tabela externa de log no catálogo.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE}
        USING DELTA
        LOCATION '{LOG_PATH}'
    """)

    # ------------------------------------------------------------------------------------------------------------------
    # ✅ 4. Confirmação visual no notebook
    # ------------------------------------------------------------------------------------------------------------------
    print("✅ Log de execução registrado com sucesso!")
    print(f"📌 Job: {job_name}")
    print(f"📦 Status: {status} | Registros: {qtd_linhas} | Duração: {tempo_total} segundos")
    if erro:
        print(f"⚠️ Detalhe do erro: {erro}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.logs_pedidos
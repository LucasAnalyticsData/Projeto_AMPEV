# Databricks notebook source
# MAGIC %md
# MAGIC | **Informações**    |            **Detalhes**         |
# MAGIC |--------------------|---------------------------------|
# MAGIC | Nome da Tabela     |     Bronze_Estabelecimentos     |
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS spark_catalog.bronze
# MAGIC LOCATION 'dbfs:/FileStore/Ampev/tables/bronze/estabelecimentos';

# COMMAND ----------

# -*- coding: utf-8 -*-
# #############################################################################
# 📦 Pipeline de Ingestão Bronze: Dados de Estabelecimentos (CSV para Delta Lake)
# #############################################################################
# 🎯 Objetivo:
# Este pipeline tem como objetivo principal realizar a ingestão em batch de dados brutos
# de estabelecimentos (originados de um arquivo CSV) para a camada Bronze em um Data Lakehouse
# baseado em Delta Lake. Ele é construído com foco em:
# - **Qualidade na Origem:** Leitura de dados CSV com schema explícito e tratamento de erros.
# - **Idempotência e Resiliência:** Lógica robusta para lidar com re-execuções e tabelas corrompidas.
# - **Upsert Eficiente:** Utilização de MERGE INTO para atualizações e inserções incrementais.
# - **Rastreabilidade:** Adição de metadados de linhagem e registro de log de execução.
# - **Otimização de Performance e Custo:** Aplicação de otimizações Spark e Delta Lake.
#
# Este código demonstra práticas avançadas em engenharia de dados para pipelines Bronze:
# - Definição de parâmetros e caminhos centralizada.
# - Schema enforcement/validation na leitura para garantir a integridade dos dados.
# - Transformações de qualidade de dados (deduplicação, tratamento de nulos, filtragem de chaves).
# - Adição de metadados de linhagem (coluna 'data_carga').
# - Uso estratégico do Delta Lake para propriedades ACID, evolução de schema e Time Travel.
# - Mecanismo de auto-recuperação para metadados de tabela Delta potencialmente corrompidos.
# - Orquestração de erros e registro de log detalhado para observabilidade.
# - Estratégias de particionamento e ZORDER para otimização de leitura e escrita.
# #############################################################################

# 📦 Pipeline Bronze - Ingestão Batch de Estabelecimentos (CSV ➝ Delta)
# 🧠 Objetivo: Carregar dados brutos para a camada Bronze com qualidade, rastreabilidade,
#             upsert eficiente e otimizações do Delta Lake.
#             Este pipeline foi projetado para ser idempotente e resiliente.

# Importações necessárias para manipulação de dados e operações Delta Lake.
from pyspark.sql.functions import current_timestamp # Importa a função para obter o timestamp atual.
from pyspark.sql.types import StructType, StructField, StringType # Importa tipos de dados para definição explícita de schema.
from delta.tables import DeltaTable # Importa a classe DeltaTable para operações avançadas como MERGE INTO.

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 1. Definição de caminhos e nomes lógicos usados no pipeline
#    (Melhor prática: Centralizar configurações para facilitar manutenção e reuso)
# ----------------------------------------------------------------------------------------------------------------------
# Caminho da fonte de dados bruta em formato CSV no DBFS (Databricks File System).
# Esta é a origem dos dados que será ingerida na camada Bronze.
SOURCE_PATH = "dbfs:/FileStore/Ampev/estabelecimentos.csv"

# Caminho físico no DBFS onde os dados da tabela Delta da camada Bronze serão armazenados.
# É uma convenção comum organizar o Data Lake por camadas (raw/bronze, silver, gold).
BRONZE_TABLE_PATH = "dbfs:/FileStore/Ampev/tables/bronze/estabelecimentos"

# Nome lógico da tabela no catálogo do Spark (Metastore).
# Permite que a tabela seja consultada via SQL (ex: SELECT * FROM bronze.estabelecimentos).
TABLE_NAME = "bronze.estabelecimentos"

# Caminho para o checkpoint (ponto de controle) do Spark Structured Streaming.
# Embora este pipeline seja batch, a inclusão do checkpoint_path é uma visão de futuro,
# facilitando a transição para um pipeline de streaming sem grandes refatorações.
CHECKPOINT_PATH = "dbfs:/FileStore/Ampev/checkpoints/bronze/estabelecimentos"

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
    StructField("Local", StringType(), True), # Campo 'Local', tipo String, permitindo nulos.
    StructField("Email", StringType(), True), # Campo 'Email', tipo String, permitindo nulos.
    StructField("EstabelecimentoID", StringType(), True), # Campo 'EstabelecimentoID', tipo String, permitindo nulos.
    StructField("Telefone", StringType(), True) # Campo 'Telefone', tipo String, permitindo nulos.
])

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 3. Leitura do CSV com controle de schema e tratamento de linhas inválidas
#    (Abordagem robusta para ingestão de dados brutos)
# ----------------------------------------------------------------------------------------------------------------------
df_raw = (
    spark.read # Inicia a operação de leitura de dados.
        .format("csv") # Especifica o formato da fonte de dados como CSV.
        .option("header", "true") # Informa ao Spark que o CSV contém uma linha de cabeçalho.
        .option("mode", "DROPMALFORMED") # Define o modo de tratamento de registros malformados.
                                         # - "DROPMALFORMED": Ignora (descarta) linhas que não se encaixam no schema.
                                         # - Alternativas: "PERMISSIVE" (default, insere _corrupt_record) ou "FAILFAST" (aborta a operação).
                                         #   "DROPMALFORMED" é comum na Bronze para ingestão rápida, mas "PERMISSIVE"
                                         #   pode ser preferível para capturar e analisar dados ruins posteriormente.
        .schema(schema) # Aplica o schema explicitamente definido, garantindo a tipagem correta.
        .load(SOURCE_PATH) # Carrega os dados do caminho da fonte especificado.
)

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 4. Limpeza e validação inicial dos dados brutos
#    (Garante a integridade mínima para a chave primária na camada Bronze)
# ----------------------------------------------------------------------------------------------------------------------
df_clean = (
    df_raw
        .dropDuplicates() # Remove linhas duplicadas em *todas* as colunas do DataFrame.
                          # Essencial para garantir a idempotência do pipeline e evitar dados redundantes.
        .na.drop() # Remove linhas que contêm pelo menos um valor nulo em *qualquer* coluna (comportamento padrão 'any').
                   # Para a Bronze, pode ser útil ser mais permissivo e tratar nulos na Silver,
                   # mas aqui removemos registros incompletos que podem prejudicar o MERGE.
        .filter("EstabelecimentoID IS NOT NULL AND TRIM(EstabelecimentoID) != ''") # Filtra para garantir que a chave
                                                                                # 'EstabelecimentoID' não seja nula ou vazia.
                                                                                # Uma chave primária válida é crucial para o UPSERT.
)

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 5. Enriquecimento com coluna de auditoria
#    (Fundamental para rastreabilidade, governança de dados e Time Travel)
# ----------------------------------------------------------------------------------------------------------------------
# Adiciona uma coluna 'data_ingestao' com o timestamp exato do momento da carga.
# Esta coluna é vital para:
# - Auditoria: Saber quando um registro foi processado pela última vez.
# - Rastreabilidade: Acompanhar a origem e o tempo de vida dos dados.
# - Time Travel do Delta Lake: Permite consultas a versões específicas dos dados baseadas no tempo.
df_enriched = df_clean.withColumn("data_ingestao", current_timestamp())

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 6. Reparticionamento para otimização de escrita e leitura
#    (Estratégia de performance para operações futuras)
# ----------------------------------------------------------------------------------------------------------------------
# Reparticiona o DataFrame com base na coluna 'EstabelecimentoID'.
# - Benefícios:
#   - Otimiza operações de escrita no Delta Lake, pois os dados com o mesmo ID são agrupados.
#   - Melhora o desempenho de consultas futuras que filtram ou fazem join por 'EstabelecimentoID'.
#   - Reduz o volume de dados a serem embaralhados (shuffle) em operações subsequentes.
# - Considerações: 'repartition' força um shuffle completo dos dados. O número de partições padrão
#   será o número de cores do cluster ou o valor de spark.sql.shuffle.partitions.
#   Para datasets muito grandes, pode ser necessário especificar o número de partições.
df_partitioned = df_enriched.repartition("EstabelecimentoID")

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 7. Criação do banco de dados lógico Bronze, se ainda não existir
#    (Organização do Metastore para acesso via SQL)
# ----------------------------------------------------------------------------------------------------------------------
# Garante que o banco de dados 'bronze' exista no catálogo do Spark (Metastore).
# Isso é um pré-requisito para registrar tabelas dentro dele e consultá-las por nome qualificado (ex: bronze.estabelecimentos).
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 8. Registro da Tabela Delta no Catálogo (CREATE TABLE com LOCATION)
#    (Passo CRUCIAL: Garante que a tabela esteja sempre visível para comandos SQL)
# ----------------------------------------------------------------------------------------------------------------------
# Este comando é executado ANTES do bloco IF/ELSE de escrita.
# Isso garante que a tabela lógica 'bronze.estabelecimentos' esteja sempre registrada no Metastore,
# independentemente de ser a primeira execução ou uma subsequente.
# Se a tabela já existe fisicamente mas foi desregistrada, este comando a reconecta.
# Isso resolve o erro "TABLE_OR_VIEW_NOT_FOUND" para operações SQL como OPTIMIZE e VACUUM.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME}
    USING DELTA
    LOCATION '{BRONZE_TABLE_PATH}'
""")

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 9. Lógica de Escrita Delta: MERGE INTO para UPSERT ou Criação Inicial
#    (Abordagem idempotente e eficiente para a camada Bronze)
# ----------------------------------------------------------------------------------------------------------------------
# Verifica se o caminho Delta já contém uma tabela Delta.
if DeltaTable.isDeltaTable(spark, BRONZE_TABLE_PATH):
    # 🔁 Cenário: A tabela Delta já existe fisicamente. Realiza um UPSERT (MERGE INTO).
    # O MERGE INTO é a operação preferida para cargas incrementais na Bronze, pois:
    # - É atômico e garante as propriedades ACID (Atomicidade, Consistência, Isolamento, Durabilidade).
    # - Permite atualizar registros existentes e inserir novos em uma única transação.
    # - É eficiente, pois processa apenas as mudanças.
    delta_table = DeltaTable.forPath(spark, BRONZE_TABLE_PATH) # Instancia um objeto DeltaTable para a tabela alvo.

    (
        delta_table.alias("tgt") # Define um alias 'tgt' (target) para a tabela Delta existente.
        .merge(
            df_partitioned.alias("src"), # Define um alias 'src' (source) para o DataFrame de entrada.
            "tgt.EstabelecimentoID = src.EstabelecimentoID" # Condição de JOIN para identificar registros correspondentes.
                                                            # 'EstabelecimentoID' é a chave primária para o UPSERT.
        )
        .whenMatchedUpdate(set={ # Regra para quando um registro na fonte (src) corresponde a um no alvo (tgt).
            "Local": "src.Local", # Atualiza a coluna 'Local' com o valor da fonte.
            "Email": "src.Email", # Atualiza a coluna 'Email' com o valor da fonte.
            "Telefone": "src.Telefone", # Atualiza a coluna 'Telefone' com o valor da fonte.
            "data_ingestao": "src.data_ingestao" # Atualiza a data de ingestão para refletir a última modificação.
                                                # Isso é crucial para o Time Travel e auditoria de atualização.
        })
        .whenNotMatchedInsert(values={ # Regra para quando um registro na fonte (src) NÃO corresponde a nenhum no alvo (tgt).
            "EstabelecimentoID": "src.EstabelecimentoID", # Insere o 'EstabelecimentoID' do novo registro.
            "Local": "src.Local", # Insere o 'Local' do novo registro.
            "Email": "src.Email", # Insere o 'Email' do novo registro.
            "Telefone": "src.Telefone", # Insere o 'Telefone' do novo registro.
            "data_ingestao": "src.data_ingestao" # Insere a data de ingestão para o novo registro.
        })
        .execute() # Executa a operação MERGE INTO.
    )
else:
    # 🆕 Cenário: A tabela Delta NÃO existe fisicamente. Realiza a criação inicial.
    # Esta é a primeira carga de dados para o caminho da Bronze.
    (
        df_partitioned.write # Inicia a operação de escrita do DataFrame.
            .format("delta") # Especifica o formato de saída como Delta Lake.
            .partitionBy("data_ingestao") # Particiona fisicamente os dados no sistema de arquivos por 'data_ingestao'.
                                          # Isso otimiza consultas que filtram por data/período, comum em Data Lakes.
            .option("mergeSchema", "true") # Habilita a evolução automática de schema.
                                           # Permite que novas colunas na fonte sejam adicionadas automaticamente
                                           # à tabela Delta sem falhar o pipeline. Essencial para a flexibilidade da Bronze.
            .mode("overwrite") # Define o modo de escrita como 'overwrite'.
                               # Para a primeira carga, 'overwrite' garante um estado limpo e inicial da tabela.
                               # Se a intenção fosse sempre adicionar (mesmo na primeira carga), 'append' seria usado,
                               # mas 'overwrite' é mais seguro para garantir a integridade do primeiro snapshot.
            .save(BRONZE_TABLE_PATH) # Salva o DataFrame como uma tabela Delta no caminho especificado.
    )

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 10. Otimizações do Delta Lake
#    (Melhoram drasticamente a performance de leitura e gerenciam o custo de armazenamento)
# ----------------------------------------------------------------------------------------------------------------------
# As operações de OPTIMIZE e VACUUM são executadas após o registro da tabela no catálogo.

# Otimiza a tabela Delta, compactando pequenos arquivos em arquivos maiores.
# Aplica ZORDER BY na coluna 'EstabelecimentoID' para otimizar consultas que filtram por esta coluna.
spark.sql(f"""
    OPTIMIZE {TABLE_NAME}
    ZORDER BY (EstabelecimentoID)
""")

# Remove arquivos de dados antigos não referenciados pela tabela Delta, liberando espaço.
# Retém o histórico por 168 horas (7 dias) para Time Travel.
spark.sql(f"""
    VACUUM {TABLE_NAME} RETAIN 168 HOURS
""")

# ----------------------------------------------------------------------------------------------------------------------
# ✅ 11. Log técnico de auditoria
#    (Confirmação visual do sucesso do pipeline)
# ----------------------------------------------------------------------------------------------------------------------
print("✅ Pipeline Bronze finalizado com sucesso! Tabela disponível em:", TABLE_NAME)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.estabelecimentos LIMIT 10

# COMMAND ----------

# 📋 Registro de Log de Execução Delta (com diagnóstico embutido)
# 🧠 Objetivo: capturar execução de pipelines com rastreabilidade robusta e confiável

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
import time

# ------------------------------------------
# ✅ 1. Início da medição de tempo
# ------------------------------------------
start_time = time.time()

# ------------------------------------------
# ✅ 2. Nome lógico da execução (ajuste conforme o pipeline)
# ------------------------------------------
job_name = "bronze_estabelecimentos"

try:
    # ------------------------------------------
    # ✅ 3. Simulação de processamento do pipeline (substitua pelo seu df real)
    # Exemplo: leitura, transformação, escrita...
    # ------------------------------------------
    df = spark.read.option("header", "true").csv("dbfs:/FileStore/Ampev/estabelecimentos.csv")
    df = df.dropDuplicates().na.drop()
    
    # Validação de schema mínimo
    if "EstabelecimentoID" not in df.columns:
        raise Exception("Coluna obrigatória 'EstabelecimentoID' não encontrada no DataFrame.")

    # Contagem dos registros processados
    qtd_linhas = df.count()
    
    # Se chegou até aqui, deu tudo certo
    status = "SUCESSO"
    erro = None

except Exception as e:
    # Em caso de qualquer erro: captura detalhes
    qtd_linhas = 0
    status = "ERRO"
    erro = str(e)

# ------------------------------------------
# ✅ 4. Cálculo do tempo de execução
# ------------------------------------------
tempo_total = round(time.time() - start_time, 2)

# ------------------------------------------
# ✅ 5. Definição do schema do log (tipagem explícita)
# ------------------------------------------
log_schema = StructType([
    StructField("job_name", StringType(), True),
    StructField("data_execucao", TimestampType(), True),
    StructField("qtd_linhas", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("erro", StringType(), True),
    StructField("tempo_total_segundos", DoubleType(), True)
])

# ------------------------------------------
# ✅ 6. Criação do DataFrame de log
# ------------------------------------------
log_df = spark.createDataFrame([(
    job_name,
    None,            # Será preenchido com timestamp na próxima linha
    qtd_linhas,
    status,
    erro,
    tempo_total
)], schema=log_schema).withColumn("data_execucao", current_timestamp())

# ------------------------------------------
# ✅ 7. Caminhos para armazenamento do log
# ------------------------------------------
LOG_PATH = "dbfs:/FileStore/Ampev/logs/bronze_estabelecimentos"
LOG_TABLE = "bronze.logs_estabelecimentos"

# ------------------------------------------
# ✅ 8. Escrita segura em Delta Lake
# - Cria tabela se não existir
# - Permite evolução de schema
# ------------------------------------------
log_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(LOG_PATH)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE}
    USING DELTA
    LOCATION '{LOG_PATH}'
""")

# ------------------------------------------
# ✅ 9. Confirmação visual no notebook
# ------------------------------------------
print("✅ Log de execução registrado com sucesso!")
print(f"📌 Job: {job_name}")
print(f"📦 Status: {status} | Registros: {qtd_linhas} | Duração: {tempo_total} segundos")
if erro:
    print(f"⚠️ Detalhe do erro: {erro}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.logs_estabelecimentos
# Databricks notebook source
# MAGIC %md
# MAGIC | **Informações**    |            **Detalhes**         |
# MAGIC |--------------------|---------------------------------|
# MAGIC | Nome da Tabela     |          Silver_Pedidos         |
# MAGIC | Data da Ingestao   |            31/03/2025           |
# MAGIC | Ultima Atualizaçao |            30/07/2025           |
# MAGIC | Origem             | bronze.pedidos/bronze.estabelecimentos   |
# MAGIC | Responsável        |           Lucas Sousa           |
# MAGIC | Motivo             |   Criação de Camadas Silver     |
# MAGIC | Observações        |               None              |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC  | Data | Desenvolvido por | Motivo |
# MAGIC  |:----:|--------------|--------|
# MAGIC  |27/05/2025 | Lucas Sousa  | Criação do notebook |
# MAGIC  |30/07/2025 | Lucas Sousa  | Otimizações no notebook |
# MAGIC

# COMMAND ----------

# -*- coding: utf-8 -*-
# #############################################################################
# 📦 Pipeline Silver - Transformação e Enriquecimento de Pedidos
# Autor: Lucas Sousa, especialista em Engenharia de Dados
# #############################################################################
# 🎯 Objetivo do Código:
# Este script tem a missão de atuar como o principal orquestrador do pipeline
# da camada Silver. Ele executa as seguintes tarefas:
# 1. Lê os dados brutos de 'pedidos' e 'estabelecimentos' da camada Bronze.
# 2. Aplica transformações de limpeza, tipagem correta e deduplicação.
# 3. Enriquecimento: Une os dados de pedidos com as informações dos estabelecimentos.
# 4. Persistência: Grava o resultado na camada Silver usando Delta Lake,
#    garantindo idempotência (evita duplicatas na re-execução) e evolução de schema.
# 5. Governança: Registra a tabela no metastore do Spark com documentação (comentários).
# 6. Otimização: Compacta e indexa a tabela Delta para acelerar consultas futuras.
#
# Demonstra domínio técnico em:
# - Modelagem de dados (Bronze -> Silver).
# - Uso de SQL e DataFrames para transformações.
# - Gerenciamento de tabela Delta Lake (MERGE, OPTIMIZE, VACUUM).
# - Tratamento de erros e lógica robusta de escrita (criação vs. atualização).
# - Boas práticas de governança (metadados e comentários).
# - Automação de pipelines de dados.
# #############################################################################

# -----------------------------------------------------------------------------
# 🧩 Módulo 1: Importações Essenciais
# -----------------------------------------------------------------------------
# Importa funções do Spark SQL para manipulação de colunas e expressões.
from pyspark.sql.functions import current_date, current_timestamp, expr 
# Importa a classe principal para interagir com tabelas Delta.
from delta.tables import DeltaTable
# Importa módulo para tratamento de exceções específicas do Spark.
from pyspark.sql.utils import AnalysisException
# Importa módulo para interagir com o sistema de arquivos do Databricks (DBFS).
import os 
import sys # Módulo para escrever logs de erro no stderr.

# -----------------------------------------------------------------------------
# ⚙️ Módulo 2: Definições de Parâmetros e Metadados
# -----------------------------------------------------------------------------
# Define o nome do banco de dados (schema) para a camada Silver.
database = "silver"
# Define o nome da tabela de destino.
tabela = "pedidos"
# Constrói o nome completo da tabela para uso em comandos SQL.
tabela_completa = f"{database}.{tabela}"
# Define o caminho físico (localização) da tabela Delta no DBFS.
silver_path = "dbfs:/FileStore/Ampev/silver/tables/pedidos"

# Comentários de documentação para a tabela e suas colunas (governança de dados).
# Essa documentação é crucial para que os usuários finais (analistas, cientistas de dados)
# entendam a origem, o propósito e a estrutura dos dados.
comentario_tabela = "Esta tabela contém pedidos enriquecidos com dados de estabelecimentos para análise corporativa, pronta para consumo em relatórios e dashboards."
comentarios_colunas = {
    'id_pedido': 'Identificador único do pedido, agora com tipagem INT.',
    'id_estabelecimento': 'Código do estabelecimento associado ao pedido. Mantido como STRING para compatibilidade com a origem e evitar erros de CAST.',
    'produto': 'Nome do produto vendido no pedido.',
    'quantidade': 'Quantidade de itens vendidos, tipado como INT.',
    'preco': 'Preço unitário do produto, tipado como DECIMAL para precisão financeira.',
    'nome_estabelecimento': 'Nome comercial do estabelecimento, obtido através do JOIN com a tabela de estabelecimentos.',
    'email': 'Email de contato do estabelecimento, para comunicação e validação.',
    'telefone': 'Telefone principal do estabelecimento.',
    'data_pedido': 'Data da realização do pedido, tipado como DATE.',
    'data_carga': 'Data da carga técnica para a camada Silver, usada para particionamento e auditoria.',
    'data_hora_carga': 'Timestamp da carga exato (UTC-3) para granularidade de auditoria.'
}

# -----------------------------------------------------------------------------
# 🛠️ Módulo 3: Funções Auxiliares de Governança
# -----------------------------------------------------------------------------
def adiciona_comentarios_tabela(full_table_name: str, table_comment: str, column_comments: dict):
    """
    Aplica comentários descritivos a uma tabela e suas colunas no metastore do Spark.

    Args:
        full_table_name (str): Nome completo da tabela (ex: "silver.pedidos").
        table_comment (str): Comentário para a tabela.
        column_comments (dict): Dicionário com nomes das colunas e seus comentários.
    """
    print(f"📊 [INFO] Aplicando comentários na tabela '{full_table_name}' e suas colunas...")
    try:
        # Tenta aplicar o comentário na tabela.
        spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{table_comment}'")
        # Itera sobre o dicionário de comentários e aplica cada um nas colunas.
        for col_name, col_comment in column_comments.items():
            spark.sql(f"ALTER TABLE {full_table_name} CHANGE COLUMN {col_name} COMMENT '{col_comment}'")
        print(f"🟢 [INFO] Comentários aplicados com sucesso para '{full_table_name}'.")
    except AnalysisException as e:
        # Captura erros de catálogo e trata de forma não fatal, pois o pipeline já salvou os dados.
        print(f"⚠️ [AVISO] Falha ao aplicar comentários. Tabela '{full_table_name}' pode não estar registrada no metastore. Erro: {e}", file=sys.stderr)
    except Exception as e:
        print(f"⚠️ [AVISO] Ocorreu um erro inesperado ao aplicar comentários. Erro: {e}", file=sys.stderr)


# -----------------------------------------------------------------------------
# 🚀 Módulo 4: Lógica Principal de Transformação e Escrita
# -----------------------------------------------------------------------------
try:
    print("▶️ [STATUS] Iniciando o pipeline Silver de pedidos.")

    # 1. Passo Crítico de Governança: Garante que o banco de dados 'silver' exista.
    # Esta é uma prática robusta que evita o erro `TABLE_OR_VIEW_NOT_FOUND`
    # caso o schema não tenha sido criado manualmente ou por um job anterior.
    print(f"🟡 [INFO] Verificando e criando o banco de dados '{database}' se necessário.")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # 2. Leitura e Enriquecimento:
    # A query utiliza Common Table Expressions (CTEs) para melhorar a legibilidade e organizar
    # o pipeline de transformações, seguindo uma prática de especialista.
    # O JOIN Left é usado para garantir que todos os pedidos da Bronze sejam mantidos,
    # mesmo que não haja um estabelecimento correspondente (o que resultaria em nulos).
    df_pedidos_silver = spark.sql(f"""
        WITH pedidos_limpos AS (
            SELECT
                CAST(PedidoID AS INT) AS id_pedido,
                CAST(EstabelecimentoID AS STRING) AS id_estabelecimento,
                Produto AS produto,
                CAST(quantidade_vendida AS INT) AS quantidade,
                CAST(Preco_Unitario AS DECIMAL(20,2)) AS preco,
                CAST(data_venda AS DATE) AS data_pedido
            FROM bronze.pedidos
            WHERE PedidoID IS NOT NULL
        ),
        estabelecimentos_limpos AS (
            SELECT
                EstabelecimentoID AS id_estabelecimento,
                Local AS nome_estabelecimento,
                Email AS email,
                Telefone AS telefone
            FROM bronze.estabelecimentos
        )
        SELECT
            ped.id_pedido,
            ped.id_estabelecimento,
            ped.produto,
            ped.quantidade,
            ped.preco,
            est.nome_estabelecimento,
            est.email,
            est.telefone,
            ped.data_pedido
        FROM pedidos_limpos ped
        LEFT JOIN estabelecimentos_limpos est
        ON ped.id_estabelecimento = est.id_estabelecimento
    """)
    print("🟢 [INFO] Leitura e enriquecimento de dados concluídos.")
    
    # 3. Adição de Colunas Técnicas de Auditoria:
    # Adiciona colunas para rastrear a data e hora da carga, crucial para linhagem e auditoria.
    df_pedidos_silver = df_pedidos_silver.withColumn("data_carga", current_date())
    # O fuso horário é ajustado para evitar inconsistências.
    df_pedidos_silver = df_pedidos_silver.withColumn("data_hora_carga", expr("current_timestamp() - INTERVAL 3 HOURS"))
    print("🟢 [INFO] Colunas de auditoria adicionadas.")
    
    # 4. Verificação e Escrita da Tabela Delta:
    # A lógica foi aprimorada para garantir que, independentemente da carga,
    # as operações de gravação e registro no catálogo estejam sincronizadas.
    
    # Verifica se a tabela Delta já existe fisicamente no DBFS.
    if DeltaTable.isDeltaTable(spark, silver_path):
        # Se a tabela existe, usamos MERGE INTO para garantir UPSERT (atualização ou inserção).
        # A carga incremental é mais robusta com MERGE do que com APPEND + deduplicação posterior.
        print("🟡 [INFO] Tabela Delta já existe. Realizando operação MERGE para carga incremental.")
        delta_table = DeltaTable.forPath(spark, silver_path)
        
        # A lógica MERGE é a forma mais robusta e eficiente de lidar com cargas incrementais
        # e deduplicação sem ter que reprocessar toda a tabela.
        delta_table.alias("tgt") \
            .merge(
                df_pedidos_silver.alias("src"),
                "tgt.id_pedido = src.id_pedido" # Condição de junção para a MERGE.
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        print("🟢 [INFO] MERGE INTO concluído com sucesso.")
        
    else:
        # Se a tabela não existe, fazemos a primeira carga.
        print("🟡 [INFO] Tabela Delta não existe. Realizando a primeira carga e criação da tabela.")
        (
            df_pedidos_silver.write
            .format("delta")
            .mode("overwrite") # 'overwrite' na primeira carga para garantir um estado limpo.
            .option("mergeSchema", "true") # Permite a evolução de schema já na primeira carga.
            .partitionBy("data_carga") # Particionamento para otimizar leituras.
            .save(silver_path)
        )
        print("🟢 [INFO] Dados salvos e tabela Delta criada no DBFS.")

    # 5. Governança e Otimização:
    # Esta é a principal alteração para corrigir o erro. A criação da tabela no catálogo
    # agora é chamada em ambos os cenários (primeira carga ou carga incremental), garantindo que
    # o metadado no catálogo do Spark esteja sempre sincronizado com o caminho físico do Delta.
    # Isso resolve o erro `TABLE_OR_VIEW_NOT_FOUND` se a tabela existir fisicamente, mas não
    # logicamente no metastore.
    print("🟢 [INFO] Garantindo o registro da tabela no metastore.")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_completa}
        USING DELTA
        LOCATION '{silver_path}'
    """)
    print("🟢 [INFO] Tabela registrada/sincronizada com sucesso.")
    
    print("🟢 [INFO] Iniciando tarefas de governança e otimização.")
    adiciona_comentarios_tabela(tabela_completa, comentario_tabela, comentarios_colunas)
    
    # Otimização com Z-Ordering é uma técnica avançada que co-localiza dados relacionados
    # em arquivos no DBFS, acelerando consultas que filtram por essas colunas.
    # O 'id_pedido' é uma ótima escolha para Z-Ordering.
    spark.sql(f"OPTIMIZE {tabela_completa} ZORDER BY (id_pedido)")
    
    # VACUUM remove arquivos de dados que não são mais referenciados pela tabela Delta,
    # limpando o histórico e liberando espaço em disco. O RETENTION garante a segurança.
    spark.sql(f"VACUUM {tabela_completa} RETAIN 168 HOURS") # 168 horas = 7 dias.
    
    print("🚀 [STATUS] Pipeline Silver concluído com sucesso! Tabela otimizada e documentada.")

except Exception as e:
    # O bloco 'except' captura qualquer erro grave e exibe uma mensagem clara.
    print(f"❌ [ERRO CRÍTICO] O pipeline Silver falhou. Erro: {str(e)}", file=sys.stderr)
    raise e # Re-lança o erro para que a orquestração externa (como um job) possa capturá-lo.


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.pedidos LIMIT 20

# COMMAND ----------

# 📋 Registro de Log de Execução Delta – Camada Silver
# 🎯 Objetivo: rastrear a execução da camada Silver de forma auditável e resiliente

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
import time

# ------------------------------------------
# ✅ 1. Início da medição de tempo da execução
# ------------------------------------------
start_time = time.time()

# ------------------------------------------
# ✅ 2. Identificação lógica do job atual
# ------------------------------------------
job_name = "silver_pedidos"  # 🔁 Altere conforme o pipeline

try:
    # ----------------------------------------------------
    # ✅ 3. Simulação de processamento da camada Silver
    #    Substitua este trecho pelo seu pipeline real
    # ----------------------------------------------------
    df = spark.table("silver.pedidos")  # Exemplo: leitura da tabela já processada
    qtd_linhas = df.count()  # Contagem após transformação

    status = "SUCESSO"
    erro = None

except Exception as e:
    # 🧨 Captura qualquer erro ocorrido durante o pipeline
    qtd_linhas = 0
    status = "ERRO"
    erro = str(e)

# ------------------------------------------
# ✅ 4. Medição do tempo total de execução
# ------------------------------------------
tempo_total = round(time.time() - start_time, 2)

# ------------------------------------------
# ✅ 5. Definição do schema explícito para o log Delta
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
# ✅ 6. Criação do DataFrame de log com metadados técnicos
# ------------------------------------------
log_df = spark.createDataFrame([(
    job_name,
    None,  # Timestamp preenchido com a função atualizada abaixo
    qtd_linhas,
    status,
    erro,
    tempo_total
)], schema=log_schema).withColumn("data_execucao", current_timestamp())

# ------------------------------------------
# ✅ 7. Caminhos de armazenamento do log na camada Silver
# ------------------------------------------
LOG_PATH = "dbfs:/FileStore/Ampev/logs/silver_pedidos"  # Delta Storage
LOG_TABLE = "silver.logs_pedidos"  # Catálogo Delta

# ------------------------------------------
# ✅ 8. Escrita segura e resiliente no Delta Lake
# - Garante que o log seja incremental e tolerante a mudanças de schema
# ------------------------------------------
log_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(LOG_PATH)

# Cria a tabela Delta no catálogo se não existir
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LOG_TABLE}
    USING DELTA
    LOCATION '{LOG_PATH}'
""")

# ------------------------------------------
# ✅ 9. Feedback visual para o engenheiro
# ------------------------------------------
print("✅ Log de execução da camada Silver registrado com sucesso!")
print(f"📌 Job: {job_name}")
print(f"📦 Status: {status} | Registros processados: {qtd_linhas} | Duração: {tempo_total} segundos")
if erro:
    print(f"⚠️ Detalhes do erro registrado no log: {erro}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.logs_pedidos
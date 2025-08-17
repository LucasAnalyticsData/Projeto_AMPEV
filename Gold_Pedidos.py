# Databricks notebook source
# MAGIC %md
# MAGIC | **Informações**    |            **Detalhes**         |
# MAGIC |--------------------|---------------------------------|
# MAGIC | Nome da Tabela     |          Gold_Pedidos           |
# MAGIC | Data da Ingestao   |            31/03/2025           |
# MAGIC | Ultima Atualizaçao |            30/07/2025           |
# MAGIC | Origem             |         Silver.pedidos          |
# MAGIC | Responsável        |           Lucas Sousa           |
# MAGIC | Motivo             |   Criação de Camadas Gold       |
# MAGIC | Observações        |               None              |
# MAGIC
# MAGIC ## Histórico de Atualizações
# MAGIC  | Data | Desenvolvido por | Motivo |
# MAGIC  |:----:|--------------|--------|
# MAGIC  |31/07/2025 | Lucas Sousa  | Criação do notebook |
# MAGIC  

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ###1. Desempenho de Vendas por Período
# MAGIC 🎯 Objetivo: Compreender padrões sazonais e tendências de crescimento ou queda.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Esta análise fornece uma visão clara do progresso das vendas ao longo do tempo. É fundamental para a gestão financeira, pois permite que a equipe acompanhe o desempenho em tempo real, avalie a trajetória em relação a metas orçamentárias e identifique rapidamente se o negócio está no caminho certo para o sucesso anual. O uso do acumulado ajuda a suavizar as flutuações de curto prazo e a focar na tendência geral.</p>
# MAGIC
# MAGIC

# COMMAND ----------

display(spark.sql("""
WITH vendas_mensais AS (
    SELECT
        YEAR(data_pedido) AS ano,
        MONTH(data_pedido) AS mes,
        SUM(quantidade * preco) AS receita_mensal
    FROM silver.pedidos
    GROUP BY ano, mes
)
SELECT
    ano,
    mes,
    receita_mensal,
    SUM(receita_mensal) OVER (PARTITION BY ano ORDER BY mes) AS receita_acumulada_anual
FROM vendas_mensais
ORDER BY ano, mes
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Produtos Mais Rentaveis
# MAGIC 🎯 Objetivo: Identificar os itens mais rentáveis para estratégias de promoção e precificação.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Identificar os produtos de maior sucesso em termos de receita é crucial para otimizar a estratégia de negócios. Com essa informação, a empresa pode focar em campanhas de marketing eficazes, planejar o estoque de forma mais assertiva e direcionar o desenvolvimento de novos produtos. Compreender o que o cliente mais valoriza permite alocar recursos de forma mais inteligente para maximizar a lucratividade.</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     produto,
# MAGIC     SUM(quantidade * preco) AS receita_total_produto,
# MAGIC     SUM(quantidade) AS quantidade_total_vendida
# MAGIC FROM silver.pedidos
# MAGIC GROUP BY produto
# MAGIC ORDER BY receita_total_produto DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Desempenho por Estabelecimento
# MAGIC 🎯 Objetivo: Reconhecer os parceiros mais lucrativos e direcionar campanhas de relacionamento.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Esta análise é vital para o gerenciamento da sua rede de parceiros. Ela permite premiar os melhores estabelecimentos (com bonificações, por exemplo) e identificar os de pior desempenho para oferecer suporte, treinamento ou, se necessário, reavaliar a parceria. É uma base sólida para a gestão de relacionamento e expansão da rede.</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     nome_estabelecimento,
# MAGIC     SUM(quantidade * preco) AS receita_total,
# MAGIC     COUNT(DISTINCT id_pedido) AS numero_total_pedidos,
# MAGIC     SUM(quantidade) AS quantidade_total_itens_vendidos
# MAGIC FROM silver.pedidos
# MAGIC GROUP BY nome_estabelecimento
# MAGIC ORDER BY receita_total DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Ticket Médio por Estabelecimento
# MAGIC 🎯 Objetivo: Avaliar a lucratividade média por cliente e entender o comportamento de compra.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">O ticket médio é um indicador-chave da qualidade das vendas. Aumentar este valor significa que cada cliente está gerando mais receita por transação, o que pode ser resultado de estratégias de cross-selling (venda de produtos complementares) ou up-selling (venda de produtos de maior valor). Monitorar o ticket médio por estabelecimento e por período ajuda a identificar onde essas estratégias estão funcionando e quais estabelecimentos têm maior poder de venda.</p>

# COMMAND ----------

display(spark.sql("""
WITH valor_por_pedido AS (
    SELECT
        id_pedido,
        nome_estabelecimento,
        SUM(quantidade * preco) AS valor_do_pedido
    FROM silver.pedidos
    GROUP BY id_pedido, nome_estabelecimento
)
SELECT
    nome_estabelecimento,
    AVG(valor_do_pedido) AS ticket_medio
FROM valor_por_pedido
GROUP BY nome_estabelecimento
ORDER BY ticket_medio DESC
"""))


# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Estabelecimentos com Maior Número de Pedidos
# MAGIC 🎯 Objetivo: Avaliar a popularidade e o engajamento dos estabelecimentos.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Esta métrica mede a popularidade e a capacidade de um estabelecimento de atrair e processar um grande volume de transações. Diferente da receita, ela foca na frequência e no engajamento dos clientes, sendo um indicador valioso para entender a capilaridade da sua marca.</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     nome_estabelecimento,
# MAGIC     COUNT(DISTINCT id_pedido) AS numero_total_pedidos
# MAGIC FROM silver.pedidos
# MAGIC GROUP BY nome_estabelecimento
# MAGIC ORDER BY numero_total_pedidos DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. Mix de Produtos Vendidos por Estabelecimento
# MAGIC 🎯 Objetivo: Identificar oportunidades de cross-selling e verificar a diversidade do portfólio.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">O "insight" que você mencionou está correto: entender a variedade de produtos que um estabelecimento vende ajuda a identificar oportunidades de cross-selling e a verificar a saúde e a diversidade do portfólio. Um estabelecimento com um mix variado provavelmente tem clientes mais engajados e pode ser um modelo a ser replicado.</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     nome_estabelecimento,
# MAGIC     COUNT(DISTINCT produto) AS numero_produtos_unicos
# MAGIC FROM silver.pedidos
# MAGIC GROUP BY nome_estabelecimento
# MAGIC ORDER BY numero_produtos_unicos DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. Tempo Médio entre Pedidos (Frequência de Compra)
# MAGIC 🎯 Objetivo: Prever o churn (perda de clientes) e identificar clientes fiéis.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Este é um indicador avançado e crucial para prever o churn (perda de clientes) e identificar clientes fiéis. Clientes que compram com alta frequência são valiosos. Ao detectar um aumento no tempo entre as compras, a empresa pode agir de forma proativa, enviando ofertas personalizadas para reter o cliente.</p>

# COMMAND ----------

display(spark.sql("""
WITH pedidos_com_datas_anteriores AS (
    SELECT
        nome_estabelecimento,
        data_pedido,
        LAG(data_pedido, 1) OVER (PARTITION BY nome_estabelecimento ORDER BY data_pedido) AS data_pedido_anterior
    FROM silver.pedidos
),
diferenca_dias AS (
    SELECT
        nome_estabelecimento,
        DATEDIFF(data_pedido, data_pedido_anterior) AS dias_entre_pedidos
    FROM pedidos_com_datas_anteriores
    WHERE data_pedido_anterior IS NOT NULL
)
SELECT
    nome_estabelecimento,
    AVG(dias_entre_pedidos) AS tempo_medio_entre_pedidos
FROM diferenca_dias
GROUP BY nome_estabelecimento
ORDER BY tempo_medio_entre_pedidos ASC
"""))



# COMMAND ----------

# MAGIC %md
# MAGIC ###8. Produtos com Baixa Saída
# MAGIC 🎯 Objetivo: Gerenciar inventário e avaliar a eficiência do portfólio.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Conhecer os produtos com baixa saída é o lado oposto da análise de "mais vendidos". Este insight é fundamental para gerenciar o inventário, decidir se um produto deve ser descontinuado ou se precisa de uma promoção agressiva para escoar o estoque. É uma análise de eficiência de portfólio.</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     produto,
# MAGIC     SUM(quantidade) AS quantidade_total_vendida_recente
# MAGIC FROM silver.pedidos
# MAGIC WHERE data_pedido >= DATE_SUB(current_date(), 90) -- Exemplo: últimos 90 dias
# MAGIC GROUP BY produto
# MAGIC ORDER BY quantidade_total_vendida_recente ASC
# MAGIC LIMIT 10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###9. Análise de Crescimento Mensal (Mês-a-Mês)
# MAGIC 🎯 Objetivo: Monitorar o ritmo de crescimento e reagir rapidamente a tendências.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Diferente do acumulado, esta análise compara a receita de cada mês com a do mês anterior. É uma métrica essencial para monitorar o ritmo de crescimento e reagir rapidamente a tendências de alta ou baixa. Ajuda a responder a perguntas como "O crescimento de vendas desacelerou em relação ao mês passado?"</p>

# COMMAND ----------

display(spark.sql("""
WITH receita_mensal AS (
    SELECT
        DATE_TRUNC('month', data_pedido) AS mes_ano,
        SUM(quantidade * preco) AS receita_mensal
    FROM silver.pedidos
    GROUP BY mes_ano
)
SELECT
    mes_ano,
    receita_mensal,
    LAG(receita_mensal, 1) OVER (ORDER BY mes_ano) AS receita_mes_anterior,
    (receita_mensal - LAG(receita_mensal, 1) OVER (ORDER BY mes_ano)) / LAG(receita_mensal, 1) OVER (ORDER BY mes_ano) * 100 AS crescimento_mensal_percentual
FROM receita_mensal
ORDER BY mes_ano DESC
"""))


# COMMAND ----------

# MAGIC %md
# MAGIC ###10. Variação de Preço por Produto
# MAGIC 🎯 Objetivo: Detectar inconsistências ou oportunidades de ajustes comerciais.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Este insight ajuda a monitorar a consistência dos preços e a identificar possíveis problemas de dados ou inconsistências na estratégia de precificação. Uma variação significativa pode indicar promoções, diferenças entre fornecedores, ou até mesmo erros de registro. Monitorar isso garante a integridade dos dados e a estabilidade da sua estratégia de preços.</p>

# COMMAND ----------

display(spark.sql("""
SELECT
    produto,
    MIN(preco) AS preco_minimo,
    MAX(preco) AS preco_maximo,
    AVG(preco) AS preco_medio,
    (MAX(preco) - MIN(preco)) AS variacao_de_preco
FROM silver.pedidos
GROUP BY produto
ORDER BY variacao_de_preco DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ###11. Vendas Acumuladas no Ano por Estabelecimento
# MAGIC 🎯 Objetivo: Acompanhar o desempenho acumulado de cada parceiro ao longo do ano.
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Essa análise é uma extensão do "Faturamento por Estabelecimento" para a dimensão temporal. Ela permite que a gestão acompanhe o desempenho acumulado de cada parceiro ao longo do ano. Isso é útil para definir e monitorar metas de vendas para cada estabelecimento, além de identificar quem está se destacando na corrida anual.</p>

# COMMAND ----------

display(spark.sql("""
WITH vendas_mensais_estabelecimento AS (
    SELECT
        nome_estabelecimento,
        YEAR(data_pedido) AS ano,
        MONTH(data_pedido) AS mes,
        SUM(quantidade * preco) AS receita_mensal
    FROM silver.pedidos
    GROUP BY nome_estabelecimento, ano, mes
)
SELECT
    nome_estabelecimento,
    ano,
    mes,
    receita_mensal,
    SUM(receita_mensal) OVER (PARTITION BY nome_estabelecimento, ano ORDER BY mes) AS receita_acumulada_estabelecimento
FROM vendas_mensais_estabelecimento
ORDER BY nome_estabelecimento, ano, mes
"""))


# COMMAND ----------

# MAGIC %md
# MAGIC ###12. Análise de Crescimento de Vendas Mês a Mês e Ano a Ano
# MAGIC 🎯 Objetivo: Avaliar o desempenho de vendas no curto (mês a mês) e longo prazo (ano a ano).
# MAGIC
# MAGIC <h4 style="color: #007bff; font-size: 1.2em;">Importância Estratégica:</h4>
# MAGIC <p style="font-size: 1.1em;">Esta é uma análise de crescimento abrangente. Ela permite avaliar o desempenho de vendas tanto no curto prazo (mês a mês) quanto no longo prazo (ano a ano), oferecendo uma visão completa da saúde do negócio. A capacidade de comparar o desempenho de um mês com o mesmo mês do ano anterior é crucial para eliminar a sazonalidade e entender o crescimento real da empresa.

# COMMAND ----------

display(spark.sql("""
WITH vendas_mensais AS (
    SELECT
        DATE_TRUNC('month', data_pedido) AS mes_ano,
        SUM(quantidade * preco) AS receita,
        SUM(quantidade) AS quantidade_vendas
    FROM silver.pedidos
    GROUP BY mes_ano
),
comparacao_anual AS (
    SELECT
        mes_ano,
        receita,
        quantidade_vendas,
        LAG(receita, 12, 0) OVER (ORDER BY mes_ano) AS receita_ano_anterior,
        LAG(quantidade_vendas, 12, 0) OVER (ORDER BY mes_ano) AS quantidade_ano_anterior
    FROM vendas_mensais
)
SELECT
    mes_ano,
    receita,
    quantidade_vendas,
    (receita - LAG(receita, 1, 0) OVER (ORDER BY mes_ano)) / LAG(receita, 1, 0) OVER (ORDER BY mes_ano) * 100 AS crescimento_receita_mensal_percentual,
    (receita - receita_ano_anterior) / receita_ano_anterior * 100 AS crescimento_receita_anual_percentual,
    (quantidade_vendas - LAG(quantidade_vendas, 1, 0) OVER (ORDER BY mes_ano)) / LAG(quantidade_vendas, 1, 0) OVER (ORDER BY mes_ano) * 100 AS crescimento_quantidade_mensal_percentual,
    (quantidade_vendas - quantidade_ano_anterior) / quantidade_ano_anterior * 100 AS crescimento_quantidade_anual_percentual
FROM comparacao_anual
ORDER BY mes_ano DESC
"""))

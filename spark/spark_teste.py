from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as F
import os

os.environ["PYSPARK_PYTHON"] = r"D:\Programas\Python310\python.exe"  # Substitua pelo caminho correto


# Criando a sessão Spark
spark = SparkSession.builder \
    .appName("Exemplo CSV") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .getOrCreate()

spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
spark.conf.set("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
spark.conf.set("spark.sql.adaptive.enabled", "false")


try:
    # Lendo o CSV
    print("Lendo o arquivo CSV...")
    input_path = "D:/Estudos/Projetos/Spark/exemplo1.csv"
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Exibindo as 5 primeiras linhas
    print("Exibindo as primeiras 5 linhas do DataFrame:")
    df.show(5)

    # Exibir esquema do DataFrame
    print("Esquema do DataFrame:")
    df.printSchema()

    # Estatísticas descritivas
    print("Estatísticas descritivas do DataFrame:")
    df.describe().show()

    # Remover duplicatas
    print("Removendo duplicatas do DataFrame...")
    df = df.dropDuplicates()
    print("DataFrame após remoção de duplicatas:")
    df.show(5)

    # Substituir valores nulos
    print("Substituindo valores nulos por 0...")
    df = df.fillna(0)
    print("DataFrame após substituir valores nulos:")
    df.show(5)

    # Verificar a existência da coluna "departamento"
    if "departamento" in df.columns:
        print("Extraindo departamentos distintos...")
        departamentos = df.select("departamento").distinct().rdd.flatMap(lambda x: x).collect()
        print(f"Departamentos encontrados: {departamentos}")

        # Salvando os dados por departamento
        output_base_path = "D:/Estudos/Projetos/Spark/output/departamento_"
        
        for dep in departamentos:
            print(f"Filtrando dados para o departamento: {dep}")
            
            # Filtrando e reparticionando os dados por departamento
            departamento_df = df.filter(F.col("departamento") == dep)
            
            # Usando repartition para garantir que os dados estejam bem distribuídos
            departamento_df.repartition(1).write.csv(f"{output_base_path}{dep}", mode="overwrite", header=True)
            
            print(f"Dados do departamento '{dep}' salvos em CSV.")
    else:
        print("A coluna 'departamento' não foi encontrada. Certifique-se de que o arquivo possui esta coluna.")

    # Adicionar uma nova coluna 'faixa_etaria'
    print("Adicionando uma nova coluna 'faixa_etaria' com base na idade...")
    if "idade" in df.columns:
        df = df.withColumn(
            "faixa_etaria",
            when(col("idade") < 30, "Jovem")
            .when((col("idade") >= 30) & (col("idade") < 40), "Adulto")
            .otherwise("Sênior")
        )
        print("DataFrame com a nova coluna 'faixa_etaria':")
        df.show(5)
    else:
        print("A coluna 'idade' não foi encontrada. A coluna 'faixa_etaria' não será adicionada.")

    # Agrupamento por departamento
    if "departamento" in df.columns and "salario" in df.columns:
        print("Agrupando dados por departamento...")
        df_grouped = df.groupBy("departamento").agg(
            {"salario": "sum", "salario": "avg", "nome": "count"}
        ).withColumnRenamed("sum(salario)", "salario_total") \
         .withColumnRenamed("avg(salario)", "salario_medio") \
         .withColumnRenamed("count(nome)", "numero_funcionarios")

        print("Resultados do agrupamento por departamento:")
        df_grouped.show()
    else:
        print("As colunas 'departamento' e/ou 'salario' não foram encontradas para o agrupamento.")

    # Escrever dados processados em vários formatos
    output_path = "D:/Estudos/Projetos/Spark/output/processed_data"
    print("Escrevendo dados processados em CSV...")
    df.write.csv(f"{output_path}_csv", mode="overwrite", header=True)

    print("Escrevendo dados processados em JSON...")
    df.write.json(f"{output_path}_json", mode="overwrite")

    print("Escrevendo dados processados em Parquet...")
    df.write.parquet(f"{output_path}_parquet", mode="overwrite")

    # Registrar DataFrame como tabela SQL
    print("Registrando DataFrame como tabela temporária SQL...")
    df.createOrReplaceTempView("dados")

    # Consulta SQL para calcular a média de salário por departamento
    if "salario" in df.columns and "departamento" in df.columns:
        print("Executando consulta SQL para calcular a média de 'salario' por departamento...")
        resultado = spark.sql("SELECT departamento, AVG(salario) as media_salario FROM dados GROUP BY departamento")
        print("Resultado da consulta SQL:")
        resultado.show()

    # Dividir dados
    print("Dividindo os dados em conjunto de treinamento (80%) e teste (20%)...")
    train, test = df.randomSplit([0.7, 0.3])

    # Mostrar amostras
    print("Amostra do conjunto de treinamento:")
    train.show(5)

    print("Amostra do conjunto de teste:")
    test.show(5)

except Exception as e:
    print("Ocorreu um erro durante a execução do script:")
    print(e)

finally:
    # Encerrando a SparkSession
    spark.stop()
    print("SparkSession encerrada.")

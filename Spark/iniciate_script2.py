
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as F

# Criar a SparkSession
spark = SparkSession.builder \
    .appName("Processamento Iniciante") \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.cores", "2") \
    .getOrCreate()

# Configurações para evitar mensagens de erro e advertência
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
spark.conf.set("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Ler arquivo CSV
input_path = "D:/Estudos/Projetos/Spark/exemplo1.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)


# Exemplo de transformação: Adicionar uma nova coluna 'bonus' calculando 10% do salário
df = df.withColumn("bonus", col("salario") * 0.10)

# Salvar em formato Parquet
df.write.mode("overwrite").parquet("D:/Estudos/Projetos/Spark/dados_bonificados.parquet")

# 1.2 Filtrar dados - Selecionar apenas os funcionários com salário maior que 5000
df_filtered = df.filter(col("salario") > 5000)

# Mostrar o DataFrame filtrado
df_filtered.show()

# 1.3 Agregar dados - Calcular a média de salários por departamento
df_grouped = df.groupBy("departamento").avg("salario")

# Mostrar o resultado agregado
df_grouped.show()

# Encerrando a SparkSession
spark.stop()
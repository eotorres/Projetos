from pyspark.sql import SparkSession
from pyspark import SparkContext  # Correção: importação do SparkContext do módulo correto
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, StructType, StructField, StringType
from pyspark.sql import functions as F
import os
import time

# Definir o Python para ser usado com o PySpark
os.environ["PYSPARK_PYTHON"] = r"D:\Programas\Python310\python.exe"  # Substitua pelo caminho correto

# Criar a SparkSession
spark = SparkSession.builder \
    .appName("Processamento Avançado") \
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
input_path = "D:/Estudos/Projetos/Spark/exemplo.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Função Python para UDF
def salario_bonus(salario):
    if salario > 5000:
        return salario * 0.10
    return salario * 0.05

# Registrando a função UDF
salario_bonus_udf = udf(salario_bonus, FloatType())  # Usando FloatType para valores decimais

# Aplicando a UDF ao DataFrame
df = df.withColumn("bonus", salario_bonus_udf(col("salario")))
df.show(5)

# Convertendo DataFrame para RDD
rdd = df.rdd

# Realizando uma operação map no RDD
rdd_transformed = rdd.map(lambda row: (row['nome'], row['salario'], row['bonus']))  # Ajustado para incluir 'bonus'

# Convertendo de volta para DataFrame com as colunas adequadas
df_transformed = rdd_transformed.toDF(["nome", "salario", "bonus"])
df_transformed.show(5)

# Especificando o esquema para os dados de streaming
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("salario", FloatType(), True),
    StructField("departamento", StringType(), True)
])

# Configurando o Spark para streaming
# Certifique-se de que a pasta "streaming" contenha arquivos CSV para que o stream funcione corretamente
streaming_df = spark.readStream.format("csv").option("header", "true").schema(schema).load(r"D:/Estudos/Projetos/Spark/streaming")

# Realizando operações de agregação em tempo real
streaming_result = streaming_df.groupBy("departamento").agg(
    F.avg("salario").alias("media_salario")
)

# Definindo a localização do checkpoint
checkpoint_dir = "D:/Estudos/Projetos/Spark/checkpoints"

# Escrevendo a saída para console (modo de debug)
query = streaming_result.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(once=True)  # Garantindo que o streaming finalize após a leitura dos arquivos

# Iniciando o streaming e aguardando a finalização
query = query.start()

# Verificando o status do stream durante a execução
while query.isActive:
    print("O streaming está ativo...")
    time.sleep(5)

# Esperando até que o streaming termine
query.awaitTermination()

# Encerrando a SparkSession
spark.stop()

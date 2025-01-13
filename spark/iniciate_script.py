
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

# Exibir as 5 primeiras linhas
df.show(5)

# Exibir o esquema dos dados
df.printSchema()

# Estatísticas descritivas
df.describe().show()

# Selecionando uma coluna
df.select("nome").show()

# Filtrando dados
df.filter(df["idade"] > 30).show()

# Contando o número de registros
print(df.count())

# Agrupando por departamento e calculando a média de salário
df.groupBy("departamento").avg("salario").show()

# Criando uma nova coluna baseada em condições
df = df.withColumn("faixa_etaria", 
                  when(col("idade") < 30, "Jovem")
                  .when((col("idade") >= 30) & (col("idade") < 40), "Adulto")
                  .otherwise("Sênior"))
df.show(5)

# Encerrando a SparkSession
spark.stop()

from kafka import KafkaConsumer
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import from_json, col, split, explode, regexp_replace, lower
#from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark import SparkConf, SparkContext
import re
import json

def lines(line):
    # Usar expresión regular para encontrar solo letras, números y espacios (unicode)
    return re.sub(r'[^a-zA-ZáéíóúüñÁÉÍÓÚÜÑ\s]', '', line)

# Configurar SparkSession
spark = SparkConf().setAppName("processtext")
sc = SparkContext(conf=spark)

consumer = KafkaConsumer(
    'quickstart-events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='mygroup'
)

batch= 0

print('Waiting for messages...')
id =0
rdd_user = sc.parallelize([])
rdd_count = sc.parallelize([])
for message in consumer:
    if batch > 10:
        tuplas = rdd_user.collect()
        sc.parallelize(tuplas).saveAsTextFile("txt/tuples/"+str(id))

        wc = rdd_count.collect()
        sc.parallelize(wc).saveAsTextFile("txt/count/"+str(id))
        rdd_user = sc.parallelize([])
        rdd_count = sc.parallelize([])
        batch =0
        id+=1


    msg = message.value.decode("utf-8")
    msg =  json.loads(msg)
    comment = msg["comentario"].replace('[', ' ').replace(']', ' ').replace('"', '')

    text_file = sc.parallelize(comment.split(" "))
    filtered_rdd = text_file.map(lambda line: lines(line).lower())

    words_rdd = filtered_rdd.flatMap(lambda line: line.split(" "))
    word_counts = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    rdd_count=rdd_count.union(word_counts)


    tmp =filtered_rdd.flatMap(lambda l: l.split(" ")).map(lambda l : (l,msg["user"]))
    rdd_user= rdd_user.union(tmp)
    
    batch+=1

spark.stop()

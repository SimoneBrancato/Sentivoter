from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.types as tp
from pyspark.sql.functions import col, from_json, explode

kafkaServer = "kafka:9092"
topic = "sentivoter"

# Define kafka messages structure
kafka_schema =  tp.StructType([
    tp.StructField('fullText', tp.StringType(), True),
    tp.StructField('@version', tp.StringType(), True),
    tp.StructField('@timestamp', tp.StringType(), True),
    tp.StructField('url_video', tp.StringType(), True),
    tp.StructField('host', tp.StringType(), True),
    tp.StructField('title', tp.StringType(), True),
    tp.StructField('views', tp.IntegerType(), True),
    tp.StructField('likes', tp.IntegerType(), True),
    tp.StructField('headers', tp.StructType([
        tp.StructField('request_path', tp.StringType(), True),
        tp.StructField('http_version', tp.StringType(), True),
        tp.StructField('http_host', tp.StringType(), True),
        tp.StructField('content_type', tp.StringType(), True),
        tp.StructField('connection', tp.StringType(), True),
        tp.StructField('http_accept', tp.StringType(), True),
        tp.StructField('accept_encoding', tp.StringType(), True),
        tp.StructField('request_method', tp.StringType(), True),
        tp.StructField('content_length', tp.StringType(), True),
        tp.StructField('http_user_agent', tp.StringType(), True)
    ]), True),
    tp.StructField('id_video', tp.StringType(), True),
    tp.StructField('published_at', tp.StringType(), True),
    tp.StructField('comments', tp.ArrayType(tp.StructType([
            tp.StructField('cid', tp.StringType(), True),
            tp.StructField('published_at', tp.StringType(), True),
            tp.StructField('author', tp.StringType(), True),
            tp.StructField('text', tp.StringType(), True),
            tp.StructField('votes', tp.IntegerType(), True)
        ])), True)
])

# Define Spark connection to ElasticSearch
print("Defining SparkConf")
sparkConf = SparkConf().set("es.nodes", "http://elasticsearch") \
                        .set("es.port", "9200") \
                        .set("spark.driver.memory","64G") \
                        .set("spark.driver.maxResultSize", "0") \
                        .set("spark.kryoserializer.buffer.max", "2000M") \
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                  
# Build Spark Session
print("Starting Spark Session")
spark = SparkSession.builder \
    .appName("Spark") \
    .master("local[16]") \
    .config(conf=sparkConf) \
    .getOrCreate()

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

# Read the stream from Kafka
print("Reading stream from kafka...")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("failOnDataLoss", "false") \
    .option("subscribe", topic) \
    .load()


# Step 1: Parse the Kafka message value using from_json
df_parsed = df.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), kafka_schema).alias("data")) \
              .select("data.*")

df_video = df_parsed.select(
                            col("published_at").alias("timestamp"),
                            "title",
                            "url_video",
                            "id_video",
                            "views",
                            "likes",
                            "fullText",
                    )

df_comments = df_parsed.select("id_video", explode("comments").alias("comment")) \
                       .select(
                           col("id_video"),
                           col("comment.cid").alias("cid"),
                           col("comment.published_at").alias("published_at"),
                           col("comment.author").alias("author"),
                           col("comment.text").alias("text"),
                           col("comment.votes").alias("votes")
                       )

video_query = df_video.writeStream \
                      .format("org.elasticsearch.spark.sql") \
                      .option("es.resource", "sentivoter_videos") \
                      .option("es.mapping.id", "id_video") \
                      .option("checkpointLocation", "/tmp/sentivoter_videos_checkpoint/") \
                      .start()

comments_query = df_comments.writeStream \
                            .format("org.elasticsearch.spark.sql") \
                            .option("es.resource", "sentivoter_comments") \
                            .option("es.mapping.id", "cid") \
                            .option("checkpointLocation", "/tmp/sentivoter_comments_checkpoint/") \
                            .start()

# Aspetta che entrambi i flussi di scrittura terminino
video_query.awaitTermination()
comments_query.awaitTermination()
                      

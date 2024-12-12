from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.types as tp
from pyspark.sql.functions import col, from_json, explode, udf
import tweetnlp
from transformers import logging
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
logging.set_verbosity_error()

kafkaServer = "kafka:9092"
topic = "sentivoter"

sentiment_model = tweetnlp.load_model('sentiment')

def split_text(text, chunk_size=500):
    chunks = []
    start = 0

    while True:
        chunk = text[start:start + chunk_size]
        if not chunk:  
            break
        chunks.append(chunk)
        start += chunk_size

    return chunks

def get_sentiment(text):
    if text is None:
        return {"label": "neutral", "negative": 0.0, "neutral": 1.0, "positive": 0.0}

    chunks = split_text(text)

    aggregated_probabilities = {'negative': 0, 'neutral': 0, 'positive': 0}
    
    for chunk in chunks:
        result = sentiment_model.sentiment(chunk, return_probability=True)
        probabilities = result['probability']
        for sentiment, probability in probabilities.items():
            aggregated_probabilities[sentiment] += probability
    
    num_chunks = len(chunks)
    average_probabilities = {}

    for sentiment, prob in aggregated_probabilities.items():
        average_probabilities[sentiment] = float(prob / num_chunks) 

    average_probabilities["label"] = str(max(average_probabilities, key=average_probabilities.get))

    return average_probabilities

# Define kafka messages structure
kafka_schema =  tp.StructType([
    tp.StructField('fullText', tp.StringType(), True),
    tp.StructField('url_video', tp.StringType(), True),
    tp.StructField('host', tp.StringType(), True),
    tp.StructField('title', tp.StringType(), True),
    tp.StructField('views', tp.IntegerType(), True),
    tp.StructField('likes', tp.IntegerType(), True),
    tp.StructField('id_video', tp.StringType(), True),
    tp.StructField('channel', tp.StringType(), True),
    tp.StructField('channel_bias', tp.StringType(), True),
    tp.StructField('state', tp.StringType(), True),
    tp.StructField('timestamp', tp.StringType(), True),
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

sentiment_schema = tp.StructType([
    tp.StructField("label", tp.StringType(), True),
    tp.StructField("negative", tp.FloatType(), True),
    tp.StructField("neutral", tp.FloatType(), True),
    tp.StructField("positive", tp.FloatType(), True)
])

sentiment_udf = udf(get_sentiment, sentiment_schema)

# Step 1: Parse the Kafka message value using from_json
df_parsed = df.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), kafka_schema).alias("data")) \
              .select("data.*")

df_video = df_parsed.select(
                            col("timestamp"),
                            col("channel"),
                            col("channel_bias"),
                            col("state"),
                            col("title"),
                            col("url_video"),
                            col("id_video"),
                            col("views"),
                            col("likes"),
                            col("fullText"),
                    )

df_video = df_video.withColumn("sentiment", sentiment_udf(df_parsed["fullText"]))

df_video = df_video.select(
                            col("timestamp"),
                            col("channel"),
                            col("channel_bias"),
                            col("state"),
                            col("title"),
                            col("url_video"),
                            col("id_video"),
                            col("views"),
                            col("likes"),
                            col("fullText"),
                            col("sentiment.label").alias("sentiment_label"),
                            col("sentiment.negative").alias("negative"),
                            col("sentiment.neutral").alias("neutral"),
                            col("sentiment.positive").alias("positive")
                        )

df_comments = df_parsed.select("id_video", "timestamp", "channel", "channel_bias", "state", explode("comments").alias("comment")) \
                       .select(
                           col("id_video"),
                           col("timestamp").alias("video_timestamp"),
                           col("channel"),
                           col("channel_bias"),
                           col("state"),
                           col("comment.cid").alias("cid"),
                           col("comment.published_at").alias("published_at"),
                           col("comment.author").alias("author"),
                           col("comment.text").alias("text"),
                           col("comment.votes").alias("likes")
                       )

df_comments = df_comments.withColumn("sentiment", sentiment_udf(df_comments["text"]))

df_comments = df_comments.select(
                           col("id_video"),
                           col("video_timestamp"),
                           col("channel"),
                           col("channel_bias"),
                           col("state"),
                           col("cid"),
                           col("published_at"),
                           col("author"),
                           col("text"),
                           col("likes"),
                           col("sentiment.label").alias("sentiment_label"),
                           col("sentiment.negative").alias("negative"),
                           col("sentiment.neutral").alias("neutral"),
                           col("sentiment.positive").alias("positive")
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

video_query.awaitTermination()
comments_query.awaitTermination()
                      

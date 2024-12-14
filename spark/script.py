from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.types as tp
from pyspark.sql.functions import col, from_json, explode, udf
import tweetnlp
from transformers import logging
import warnings

# To reduce verbose output
warnings.filterwarnings("ignore", category=FutureWarning)
logging.set_verbosity_error()

# Define parameters to connect to Kafka Broker
kafkaServer = "kafka:9092"
topic = "sentivoter"

# Load sentiment and emotion models from TweetNLP
sentiment_model = tweetnlp.load_model('sentiment')
emotion_model = tweetnlp.load_model('emotion')

# To split the text by chunk_size
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

# Analyze text sentiment by chunks and calculate total sentiment analysis
def get_sentiment(text):
    if text is None:
        return {"sentiment_label": "neutral", "negative": 0.0, "neutral": 1.0, "positive": 0.0}

    chunks = split_text(text)
    aggregated_probabilities = {
                                'negative': 0,
                                'neutral': 0, 
                                'positive': 0
                            }
    
    for chunk in chunks:
        result = sentiment_model.sentiment(chunk, return_probability=True)
        probabilities = result['probability']
        for sentiment, probability in probabilities.items():
            aggregated_probabilities[sentiment] += probability
    
    num_chunks = len(chunks)
    average_probabilities = {}

    for sentiment, prob in aggregated_probabilities.items():
        average_probabilities[sentiment] = float(prob / num_chunks) 

    average_probabilities["sentiment_label"] = str(max(average_probabilities, key=average_probabilities.get))
    return average_probabilities

# Analyze text emotion by chunks and calculate total emotion analysis
def get_emotion(text):

    chunks = split_text(text)
    aggregated_probabilities = {
                                'anger': 0.0,
                                'anticipation': 0.0,
                                'disgust': 0., 
                                'fear': 0.0, 
                                'joy': 0.0, 
                                'love': 0.0, 
                                'optimism': 0.0, 
                                'pessimism': 0.0, 
                                'sadness': 0.0, 
                                'surprise': 0.0, 
                                'trust': 0.0
                            }
    
    for chunk in chunks:
        result = emotion_model.emotion(chunk, return_probability=True)
        probabilities = result['probability']
        for emotion, probability in probabilities.items():
            aggregated_probabilities[emotion] += probability

    return {"emotion_label": str(max(aggregated_probabilities, key=aggregated_probabilities.get))}

# Define Kafka messages structure
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
sparkConf = SparkConf().set("es.nodes", "http://elasticsearch") \
                        .set("es.port", "9200") \
                        .set("spark.driver.maxResultSize", "0") \
                        .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
                        .set("spark.kryoserializer.buffer.max", "2000M") \
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                        .set("spark.memory.offHeap.enabled","true") \
                        .set("spark.memory.offHeap.size","16g")
              
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

# Define sentiment_schema
sentiment_schema = tp.StructType([
    tp.StructField("sentiment_label", tp.StringType(), True),
    tp.StructField("negative", tp.FloatType(), True),
    tp.StructField("neutral", tp.FloatType(), True),
    tp.StructField("positive", tp.FloatType(), True)
])

# Define UDF by binding get_sentiment function and sentiment_schema
sentiment_udf = udf(get_sentiment, sentiment_schema)

# Define emotion_schema
emotion_schema = tp.StructType([
    tp.StructField("emotion_label", tp.StringType(), True)
])

# Define UDF by binding get_emotion function and emotion_schema
emotion_udf = udf(get_emotion, emotion_schema)

# Parse the Kafka message value using from_json
df_parsed = df.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), kafka_schema).alias("data")) \
              .select("data.*")

# Select relevant data for video dataframe from the parsed dataframe
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

# Enrich video dataframe with sentiment analysis
df_video = df_video.withColumn("sentiment", sentiment_udf(df_parsed["fullText"]))

# Expand dataframe and select relevant columns 
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
                            col("sentiment.sentiment_label").alias("sentiment_label"),
                            col("sentiment.negative").alias("negative"),
                            col("sentiment.neutral").alias("neutral"),
                            col("sentiment.positive").alias("positive")
                        )

# Flatten comments dataframe
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

# Enrich comments dataframe with sentiment analysis and emotion analysis
df_comments = df_comments.withColumn("sentiment", sentiment_udf(df_comments["text"]))
df_comments = df_comments.withColumn("emotion", emotion_udf(df_comments["text"]))

# Expand dataframe and select relevant columns 
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
                           col("sentiment.sentiment_label").alias("sentiment_label"),
                           col("sentiment.negative").alias("negative"),
                           col("sentiment.neutral").alias("neutral"),
                           col("sentiment.positive").alias("positive"),
                           col("emotion.emotion_label").alias("emotion_label")
                        )

# Send video dataframe to Elasticsearch
video_query = df_video.writeStream \
                      .format("org.elasticsearch.spark.sql") \
                      .option("es.resource", "sentivoter_videos") \
                      .option("es.mapping.id", "id_video") \
                      .option("checkpointLocation", "/tmp/sentivoter_videos_checkpoint/") \
                      .start()

# Send comments dataframe to Elasticseach
comments_query = df_comments.writeStream \
                            .format("org.elasticsearch.spark.sql") \
                            .option("es.resource", "sentivoter_comments") \
                            .option("es.mapping.id", "cid") \
                            .option("checkpointLocation", "/tmp/sentivoter_comments_checkpoint/") \
                            .start()

video_query.awaitTermination()
comments_query.awaitTermination()
                      

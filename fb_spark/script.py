from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.types as tp
from pyspark.sql.functions import col, from_json, udf
import tweetnlp
from transformers import logging
import warnings
import torch

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")
print(f"CUDA available: {torch.cuda.is_available()}")
print(f"CUDA device count: {torch.cuda.device_count()}")
print(f"Current device: {torch.cuda.current_device()}")
print(f"Device name: {torch.cuda.get_device_name(0)}")

sentiment_model = tweetnlp.load_model('sentiment')
print(f"Model device: {sentiment_model.model.device}")

emotion_model = tweetnlp.load_model('emotion')
print(f"Model device: {emotion_model.model.device}")

# To reduce verbose output
warnings.filterwarnings("ignore", category=FutureWarning)
logging.set_verbosity_error()

# Define parameters to connect to Kafka Broker
kafkaServer = "kafka:9092"
fb_posts_topic = "fb_sentivoter_posts"
fb_comments_topic = "fb_sentivoter_comments"

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
    try:
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
        
        num_chunks = len(chunks) if len(chunks) > 0 else 1
        average_probabilities = {}

        for sentiment, prob in aggregated_probabilities.items():
            average_probabilities[sentiment] = float(prob / num_chunks) 
        
        average_probabilities["sentiment_label"] = str(max(average_probabilities, key=average_probabilities.get))

    except Exception:
        average_probabilities = {"sentiment_label": "neutral", "negative": 0.0, "neutral": 1.0, "positive": 0.0}
    finally:
        return average_probabilities

# Analyze text emotion by chunks and calculate total emotion analysis
def get_emotion(text):
    try:
        if text is None:
            return {"emotion_label": "null"}
        
        chunks = split_text(text)
        aggregated_probabilities = {
                                    'anger': 0.0,
                                    'anticipation': 0.0,
                                    'disgust': 0.0, 
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
        result = {"emotion_label": str(max(aggregated_probabilities, key=aggregated_probabilities.get))}

    except Exception:
        result = {"emotion_label": "null"}
        
    finally:
        return result

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

fb_posts_kafka_schema = tp.StructType([
    tp.StructField('uuid', tp.StringType(), True),
    tp.StructField('retrieving_time', tp.StringType(), True),  
    tp.StructField('timestamp', tp.StringType(), True),
    tp.StructField('candidate', tp.StringType(), True),
    tp.StructField('content', tp.StringType(), True),
    tp.StructField('like', tp.IntegerType(), True),
    tp.StructField('love', tp.IntegerType(), True),
    tp.StructField('care', tp.IntegerType(), True),
    tp.StructField('haha', tp.IntegerType(), True),
    tp.StructField('wow', tp.IntegerType(), True),
    tp.StructField('angry', tp.IntegerType(), True),
    tp.StructField('sad', tp.IntegerType(), True)
])

fb_comments_kafka_schema = tp.StructType([
    tp.StructField('uuid', tp.StringType(), True),
    tp.StructField('account', tp.StringType(), True),
    tp.StructField('post_id', tp.StringType(), True),
    tp.StructField('retrieving_time', tp.StringType(), True),  
    tp.StructField('timestamp', tp.StringType(), True),
    tp.StructField('content', tp.StringType(), True),
    tp.StructField('candidate', tp.StringType(), True),
    tp.StructField('like', tp.IntegerType(), True),
    tp.StructField('love', tp.IntegerType(), True),
    tp.StructField('care', tp.IntegerType(), True),
    tp.StructField('haha', tp.IntegerType(), True),
    tp.StructField('wow', tp.IntegerType(), True),
    tp.StructField('angry', tp.IntegerType(), True),
    tp.StructField('sad', tp.IntegerType(), True)
])

# Define Spark connection to ElasticSearch
sparkConf = SparkConf().set("es.nodes", "http://elasticsearch") \
                        .set("es.port", "9200") \
                        .set("spark.driver.memory","3G") \
                        .set("spark.executor.memory", "3G") \
                        .set("spark.driver.maxResultSize", "0") \
                        .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails")
              
# Build Spark Session
print("Starting Spark Session")
spark = SparkSession.builder \
    .appName("Spark-YT") \
    .master("local[8]") \
    .config(conf=sparkConf) \
    .getOrCreate()

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

# Read the stream from Kafka
print("Reading stream from kafka...")

df_facebook_posts = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", fb_posts_topic) \
    .load()

# Parse the Kafka message value using from_json
df_facebook_posts = df_facebook_posts.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), fb_posts_kafka_schema).alias("data")) \
              .select("data.*")

# Select relevant data for video dataframe from the parsed dataframe
df_facebook_posts = df_facebook_posts.select(
                            col("uuid"),
                            col("retrieving_time"),
                            col("timestamp"),
                            col("candidate"),
                            col("content"),
                            col("like"),
                            col("love"),
                            col("care"),
                            col("haha"),
                            col("wow"),
                            col("angry"),
                            col("sad"),
                    )

# Enrich facebook posts dataframe with sentiment and emotion analysis
df_facebook_posts = df_facebook_posts.withColumn("sentiment", sentiment_udf(df_facebook_posts["content"]))
df_facebook_posts = df_facebook_posts.withColumn("emotion", emotion_udf(df_facebook_posts["content"]))

# Expand dataframe and select relevant columns 
df_facebook_posts = df_facebook_posts.select(
                            col("uuid"),
                            col("retrieving_time"),
                            col("timestamp"),
                            col("candidate"),
                            col("content"),
                            col("like"),
                            col("love"),
                            col("care"),
                            col("haha"),
                            col("wow"),
                            col("angry"),
                            col("sad"),
                            col("sentiment.sentiment_label").alias("sentiment_label"),
                            col("sentiment.negative").alias("negative"),
                            col("sentiment.neutral").alias("neutral"),
                            col("sentiment.positive").alias("positive"),
                            col("emotion.emotion_label").alias("emotion_label")
                    )

df_facebook_comments = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", fb_comments_topic) \
    .load()

# Parse the Kafka message value using from_json
df_facebook_comments = df_facebook_comments.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), fb_comments_kafka_schema).alias("data")) \
              .select("data.*")

# Select relevant data for video dataframe from the parsed dataframe
df_facebook_comments = df_facebook_comments.select(
                            col("uuid"),
                            col("post_id"),
                            col("retrieving_time"),
                            col("timestamp"),
                            col("account"),
                            col("content"),
                            col("candidate"),
                            col("like"),
                            col("love"),
                            col("care"),
                            col("haha"),
                            col("wow"),
                            col("angry"),
                            col("sad"),
                    )

# Enrich facebook posts dataframe with sentiment and emotion analysis
df_facebook_comments = df_facebook_comments.withColumn("sentiment", sentiment_udf(df_facebook_comments["content"]))
df_facebook_comments = df_facebook_comments.withColumn("emotion", emotion_udf(df_facebook_comments["content"]))

# Expand dataframe and select relevant columns 
df_facebook_comments = df_facebook_comments.select(
                            col("uuid"),
                            col("post_id"),
                            col("retrieving_time"),
                            col("timestamp"),
                            col("account"),
                            col("content"),
                            col("candidate"),
                            col("like"),
                            col("love"),
                            col("care"),
                            col("haha"),
                            col("wow"),
                            col("angry"),
                            col("sad"),
                            col("sentiment.sentiment_label").alias("sentiment_label"),
                            col("sentiment.negative").alias("negative"),
                            col("sentiment.neutral").alias("neutral"),
                            col("sentiment.positive").alias("positive"),
                            col("emotion.emotion_label").alias("emotion_label")
                    )

# Send facebook posts dataframe to Elasticsearch
fb_posts_query = df_facebook_posts.writeStream \
                    .format("es") \
                    .option("failOnDataLoss", "false") \
                    .option("checkpointLocation", "/tmp/fb_sentivoter_posts_checkpoint/") \
                    .trigger(once=True) \
                    .start("fb_sentivoter_posts") \
                    .awaitTermination()

# Send facebook comments dataframe to Elasticsearch
fb_comments_query = df_facebook_comments.writeStream \
                        .format("es") \
                        .option("failOnDataLoss", "false") \
                        .option("checkpointLocation", "/tmp/fb_sentivoter_comments_checkpoint/") \
                        .trigger(once=True) \
                        .start("fb_sentivoter_comments") \
                        .awaitTermination()



                      

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.types as tp
from pyspark.sql.functions import col, from_json, udf
import tweetnlp
from transformers import logging
import warnings

# To reduce verbose output
warnings.filterwarnings("ignore", category=FutureWarning)
logging.set_verbosity_error()

# Define parameters to connect to Kafka Broker
kafkaServer = "kafka:9092"
yt_topic = "yt_sentivoter_comments"

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

# Define Kafka messages structure
yt_kafka_schema =  tp.StructType([
    tp.StructField('url_video', tp.StringType(), True),
    tp.StructField('title', tp.StringType(), True),
    tp.StructField('views', tp.IntegerType(), True),
    tp.StructField('video_likes', tp.IntegerType(), True),
    tp.StructField('id_video', tp.StringType(), True),
    tp.StructField('channel', tp.StringType(), True),
    tp.StructField('channel_bias', tp.StringType(), True),
    tp.StructField('state', tp.StringType(), True),
    tp.StructField('video_timestamp', tp.StringType(), True),
    tp.StructField('comment_cid', tp.StringType(), True),
    tp.StructField('comment_published_at', tp.StringType(), True),
    tp.StructField('comment_author', tp.StringType(), True),
    tp.StructField('comment_text', tp.StringType(), True),
    tp.StructField('comment_votes', tp.IntegerType(), True),
])

# Define Spark connection to ElasticSearch
sparkConf = SparkConf().set("es.nodes", "http://elasticsearch") \
                        .set("es.port", "9200") \
                        .set("spark.driver.memory","2G") \
                        .set("spark.executor.memory", "2G") \
                        .set("spark.driver.maxResultSize", "0") \
                        .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails")
              
# Build Spark Session
print("Starting Spark Session")
spark = SparkSession.builder \
    .appName("Spark-YT") \
    .master("local[3]") \
    .config(conf=sparkConf) \
    .getOrCreate()

# To reduce verbose output
spark.sparkContext.setLogLevel("ERROR") 

# Read the stream from Kafka
print("Reading stream from kafka...")
df_youtube = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", yt_topic) \
    .load()

# Parse the Kafka message value using from_json
df_youtube_parsed = df_youtube.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), yt_kafka_schema).alias("data")) \
              .select("data.*")

# Select comments dataframe
df_youtube_comments = df_youtube_parsed.select(
                           col("id_video"),
                           col("video_timestamp").alias("timestamp"),
                           col("channel"),
                           col("channel_bias"),
                           col("state"),
                           col("url_video"),
                           col("title"),
                           col("video_likes"),
                           col("views"),
                           col("comment_cid").alias("comment_id"),
                           col("comment_published_at"),
                           col("comment_author"),
                           col("comment_text"),
                           col("comment_votes")
                       )

# Enrich comments dataframe with sentiment analysis and emotion analysis
df_youtube_comments = df_youtube_comments.withColumn("sentiment", sentiment_udf(df_youtube_comments["comment_text"]))
df_youtube_comments = df_youtube_comments.withColumn("emotion", emotion_udf(df_youtube_comments["comment_text"]))

# Expand dataframe and select relevant columns 
df_youtube_comments = df_youtube_comments.select(
                           col("id_video"),
                           col("timestamp"),
                           col("channel"),
                           col("channel_bias"),
                           col("state"),
                           col("url_video"), 
                           col("title"),
                           col("video_likes"),
                           col("views"),
                           col("comment_id"),
                           col("comment_published_at"),
                           col("comment_author"),
                           col("comment_text"),
                           col("comment_votes"),
                           col("sentiment.sentiment_label").alias("sentiment_label"),
                           col("sentiment.negative").alias("negative"),
                           col("sentiment.neutral").alias("neutral"),
                           col("sentiment.positive").alias("positive"),
                           col("emotion.emotion_label").alias("emotion_label")
                        )

# Send youtube comments dataframe to Elasticseach
yt_comments_query = df_youtube_comments.writeStream \
                        .format("es") \
                        .option("failOnDataLoss", "false") \
                        .option("checkpointLocation", "/tmp/sentivoter_comments_checkpoint/") \
                        .option("es.batch.write.retry.count", "10") \
                        .option("es.batch.write.retry.wait", "100ms") \
                        .start("yt_sentivoter_comments")

yt_comments_query.awaitTermination()

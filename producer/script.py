from datetime import datetime
import mysql.connector
from elasticsearch import Elasticsearch
from datetime import datetime
import time
import json
import glob
import os
import requests

LOGSTASH_URL: str = "http://logstash:9700"

es = Elasticsearch(['http://elasticsearch:9200'])

def initialize_elasticsearch_indexes():
    yt_comments_index_name = 'yt_sentivoter_comments'
    yt_comments_mapping = {
        "mappings": {
            "dynamic_templates": [
                {
                    "timestamps": {
                        "match": "timestamp",  
                        "mapping": { "type": "date" }
                    }
                }
            ],
            "properties": {
                "id_video": { "type": "keyword" }, 
                "comment_id": { "type": "keyword" },
                "channel": { "type": "keyword" },
                "channel_bias": { "type": "keyword" },
                "state": { "type": "keyword" },
                "timestamp": { "type": "date" }, 
                "title": { "type": "text"},
                "url_video": { "type": "text"},
                "video_likes": { "type": "integer"},
                "views":  { "type": "integer"},
                "comment_published_at": { "type": "date" },
                "comment_author": { "type": "text" },
                "comment_text": { "type": "text" },
                "comment_votes": { "type": "integer" },
                "sentiment_label": { "type": "keyword" },
                "negative": { "type": "float" },
                "neutral": { "type": "float" },
                "positive": { "type": "float" },
                "emotion_label": { "type": "keyword" }
            }
        }
    }

    if not es.indices.exists(index=yt_comments_index_name):
        es.indices.create(index=yt_comments_index_name, body=yt_comments_mapping, ignore=400)
        print(f"Successfully created index '{yt_comments_index_name}'")
    else:
        print(f"Index '{yt_comments_index_name}' already exists")

    yt_videos_index_name = 'yt_sentivoter_videos'
    yt_videos_mapping = {
        "mappings": {
            "dynamic_templates": [
                {
                    "timestamps": {
                        "match": "timestamp",  
                        "mapping": { "type": "date" }
                    }
                }
            ],
            "properties": {
                "timestamp": { "type": "date" },
                "title": { "type": "text" },
                "channel": { "type": "keyword" },
                "channel_bias": { "type": "keyword" },
                "state": { "type": "keyword" },
                "url_video": { "type": "text" },
                "id_video": { "type": "keyword" },
                "views": { "type": "integer" },
                "video_likes": { "type": "integer" },
                "comments": { "type": "integer"},
                "sentiment_label": { "type": "keyword" },
                "negative": { "type": "float" },
                "neutral": { "type": "float" },
                "positive": { "type": "float" }
            }
        }
    }
    
    if not es.indices.exists(index=yt_videos_index_name):
        es.indices.create(index=yt_videos_index_name, body=yt_videos_mapping, ignore=400)
        print(f"Successfully created index '{yt_videos_index_name}'")
    else:
        print(f"Index '{yt_videos_index_name}' already exists")

    fb_posts_index_name = 'fb_sentivoter_posts'
    fb_posts_mapping = {
        "mappings": {
            "dynamic_templates": [
                {
                    "timestamps": {
                        "match": "timestamp",  
                        "mapping": { "type": "date" }
                    }
                }
            ],
            "properties": {
                "uuid": { "type": "keyword" },
                "retrieving_time": { "type": "date" },
                "timestamp" : { "type": "date" },
                "candidate": { "type": "text" },
                "content" : { "type": "text" },
                "like" : { "type": "integer" },
                "love" : { "type": "integer" },
                "care" : { "type": "integer" },
                "haha": { "type": "integer" },
                "wow" : { "type": "integer" },
                "angry" : { "type": "integer" }, 
                "sad" : { "type": "integer" },
                "sentiment_label": { "type": "keyword" },
                "negative": { "type": "float" },
                "neutral": { "type": "float" },
                "positive": { "type": "float" },
                "emotion_label": { "type": "keyword" } 
            }
        }
    }

    if not es.indices.exists(index=fb_posts_index_name):
        es.indices.create(index=fb_posts_index_name, body=fb_posts_mapping)
        print(f"Successfully created index '{fb_posts_index_name}'")
    else:
        print(f"Index '{fb_posts_index_name}' already exists")

    fb_comments_index_name = 'fb_sentivoter_comments'
    fb_comments_mapping = {
        "mappings": {
            "dynamic_templates": [
                {
                    "timestamps": {
                        "match": "timestamp",  
                        "mapping": { "type": "date" }
                    }
                }
            ],
            "properties": {
                "uuid": { "type": "keyword" },
                "post_id": { "type": "keyword" },
                "retrieving_time": { "type": "date" },
                "timestamp" : { "type": "date" },
                "candidate": { "type": "text" },
                "account": { "type": "text" },
                "content" : { "type": "text" },
                "like" : { "type": "integer" },
                "love" : { "type": "integer" },
                "care" : { "type": "integer" },
                "haha": { "type": "integer" },
                "wow" : { "type": "integer" },
                "angry" : { "type": "integer" }, 
                "sad" : { "type": "integer" },
                "sentiment_label": { "type": "keyword" },
                "negative": { "type": "float" },
                "neutral": { "type": "float" },
                "positive": { "type": "float" },
                "emotion_label": { "type": "keyword" } 
            }
        }
    }

    if not es.indices.exists(index=fb_comments_index_name):
        es.indices.create(index=fb_comments_index_name, body=fb_comments_mapping)
        print(f"Successfully created index '{fb_comments_index_name}'")
    else:
        print(f"Index '{fb_comments_index_name}' already exists")

# Retrieves the last timestamp in the specified index
def get_latest_timestamp(index, timestamp_field, channel_id):
    try:
        response = es.search(
            index=index,
            body={
                "size": 1,
                "sort": [{timestamp_field: "desc"}],
                "_source": [timestamp_field],
                "query": {
                    "term": {"channel": channel_id}
                }
            }
        )
        if response['hits']['total']['value'] > 0:
            return response['hits']['hits'][0]['_source'][timestamp_field]
        else:
            return None
    except Exception as e:
        print(f"Exception while retrieving last timestamp in Elasticseach: {e}")
        return None

def fetch_data_from_mysql(table):
    # Define connection parameters to db
    db_config = {
        'host': 'mysql_database',  
        'port': '3306',
        'user': 'root',                  
        'password': 'root',     
        'database': 'Elections'          
    }

    # Connect to db
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)  # Ritorna i risultati come dizionari
    
    # Fetch data from db
    cursor.execute(f"SELECT * FROM {table};")
    rows = cursor.fetchall()

    print(f"Retrieved {str(len(rows))} rows.")

    # Modify each dictionary and send it to Logstash
    for data in rows:
        for key, value in data.items():
            if isinstance(value, datetime):  
                data[key] = value.isoformat()
        
        data["social"] = "facebook"
        data["section"] = table

        requests.post(LOGSTASH_URL, json=data)

    conn.close()
    cursor.close()

if __name__ == "__main__":
        
    #initialize_elasticsearch_indexes()

    #print("Sending facebook data...")
    #fetch_data_from_mysql(table="Posts")
    #fetch_data_from_mysql(table="Comments_with_candidate_column")

    #print("Sending youtube videos...")

    #for filepath in glob.glob("/videos_data/*.json"):
    #    time.sleep(30)
    #    print(f"Opening {filepath}")
    #    with open(filepath, "r") as file:
    #        data = json.load(file)
    #        print(f"Sending data from {filepath}")
    #        for element in data:               
    #            requests.post(LOGSTASH_URL, json=element)

    time.sleep(120)

    print("Sending youtube comments...") 

    for filepath in glob.glob("/comments_data/*.json"):
        time.sleep(30)
        with open(filepath, "r") as file:
            print(f"Opening {filepath}")
            data = json.load(file)
            print(f"Sending data from {filepath}")
            for element in data:
                requests.post(LOGSTASH_URL, json=element)
from datetime import datetime
from elasticsearch import Elasticsearch
import mysql.connector
import requests

LOGSTASH_URL: str = "http://logstash:9700"
ELASTICSEARCH_URL: str = "http://elasticsearch:9200"
es = Elasticsearch([ELASTICSEARCH_URL])

def initialize_elasticsearch_indexes():
    fb_posts_index_name = 'fb_sentivoter_posts'
    fb_posts_mapping = {
        "mappings": {
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
            "properties": {
                "uuid": { "type": "keyword" },
                "post_id": { "type": "keyword" },
                "retrieving_time": { "type": "date" },
                "timestamp" : { "type": "date" },
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
    cursor.execute(f"SELECT * FROM {table} WHERE retrieving_time>'2024-12-10' LIMIT 4;")
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

def main():
    initialize_elasticsearch_indexes()
    fetch_data_from_mysql(table="Posts")
    fetch_data_from_mysql(table="Comments")

if __name__ == "__main__":
    main()
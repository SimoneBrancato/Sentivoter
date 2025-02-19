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
        

    print("Sending facebook data...")
    fetch_data_from_mysql(table="Posts")
    fetch_data_from_mysql(table="Comments_with_candidate_column")

    print("Sending youtube videos...")

    for filepath in glob.glob("/videos_data/*.json"):
        time.sleep(30)
        print(f"Opening {filepath}")
        with open(filepath, "r") as file:
            data = json.load(file)
            print(f"Sending data from {filepath}")
            for element in data:               
                requests.post(LOGSTASH_URL, json=element)


    print("Sending youtube comments...") 

    for filepath in glob.glob("/comments_data/*.json"):
        with open(filepath, "r") as file:
            print(f"Opening {filepath}")
            data = json.load(file)
            print(f"Sending data from {filepath}")
            for element in data:
                requests.post(LOGSTASH_URL, json=element)
from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta
from youtube_transcript_api import YouTubeTranscriptApi
from datetime import datetime
from youtube_comment_downloader import *
from elasticsearch import Elasticsearch
import yt_dlp
from datetime import datetime
import time
import os
import requests

LOGSTASH_URL: str = "http://logstash:9700"
YT_API_KEY: str = os.getenv('YT_API_KEY')
CHANNEL_ID: str = os.getenv('CHANNEL_ID')
STATE: str = os.getenv('STATE')
CHANNEL_BIAS: str = os.getenv('BIAS')
KEYWORDS: str = "Harris|Trump|Elections"

es = Elasticsearch(['http://elasticsearch:9200'])

youtube = build('youtube', 'v3', developerKey=YT_API_KEY)
yt_downloader = yt_dlp.YoutubeDL({'quiet': True})
downloader = YoutubeCommentDownloader()

def initialize_elasticsearch_indexes():
    comments_index_name = 'sentivoter_comments'
    comments_mapping = {
        "mappings": {
            "properties": {
                "id_video": { "type": "keyword" },
                "cid": { "type": "keyword" },
                "channel": { "type": "keyword" },
                "channel_bias": { "type": "keyword" },
                "state": { "type": "keyword" },
                "video_timestamp": { "type": "date" },
                "published_at": { "type": "date" },
                "author": { "type": "text" },
                "text": { "type": "text" },
                "likes": { "type": "integer" },
                "sentiment_label": { "type": "keyword" },
                "negative": { "type": "float" },
                "neutral": { "type": "float" },
                "positive": { "type": "float" }
            }
        }
    }

    if not es.indices.exists(index=comments_index_name):
        es.indices.create(index=comments_index_name, body=comments_mapping)
        print(f"Successfully created index '{comments_index_name}'")
    else:
        print(f"Index '{comments_index_name}' already exists")

    videos_index_name = 'sentivoter_videos'
    videos_mapping = {
        "mappings": {
            "properties": {
                "timestamp": { "type": "date" },
                "title": { "type": "text" },
                "channel": { "type": "keyword" },
                "channel_bias": { "type": "keyword" },
                "state": { "type": "keyword" },
                "url_video": { "type": "text" },
                "id_video": { "type": "keyword" },
                "views": { "type": "integer" },
                "likes": { "type": "integer" },
                "fullText": { "type": "text" },
                "sentiment_label": { "type": "keyword" },
                "negative": { "type": "float" },
                "neutral": { "type": "float" },
                "positive": { "type": "float" }
            }
        }
    }
    
    if not es.indices.exists(index=videos_index_name):
        es.indices.create(index=videos_index_name, body=videos_mapping)
        print(f"Successfully created index '{videos_index_name}'")
    else:
        print(f"Index '{videos_index_name}' already exists")

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

def get_videos(channel_id, keywords, start_date):
    videos = []
    next_page_token = None

    current_date = start_date
    now = datetime.now()
    print(f"Retrieving videos from {start_date} to {now}")

    try:
        while current_date < now:
            sliding_date = current_date + relativedelta(days=10)

            if sliding_date > now:
                sliding_date = now

            current_date_str = current_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            sliding_date_str = sliding_date.strftime('%Y-%m-%dT%H:%M:%SZ')

            while True:
                request = youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    q=keywords,
                    type="video",
                    order="date",
                    maxResults=50,
                    publishedAfter=current_date_str,
                    publishedBefore=sliding_date_str,
                    pageToken=next_page_token
                )

                response = request.execute()

                for item in response['items']:
                    video_url = f"https://www.youtube.com/watch?v={item['id']['videoId']}"
                    if video_url not in videos:
                        videos.append(video_url)

                next_page_token = response.get('nextPageToken')

                if not next_page_token:
                    next_page_token = None
                    break

            current_date = sliding_date

    except Exception as e:
        print(f"Error during video extraction: {e}")

    finally:
        return videos
    
def get_video_comments(video_url):
    try:
        comments_data = downloader.get_comments_from_url(video_url, sort_by=SORT_BY_POPULAR)
        comments = []

        for comment in comments_data:
            comment_json = {
                'cid': comment['cid'],
                'published_at': datetime.fromtimestamp(comment['time_parsed']).isoformat(),
                'author': comment['author'],
                'text': comment['text'],
                'votes': int(comment['votes']) if comment['votes'] else 0
            }
            comments.append(comment_json)

        return comments

    except Exception:
        print("Error while extracting comments. Returning")
        return comments
    
def scrape_videos_by_list(videos: list):
    for video_url in videos:

        time.sleep(15)
        
        try:
            info_dict = yt_downloader.extract_info(video_url, download=False)
            text = YouTubeTranscriptApi.get_transcript(str(info_dict['id']))
            fullText = " ".join(response['text'].lower() for response in text)
            comments = get_video_comments(video_url)
        except Exception:
            print("Extracting failed for current video. Continue.")
            continue

        print("---------------------------------------------------------")
        print(f"Title: {video_url}")
        print(f"Comments: {len(comments)}")
        print(f"Timestamp: {str(datetime.fromtimestamp(info_dict['timestamp']).isoformat())}")
        print("---------------------------------------------------------")
        
        result_json = {
            'channel': CHANNEL_ID,
            'channel_bias': CHANNEL_BIAS,
            'state': STATE, 
            'url_video': video_url,
            'id_video': info_dict['id'],
            'title': info_dict['title'],
            'timestamp': datetime.fromtimestamp(info_dict['timestamp']).isoformat(),  
            'likes': info_dict['like_count'],
            'views': info_dict['view_count'],
            'fullText': fullText,
            'comments': comments
        }

        requests.post(LOGSTASH_URL, json=result_json)

def main():
    
    initialize_elasticsearch_indexes()

    latest_video_timestamp = get_latest_timestamp(index='sentivoter_videos', timestamp_field='timestamp', channel_id=CHANNEL_ID)
    latest_comment_timestamp = get_latest_timestamp(index='sentivoter_comments', timestamp_field='video_timestamp', channel_id=CHANNEL_ID)

    if latest_video_timestamp is None and latest_comment_timestamp is None:
        print("No data found in Elasticsearch. Starting scraping for the first time.")
        start_date = datetime(2024, 9, 1)
        
        print(f"Starting from: {start_date}")
        retrieved_videos = get_videos(CHANNEL_ID, KEYWORDS, start_date)
        print(f"Retrieved {len(retrieved_videos)} videos.")

        scrape_videos_by_list(retrieved_videos)

    elif latest_comment_timestamp and latest_video_timestamp:
        print("Latest video timestamp:", latest_video_timestamp)
        print("Latest comment timestamp:", latest_comment_timestamp)

        start_date = datetime.fromisoformat(latest_video_timestamp)

        print(f"Starting from: {start_date}")
        retrieved_videos = get_videos(CHANNEL_ID, KEYWORDS, start_date)
        print(f"Retrieved {len(retrieved_videos)} videos.")

        scrape_videos_by_list(retrieved_videos)

    else:
        print(f"Something wrong with timestamps.\nLatest video timestamp: {latest_video_timestamp}\nLatest comment timestamp: {latest_comment_timestamp}")

if __name__ == "__main__":
    main()

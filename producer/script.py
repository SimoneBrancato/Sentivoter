from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta
from youtube_transcript_api import YouTubeTranscriptApi
from itertools import islice
from datetime import datetime
from youtube_comment_downloader import *
import json
import yt_dlp
from datetime import datetime
import os
import requests

LOGSTASH_URL = "http://logstash:9700"
YT_API_KEY = os.getenv('YT_API_KEY')
CHANNEL_ID = os.getenv('CHANNEL_ID')

youtube = build('youtube', 'v3', developerKey=YT_API_KEY)

def get_videos(channel_id, keywords, start_date):
    videos = []
    next_page_token = None

    end_date = datetime.now() - relativedelta(weeks=1)
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    try:

        while end_date > start_date:
            sliding_date = end_date - relativedelta(weeks=1)
            sliding_date_str = sliding_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        
            while sliding_date > start_date:
                request = youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    q=keywords,  
                    type="video",
                    order="date",
                    maxResults=50,
                    publishedAfter=sliding_date_str,
                    publishedBefore=end_date_str,   
                    pageToken=next_page_token  
                )

                response = request.execute()

                for item in response['items']:
                    video_url = f"https://www.youtube.com/watch?v={item['id']['videoId']}"
                    videos.append(video_url)

                # next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                
            end_date = sliding_date
            
        return videos
    
    except Exception as e:
        print(e)
        return videos
        
def get_video_comments(video_url):
    downloader = YoutubeCommentDownloader()
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

def main():
    yt_downloader = yt_dlp.YoutubeDL({'quiet': True})
    keywords = "Harris|Trump|Elections"
    start_date = datetime(2024, 10, 10)
    videos = get_videos(CHANNEL_ID, keywords, start_date)
    
    print(f"Retrieved {len(videos)} videos.")

    for video_url in videos:
        time.sleep(10)
        
        try:
            info_dict = yt_downloader.extract_info(video_url, download=False)
            text = YouTubeTranscriptApi.get_transcript(str(info_dict['id']))
            fullText = " ".join(response['text'].lower() for response in text)
            comments = get_video_comments(video_url)
        except Exception:
            print("Extracting failed for current video. Continue.")
            continue

        print("---------------------------------------------------------")
        print(f"Video: {video_url}")
        print(f"Comments: {len(comments)}")
        print("---------------------------------------------------------")
        
        result_json = {
            'url_video': video_url,
            'id_video': info_dict['id'],
            'title': info_dict['title'],
            'published_at': datetime.fromtimestamp(info_dict['timestamp']).isoformat(),  
            'likes': info_dict['like_count'],
            'views': info_dict['view_count'],
            'fullText': fullText,
            'comments': comments
        }

        requests.post(LOGSTASH_URL, json=result_json)

if __name__ == "__main__":
    main()

from youtube_transcript_api import YouTubeTranscriptApi
from youtube_comment_downloader import *
import yt_dlp
from datetime import datetime

yt_downloader = yt_dlp.YoutubeDL({'quiet': True})
downloader = YoutubeCommentDownloader()

CHANNEL_NAME: str = "CBSNews"
STATE: str = "NY"
CHANNEL_BIAS: str = "LEAN_DEMOCRAT"
LINKS: str = "../yt_links_producer/yt_links/cbsnews.txt"
OUTPUT_JSON_FILE: str = "CBS.json"

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
        comments = []
        return comments

def scrape_videos_by_list(videos: list):
    json_list = []
    
    for video_url in videos:

        time.sleep(1)
        
        try:
            info_dict = yt_downloader.extract_info(video_url, download=False)
            text = YouTubeTranscriptApi.get_transcript(str(info_dict['id']))
            fullText = " ".join(response['text'].lower() for response in text)
            comments = get_video_comments(video_url)
        except Exception:
            print("Extracting failed for current video. Continue.")
            continue
        
        result_json = {
            'channel': CHANNEL_NAME,
            'channel_bias': CHANNEL_BIAS,
            'state': STATE, 
            'url_video': video_url,
            'id_video': info_dict['id'],
            'title': info_dict['title'],
            'timestamp': datetime.fromtimestamp(info_dict['timestamp']).isoformat(),  
            'likes': info_dict['like_count'],
            'views': info_dict['view_count'],
            'fullText': fullText,
            'social': 'youtube',
            'comments': comments
        }

        json_list.append(result_json)
    return json_list

def main():
     
    with open(LINKS, 'r') as links_file:
        links = [line.strip() for line in links_file]

    print(f"Retrieved {len(links)} videos from text file.")
    json_list = scrape_videos_by_list(links)

    with open(OUTPUT_JSON_FILE, "w") as file:
        json.dump(json_list, file, indent=4)





if __name__ == "__main__":
    main()
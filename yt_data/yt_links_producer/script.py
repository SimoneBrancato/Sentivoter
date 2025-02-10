from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os

ABCNEWS_CID: str = "UCBi2mrWuNuyYy4gbM6fU18Q"
FOXNEWS_CID: str = "UCXIJgqnII2ZOINSWNOGFThA"
NBCNEWS_CID: str = "UCeY0bbntWzzVIaj2z3QigXg"
CBSNEWS_CID: str = "UC8p1vwvWtl6T73JiExfWs1g"
USATODAY_CID: str = "UCP6HGa63sBC7-KHtkme-p-g"
THE_YOUNG_TURKS_CID: str = "UC1yBKRuGpC1tSM73A0ZjYjQ"
GOOD_MORNINGA_AMERICA_CID: str = "UCH1oRy1dINbMVp3UFWrKP0w"
THE_WASHINGTON_POST_CID: str = "UCHd62-u_v4DvJ8TCFtpi4GA"
NEW_YORK_TIMES_CID: str = "UCqnbDFdCpuN8CMEg0VuEBqA"
THE_WALL_STREET_JOURNAL_CID: str = "UCK7tptUDHh-RYDsdxO1-5QQ"

YT_API_KEY: str = os.getenv('YT_API_KEY')

KEYWORDS: str = "Harris|Trump|Elections"

youtube = build('youtube', 'v3', developerKey=YT_API_KEY)

def links_retriever(channel_id, keywords, start_date, end_date):
    videos = []
    next_page_token = None

    current_date = start_date
    print(f"Retrieving videos from {start_date} to {end_date}")

    try:
        while current_date < end_date:
            sliding_date = current_date + relativedelta(days=10)

            if sliding_date > end_date:
                sliding_date = end_date

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

def main():
    links = links_retriever(ABCNEWS_CID, KEYWORDS, datetime(2024,9,1), datetime(2025,1,20))
    with open('thewall.txt', 'w') as file:
        for link in links:
            file.write(link+'\n')

if __name__ == "__main__":
    main()

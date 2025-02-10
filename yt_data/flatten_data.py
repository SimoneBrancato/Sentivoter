import json

def flatten_comments(json_data):
    extracted_data = []

    for video in json_data:
        for comment in video['comments']:
            extracted_data.append({
                'channel': video['channel'],
                'channel_bias': video['channel_bias'],
                'state': video['state'],
                'url_video': video['url_video'],
                'id_video': video['id_video'],
                'title': video['title'],
                'video_timestamp': video['timestamp'],
                'video_likes': video['likes'],
                'views': video['views'],
                'social': video['social'],
                'comment_cid': comment['cid'],
                'comment_published_at': comment['published_at'],
                'comment_author': comment['author'],
                'comment_text': comment['text'],
                'comment_votes': comment['votes']
            })
    return extracted_data

def flatten_video(json_data):
    extracted_data = []

    for video in json_data:
        extracted_data.append({
            'channel': video['channel'],
            'channel_bias': video['channel_bias'],
            'state': video['state'],
            'url_video': video['url_video'],
            'id_video': video['id_video'],
            'title': video['title'],
            'video_timestamp': video['timestamp'],
            'fullText': video['fullText'],
            'video_likes': video['likes'],
            'views': video['views'],
            'comments': len(video['comments']),
            'social': video['social']
        })
    return extracted_data

if __name__ == "__main__":
    channel_list = ["ABC","CBS","FOX","GMA","NBC","NYT","TYT","USA","WPO","WSJ"]

    for channel in channel_list:
        INPUT_PATH: str = f"./data/{channel}.json"
        OUTPUT_COMMENTS_PATH: str = f"./flattened_comments_data/{channel}_comments.json"
        OUTPUT_VIDEOS_PATH: str = f"./flattened_videos_data/{channel}_videos.json"

        with open(INPUT_PATH, "r") as f:
            json_data = json.load(f)
        
        comments_flattened_data = flatten_comments(json_data)
        with open(OUTPUT_COMMENTS_PATH, "w") as f:
            json.dump(comments_flattened_data, f, indent=4)
        print(F"Estrazione completata. File salvato come {OUTPUT_COMMENTS_PATH}")

        videos_flattened_data = flatten_video(json_data)
        with open(OUTPUT_VIDEOS_PATH, "w") as f:
            json.dump(videos_flattened_data, f, indent=4)
        print(F"Estrazione completata. File salvato come {OUTPUT_VIDEOS_PATH}")


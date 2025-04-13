## What is Sentivoter?
Sentivoter is a cross-media data collection and sentiment analysis pipeline designed to monitor public opinion on the 2024 U.S. presidential election.
The project aims to provide in-dept analysis of sentiment and emotions expressed by users on Facebook and YouTube, focusing on content related to Kamala Harris and Donald Trump.

<div align="center">
  <img src="https://github.com/user-attachments/assets/84e72f49-2c17-4ac8-a193-92f0e0f17312" alt="ChatGPT Image" width="400"/>
</div>

## Prerequisites
- **Docker/Docker Compose:** Ensure you have a fully functional [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installation on your local computer.
- **Prepare the dataset** since the analysis will be in batch mode, you first need to prepare the required files.
  - Facebook data should be collected using the Elections-Crawler module and exporting the scraped content into a MySQL dump located at `mysql/dump.sql`.
  - YouTube data can be generated using the scripts provided in the `yt_data` directory. Make sure to flatten the raw data to make it ready for the ingestion phase.

The expected data format is described in the next section.

## Data Format
Each entry in `yt_data/flattened_comments_data/*.json` represents a single comment enriched with metadata from the video it belongs to. 
The expected format is as follows:
```json
{
  "channel": "WashingtonPost",        // Name of the YouTube channel
  "channel_bias": "DEMOCRAT",         // Channel political leaning (e.g., DEMOCRAT or REPUBLICAN)
  "state": "DC",                      // U.S. state associated with the channel
  "url_video": "https://www.you...",  // Full URL of the video
  "id_video": "k8cUC0V0C3U",          // YouTube video ID
  "title": "Debunking Trump’s...",    // Title of the video
  "video_timestamp": "2024-09...",    // Upload datetime of the video (ISO format)
  "video_likes": 59,                  // Number of likes the video received
  "views": 2063,                      // Number of views for the video
  "social": "youtube",                // Social media source (constant: "youtube")
  "comment_cid": "UgzeZQE7l...",      // Comment ID
  "comment_published_at": "202...",   // Comment timestamp (ISO format)
  "comment_author": "@desir...",      // Username of the commenter
  "comment_text": "Highes...",        // Text content of the comment
  "comment_votes": 9                  // Number of likes/upvotes the comment received
}
```

Each entry in `yt_data/flattened_videos_data/*.json` represents a single video enriched with metadata from the video it belongs to.
```json
{
  "channel": "USAToday",              // Name of the YouTube channel
  "channel_bias": "LEAN_DEMOCRAT",    // Channel political leaning (e.g., DEMOCRAT, LEAN_DEMOCRAT)
  "state": "DC",                      // U.S. state associated with the channel
  "url_video": "https://www.yout...", // Full URL of the video
  "id_video": "7YXAno3DS1U",          // YouTube video ID
  "title": "House Spea...",           // Title of the video
  "video_timestamp": "2024-09-1...",  // Upload datetime of the video (ISO format)
  "fullText": "[music] we have...",   // Video transcript
  "video_likes": 172,                 // Number of likes
  "views": 14701,                     // Total views
  "comments": 68,                     // Number of comments
  "social": "youtube"                 // Source identifier (constant: "youtube")
}
```
The Facebook data, collected via the Elections-Crawler module, is stored as a MySQL dump (mysql/dump.sql). The dump includes two main tables:

```sql
CREATE TABLE Comments_with_candidate_column (
  uuid char(100) NOT NULL,          -- Unique identifier for the comment
  post_id char(100) DEFAULT NULL,   -- ID of the parent post
  candidate varchar(50) NOT NULL,   -- Candidate associated with the post
  timestamp datetime DEFAULT NULL,  -- Comment timestamp
  account char(36) DEFAULT NULL,    -- User of the commenter
  content varchar(1000) NOT NULL,   -- Comment text
  like int DEFAULT '0',
  love int DEFAULT '0',
  care int DEFAULT '0',
  haha int DEFAULT '0',
  wow int DEFAULT '0',
  angry int DEFAULT '0',
  sad int DEFAULT '0'
);

CREATE TABLE Posts (
  uuid char(100) NOT NULL,            -- Unique identifier for the post
  retrieving_time datetime NOT NULL,  -- When the post was scraped
  timestamp datetime DEFAULT NULL,    -- Original post timestamp
  candidate varchar(50) NOT NULL,     -- Candidate associated with the post
  content varchar(1000) NOT NULL,     -- Post text content
  like int DEFAULT '0',
  love int DEFAULT '0',
  care int DEFAULT '0',
  haha int DEFAULT '0',
  wow int DEFAULT '0',
  angry int DEFAULT '0',
  sad int DEFAULT '0',
  PRIMARY KEY (uuid,retrieving_time)
);
```

## Project Architecture
The data pipeline is structured as follows:
- **Producers:** read data from the directories and push it into the data pipeline.
- **Logstash:** ingestion layer, forwards data into 4 differents Kafka Topics.
- **Apache Kafka:** manages data streams in 4 different topics ensuring decoupled communication.
- **Apache Spark Cluster:** consumes data from Kafka and performs batch sentiment and emotion analysis running TweetNLP models. Sends the enriched data into 4 different Elasticsearch indexes.
- **Elasticsearch:** stores enriched data and provides high-performances querying
- **Kibana:** provides interactive visualizations and dashboards for exploring sentiment trends, emotional tones, and engagement across platforms and candidates.

  
![image](https://github.com/user-attachments/assets/ec4a72ee-15cb-4626-bc5a-a87e62f32988)




## Contacts
- **E-Mail:** simonebrancato18@gmail.com
- **LinkedIn:** [Simone Brancato](https://www.linkedin.com/in/simonebrancato18/)
- **GitHub:** [Simone Brancato](https://github.com/SimoneBrancato)


















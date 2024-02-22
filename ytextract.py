import mysql.connector
import pymongo
import traceback
from googleapiclient.discovery import build
from datetime import datetime

# Function to connect to YouTube API
def connect_to_youtube_api(api_key):
    try:
        youtube_api = build("youtube", "v3", developerKey=api_key)
        print("Connecting to YouTube API...")
        return youtube_api
    except Exception as e:
        print(f"An error occurred while connecting to YouTube API: {e}")
        return None

# Function to fetch data from YouTube API
def fetch_youtube_data(youtube_api, channel_ids):
    try:
        # Initialize lists for storing data
        channel_data = []
        video_info = []
        playlist_info = []
        comment_info = []

        for channel_id in channel_ids:
            # Fetch channel information
            channel_request = youtube_api.channels().list(part="snippet,statistics", id=channel_id)
            response = channel_request.execute()
            if response.get("items"):
                item = response["items"][0]
                channel_info = {
                    "channel_id": item["id"],
                    "channel_name": item["snippet"]["title"],
                    "subscribers": int(item["statistics"]["subscriberCount"]),
                    "total_videos": int(item["statistics"]["videoCount"])
                }
                channel_data.append(channel_info)

                # Fetch videos for the channel
                videos_request = youtube_api.search().list(part="snippet", channelId=channel_id, type="video",
                                                            maxResults=10)
                videos_response = videos_request.execute()
                for video_item in videos_response.get("items", []):
                    video_id = video_item["id"]["videoId"]
                    video_snippet = video_item["snippet"]
                    video_published_at = datetime.strptime(video_snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
                    video_info.append({
                        "video_id": video_id,
                        "title": video_snippet["title"],
                        "description": video_snippet.get("description", ""),
                        "thumbnail_url": video_snippet["thumbnails"]["default"]["url"],
                        "channel_title": video_snippet["channelTitle"],
                        "published_at": video_published_at
                    })

                    try:
                        # Fetch comments for each video
                        comments_request = youtube_api.commentThreads().list(part="snippet", videoId=video_id,
                                                                             maxResults=10)
                        comments_response = comments_request.execute()
                        for comment_item in comments_response.get("items", []):
                            comment_snippet = comment_item["snippet"]["topLevelComment"]["snippet"]
                            comment_published_at = datetime.strptime(comment_snippet["publishedAt"],
                                                                      "%Y-%m-%dT%H:%M:%SZ")
                            comment_info.append({
                                "comment_id": comment_item["id"],
                                "video_id": video_id,
                                "author": comment_snippet["authorDisplayName"],
                                "text": comment_snippet["textDisplay"],
                                "like_count": comment_snippet["likeCount"],
                                "published_at": comment_published_at
                            })
                    except Exception as e:
                        print(f"An error occurred while fetching comments for video {video_id}: {e}")

                # Fetch playlists for the channel
                playlists_request = youtube_api.playlists().list(part="snippet", channelId=channel_id,
                                                                 maxResults=10)
                playlists_response = playlists_request.execute()
                for playlist_item in playlists_response.get("items", []):
                    total_videos = playlist_item.get("contentDetails", {}).get("itemCount", 0)
                    playlist_info.append({
                        "playlist_id": playlist_item["id"],
                        "title": playlist_item["snippet"]["title"],
                        "description": playlist_item["snippet"]["description"],
                        "channel_title": playlist_item["snippet"]["channelTitle"],
                        "total_videos": total_videos
                    })

            else:
                print(f"No channel found with ID: {channel_id}")

        return channel_data, video_info, playlist_info, comment_info

    except Exception as e:
        print(f"An error occurred while fetching YouTube data: {e}")
        return None, None, None, None

# Function to insert data into MongoDB
def insert_into_mongodb(mongo_client, channel_data, video_info, playlist_info, comment_info):
    try:
        mongo_db = mongo_client.youtube
        if channel_data:
            mongo_db.channels.insert_many(channel_data)
        if video_info:
            mongo_db.video.insert_many(video_info)
        if playlist_info:
            mongo_db.playlist.insert_many(playlist_info)
        if comment_info:
            mongo_db.comment.insert_many(comment_info)
        print("Data inserted into MongoDB.")
        return True
    except Exception as e:
        print(f"An error occurred while inserting data into MongoDB: {e}")
        return False

# Function to create MySQL database and tables
def create_mysql_database_and_tables(mysql_connection):
    try:
        mysql_cursor = mysql_connection.cursor()

        # Create MySQL database if not exists
        mysql_cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_data")
        mysql_cursor.execute("USE youtube_data")

        # Create MySQL tables
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                channel_id VARCHAR(255) ,
                channel_name VARCHAR(255),
                subscribers INT,
                total_videos INT
            )
        """)
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS video_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_id VARCHAR(255),
                title VARCHAR(255),
                description TEXT,
                thumbnail_url TEXT,
                channel_title VARCHAR(255),
                published_at DATETIME
            )
        """)
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                playlist_id VARCHAR(255),
                title VARCHAR(255),
                description TEXT,
                channel_title VARCHAR(255),
                total_videos INT
            )
        """)
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                comment_id VARCHAR(255),
                video_id VARCHAR(255),
                author VARCHAR(255),
                text TEXT,
                like_count INT,
                published_at DATETIME
            )
        """)
        print("MySQL database and tables created successfully.")
        return True
    except Exception as e:
        print(f"An error occurred while creating MySQL database and tables: {e}")
        return False

# Function to migrate data from MongoDB to MySQL
def migrate_data_to_mysql(mysql_connection, mongo_client):
    try:
        mysql_cursor = mysql_connection.cursor()
        mongo_db = mongo_client.youtube

        # Fetch data from MongoDB
        channel_data = list(mongo_db.channels.find())
        video_info = list(mongo_db.video.find())
        playlist_info = list(mongo_db.playlist.find())
        comment_info = list(mongo_db.comment.find())

        # Migrate data to MySQL
        for entry in channel_data:
            mysql_cursor.execute("SELECT * FROM channel_data WHERE channel_id = %s", (entry["channel_id"],))
            result = mysql_cursor.fetchone()
            if not result:
                mysql_cursor.execute("INSERT INTO channel_data (channel_id, channel_name, subscribers, total_videos) VALUES (%s, %s, %s, %s)",
                                     (entry["channel_id"], entry["channel_name"], entry["subscribers"], entry["total_videos"]))
        for entry in video_info:
            mysql_cursor.execute("INSERT INTO video_info (video_id, title, description, thumbnail_url, channel_title, published_at) VALUES (%s, %s, %s, %s, %s, %s)",
                                 (entry["video_id"], entry["title"], entry["description"], entry["thumbnail_url"], entry["channel_title"], entry["published_at"]))
        for entry in playlist_info:
            mysql_cursor.execute("INSERT INTO playlist_info (playlist_id, title, description, channel_title, total_videos) VALUES (%s, %s, %s, %s, %s)",
                                 (entry["playlist_id"], entry["title"], entry["description"], entry["channel_title"], entry["total_videos"]))
        for entry in comment_info:
            mysql_cursor.execute("INSERT INTO comment_info (comment_id, video_id, author, text, like_count, published_at) VALUES (%s, %s, %s, %s, %s, %s)",
                                 (entry["comment_id"], entry["video_id"], entry["author"], entry["text"], entry["like_count"], entry["published_at"]))

        mysql_connection.commit()
        print("Data migrated from MongoDB to MySQL successfully.")
        return True
    except Exception as e:
        print(f"An error occurred while migrating data from MongoDB to MySQL: {e}")
        return False

# Main function
def main():
    try:
        # Define API key
        api_key = "AIzaSyBE5C2pQ9xw6mcwdyciOREzhVUzUf1OZec"

        # Connect to YouTube API
        youtube_api = connect_to_youtube_api(api_key)
        if not youtube_api:
            return

        # Enter the channel IDs separated by comma
        channel_ids = input("Enter the channel IDs separated by comma: ").split(",")
        print(f"Entered channel IDs: {channel_ids}")

        # Fetch data from YouTube API
        channel_data, video_info, playlist_info, comment_info = fetch_youtube_data(youtube_api, channel_ids)
        if not channel_data or not video_info or not playlist_info or not comment_info:
            return

        # Connect to MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        if not mongo_client:
            return

        # Insert data into MongoDB
        if not insert_into_mongodb(mongo_client, channel_data, video_info, playlist_info, comment_info):
            return

        # Connect to MySQL
        mysql_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="123456"
        )
        if not mysql_connection:
            return

        # Create MySQL database and tables
        if not create_mysql_database_and_tables(mysql_connection):
            return

        # Migrate data from MongoDB to MySQL
        if not migrate_data_to_mysql(mysql_connection, mongo_client):
            return

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

# Call the main function
if __name__ == "__main__":
    main()

import mysql.connector
import pymongo
import traceback
from googleapiclient.discovery import build
from datetime import datetime

# Function to connect to MySQL
def connect_to_mysql(host, user, password):
    try:
        mysql_connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        print("Connecting to MySQL...")
        print("MySQL connection successful.")
        return mysql_connection
    except mysql.connector.Error as err:
        print(f"An error occurred while connecting to MySQL: {err}")
        return None

# Function to create MySQL database
def create_mysql_database(mysql_connection, database):
    try:
        mysql_cursor = mysql_connection.cursor()
        mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        print("Creating MySQL database...")
        print(f"MySQL database created successfully (if it didn't exist).")
        mysql_cursor.execute(f"USE {database}")  # Switch to the database
    except mysql.connector.Error as err:
        print(f"An error occurred while creating MySQL database: {err}")

# Function to create MySQL tables
def create_mysql_tables(mysql_connection):
    try:
        mysql_cursor = mysql_connection.cursor()
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                channel_id VARCHAR(255) UNIQUE,
                channel_name VARCHAR(255),
                subscribers INT,
                total_videos INT
            )
        """)
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS video_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_id VARCHAR(255) UNIQUE,
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
                playlist_id VARCHAR(255) UNIQUE,
                title VARCHAR(255),
                description TEXT,
                channel_title VARCHAR(255),
                total_videos INT
            )
        """)
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS comment_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                comment_id VARCHAR(255) UNIQUE,
                video_id VARCHAR(255),
                author VARCHAR(255),
                text TEXT,
                like_count INT,
                published_at DATETIME
            )
        """)
        print("Creating MySQL tables...")
    except mysql.connector.Error as err:
        print(f"An error occurred while creating MySQL tables: {err}")

# Function to connect to MongoDB
def connect_to_mongodb(uri):
    try:
        mongo_client = pymongo.MongoClient(uri)
        print("Connecting to MongoDB...")
        return mongo_client
    except pymongo.errors.ConnectionFailure as err:
        print(f"An error occurred while connecting to MongoDB: {err}")
        return None

# Function to fetch data from MongoDB
def fetch_data_from_mongodb(mongo_client, collection_name):
    try:
        mongo_db = mongo_client.youtube
        collection = mongo_db[collection_name]
        data = list(collection.find())
        return data
    except Exception as e:
        print(f"An error occurred while fetching data from MongoDB: {e}")

# Function to migrate data to MySQL
def migrate_to_mysql(mysql_connection, channel_data, video_info, playlist_info, comment_info):
    try:
        mysql_cursor = mysql_connection.cursor()

        for entry in channel_data:
            # Check if channel ID already exists in MySQL
            mysql_cursor.execute("SELECT * FROM channel_data WHERE channel_id = %s", (entry["channel_id"],))
            result = mysql_cursor.fetchone()
            if result:
                print(f"Channel with ID {entry['channel_id']} already exists in MySQL. Skipping insertion.")
            else:
                mysql_cursor.execute("""
                    INSERT INTO channel_data (channel_id, channel_name, subscribers, total_videos)
                    VALUES (%s, %s, %s, %s);
                """, (entry["channel_id"], entry["channel_name"], entry["subscribers"], entry["total_videos"]))

        for entry in video_info:
            # Convert MongoDB ObjectId to str
            entry["video_id"] = str(entry["video_id"])
            mysql_cursor.execute("""
                INSERT INTO video_info (video_id, title, description, thumbnail_url, channel_title, published_at)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
            entry["video_id"], entry["title"], entry["description"], entry["thumbnail_url"], entry["channel_title"],
            entry["published_at"]))

        for entry in playlist_info:
            # Convert MongoDB ObjectId to str
            entry["playlist_id"] = str(entry["playlist_id"])
            # Check if playlist ID already exists in MySQL
            mysql_cursor.execute("SELECT * FROM playlist_info WHERE playlist_id = %s", (entry["playlist_id"],))
            result = mysql_cursor.fetchone()
            if result:
                print(f"Playlist with ID {entry['playlist_id']} already exists in MySQL. Skipping insertion.")
            else:
                mysql_cursor.execute("""
                    INSERT INTO playlist_info (playlist_id, title, description, channel_title, total_videos)
                    VALUES (%s, %s, %s, %s, %s);
                """, (entry["playlist_id"], entry["title"], entry["description"], entry["channel_title"],
                      entry["total_videos"]))

        for entry in comment_info:
            # Convert MongoDB ObjectId to str
            entry["comment_id"] = str(entry["comment_id"])
            mysql_cursor.execute("""
                INSERT INTO comment_info (comment_id, video_id, author, text, like_count, published_at)
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (entry["comment_id"], entry["video_id"], entry["author"], entry["text"], entry["like_count"],
                  entry["published_at"]))

        mysql_connection.commit()

    except mysql.connector.Error as err:
        print(f"An error occurred while migrating data to MySQL: {err}")
        traceback.print_exc()

# Function to fetch data from YouTube API and insert into MongoDB
def fetch_and_insert_youtube_data(youtube_api, mongo_client, channel_ids):
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

        if comment_info:
            # Insert comment data into MongoDB
            mongo_db = mongo_client.youtube
            comment_collection = mongo_db.comment
            comment_collection.insert_many(comment_info)
            print("Comment data inserted into MongoDB.")

        if video_info:
            # Insert video data into MongoDB
            video_collection = mongo_db.video
            video_collection.insert_many(video_info)
            print("Video data inserted into MongoDB.")

        if playlist_info:
            # Insert playlist data into MongoDB
            playlist_collection = mongo_db.playlist
            playlist_collection.insert_many(playlist_info)
            print("Playlist data inserted into MongoDB.")

        if channel_data:
            # Insert channel data into MongoDB
            channels_collection = mongo_db.channels
            channels_collection.insert_many(channel_data)
            print("Channel data inserted into MongoDB.")

    except Exception as e:
        print(f"An error occurred while fetching and inserting YouTube data: {e}")
        traceback.print_exc()

# Main function
def main():
    try:
        # Define API key
        api_key = "AIzaSyDVlkGd1D4zEtr-fSDDOmN3uObLl19y8Ws"

        # Build the YouTube API service
        youtube_api = build("youtube", "v3", developerKey=api_key)

        # MongoDB URI
        mongo_uri = "mongodb://localhost:27017/"

        # Connect to MongoDB
        mongo_client = connect_to_mongodb(mongo_uri)
        if not mongo_client:
            return

        # MySQL configurations
        mysql_host = "localhost"
        mysql_user = "root"
        mysql_password = "123456"
        mysql_database = "youtube_data"

        # Connect to MySQL
        mysql_connection = connect_to_mysql(mysql_host, mysql_user, mysql_password)
        if not mysql_connection:
            return

        # Create the MySQL database if it doesn't exist
        create_mysql_database(mysql_connection, mysql_database)

        # Now select the database
        mysql_cursor = mysql_connection.cursor()
        mysql_cursor.execute(f"USE {mysql_database}")

        # Create MySQL tables
        create_mysql_tables(mysql_connection)

        # Enter the channel IDs separated by comma
        channel_ids = input("Enter the channel IDs separated by comma: ").split(",")
        print(f"Entered channel IDs: {channel_ids}")

        # Fetch data from YouTube API and insert into MongoDB
        fetch_and_insert_youtube_data(youtube_api, mongo_client, channel_ids)

        # Fetch data from MongoDB collections
        channel_data = fetch_data_from_mongodb(mongo_client, "channels")
        video_info = fetch_data_from_mongodb(mongo_client, "video")
        playlist_info = fetch_data_from_mongodb(mongo_client, "playlist")
        comment_info = fetch_data_from_mongodb(mongo_client, "comment")

        # Migrate data to MySQL
        migrate_to_mysql(mysql_connection, channel_data, video_info, playlist_info, comment_info)

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()

# Call the main function
if __name__ == "__main__":
    main()

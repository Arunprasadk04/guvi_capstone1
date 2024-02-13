from googleapiclient.discovery import build
import pymongo
import mysql.connector

def create_mongo_connection():
    return pymongo.MongoClient("mongodb://localhost:27017/")

def fetch_channel_data(api, channel_id):
    # Fetch channel data from YouTube API
    channel_response = api.channels().list(
        part="snippet,statistics",
        id=channel_id
    ).execute()

    if 'items' in channel_response:
        channel_data = channel_response['items'][0]['snippet']
        channel_statistics = channel_response['items'][0]['statistics']
        return {
            "channel_id": channel_id,
            "channel_name": channel_data['title'],
            "subscribers": int(channel_statistics.get('subscriberCount', 0)),
            "total_videos": int(channel_statistics.get('videoCount', 0))
        }
    else:
        return None

def fetch_video_info(api, channel_id):
    # Fetch video info from YouTube API
    video_response = api.search().list(
        part="snippet",
        type="video",
        channelId=channel_id,
        maxResults=5
    ).execute()

    video_info = []
    if 'items' in video_response:
        video_items = video_response['items']

        for video_item in video_items:
            video_snippet = video_item.get('snippet', {})
            video_info.append({
                "video_id": video_item['id']['videoId'],
                "title": video_snippet.get('title', ''),
                "description": video_snippet.get('description', ''),
                "thumbnail_url": video_snippet['thumbnails']['default']['url'],
                "channel_title": video_snippet.get('channelTitle', ''),
            })

    return video_info

def create_mysql_tables(mysql_cursor):
    # Create MySQL tables if not exists
    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_data (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            subscribers INT,
            total_videos INT
        );
    """)

    mysql_cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_info (
            video_id VARCHAR(255) PRIMARY KEY,
            title VARCHAR(255),
            description TEXT,
            thumbnail_url VARCHAR(255),
            channel_title VARCHAR(255)
        );
    """)

def migrate_to_mysql(mysql_cursor, channel_data, video_info):
    # Migrate data to MySQL
    for entry in channel_data:
        mysql_cursor.execute("""
            INSERT INTO channel_data (channel_id, channel_name, subscribers, total_videos)
            VALUES (%s, %s, %s, %s);
        """, (entry["channel_id"], entry["channel_name"], entry["subscribers"], entry["total_videos"]))

    for entry in video_info:
        mysql_cursor.execute("""
            INSERT INTO video_info (video_id, title, description, thumbnail_url, channel_title)
            VALUES (%s, %s, %s, %s, %s);
        """, (entry["video_id"], entry["title"], entry["description"], entry["thumbnail_url"], entry["channel_title"]))


def main():
    # Define your API key here
    api_key = "AIzaSyDVlkGd1D4zEtr-fSDDOmN3uObLl19y8Ws"

    # Build the YouTube API service
    youtube_api = build("youtube", "v3", developerKey=api_key)

    try:
        # MongoDB connection
        mongo_client = create_mongo_connection()
        mongo_db = mongo_client["youtube_data"]
        mongo_channel_collection = mongo_db["channel_data"]
        mongo_video_collection = mongo_db["video_info"]

        # MySQL connection
        print("Connecting to MySQL...")
        mysql_connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="123456",
            database="youtube_data"
        )

        mysql_cursor = mysql_connection.cursor()

        print("Creating MySQL tables...")
        create_mysql_tables(mysql_cursor)

        # List of channel IDs
        channel_ids = ["UC3OgHegrmWFACe0Nu_winZQ", "UCBaGHCaAi-2994YboAFTi7g"]

        for channel_id in channel_ids:
            # Fetch channel data
            print(f"Fetching channel data for {channel_id}...")
            channel_data = fetch_channel_data(youtube_api, channel_id)

            if channel_data:
                # Store channel data in MongoDB
                print("Storing channel data in MongoDB...")
                mongo_channel_collection.insert_one(channel_data)

                # Fetch video info
                print("Fetching video info...")
                video_info = fetch_video_info(youtube_api, channel_id)

                # Store video info in MongoDB
                print("Storing video info in MongoDB...")
                mongo_video_collection.insert_many(video_info)

                # Migrate data to MySQL
                print("Migrating data to MySQL...")
                migrate_to_mysql(mysql_cursor, [channel_data], video_info)

        # Commit the changes in MySQL
        print("Committing changes in MySQL...")
        mysql_connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close connections
        try:
            mysql_cursor.close()
        except NameError:
            pass
        try:
            mysql_connection.close()
        except NameError:
            pass
        try:
            mongo_client.close()
        except NameError:
            pass
        print("Migration completed successfully!")

if __name__ == "__main__":
    main()

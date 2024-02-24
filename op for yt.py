import mysql.connector
from pymongo import MongoClient
import streamlit as st
import pandas as pd

def create_mysql_connection():
    try:
        mysql_connection = mysql.connector.connect(host="localhost", user="root", password="123456")
        if mysql_connection.is_connected():
            print("Connected to MySQL server")
            return mysql_connection
        else:
            print("Failed to connect to MySQL server.")
            return None
    except Exception as e:
        print(f"An error occurred while connecting to MySQL server: {e}")
        return None

def create_mysql_database(mysql_connection):
    try:
        mysql_cursor = mysql_connection.cursor()
        mysql_cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_data")
        mysql_cursor.execute("USE youtube_data")

        # Create tables if not exists
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                channel_id VARCHAR(255),
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
                published_at DATETIME,
                channel_id VARCHAR(255)
            )
        """)
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist_info (
                id INT AUTO_INCREMENT PRIMARY KEY,
                playlist_id VARCHAR(255),
                title VARCHAR(255),
                description TEXT,
                channel_title VARCHAR(255),
                total_videos INT,
                channel_id VARCHAR(255)
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
                published_at DATETIME,
                channel_id VARCHAR(255)
            )
        """)
        print("MySQL database 'youtube_data' and tables created successfully.")
        return True
    except Exception as e:
        print(f"An error occurred while creating MySQL database and tables: {e}")
        return False

def migrate_channel_data(mysql_connection, mongo_client, channel_id):
    try:
        mysql_cursor = mysql_connection.cursor()
        mongo_db = mongo_client.youtube

        # Fetch channel data from MongoDB for the specified channel ID
        channel_data = mongo_db.channels.find_one({"channel_id": channel_id})
        print("Fetched channel data:", channel_data)

        if channel_data:
            # Migrate channel data to MySQL
            mysql_cursor.execute(
                "INSERT INTO channel_data (channel_id, channel_name, subscribers, total_videos) "
                "VALUES (%s, %s, %s, %s)",
                (channel_data["channel_id"], channel_data["channel_name"], channel_data["subscribers"], channel_data["total_videos"]))

            mysql_connection.commit()
            print("Channel data migrated from MongoDB to MySQL successfully.")
            return True
        else:
            print("No channel data found for the specified channel ID.")
            return False
    except Exception as e:
        print(f"An error occurred while migrating channel data from MongoDB to MySQL: {e}")
        return False

def migrate_video_data(mysql_connection, mongo_client, channel_id):
    try:
        mysql_cursor = mysql_connection.cursor()
        mongo_db = mongo_client.youtube

        # Fetch video data from MongoDB for the specified channel ID
        video_data = list(mongo_db.videos.find({"channel_id": channel_id}))
        print("Fetched video data:", video_data)

        # Migrate video data to MySQL
        for entry in video_data:
            mysql_cursor.execute(
                "INSERT INTO video_info (video_id, title, description, thumbnail_url, channel_title, published_at, channel_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (entry["video_id"], entry["title"], entry["description"], entry["thumbnail_url"],
                 entry["channel_title"], entry["published_at"], channel_id))

        mysql_connection.commit()
        print("Video data migrated from MongoDB to MySQL successfully.")
        return True
    except Exception as e:
        print(f"An error occurred while migrating video data from MongoDB to MySQL: {e}")
        return False

def migrate_playlist_data(mysql_connection, mongo_client, channel_id):
    try:
        mysql_cursor = mysql_connection.cursor()
        mongo_db = mongo_client.youtube

        # Fetch playlist data from MongoDB for the specified channel ID
        playlist_data = list(mongo_db.playlists.find({"channel_id": channel_id}))
        print("Fetched playlist data:", playlist_data)

        # Migrate playlist data to MySQL
        for entry in playlist_data:
            mysql_cursor.execute(
                "INSERT INTO playlist_info (playlist_id, title, description, channel_title, total_videos, channel_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (entry["playlist_id"], entry["title"], entry["description"], entry["channel_title"],
                 entry["total_videos"], channel_id))

        mysql_connection.commit()
        print("Playlist data migrated from MongoDB to MySQL successfully.")
        return True
    except Exception as e:
        print(f"An error occurred while migrating playlist data from MongoDB to MySQL: {e}")
        return False

def migrate_comment_data(mysql_connection, mongo_client, channel_id):
    try:
        mysql_cursor = mysql_connection.cursor()
        mongo_db = mongo_client.youtube

        # Fetch comment data from MongoDB for the specified channel ID
        comment_data = list(mongo_db.comments.find({"channel_id": channel_id}))
        print("Fetched comment data:", comment_data)

        # Migrate comment data to MySQL
        for entry in comment_data:
            mysql_cursor.execute(
                "INSERT INTO comment_info (comment_id, video_id, author, text, like_count, published_at, channel_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (entry["comment_id"], entry["video_id"], entry["author"], entry["text"], entry["like_count"],
                 entry["published_at"], channel_id))

        mysql_connection.commit()
        print("Comment data migrated from MongoDB to MySQL successfully.")
        return True
    except Exception as e:
        print(f"An error occurred while migrating comment data from MongoDB to MySQL: {e}")
        return False

# Function to fetch data from MySQL based on table name
def fetch_data_from_mysql(mysql_connection, table_name):
    try:
        mysql_cursor = mysql_connection.cursor()
        # Execute a query to select all records from the specified table
        mysql_cursor.execute(f"SELECT * FROM {table_name}")
        # Fetch all records
        data = mysql_cursor.fetchall()
        # Get column names
        columns = [i[0] for i in mysql_cursor.description]
        return data, columns
    except Exception as e:
        st.error(f"An error occurred while fetching data from MySQL: {e}")
        return None, None

def main():
    try:
        mysql_connection = create_mysql_connection()
        if not mysql_connection:
            print("Failed to connect to MySQL server.")
            return

        if not create_mysql_database(mysql_connection):
            print("Failed to create MySQL database and tables.")
            return

        # Streamlit input for channel ID
        st.title("YouTube Data Migration")
        channel_id = st.text_input("Enter the channel ID:")
        migrate_button = st.button("Migrate Data")

        if migrate_button:
            if channel_id:
                # MongoDB connection
                mongo_client = MongoClient("mongodb://localhost:27017/")

                # Trigger migration process
                result_channel = migrate_channel_data(mysql_connection, mongo_client, channel_id)
                result_video = migrate_video_data(mysql_connection, mongo_client, channel_id)
                result_playlist = migrate_playlist_data(mysql_connection, mongo_client, channel_id)
                result_comment = migrate_comment_data(mysql_connection, mongo_client, channel_id)

                if result_channel and result_video and result_playlist and result_comment:
                    st.success("Data migration successful.")
                else:
                    st.error("Data migration failed. Please check the logs for details.")
            else:
                st.warning("Please enter a valid channel ID.")

        # Dropdown menu to select table name
        table_name = st.selectbox("Select table name:", ["channel_data", "video_info", "playlist_info", "comment_info"])

        if table_name:
            # Fetch data from MySQL based on selected table name
            data, columns = fetch_data_from_mysql(mysql_connection, table_name)
            if data and columns:
                # Display data in a DataFrame
                df = pd.DataFrame(data, columns=columns)
                st.write(f"Displaying {table_name} records:")
                st.write(df)
            else:
                st.warning("No data found for the selected table.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

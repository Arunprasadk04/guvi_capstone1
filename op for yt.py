import streamlit as st
import pandas as pd
import mysql.connector

# MySQL connection configuration
mysql_config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "youtube_data",
}

# Function to fetch data from MySQL
@st.cache
def fetch_data_from_mysql(query):
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    connection.close()
    return data

# Function to display channel data
def display_channel_data():
    query = "SELECT * FROM channel_data;"
    channel_data = fetch_data_from_mysql(query)
    st.write("### Channel Data")
    st.write(pd.DataFrame(channel_data))

# Function to display video info
def display_video_info():
    query = "SELECT * FROM video_info;"
    video_info = fetch_data_from_mysql(query)
    st.write("### Video Info")
    st.write(pd.DataFrame(video_info))

# Function to display playlist data
def display_playlist_info():
    query = "SELECT * FROM playlist_info;"
    playlist_info = fetch_data_from_mysql(query)
    st.write("### Playlist Info")
    st.write(pd.DataFrame(playlist_info))

# Function to display comment data
def display_comment_info():
    query = "SELECT * FROM comment_info;"
    comment_info = fetch_data_from_mysql(query)
    st.write("### Comment Info")
    st.write(pd.DataFrame(comment_info))

# Streamlit app
def main():
    st.title("YouTube Data Viewer")

    # Display channel data
    display_channel_data()

    # Display video info
    display_video_info()

    # Display playlist data
    display_playlist_info()

    # Display comment data
    display_comment_info()

if __name__ == "__main__":
    main()

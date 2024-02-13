import streamlit as st
from googleapiclient.discovery import build
import pymongo
from sqlalchemy import create_engine
import pandas as pd

st.title("YouTube Analyzer")

# Your Streamlit app code will go here
channel_id = st.text_input("Enter YouTube Channel ID:")

# Define your API key here
api_key = "AIzaSyByMIKq-bwP-XwUOuceSoZl3b_RybRRoqU"

# Build the YouTube API service
youtube = build("youtube", "v3", developerKey=api_key)

# Fetch channel details
if channel_id:
    channel_data = youtube.channels().list(part="snippet,statistics", id=channel_id).execute()

    # Extract relevant data
    channel_name = channel_data["items"][0]["snippet"]["title"]
    subscribers = channel_data["items"][0]["statistics"]["subscriberCount"]
    total_videos = channel_data["items"][0]["statistics"]["videoCount"]

    st.write(f"Channel Name: {channel_name}")
    st.write(f"Subscribers: {subscribers}")
    st.write(f"Total Videos: {total_videos}")


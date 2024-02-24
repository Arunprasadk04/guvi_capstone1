# youtube_data_extraction.py

import pymongo
from googleapiclient.discovery import build
from datetime import datetime

def connect_to_youtube_api(api_key):
    try:
        youtube_api = build("youtube", "v3", developerKey=api_key)
        print("Connecting to YouTube API...")
        return youtube_api
    except Exception as e:
        print(f"An error occurred while connecting to YouTube API: {e}")
        return None

def fetch_youtube_data(youtube_api, channel_ids):
    try:
        channel_data = []
        video_info = []
        playlist_info = []
        comment_info = []

        for channel_id in channel_ids:
            # Fetch channel data
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

                # Fetch videos
                videos_request = youtube_api.search().list(part="snippet", channelId=channel_id, type="video",
                                                            maxResults=10)  # Increase maxResults to fetch more videos
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
                        "published_at": video_published_at,
                        "channel_id": channel_id  # Add channel_id to video data
                    })

                    # Fetch comments for each video
                    try:
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
                                "published_at": comment_published_at,
                                "channel_id": channel_id  # Add channel_id to comment data
                            })
                    except Exception as e:
                        print(f"An error occurred while fetching comments for video {video_id}: {e}")

                # Fetch playlists
                playlists_request = youtube_api.playlists().list(part="snippet", channelId=channel_id,
                                                                 maxResults=10)  # Increase maxResults to fetch more playlists
                playlists_response = playlists_request.execute()
                for playlist_item in playlists_response.get("items", []):
                    total_videos = playlist_item.get("contentDetails", {}).get("itemCount", 0)
                    playlist_info.append({
                        "playlist_id": playlist_item["id"],
                        "title": playlist_item["snippet"]["title"],
                        "description": playlist_item["snippet"]["description"],
                        "channel_title": playlist_item["snippet"]["channelTitle"],
                        "total_videos": total_videos,
                        "channel_id": channel_id  # Add channel_id to playlist data
                    })

            else:
                print(f"No channel found with ID: {channel_id}")

        return channel_data, video_info, playlist_info, comment_info

    except Exception as e:
        print(f"An error occurred while fetching YouTube data: {e}")
        return None, None, None, None

def insert_into_mongodb(mongo_client, channel_data, video_info, playlist_info, comment_info):
    try:
        mongo_db = mongo_client.youtube
        if channel_data:
            mongo_db.channels.insert_many(channel_data)
        if video_info:
            mongo_db.videos.insert_many(video_info)
        if playlist_info:
            mongo_db.playlists.insert_many(playlist_info)
        if comment_info:
            mongo_db.comments.insert_many(comment_info)
        print("Data inserted into MongoDB.")
        return True
    except Exception as e:
        print(f"An error occurred while inserting data into MongoDB: {e}")
        return False

def main():
    try:
        api_key = "AIzaSyBG4ZLVbqLGroYPU2ANDmH3oGpdqZIwe1I"
        youtube_api = connect_to_youtube_api(api_key)
        if not youtube_api:
            print("Failed to connect to the YouTube API. Exiting.")
            return

        channel_ids = input("Enter the channel IDs separated by comma: ").split(",")
        print(f"Entered channel IDs: {channel_ids}")

        channel_data, video_info, playlist_info, comment_info = fetch_youtube_data(youtube_api, channel_ids)
        if not channel_data or not video_info or not playlist_info or not comment_info:
            print("Failed to fetch data from the YouTube API. Exiting.")
            return

        # MongoDB connection
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

        if not insert_into_mongodb(mongo_client, channel_data, video_info, playlist_info, comment_info):
            print("Failed to insert data into MongoDB. Exiting.")
            return

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

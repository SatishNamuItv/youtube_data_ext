import csv
from datetime import datetime
from googleapiclient.discovery import build
import isodate
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# YouTube API Key - Replace with your own key
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# List of usernames/handles for channels
channel_usernames = [
    # 'amazonwebservices',                # AWS
    # 'GoogleCloudPlatform', # Google Cloud
    # 'Oracle',              # Oracle
    'itversity',           # ITVersity
    # 'Databricks',          # Databricks
    # 'MicrosoftAzure'       # Microsoft Azure
]

def get_youtube_service():
    """Initialize YouTube API client."""
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def fetch_channel_id(username):
    """Fetch the channel ID for a given username or handle."""
    youtube = get_youtube_service()
    request = youtube.channels().list(
        part="id",
        forUsername=username
    )
    response = request.execute()

    if response.get('items'):
        return response['items'][0]['id']
    else:
        # Try with @handle format if username lookup fails
        request = youtube.channels().list(
            part="id",
            forHandle=f"@{username}"
        )
        response = request.execute()

        if response.get('items'):
            return response['items'][0]['id']
        else:
            print(f"No channel found for username: {username}")
            return None

def fetch_channel_data(channel_ids):
    """Fetch details for all channels and return as a list of dictionaries."""
    youtube = get_youtube_service()
    channels = []

    for channel_id in channel_ids:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=channel_id
        )
        response = request.execute()

        for item in response.get('items', []):
            snippet = item['snippet']
            stats = item['statistics']
            channels.append({
                'channel_id': item['id'],
                'title': snippet.get('title', ''),
                # 'description': snippet.get('description', ''),
                'creation_date': datetime.strptime(snippet['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').date(),
                'subscriber_count': stats.get('subscriberCount', 'N/A'),
                'total_views': stats.get('viewCount', 'N/A'),
                'total_videos': stats.get('videoCount', 'N/A'),
                'country': snippet.get('country', 'N/A')
            })

    return channels

def fetch_playlist_data(channel_ids):
    """Fetch all playlists for the given channels."""
    youtube = get_youtube_service()
    playlists = []

    for channel_id in channel_ids:
        next_page_token = None
        while True:
            response = youtube.playlists().list(
                channelId=channel_id,
                part="snippet,contentDetails",
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in response.get('items', []):
                snippet = item['snippet']
                playlists.append({
                    'playlist_id': item['id'],
                    'channel_id': channel_id,
                    'title': snippet.get('title', ''),
                    # 'description': snippet.get('description', ''),
                    'creation_date': snippet.get('publishedAt', ''),
                    'total_videos': item['contentDetails']['itemCount']
                })

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

    return playlists

def fetch_video_data(playlist_id):
    """Fetch videos from a given playlist."""
    youtube = get_youtube_service()
    videos = []

    next_page_token = None
    while True:
        response = youtube.playlistItems().list(
            playlistId=playlist_id,
            part="contentDetails",
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        video_ids = [item['contentDetails']['videoId'] for item in response['items']]
        videos.extend(fetch_video_details(video_ids, playlist_id))

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return videos

def fetch_video_details(video_ids, playlist_id):
    """Fetch detailed video information and link it to a playlist."""
    youtube = get_youtube_service()
    videos = []

    for video_id in video_ids:
        response = youtube.videos().list(
            id=video_id,
            part="snippet,statistics,contentDetails"
        ).execute()

        for item in response.get('items', []):
            snippet = item['snippet']
            statistics = item['statistics']
            content_details = item['contentDetails']

            videos.append({
                'video_id': item['id'],
                'playlist_id': playlist_id,
                'title': snippet.get('title', ''),
                # 'description': snippet.get('description', ''),
                'publish_date': snippet.get('publishedAt', ''),
                'view_count': statistics.get('viewCount', 'N/A'),
                'like_count': statistics.get('likeCount', 'N/A'),
                'comment_count': statistics.get('commentCount', 'N/A'),
                'duration': isodate.parse_duration(content_details['duration']).total_seconds()
            })

    return videos

def save_to_csv(data, filename, fieldnames):
    """Save data to a CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    # Fetch channel IDs for the provided usernames/handles
    channel_ids = [fetch_channel_id(username) for username in channel_usernames]
    channel_ids = [cid for cid in channel_ids if cid]  # Filter out None values

    # Fetch and save channel data
    channels = fetch_channel_data(channel_ids)
    if channels:
        save_to_csv(channels, 'channels.csv', channels[0].keys())
        print("Channel data saved to 'channels.csv'")

    # Fetch and save playlist data
    playlists = fetch_playlist_data(channel_ids)
    if playlists:
        save_to_csv(playlists, 'playlists.csv', playlists[0].keys())
        print("Playlist data saved to 'playlists.csv'")

    # Fetch and save video data for each playlist
    all_videos = []
    for playlist in playlists:
        videos = fetch_video_data(playlist['playlist_id'])
        all_videos.extend(videos)

    if all_videos:
        save_to_csv(all_videos, 'videos.csv', all_videos[0].keys())
        print("Video data saved to 'videos.csv'")

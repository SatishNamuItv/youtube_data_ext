import csv
import logging
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import isodate
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os
import ssl

# Load environment variables
load_dotenv()
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Disable SSL verification warnings (Temporary Fix)
ssl._create_default_https_context = ssl._create_unverified_context

# List of YouTube channel usernames/handles
channel_usernames = ['itversity', 'amazonwebservices', 'Databricks', 'oracle', 'MicrosoftAzure']

def get_youtube_service():
    """Initialize a new YouTube API client for each thread."""
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

def fetch_channel_id(username):
    """Fetch the channel ID for a given username/handle."""
    youtube = get_youtube_service()  # Fresh API client per call
    try:
        response = youtube.channels().list(part="id", forUsername=username).execute()
        if response.get('items'):
            return response['items'][0]['id']

        # Try fetching by handle if username lookup fails
        response = youtube.channels().list(part="id", forHandle=f"@{username}").execute()
        return response['items'][0]['id'] if response.get('items') else None
    except HttpError as e:
        logging.error(f"HTTP error fetching ID for {username}: {e}")
    except Exception as e:
        logging.error(f"Error fetching ID for {username}: {e}")
    return None

def fetch_channel_data(channel_ids):
    """Fetch channel details for all channel IDs."""
    channels = []
    for channel_id in channel_ids:
        youtube = get_youtube_service()  # Fresh API client per call
        try:
            response = youtube.channels().list(part="snippet,statistics", id=channel_id).execute()
            for item in response['items']:
                channels.append({
                    'channel_id': item['id'],
                    'title': item['snippet'].get('title', ''),
                    'creation_date': datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').date(),
                    'subscriber_count': item['statistics'].get('subscriberCount', 'N/A'),
                    'total_views': item['statistics'].get('viewCount', 'N/A'),
                    'total_videos': item['statistics'].get('videoCount', 'N/A'),
                    'country': item['snippet'].get('country', 'N/A')
                })
        except HttpError as e:
            logging.error(f"HTTP error fetching data for channel {channel_id}: {e}")
        except Exception as e:
            logging.error(f"Error fetching data for channel {channel_id}: {e}")
    return channels

def fetch_playlist_data(channel_id):
    """Fetch playlists for a given channel."""
    playlists = []
    next_page_token = None

    youtube = get_youtube_service()  # Fresh API client per call
    try:
        while True:
            response = youtube.playlists().list(
                channelId=channel_id, part="snippet,contentDetails", maxResults=50, 
                pageToken=next_page_token
            ).execute()

            playlists.extend([{
                'playlist_id': item['id'],
                'channel_id': channel_id,
                'title': item['snippet'].get('title', ''),
                'creation_date': item['snippet'].get('publishedAt', ''),
                'total_videos': item['contentDetails']['itemCount']
            } for item in response['items']])

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    except HttpError as e:
        logging.error(f"HTTP error fetching playlists for channel {channel_id}: {e}")
    except Exception as e:
        logging.error(f"Error fetching playlists for channel {channel_id}: {e}")
    return playlists

def fetch_video_data(playlist_id):
    """Fetch videos for a given playlist."""
    videos = []
    video_order = 1  # To track the order of videos in the playlist
    next_page_token = None
    youtube = get_youtube_service()  # Fresh API client per call
    try:
        while True:
            response = youtube.playlistItems().list(
                playlistId=playlist_id, part="contentDetails", maxResults=50, 
                pageToken=next_page_token
            ).execute()

            video_ids = [item['contentDetails']['videoId'] for item in response['items']]
            videos_details = fetch_video_details(video_ids)
            
            # Append videos with additional playlist-related metadata
            for video in videos_details:
                videos.append({
                    'playlist_id': playlist_id,
                    'video_id': video['video_id'],
                    'video_order': video_order,  # Incremental order for the playlist
                    'created_at': datetime.now().isoformat(),  # Current timestamp
                    'updated_at': datetime.now().isoformat(),  # Same for now
                    'title': video['title'],
                    'view_count': video['view_count'],
                    'like_count': video['like_count'],
                    'comment_count': video['comment_count'],
                    'duration': video['duration'],
                })
                video_order += 1

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    except HttpError as e:
        logging.error(f"HTTP error fetching videos for playlist {playlist_id}: {e}")
    except Exception as e:
        logging.error(f"Error fetching videos for playlist {playlist_id}: {e}")
    
    return videos

def fetch_video_details(video_ids):
    """Fetch detailed information for videos in batches."""
    videos = []
    youtube = get_youtube_service()  # Fresh API client per call
    try:
        response = youtube.videos().list(
            id=','.join(video_ids), part="snippet,statistics,contentDetails"
        ).execute()

        videos.extend([{
            'video_id': item['id'],
            'title': item['snippet'].get('title', ''),
            'publish_date': item['snippet'].get('publishedAt', ''),
            'view_count': item['statistics'].get('viewCount', 'N/A'),
            'like_count': item['statistics'].get('likeCount', 'N/A'),
            'comment_count': item['statistics'].get('commentCount', 'N/A'),
            'duration': isodate.parse_duration(item['contentDetails']['duration']).total_seconds()
        } for item in response['items']])
    except HttpError as e:
        logging.error(f"HTTP error fetching video details: {e}")
    except Exception as e:
        logging.error(f"Error fetching video details: {e}")
    return videos

def save_to_csv(data, filename):
    """Save data to a CSV file."""
    if data:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

def main():
    logging.info("Fetching channel IDs...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        channel_ids = list(executor.map(fetch_channel_id, channel_usernames))
    channel_ids = [cid for cid in channel_ids if cid]

    if not channel_ids:
        logging.error("No channel IDs fetched. Check API key and network.")
        return

    logging.info("Fetching channel data...")
    channels = fetch_channel_data(channel_ids)
    save_to_csv(channels, 'channels.csv')

    logging.info("Fetching playlists...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        playlists = [p for sublist in executor.map(fetch_playlist_data, channel_ids) for p in sublist]
    save_to_csv(playlists, 'playlists.csv')

    logging.info("Fetching videos and playlist-video data...")
    all_videos = []
    playlist_videos = []  # To store the playlist-video relationship
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_video_data, playlist['playlist_id']) for playlist in playlists]
        for future in as_completed(futures):
            videos = future.result()
            all_videos.extend(videos)
            # Split videos into two CSVs: one for video data, one for playlist-video relations
            playlist_videos.extend([{
                'playlist_id': video['playlist_id'],
                'video_id': video['video_id'],
                'video_order': video['video_order'],
                'created_at': video['created_at'],
                'updated_at': video['updated_at']
            } for video in videos])

    save_to_csv(all_videos, 'videos.csv')
    save_to_csv(playlist_videos, 'playlist_videos.csv')  # Save the playlist-video model

if __name__ == "__main__":
    main()

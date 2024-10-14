import csv
import logging
from datetime import datetime
from googleapiclient.discovery import build
import isodate
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variables
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of YouTube channel usernames/handles
channel_usernames = [
    
    'itversity']

def get_youtube_service():
    """Initialize YouTube API client."""
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def fetch_channel_id(username):
    """Fetch the channel ID for a given username/handle."""
    youtube = get_youtube_service()
    try:
        request = youtube.channels().list(part="id", forUsername=username)
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['id']
        else:
            request = youtube.channels().list(part="id", forHandle=f"@{username}")
            response = request.execute()
            return response['items'][0]['id'] if response.get('items') else None
    except Exception as e:
        logging.error(f"Error fetching ID for {username}: {e}")
        return None

def fetch_channel_data(channel_ids):
    """Fetch details for all channels."""
    youtube = get_youtube_service()
    channels = []

    for channel_id in channel_ids:
        try:
            request = youtube.channels().list(part="snippet,statistics", id=channel_id)
            response = request.execute()
            for item in response['items']:
                channels.append({
                    'channel_id': item['id'],
                    'title': item['snippet'].get('title', ''),
                    'creation_date': datetime.strptime(
                        item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').date(),
                    'subscriber_count': item['statistics'].get('subscriberCount', 'N/A'),
                    'total_views': item['statistics'].get('viewCount', 'N/A'),
                    'total_videos': item['statistics'].get('videoCount', 'N/A'),
                    'country': item['snippet'].get('country', 'N/A')
                })
        except Exception as e:
            logging.error(f"Error fetching data for channel {channel_id}: {e}")

    return channels

def fetch_playlist_data(channel_id):
    """Fetch playlists for a channel."""
    youtube = get_youtube_service()
    playlists = []
    next_page_token = None

    try:
        while True:
            response = youtube.playlists().list(
                channelId=channel_id, part="snippet,contentDetails", maxResults=50, 
                pageToken=next_page_token).execute()

            for item in response['items']:
                playlists.append({
                    'playlist_id': item['id'],
                    'channel_id': channel_id,
                    'title': item['snippet'].get('title', ''),
                    'creation_date': item['snippet'].get('publishedAt', ''),
                    'total_videos': item['contentDetails']['itemCount']
                })

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    except Exception as e:
        logging.error(f"Error fetching playlists for channel {channel_id}: {e}")

    return playlists

def fetch_video_data(playlist_id):
    """Fetch videos from a playlist."""
    youtube = get_youtube_service()
    videos = []
    playlist_videos = []
    next_page_token = None
    video_order = 1

    try:
        while True:
            response = youtube.playlistItems().list(
                playlistId=playlist_id, part="contentDetails", maxResults=50, 
                pageToken=next_page_token).execute()

            for item in response['items']:
                video_id = item['contentDetails']['videoId']
                playlist_videos.append({
                    'playlist_id': playlist_id,
                    'video_id': video_id,
                    'video_order': video_order
                })
                video_order += 1

                videos.extend(fetch_video_details([video_id]))

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    except Exception as e:
        logging.error(f"Error fetching videos for playlist {playlist_id}: {e}")

    return videos, playlist_videos

def fetch_video_details(video_ids):
    """Fetch detailed information for videos."""
    youtube = get_youtube_service()
    videos = []

    try:
        response = youtube.videos().list(
            id=','.join(video_ids), part="snippet,statistics,contentDetails").execute()

        for item in response['items']:
            videos.append({
                'video_id': item['id'],
                'title': item['snippet'].get('title', ''),
                'publish_date': item['snippet'].get('publishedAt', ''),
                'view_count': item['statistics'].get('viewCount', 'N/A'),
                'like_count': item['statistics'].get('likeCount', 'N/A'),
                'comment_count': item['statistics'].get('commentCount', 'N/A'),
                'duration': isodate.parse_duration(item['contentDetails']['duration']).total_seconds()
            })
    except Exception as e:
        logging.error(f"Error fetching video details: {e}")

    return videos

def save_to_csv(data, filename):
    """Save data to CSV."""
    if data:
        fieldnames = data[0].keys()
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

def main():
    # Fetch channel IDs
    logging.info("Fetching channel IDs...")
    channel_ids = [fetch_channel_id(username) for username in channel_usernames]
    channel_ids = [cid for cid in channel_ids if cid]

    # Fetch and save channels data
    logging.info("Fetching channels...")
    channels = fetch_channel_data(channel_ids)
    save_to_csv(channels, 'channels.csv')

    # Fetch and save playlists data
    logging.info("Fetching playlists...")
    playlists = []
    for channel_id in channel_ids:
        playlists.extend(fetch_playlist_data(channel_id))
    save_to_csv(playlists, 'playlists.csv')

    # Fetch and save videos and playlist-videos data concurrently
    logging.info("Fetching videos and playlist-video mappings...")
    all_videos = []
    all_playlist_videos = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_video_data, playlist['playlist_id']) for playlist in playlists]
        for future in as_completed(futures):
            videos, playlist_videos = future.result()
            all_videos.extend(videos)
            all_playlist_videos.extend(playlist_videos)

    save_to_csv(all_videos, 'videos.csv')
    save_to_csv(all_playlist_videos, 'playlist_videos.csv')

if __name__ == "__main__":
    main()

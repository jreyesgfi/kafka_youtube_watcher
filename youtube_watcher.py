#!/usr/bin/env python
import sys
import logging
import requests
import json
from pprint import pformat
from config import config

# Playlist fetching
def fetch_playlist_items_page(google_api_key, youtube_playlist_id,page_token):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", 
                            params={
                                "key":google_api_key,
                                "playlistId":youtube_playlist_id,
                                "part":"contentDetails",
                                "pageToken":page_token
                                })
    payload = json.loads( response.text)
    logging.debug("GOT %s", payload)
    return payload

def fetch_playlist_items(google_api_key,youtube_playlist_id, page_token=None):
    payload = fetch_playlist_items_page(google_api_key,youtube_playlist_id, page_token)
    
    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_playlist_items(google_api_key, youtube_playlist_id, next_page_token)

# Video fetching       
def fetch_video(google_api_key,video_id):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", 
                            params={
                                "key":google_api_key,
                                "id":video_id,
                                "part":"snippet,statistics",
                                })
    
    payload = json.loads( response.text)
    return payload
# Format
def summarize_video(video):
    return {
        "video_id":video["id"],
        "title":video["snippet"]["title"],
        "views":int(video["statistics"].get("viewCount",0)),
        "likes":int(video["statistics"].get("likeCount",0)),
        "comments":int(video["statistics"].get("commentCount",0)),
    }


def main():
    logging.info("START")

    # Return all videos from our playlist
    google_api_key = config["google_api_key"]
    youtube_playlist_id = config["youtube_playlist_id"]
    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
        video_id = video_item["contentDetails"]["videoId"]
        
        video = fetch_video(google_api_key, video_id)["items"][0]
        logging.info("GOT %s", pformat(video))
        logging.debug("GOT %s", pformat(summarize_video(video)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
#!/usr/bin/env python
import sys
import logging
import requests
import json
from pprint import pformat
from config import config
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

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


def on_delivery(err,record):
    pass

def main():
    logging.info("START")

    # Format the Kafka query
    schema_registry_client = SchemaRegistryClient(config["schema_registry"])
    youtube_videos_value_schema = schema_registry_client.get_latest_version("youtube_videos-value")
    kafka_config = config["kafka"] | {
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(
            schema_registry_client,
            youtube_videos_value_schema.schema.schema_str
        ),
    }
    producer = SerializingProducer(kafka_config)

    # Return all videos from our playlist
    google_api_key = config["google_api_key"]
    youtube_playlist_id = config["youtube_playlist_id"]
    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
        video_id = video_item["contentDetails"]["videoId"]
        
        video = fetch_video(google_api_key, video_id)["items"][0]
        logging.debug("GOT %s", pformat(summarize_video(video)))

        producer.produce(
            topic="youtube_videos",
            key = video_id,
            value = {
                "TITLE":video["snippet"]["title"],
                "VIEWS":int(video["statistics"].get("viewCount",0)),
                "LIKES":int(video["statistics"].get("likeCount",0)),
                "COMMENTS":int(video["statistics"].get("commentCount",0)),
            },
            on_delivery = on_delivery
        )

    producer.flush()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
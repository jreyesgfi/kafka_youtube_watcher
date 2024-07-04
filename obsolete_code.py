#youtube_watcher.py
def fetch_videos_page(google_api_key, video_id,page_token):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", 
                            params={
                                "key":google_api_key,
                                "id":video_id,
                                "part":"snippet,statistics",
                                "pageToken":page_token
                                })
    
    payload = json.loads( response.text)
    for video in payload["items"]:
        logging.info("GOT %s", pformat(summarize_video(video)))
    return payload

def fetch_videos(google_api_key,video_id, page_token=None):
    payload = fetch_videos_page(google_api_key,video_id, page_token)
    
    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_videos(google_api_key, video_id, next_page_token)
 
from config import*

#====================INSERT TO MONGODB - channel,video,playlist,comment=================================================
def get_channel_data(youtube, channel_id):
    channel_info_request = youtube.channels().list(
        part="snippet,contentDetails,statistics,status,topicDetails",
        id=channel_id
    ).execute()

    channel = channel_info_request['items'][0]
    channel_image = dict(image=channel['snippet']['thumbnails']['medium']['url'])

    channel_type = channel_info_request['items'][0]['topicDetails']['topicCategories']
    filtered_type = [i for i in channel_type if 'Lifestyle' not in i]
    type_of_channel = filtered_type[0].split('/')[-1] if filtered_type else channel_type[0].split('/')[-1]

    channel_data = {
        'channel_id': channel_id,
        'channel_name': channel['snippet']['title'],
        'subscribers_count': channel['statistics']['subscriberCount'],
        'video_count': channel['statistics']['videoCount'],
        'view_count': channel['statistics']['viewCount'],
        'channel_description': channel['snippet']['description'],
        'Channel_Status': channel['status']['privacyStatus'],
        'Channel_Type': type_of_channel,
        'playlist_id': channel['contentDetails']['relatedPlaylists']['uploads'],
    }
    return channel_data, channel_image

def get_video_ids(youtube, playlist_id):
    video_ids = []
    next_page_token = None
    while True:
        try:
            video_id_request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            v_id = video_id_request['items']
            for item in v_id:
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)

            next_page_token = video_id_request.get('nextPageToken')
            if next_page_token is None:
                break
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            break
    return video_ids


def get_video_data(youtube, video_id):
    videos_data = []
    for video_ids in video_id:
        video_data_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_ids
        ).execute()

        video_info = video_data_request['items'][0]

        video_data = {
            'video_id': video_info['id'],
            'channel_id': video_info['snippet']['channelId'],
            'video_name': video_info['snippet']['title'],
            'video_description': video_info['snippet']['description'],
            'published_date': video_info['snippet']['publishedAt'],
            'channel_title': video_info['snippet']['channelTitle'],
            'category_id': video_info['snippet']['categoryId'],
            'view_count': video_info['statistics']['viewCount'],
            'like_count': video_info['statistics'].get('likeCount', 0),
            "dislike_Count": video_info['statistics'].get('dislikeCount', 0),
            "favorite_Count": video_info['statistics'].get('favoriteCount', 0),
            'comment_count': video_info['statistics'].get('commentCount', 0),
            'duration': video_info['contentDetails']['duration'],
            'thumbnail': video_info['snippet']['thumbnails']['default']['url'],
            'caption_status': video_info['contentDetails']['caption']
        }
        videos_data.append(video_data)
    return videos_data

def get_comment_data(youtube, video_id):
    comments_data = []
    for ids in video_id:
        try:
            video_data_request = youtube.commentThreads().list(
                part="snippet",
                videoId=ids,
                maxResults=50
            ).execute()
            video_info = video_data_request['items']
            for comment in video_info:
                comment_info = {
                    'Video_id': comment['snippet']['videoId'],
                    'Comment_Id': comment['snippet']['topLevelComment']['id'],
                    'Comment_Text': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published_At': comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                }
                comments_data.append(comment_info)
        except HttpError as e:
            if e.resp.status == 403 and 'disabled comments' in str(e):
                comment_info = {
                    'Video_id': ids,
                    'Comment_Id': 'comments_disabled',
                }
                comments_data.append(comment_info)
            else:
                print(f"An error occurred while retrieving comments for video: {ids}")
                print(f"Error details: {e}")
    return comments_data




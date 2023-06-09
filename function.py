from database import*

import pandas as pd
import numpy as np
import re
from datetime import datetime






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


#===========================FETCHING CHANNEL,VIDEO,COMMENT FROM MONGODB DATABASE========================================
def channel_video_comment():
    channel_data, channel_image = get_channel_data(youtube, channel_id)
    playlist_id = channel_data['playlist_id']
    video_id = get_video_ids(youtube, playlist_id)
    video_data = get_video_data(youtube, video_id)
    comment_data = get_comment_data(youtube, video_id)
    channel = {
        'channel_info': channel_data,
        'video_info': {}
    }
    for count, vid_data in enumerate(video_data, 1):
        v_id = f"Video_Id_{count}"
        cmt = {}
        for i in comment_data:
            if i["Video_id"] == vid_data["video_id"]:
                c_id = f"Comment_Id_{len(cmt) + 1}"
                cmt[c_id] = {
                    "Comment_Id": i.get("Comment_Id",'comments_disabled'),
                    "Comment_Text": i.get("Comment_Text", 'comments_disabled'),
                    "Comment_Author": i.get("Comment_Author", 'comments_disabled'),
                    "Comment_Published_At": i.get("Comment_Published_At", 'comments_disabled')
                }
        vid_data["Comments"] = cmt
        channel['video_info'][v_id] = vid_data
    return channel


#Fetching Channel list from  MongoDB
def channel_list():
    channel_list = []
    for result in collection.find():
        channel_name = result['channel_info']['channel_name']
        channel_list.append(channel_name)
    channel_list.reverse()  # Reverse the list
    return channel_list
channel_list()

#=======================================CONVERT TO DATATIME=============================================================
def duration_convert_to_time(duration):
    hours = 0
    minutes = 0
    seconds = 0
    # Extract hours, minutes, and seconds using regular expressions
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    match = re.match(pattern, duration)
    if match:
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
    else:
        print('Not found')
    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds


#=============================EXTRACT CHANNEL,PLAYLIST,VIDEO,COMMENT TO CONVERT AS DATAFRAME============================
def extract_channel(channel_name_to_find):
    channel_details = []
    finding=collection.find({"channel_info.channel_name": channel_name_to_find})
    for result in finding:
        channel_dict=result['channel_info']
        channel_info = {
                        'channel_name': channel_dict['channel_name'],
                        'channel_id': channel_dict['channel_id'],
                        'channel_views': channel_dict['view_count'],
                        'channel_type': channel_dict['Channel_Type'],
                        'channel_description':channel_dict['channel_description'],
                        'channel_status': channel_dict['Channel_Status'],
                        }
        channel_details.append(channel_info)

    return channel_details


def extract_playlist_id(channel_name_to_find):
    playlist = []
    finding = collection.find({"channel_info.channel_name": channel_name_to_find})
    for result in finding:
        playlist_dict = result['channel_info']
        playlist_info = {
            'channel_name': playlist_dict['channel_name'],
            'channel_id': playlist_dict['channel_id'],
            'playlist_id': playlist_dict['playlist_id'],

        }
        playlist.append(playlist_info)

    return playlist

def extract_video(channel_name_to_find):
    video_details = []
    finding = collection.find({"channel_info.channel_name": channel_name_to_find})
    for result in finding:
        channel_dict = result['channel_info']
        video_dict = result['video_info']
        for video_id, video_data in video_dict.items():
            duration = video_data["duration"]
            video_published_at_str = video_data['published_date']
            try:
                video_published_at = datetime.strptime(video_published_at_str, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                continue  # Skip comments with invalid datetime format

            video_info = {
                'channel_name': channel_dict['channel_name'],
                'playlist_id': channel_dict['playlist_id'],
                'video_id': video_data['video_id'],
                'video_name': video_data['video_name'],
                'video_description': video_data.get('video_description', 0),
                'published_date': video_published_at,
                'view_count': video_data['view_count'],
                'like_count': video_data['like_count'],
                'favorite_Count': video_data['favorite_Count'],
                'dislike_count': video_data['dislike_Count'],
                'comment_count': video_data['comment_count'],
                'duration': duration_convert_to_time(duration),
                'thumbnail': video_data['thumbnail'],
                'caption_status': video_data['caption_status']
            }

            video_details.append(video_info)

    return video_details


def extract_comment(channel_name_to_find):
    comment_list = []

    finding = collection.find({"channel_info.channel_name": channel_name_to_find})
    for result in finding:
        channel_dict = result['channel_info']
        video_dict = result['video_info']

        for video_id, video_data in video_dict.items():
            video_ids = video_data.get('video_id')
            comments = video_data.get('Comments', {})  # Retrieve comments dictionary, default to empty if no comments

            # Accessing comments for each video
            for comment_id, comment_data in comments.items():
                comment_published_at_str = comment_data['Comment_Published_At']
                try:
                    comment_published_at = datetime.strptime(comment_published_at_str, '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    continue  # Skip comments with invalid datetime format

                comment_info = {
                    'channel_name': channel_dict['channel_name'],
                    'video_id': video_ids,
                    'comment_id': comment_data['Comment_Id'],
                    'comment_text': comment_data['Comment_Text'],
                    'comment_author': comment_data['Comment_Author'],
                    'comment_published_at': comment_published_at,
                }
                comment_list.append(comment_info)

    return comment_list





#==========================CREATING CHANNEL,PLAYLIST,VIDEO,COMMENT DATAFRAME============================================
def create_channel_df_table(channel_name_to_find):
    channel_details = extract_channel(channel_name_to_find)
    channel_df = pd.DataFrame(channel_details)

    channel_df['channel_name'] = channel_df['channel_name'].astype(str)
    channel_df['channel_id'] = channel_df['channel_id'].astype(str)
    channel_df['channel_views'] = channel_df['channel_views'].astype(np.int64)
    channel_df['channel_type'] = channel_df['channel_type'].astype(str)
    channel_df['channel_description'] = channel_df['channel_description'].astype(str)
    channel_df['channel_status'] = channel_df['channel_status'].astype(str)
    return channel_df


def create_playlist_df_table(channel_name_to_find):
    playlist_details = extract_playlist_id(channel_name_to_find)
    playlist_df = pd.DataFrame(playlist_details)

    playlist_df['channel_name'] = playlist_df['channel_name'].astype(str)
    playlist_df['channel_id'] = playlist_df['channel_id'].astype(str)
    playlist_df['playlist_id'] = playlist_df['playlist_id'].astype(str)

    return playlist_df

def create_video_df_table(channel_name_to_find):
    video_details = extract_video(channel_name_to_find)
    video_df = pd.DataFrame(video_details)

    video_df['channel_name'] = video_df['channel_name'].astype(str)
    video_df['playlist_id'] = video_df['playlist_id'].astype(str)
    video_df['video_id'] = video_df['video_id'].astype(str)
    video_df['video_name'] = video_df['video_name'].astype(str)
    video_df['video_description'] = video_df['video_description'].astype(str)
    video_df['published_date'] = pd.to_datetime(video_df['published_date'])
    video_df['view_count'] = video_df['view_count'].astype(int)
    video_df['like_count'] = video_df['like_count'].astype(int)
    video_df['favorite_Count'] = video_df['favorite_Count'].astype(int)
    video_df['comment_count'] = video_df['comment_count'].astype(int)
    video_df['thumbnail'] = video_df['thumbnail'].astype(str)
    video_df['caption_status'] = video_df['caption_status'].astype(str)

    return video_df


def create_comment_df_table(channel_name_to_find):
    comment_details = extract_comment(channel_name_to_find)
    comment_df = pd.DataFrame(comment_details)

    comment_df['channel_name'] = comment_df['channel_name'].astype(str)
    comment_df['video_id'] = comment_df['video_id'].astype(str)
    comment_df['comment_id'] = comment_df['comment_id'].astype(str)
    comment_df['comment_text'] = comment_df['comment_text'].astype(str)
    comment_df['comment_author'] = comment_df['comment_author'].astype(str)
    comment_df['comment_published_at'] = pd.to_datetime(comment_df['comment_published_at'])
    return comment_df

#============================================MIGRATING NOSQL TO SQL=====================================================
def NOSQL_TO_SQL(channel_name_to_find):
    channel_df = create_channel_df_table(channel_name_to_find)
    playlist_df = create_playlist_df_table(channel_name_to_find)
    video_df = create_video_df_table(channel_name_to_find)
    comment_df = create_comment_df_table(channel_name_to_find)
    return channel_df, playlist_df, video_df, comment_df


#========================RAISE SQL QUERY TO SOLVE QUESTION =============================================================
def qust_1():
    mycursor.execute("SELECT channel_data.channel_name, video_data.video_name FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME']).reset_index(drop=True)
    data_df.index += 1
    return data_df


def qust_2():
    mycursor.execute("SELECT channel_data.channel_name, COUNT(video_data.video_id) AS video_count FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name GROUP BY channel_data.channel_name ORDER BY video_count DESC;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df



def qust_3():
    print("TOP 10 CHANNEL VIEWS")
    mycursor.execute(
        "SELECT channel_data.channel_name,video_data.video_name,  video_data.view_count FROM video_data JOIN channel_data ON video_data.channel_name = channel_data.channel_name ORDER BY video_data.view_count DESC LIMIT 10;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME', 'VIEW_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df

def qust_4():
    mycursor.execute("SELECT video_name, comment_count from video_data ORDER BY comment_count DESC;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['VIDEO_NAME', 'COMMENT_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def qust_5():
    mycursor.execute("SELECT  channel_data.channel_name, video_data.video_name,video_data.like_count FROM video_data JOIN channel_data ON video_data.channel_name = channel_data.channel_name ORDER BY video_data.like_count DESC;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME','LIKE_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def qust_6():
    mycursor.execute("SELECT video_name, like_count, dislike_count FROM video_data ORDER BY like_count DESC, dislike_count ASC;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['VIDEO_NAME', 'LIKE_COUNT','DISLIKE_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def qust_7():
    mycursor.execute("SELECT channel_name, channel_views FROM channel_data ORDER BY channel_views DESC;")
    data= mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'TOTAL_CHANNEL_VIEWS']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def qust_8():
    mycursor.execute("SELECT channel_data.channel_name, video_data.video_name, video_data.published_date FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name WHERE EXTRACT(YEAR FROM video_data.published_date) = 2022;")
    data= mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME', '2022_VIDEO']).reset_index(drop=True)
    data_df.index += 1
    return data_df
def qust_9():
    mycursor.execute("SELECT channel_data.channel_name, ROUND(AVG(video_data.duration), 2) AS average_duration FROM channel_data JOIN video_data ON channel_data.channel_name = video_data.channel_name GROUP BY channel_data.channel_name ;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'AVERAGE_DURATION']).reset_index(drop=True)
    data_df.index += 1
    return data_df

def qust_10():
    mycursor.execute("SELECT channel_name, video_name, comment_count FROM video_data ORDER BY comment_count DESC;")
    data = mycursor.fetchall()
    data_df = pd.DataFrame(data, columns=['CHANNEL_NAME', 'VIDEO_NAME','COMMENT_COUNT']).reset_index(drop=True)
    data_df.index += 1
    return data_df
import pandas as pd
import numpy as np
import re
from datetime import datetime
from function_upload_fetch import*
from config import*

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





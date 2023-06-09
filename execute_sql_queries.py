import pandas as pd
from config import*

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
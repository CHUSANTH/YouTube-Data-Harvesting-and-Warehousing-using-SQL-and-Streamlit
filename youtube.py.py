import pymysql
import pandas as pd
from datetime import datetime
import streamlit as st
import googleapiclient.discovery
from streamlit_option_menu import option_menu
import isodate # type: ignore

api_key="AIzaSyB2TxX0C48PF7GfUu1KUOlBXKfN29fzHw0"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

def get_channel_details(channel_id):
    request = youtube.channels().list(
    part="snippet,statistics,brandingSettings,status,contentDetails",   
    id=channel_id)
    response = request.execute()
    data={
         'channel_id':response['items'][0]['id'],
         'channel_name':response['items'][0]['snippet']['title'],
         'subscribers':int(response['items'][0]['statistics']["subscriberCount"]),
         'channel_views':int(response['items'][0]['statistics']['viewCount']),
         'channel_description':response['items'][0]['snippet']['description'],
         'playlist_id':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
         'Total_videos':response['items'][0]["statistics"]["videoCount"]}
    return data

def profile_pic(channel_id):
    request = youtube.channels().list(
    part="snippet,statistics,brandingSettings,status,contentDetails",   
    id=channel_id)
    response = request.execute()
    data={'profile_picture':response['items'][0]['snippet']['thumbnails']['default']['url']}
    return data
   
def get_playlist_details(channel_id):
    
    playlist_data=[]
    #creating page_tokens for accessing all the playlist data 
    #initializing the value by None inorder to start with first token
    next_page_token=None
    while True:
      response = youtube.playlists().list(part="snippet,contentDetails",channelId=channel_id,
                 maxResults=50,pageToken=next_page_token).execute()
      for item in response['items']:
        data={
            'playlist_id':item['id'],
            'playlist_name':item['snippet']['title'],
            'video_count':int(item['contentDetails']['itemCount']),
            'channel_id':channel_id}
        playlist_data.append(data)                             
      #assigning next token value to the variable.if there is no token then get will return None value
      next_page_token=response.get('nextPageToken')     
      #breaking the while loop token when loop after hits the last video data by having None value in the variable
      if next_page_token is None:
        break
    
    df=pd.DataFrame(playlist_data)
    return df 

def get_video_ids(channel_id):
    
    #creating a list named Videos_ids where it stores ids of all videos from the channel
    video_ids=[]
    # getting the playlist_id from channel details
    response_playlist_id=youtube.channels().list(part="contentDetails",id=channel_id).execute()
    playlist_id=response_playlist_id['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    #initializing the value by None inorder to start with first token
    next_page_token= None
    while True:
      request = youtube.playlistItems().list(
        part="snippet",
        maxResults=50,
        pageToken=next_page_token,
        playlistId=playlist_id)
      response = request.execute()
      #using for loop in order to get the ids of all the videos
      for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
      #assigning next token value to the variable.if there is no token then get will return None value
      next_page_token=response.get('nextPageToken')
      #breaking the while loop token when loop after hits the last video data by having None value in the variable
      if next_page_token is None:
        break
    return video_ids

#converting duration from VARCHAR to INT DATA-TYPE
def convert_duration_to_seconds(duration): 
    parsed_duration = isodate.parse_duration(duration)
    return int(parsed_duration.total_seconds())

def get_video_details(video_ids):
  video_data=[]
  for video_id in video_ids:
    request = youtube.videos().list(
          part="snippet,contentDetails,statistics",
          id=video_id)
    response = request.execute()
    for item in response['items']:
      duration=item['contentDetails']['duration']
      published_data = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
      data={
                'channel_Name':item['snippet']['channelTitle'],
                'video_id':video_id,
                'video_name':item['snippet']['title'],
                'video_description':item['snippet'].get('description', ''),  
                'published_data':published_data,
                'view_count':int(item['statistics']['viewCount']),
                'like_count':int(item['statistics'].get('likeCount')),
                'favorite_count':int(item['statistics']['favoriteCount']),
                'comment_count':item['statistics'].get('commentCount'),
                'duration':convert_duration_to_seconds(duration),
                'thumbnail':item['snippet']['thumbnails']['default']['url'],
                'caption_status':item['contentDetails']['caption']}
      video_data.append(data)
  df=pd.DataFrame(video_data)
  return df 

def get_comments_details(video_ids):  
    comment_data=[]
    #Here 'try' is used to avoid the error caused by comment disabled videos
    try:  
        for video_id in video_ids:
          response = youtube.commentThreads().list(
                part="snippet",videoId=video_id,maxResults=50).execute()
          for item in response['items']:
            comment_published_data = datetime.strptime(item['snippet']["topLevelComment"]['snippet']["publishedAt"], '%Y-%m-%dT%H:%M:%SZ')
            data={
                  'comment_id':item['snippet']["topLevelComment"]['id'],
                  'video_id':item['snippet']['videoId'],
                  'comment_text':item['snippet']["topLevelComment"]['snippet']["textOriginal"],
                  'comment_author':item['snippet']["topLevelComment"]['snippet']["authorDisplayName"],
                  'comment_published_data':comment_published_data}
            comment_data.append(data)
    except:
        pass
    df=pd.DataFrame(comment_data)
    return df 

#Connecting VS-CODE to mysql
import pymysql
import pandas as pd
myconnection=pymysql.connect(host='localhost',user='root',passwd='root',port=3306)
mycursor=myconnection.cursor()
mycursor.execute("create database if not exists youtube")

#creating table for channel details
def channel_table(channel_id):
   mycursor.execute("use youtube")
   mycursor.execute("""create table if not exists channel(
                       channel_id VARCHAR(255),
                       channel_name VARCHAR(225),
                       subscribers INT,
                       channel_views INT,
                       channel_description TEXT,
                       playlist_id VARCHAR(255),
                       Total_videos INT)""")
   #calling function for channel details 
   channel_data= get_channel_details(channel_id)
   #channel_df=pd.DataFrame([channel_data]) - Wrap the channel_data in a list before creating the DataFrame
   sql="insert into channel(channel_id,channel_name,subscribers,channel_views,channel_description,playlist_id,Total_videos  )values(%s,%s,%s,%s,%s,%s,%s)"
   val=(channel_data['channel_id'],
        channel_data['channel_name'],
        channel_data['subscribers'],
        channel_data['channel_views'],
        channel_data['channel_description'],
        channel_data['playlist_id'],
        channel_data['Total_videos'])
   mycursor.execute(sql,val)
   myconnection.commit()

#creating table for playlist details
def playlist_table(channel_id):
   mycursor.execute("use youtube")
   mycursor.execute("""create table if not exists playlist(
                        playlist_id VARCHAR(255),
                        playlist_name VARCHAR(225),
                        video_count INT,      
                        channel_id VARCHAR(255))""")
    #calling function for channel details 
   playlist_df= get_playlist_details(channel_id) 
   for index,row in playlist_df.iterrows():
        sql=sql="insert into playlist(playlist_id,playlist_name,video_count,channel_id)values(%s,%s,%s,%s)"
        val=(row['playlist_id'],row['playlist_name'],row['video_count'],row['channel_id'])
        mycursor.execute(sql,val)
        myconnection.commit()
   myconnection.commit()

#creating table for video details
def videos_table(channel_id):
  mycursor.execute("use youtube")
  mycursor.execute("""create table if not exists videos(
                        channel_Name VARCHAR(255),
                        video_id VARCHAR(255),
                        video_name VARCHAR(225),
                        video_description TEXT,      
                        published_data TIMESTAMP,
                        view_count INT,
                        like_count INT,
                        favorite_count INT,
                        comment_count VARCHAR(255),
                        duration INT,
                        thumbnail VARCHAR(255),
                        caption_status VARCHAR(255))""")         
  video_ids=get_video_ids(channel_id)
  video_df=get_video_details(video_ids)  
  for index,row in video_df.iterrows():
      sql="insert into videos(channel_Name,video_id,video_name,video_description,published_data,view_count,like_count,favorite_count,comment_count,duration,thumbnail,caption_status)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
      val=(row['channel_Name'],row['video_id'],row['video_name'],row['video_description'],
          row['published_data'],row['view_count'],row['like_count'],row['favorite_count'],
          row['comment_count'],row['duration'],row['thumbnail'],row['caption_status'])
      mycursor.execute(sql,val)
  myconnection.commit()

#creating table for comment details
def comments_table(channel_id):
  mycursor.execute("use youtube")
  mycursor.execute("""create table if not exists comments(
                        comment_id VARCHAR(255),
                        video_id VARCHAR(150),
                        comment_text TEXT,
                        comment_author VARCHAR(150),      
                        comment_published_data TIMESTAMP)""")
  video_ids=get_video_ids(channel_id)
  comments_df=get_comments_details(video_ids)
  for index,row in comments_df.iterrows():
      sql="insert into comments(comment_id,video_id,comment_text,comment_author,comment_published_data)values(%s,%s,%s,%s,%s)"
      val=(row['comment_id'],row['video_id'],row['comment_text'],row['comment_author'],row['comment_published_data'])
      mycursor.execute(sql,val)
  myconnection.commit()

#harvesting all data from api and storing the datas in mysql_db
def migrate_to_sql(channel_id):
    channel_table(channel_id)
    playlist_table(channel_id)
    videos_table(channel_id)
    comments_table(channel_id)
    return 'Data migration successful'

# Indicates duplicate Channel-Id Entry
def checking_id(channel_id):
  myconnection=pymysql.connect(host='localhost',user='root',passwd='root',port=3306)
  mycursor=myconnection.cursor()
  mycursor.execute("use youtube")
  query='''select channel_id as Channel_id from channel'''
  mycursor.execute(query)
  myconnection.commit()
  t1=mycursor.fetchall()
  df=pd.DataFrame(t1,columns=["Channel ID"])
  isthere=0
  for index in df.index:
    if df['Channel ID'][index]==channel_id:
       return True
  return False    

#streamlit
with st.sidebar:
   selected=option_menu(
    menu_title="Menu",
    options=['Home','Scrap Data','Quearies'],
    menu_icon=["collection-play-fill"],
    icons=["house-gear-fill","database-fill","quora"],
    styles = {
    "container": {"padding": "10px", "background-color": "#000000"},  # Corrected styles
    "icon": {"color": "orange", "font-size": "25px"},
    "nav-link": {
        "font-size": "25px",
        "text-align": "left",
        "margin": "0px",
        "--hover-color": "#eee"},
    "nav-link-selected": {"background-color": "green"}
    },
    default_index=0)
   
if selected=='Home':  #Home 
   st.title(":white[YOUTUBE DATA HARVESTING AND WARHOUSING]")
   st.markdown("### :green[DOMAIN :]")
   st.markdown('''
           - :orange[Social Media] 
           ''')
   st.markdown("### :green[Skills Take Away :]")
   st.markdown('''
           - :orange[Python scripting]
           - :orange[Data Collection]
           - :orange[Streamlit]
           - :orange[API integration]
           - :orange[Data Management using SQL]  
           ''')
   st.markdown("### :green[ABOUT:]")
   st.markdown('''
           An application that utilizes the Google API to extract information on a YouTube channel, 
           stores it in a :orange[**SQL DATABASE**], and enables users to search for channel details and join tables 
           to view data on a user-friendly :orange[**Streamlit application**].
         ''')
   st.markdown("### FOR FUTHER REFERANCE [click here](https://docs.google.com/document/d/1WrMDf4KnzprK37EJLr3QW0wRUB3few-1Yujv6wnYhZw/edit)")
if selected=='Scrap Data':  #Scrap
   st.title(":green[Scraping DATA]")
   selected=option_menu(
      menu_title="Options",
      options=['DATA HARVESTING','DATA WAREHOUSING'],
      icons=["database-add","database-fill-lock"],
      styles = {
    "container": {"padding": "10px", "background-color": "#000000"},  # Corrected styles
    "icon": {"color": "orange", "font-size": "25px"},
    "nav-link": {
        "font-size": "25px",
        "text-align": "left",
        "margin": "0px",
        "--hover-color": "#eee"},
    "nav-link-selected": {"background-color": "green"}
    },
      default_index=0,
      orientation='horizontal')
   
   channel_id=st.text_input("Enter the channel ID")
   if channel_id and len(channel_id)!=24:
      st.warning("Invalid channel-ID")
   if selected=='DATA HARVESTING':  
      
      if channel_id and len(channel_id)==24:
        if st.button("Scrap Channel data"):
            data=get_channel_details(channel_id)
            data_df=pd.DataFrame([data])
            st.write(data_df)
            pic=profile_pic(channel_id)
            st.image(pic['profile_picture'],caption='Profile Picture',width=150,use_column_width=200)

        if st.button("Scrap playlist data"):
            data=get_playlist_details(channel_id)
            st.write(data)

        if st.button("Scrap videos data"):
            video_id=get_video_ids(channel_id)
            data=get_video_details(video_id)
            st.write(data)

        if st.button("Scrap comments data"):
            video_id=get_video_ids(channel_id)
            data=get_comments_details(video_id)
            st.write(data)    

   if selected=='DATA WAREHOUSING':
      if len(channel_id)==24:
        if channel_id and st.button(":green[Migrate to SQL]"):
            if checking_id(channel_id):
                st.success("Channel ID Already Exist")
            else:
                output=migrate_to_sql(channel_id)
                st.success(output)
            
if selected=='Quearies':  #Queary
    st.header(":green[QUEARY SECTION]")
    #Connecting VS-CODE to mysql
    import pymysql
    import pandas as pd
    myconnection=pymysql.connect(host='localhost',user='root',passwd='root',port=3306)
    mycursor=myconnection.cursor()

    Questions=st.selectbox("SELECT YOUR QUESTIONS",("Select here","1. Names of all the videos and their corresponding channels",
                                                    "2. Which channels have the most number of videos, and how many videos do they have",
                                                    "3. What are the top 10 most viewed videos and their respective channels",
                                                    "4. How many comments were made on each video and their corresponding video names",
                                                    "5. Which videos have the highest number of likes and their corresponding channel names",
                                                    "6. Total number of likes and dislikes for each video and their corresponding video names",
                                                    "7. Total number of views for each channel, and what are their corresponding channel names",
                                                    "8. What are the names of all the channels that have published videos in the year 2022",
                                                    "9. Average duration of all videos in each channel and their corresponding channel names",
                                                    "10. Which videos have the highest number of comments and their corresponding channel names"))

    if Questions=="1. Names of all the videos and their corresponding channels":
        q1='''select video_name as videosName, channel_Name as ChannelName from videos'''
        mycursor.execute("use youtube")
        mycursor.execute(q1)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Video Names","Channel Name"])
        st.write(df)
    
    elif Questions=="2. Which channels have the most number of videos, and how many videos do they have":
        q2='''select channel_name as ChannelName, Total_videos as Total_Videos from channel order by Total_videos desc'''
        mycursor.execute("use youtube")
        mycursor.execute(q2)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Names","Number of Videos"])
        st.write(df)
    
    elif Questions=="3. What are the top 10 most viewed videos and their respective channels":
        q3='''select channel_Name as ChannelName,video_name as VideoName,view_count as views from videos where view_count is not null order by view_count desc limit 10'''
        mycursor.execute("use youtube")
        mycursor.execute(q3)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","VideoName","views"])
        st.write(df)   

    elif Questions=="4. How many comments were made on each video and their corresponding video names":
        q4='''select channel_Name as channel,video_name as VideoName ,comment_count as TotalComments from videos where comment_count is not null'''
        mycursor.execute("use youtube")
        mycursor.execute(q4)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","Video Name","Total comments"])
        st.write(df)
        
    elif Questions=="5. Which videos have the highest number of likes and their corresponding channel names":
        q5='''select channel_Name as ChannelName,video_name as VideoName ,like_count as TotalLikes from videos where like_count is not null order by like_count desc'''
        mycursor.execute("use youtube")
        mycursor.execute(q5)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","Video Name","Likes"])
        st.write(df)
    
    elif Questions=="6. Total number of likes and dislikes for each video and their corresponding video names":
        q6='''select channel_Name as ChannelName,video_name as VideoName,like_count as TotalLikes from videos'''
        mycursor.execute("use youtube")
        mycursor.execute(q6)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","Vidoe Name","Likes"])
        st.write(df)
    
    elif Questions=="7. Total number of views for each channel, and what are their corresponding channel names":
        q7='''select channel_name as ChannelName,channel_views as TotalViews from channel'''
        mycursor.execute("use youtube")
        mycursor.execute(q7)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","Total Views"])
        st.write(df)
    
    elif Questions=="8. What are the names of all the channels that have published videos in the year 2022":
        q8='''select channel_Name as ChannelName,video_name as VideoName ,published_data as 
                UploadDate from videos where extract(year from published_data) = 2022'''
        mycursor.execute("use youtube")
        mycursor.execute(q8)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","Video Name","Uploaded Date"])
        st.write(df)
    
    elif Questions=="9. Average duration of all videos in each channel and their corresponding channel names":
        q9='''select channel_Name as ChannelName,avg(duration) as AverageDuration from videos group by channel_Name'''
        mycursor.execute("use youtube")
        mycursor.execute(q9)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","Average duration of videos"])
        st.write(df)
        
    elif Questions=="10. Which videos have the highest number of comments and their corresponding channel names":
        q10='''select channel_Name as ChannelName,video_name as VideoName ,comment_count as TOTAL_Comment from videos where comment_count is not null order by comment_count desc'''
        mycursor.execute("use youtube")
        mycursor.execute(q10)
        myconnection.commit()
        t1=mycursor.fetchall()
        df=pd.DataFrame(t1,columns=["Channel Name","Video Name","Total comments"])
        st.write(df)


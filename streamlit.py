import streamlit as st
from streamlit_option_menu import option_menu

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
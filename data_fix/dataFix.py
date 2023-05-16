'''
THe purpose of this file is to :
1. Read data from mysql into pandas
2. backfill data
3. Re insert data back into database

'''
import services.dbService
import pandas as pd
import datetime
from bs4 import BeautifulSoup as bs
from requests_html import HTMLSession
import time

#need a class

def getYTdata(url,session):
    
    '''
    code_idea_source: https://www.thepythoncode.com/article/get-youtube-data-python
    
    Args:
            url:             The target url.
            session:         The object of html_session.

    Returns:
            panda series.
            - "url":                The url for the video. 
            - "date_published":     The date the video was published.
            - "duration":           The duration of the video.
            - "channel":            The name of the channel that published the video.
            - "title":              The title of the video.
    '''
    # save a timestamp before video scrape
    #log add here
    t1 = datetime.datetime.today()
    
    print("get youtube video data")
    # download HTML code
    response = session.get(url)
    # execute Javascript
    response.html.render(sleep=1)
    # create beautiful soup object to parse HTML
    soup = bs(response.html.html, "html.parser")    
    # date published
    date_published = soup.find("meta", itemprop="datePublished")['content']
    # get the duration of the video
    #time.sleep(5)
    duration = soup.find("span", {"class": "ytp-time-duration"}).text
    # get the name of the channel
    channel = soup.find("span", itemprop="author").next.next['content']
    # video title
    title = soup.find("meta", itemprop="name")['content']
    # video views (converted to integer)
    #result["views"] = result["views"] = soup.find("meta", itemprop="interactionCount")['content']
    # get the video tags
    tags = ', '.join([ meta.attrs.get("content") for meta in soup.find_all("meta", {"property": "og:video:tag"}) ])
    
    new_row = pd.Series({'url':url,'date_published': date_published,  'duration': duration,'channel': channel,'title': title,'tags': tags }) 
    #response.close()
    

    return new_row


def getRecentYT(urlList):
    session = HTMLSession()
    # save a timestamp before video scrape
    t1 = datetime.datetime.now()
    dformat = "%Y-%m-%d"
    index = 0
    
    print(f"get RecentYT started at {t1}")
    list1=[]
    ytData=pd.DataFrame(columns=["url","date_published","duration","channel","title","tags"])

    

    for url in urlList:
        
        tempD = getYTdata(url,session)
        list1.append(url)
        dp = datetime.datetime.strptime(tempD[1],dformat)

        ytData = pd.concat([ytData, tempD.to_frame().T], ignore_index=True)
        index = index + 1
        print(index)

    print("get RecentYT Task Complete")

    # show time elapsed after video scrape is complete.
    t2 = datetime.datetime.now()
    print(f"ended at {t2}")
    print(f"time elapsed: {t2 - t1}")
    
    return ytData



if __name__ == "__main__":  
    
    #DECLARE DB OBJECTS FOR READING AND INSERTING TO MLFIN DATABASE
    dbInsert = services.dbService.dbObject()
    dbSelect = services.dbService.dbObject()
    
    #READ DATA FROM MLFIN aud_vid_trans table into pandas dataframe
    query = "SELECT * FROM mlfin.aud_vid_trans"
    sqlDf = dbSelect.selectPandasDf(query)
    sqlDf.to_csv("./csv/sqlreadtest.csv")
    
    #Get the duration for each video
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)
    urlList = sqlDf['url'].tolist()
    dataFrameTemp = getRecentYT(urlList)
    
    #join the duration to a 
    sqlDf['av_duration']= dataFrameTemp.loc[:,"duration"]
    sqlDf.to_csv("./csv/sqlreadtest.csv")
    
    dbInsert.close_connection()
    dbSelect.close_connection()
    



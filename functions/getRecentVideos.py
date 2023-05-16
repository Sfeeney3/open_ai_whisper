import scrapetube
import pandas as pd
import datetime
import time
import pafy
from bs4 import BeautifulSoup as bs
from requests_html import HTMLSession,AsyncHTMLSession
import asyncio

#need a class
class ytRetriever:
    def __init__(self,today):
        print("Stage: ytRetriever init:")
              
        self.today = today
        
        print("End ytRetriever init")
        
    async def getYTdata(self,url,session):
    
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
        response = await session.get(url)
        # execute Javascript
        await response.html.arender(timeout=20, sleep = 1)
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

        return new_row


#today
    async def getRecentYT(self, churl):
        
        session = AsyncHTMLSession()
        # save a timestamp before video scrape
        t1 = datetime.datetime.now()
        today = self.today
        dformat = "%Y-%m-%d"
        index = 0
    
        print(f"get RecentYT started at {t1}")
        list1=[]
        ytData=pd.DataFrame(columns=["url","date_published","duration","channel","title","tags"])
        vidLimit = 1
        url="https://www.youtube.com/watch?v="

        dataFrame = pd.DataFrame(columns=["url","date_published","channel_name","title","tags"])
        videos=scrapetube.get_channel(channel_url=churl, limit=vidLimit, sort_by="newest")
    

        while index <= vidLimit:
        
            for video in videos:
                url1 = url+str(video['videoId'])
                #tempD = self.getYTdata(url1,session)
                tempD = await self.getYTdata(url1,session)
                #print(tempD)
                list1.append(url1)
                dp = datetime.datetime.strptime(tempD[1],dformat)
                print(dp)
                delta = today - dp 
                print("DELTA")
                print(delta.days)
                if delta.days >1:
                    break
                ytData = pd.concat([ytData, tempD.to_frame().T], ignore_index=True)
                index = index + 1
                print(index)
            
            break
    

        print("get RecentYT Task Complete")

        # show time elapsed after video scrape is complete.
        t2 = datetime.datetime.now()
        print(f"ended at {t2}")
        print(f"time elapsed: {t2 - t1}")
    
        return ytData
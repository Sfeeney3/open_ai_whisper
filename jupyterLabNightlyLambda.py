
import pandas as pd
import services.dbService
import services.avTranscriptionService as avt
import services.textSummaryService as tss
import functions.getRecentVideos as grv
import datetime
import multiprocessing
from requests_html import HTMLSession
import logging
import asyncio
import numpy as np




def init(l):
    global lock
    lock = l

if __name__ == "__main__":  

   #Need to log nightly run times in MYSQL
   t1 = datetime.datetime.now()
   print(f"Nightly started at {t1}")
   session = HTMLSession()
   #pool_obj = multiprocessing.Pool()
   #l = multiprocessing.Lock()
   #pool = multiprocessing.Pool(initializer=init, initargs=(l,))
   pool_obj = multiprocessing.Pool()
   lock = multiprocessing.Lock()

   loop = asyncio.new_event_loop()
   asyncio.set_event_loop(loop)

   
   #Youtube Video Data
   print("Stage: YT Video Data Scrape")
   with open("./config/ytchannels.txt", "r") as ytchannels: 
      # reading the file
      data = ytchannels.read()
      # replacing end splitting the text when newline ('\n') is seen.
      churls = data.split("\n")
      
   dataFrame = pd.DataFrame(columns=["url","date_published","duration","channel","title","tags"])
   today = datetime.datetime.today()
   yesterday = today - datetime.timedelta(days=1)
   
   testYTR = grv.ytRetriever(today)
   for churl in churls:
      print(churl)
      dataFrameTemp = loop.run_until_complete(testYTR.getRecentYT(churl))
      dataFrame = pd.concat([dataFrame, dataFrameTemp], ignore_index=True)   
   
   dataFrame.to_csv("./csv/test_csv.csv")
   

   
   #Transcription   
   print("Stage: Transcribe YT Videos")
   urlList = dataFrame['url'].tolist()
   
   #chunk_size = 100
   #urlList = [tempList[i:i + chunk_size] for i in range(0, len(tempList), chunk_size)]
   
   ytTrans = avt.Transcriber(dataFrame)
   with multiprocessing.Pool(processes=8, maxtasksperchild=1) as pool:
      answer = pool.map(func=ytTrans.whisperTranscribe, iterable=urlList, chunksize=1)
      tempdf = pd.DataFrame (answer, columns = ['url', 'transcript']) 
   tempdf.to_csv("./csv/URLTEMP2.csv")

   #Summarize transcription
   print("Stage: Summarize Text")
   tempdf = tss.textCompressor.summarize(tempdf)
   dataFrame = dataFrame.join(tempdf.set_index('url'), on='url')
   dataFrame.to_csv("./csv/test_csv.csv")
    
   pool_obj.terminate()
   pool_obj.join()
    
   print("Prep Dataframe for mysqldb")
   #PREP DATAFRAME FOR INSERTION TO MYSQLDB
   tempSQLdf = pd.DataFrame(columns=["url","date_published","av_duration","source","title","tags","transcription","summary"])
   tempSQLdf['url']=dataFrame.loc[:,"url"]
   tempSQLdf['date_published']= dataFrame.loc[:,"date_published"] 
   tempSQLdf['av_duration']= dataFrame.loc[:,"duration"] 
   tempSQLdf['source']= dataFrame.loc[:,"channel"]
   tempSQLdf['title']= dataFrame.loc[:,"title"]
   tempSQLdf['tags']= dataFrame.loc[:,"tags"]
   tempSQLdf['transcription']= dataFrame.loc[:,"transcript"]
   tempSQLdf['summary']= dataFrame.loc[:,"summary"]
   #tempSQLdf['keywords']= dataFrame.loc[:,"keywords"]
   tempSQLdf.set_index('url', inplace=True)
   tempSQLdf.to_csv("./csv/mysql.csv")
   
   # show time elapsed after nightly is complete.
   t2 = datetime.datetime.now()
   timeElapsed = t2 - t1
   logging.debug("Nightly ended at: ")
   logging.debug(t2)
   logging.debug("Time Elapsed: ")
   logging.debug(timeElapsed)
   print(f"Nightly ended at {t2}")
   print(f"time elapsed: {t2 - t1}")

import pandas as pd
import services.dbService
import services.avTranscriptionService as avt
import services.textSummaryService as tss
import functions.getRecentVideos
import datetime
import multiprocessing
from requests_html import HTMLSession
import logging
import asyncio




def init(l):
    global lock
    lock = l

if __name__ == "__main__":  

   t1 = datetime.datetime.now()
   #Need to log nightly run times in MYSQL
   #
   #TO DO: LOGGING
   #
   print(f"Nightly started at {t1}")
   session = HTMLSession()
   #pool_obj = multiprocessing.Pool()
   #l = multiprocessing.Lock()
   #pool = multiprocessing.Pool(initializer=init, initargs=(l,))
   pool_obj = multiprocessing.Pool()
   lock = multiprocessing.Lock()
   
   dbObjInsert = services.dbService.dbObject()
   dbObjRead = services.dbService.dbObject()
   
   #Youtube Video Data
   print("Stage: YT Video Data Scrape")
   with open("./config/ytchannels.txt", "r") as ytchannels: 
      # reading the file
      data = ytchannels.read()
      # replacing end splitting the text when newline ('\n') is seen.
      churls = data.split("\n")
      
   dataFrame = pd.DataFrame(columns=["url","date_published","channel_name","title","tags"])
   today = datetime.datetime.today()
   yesterday = today - datetime.timedelta(days=1)
   
   for churl in churls:
      print(churl)
      dataFrameTemp = functions.getRecentVideos.getRecentYT(churl,today)
      dataFrame = pd.concat([dataFrame, dataFrameTemp], ignore_index=True)
   dataFrame.to_csv("./csv/test_csv.csv")
   

   
   #Transcription   
   print("Stage: Transcribe YT Videos")
   urlList = dataFrame['url'].tolist()
   pool_obj = multiprocessing.Pool()
   lock = multiprocessing.Lock()
   
   ytTrans = avt.Transcriber(dataFrame)
   answer = pool_obj.map(ytTrans.whisperTranscribe,urlList)
   tempdf = pd.DataFrame (answer, columns = ['url', 'transcript'])
   tempdf.to_csv("./csv/URLTEMP2.csv")
   
   #Summarize transcription
   print("Stage: Summarize Text")
   tempdf = tss.textCompressor.summarize(tempdf)
   dataFrame = dataFrame.join(tempdf.set_index('url'), on='url')
   dataFrame.to_csv("./csv/test_csv.csv")
   
   
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
   
   #BEGIN insert data
   dbObjInsert.insertPandasDf(tempSQLdf)
   
   dbObjInsert.close_connection()
   dbObjRead.close_connection()
   # show time elapsed after nightly is complete.
   t2 = datetime.datetime.now()
   timeElapsed = t2 - t1
   logging.debug("Nightly ended at: ")
   logging.debug(t2)
   logging.debug("Time Elapsed: ")
   logging.debug(timeElapsed)
   print(f"Nightly ended at {t2}")
   print(f"time elapsed: {t2 - t1}")
#Imports
import whisper
from pytube import YouTube
from pathlib import Path 
import datetime
import pandas as pd
from csv import writer
import multiprocessing
import os

lock = multiprocessing.Lock()

class Transcriber:
    
    def __init__(self,dataFrame):
        print("Stage: Transcriber init:")
              
        self.dataFrame = dataFrame
        
        print("End Transcriber init")
    
    
    def whisperTranscribe(self, url):
        
        #Load whisper model
        model = whisper.load_model('base')
        
        
        #Save a timestamp before transcription
        t1 = datetime.datetime.now()
        print(f"Whisper Transcribe started at {t1}")

        youtube_video_url = url
        
        try:
            youtube_video = YouTube(youtube_video_url)
        except (Exception):
            print("error loading video")
            pass # Error loading single video
        else:                
        
            streams = youtube_video.streams.filter(only_audio=True)
            stream = streams.first()
        
            lock.acquire()
            new_string = './videos/' + str(hash(url)) + '.mp4'
            print(new_string)
            stream.download(filename=new_string)
            lock.release()
            # do the transcription
            result = model.transcribe(new_string)
            new_row = pd.Series({'url': url,  'transcript': result['text'] })

            os.remove(new_string)
        

        print("Whisper Transcribe Task Complete")
            
        # show time elapsed after transcription is complete.
        t2 = datetime.datetime.now()
        print(f"ended at {t2}")
        print(f"time elapsed: {t2 - t1}")
        
        return new_row
        

        
            
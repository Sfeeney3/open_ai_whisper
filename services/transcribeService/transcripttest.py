import whisper
from pytube import YouTube, Channel
from pathlib import Path 
import datetime
import mysql.connector


def transcribe():
    
    #check youtube channel for the recent videos

    #do something

    #script test whisper
    model = whisper.load_model('base')
    youtube_video_url = "https://www.youtube.com/watch?v=d0PIg-mKGa8"
    youtube_video = YouTube(youtube_video_url)

    streams = youtube_video.streams.filter(only_audio=True)
    #streams
    stream = streams.first()
    #stream
    #stream.download(filename='cnbc.mp4')
    #print(test1)


    # save a timestamp before transcription
    t1 = datetime.datetime.now()
    print(f"started at {t1}")

    # do the transcription
    result = model.transcribe("cnbc.mp4")
    # show time elapsed after transcription is complete.
    t2 = datetime.datetime.now()
    print(f"ended at {t2}")
    print(f"time elapsed: {t2 - t1}")


    filepath = Path('./output/output.txt')  

    filepath.parent.mkdir(parents=True, exist_ok=True) 

    #f = open(filepath, 'w')

    with open(filepath, 'w') as f:
        f.write(result['text'])
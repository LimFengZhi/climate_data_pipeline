#author: LIMFENGZHI
import os
from dotenv import load_dotenv
from de_classes.producer_classes.producer import Producer
load_dotenv()


def main():
    WEATHER_CSV = os.getenv('RAW_WEATHER_CSV') 
    p = Producer("localhost:9092")
    weather_df = p.load_csv(WEATHER_CSV)
    p._send_df(weather_df, topic="weather_data")
    p.close()
    
if __name__ == "__main__":
    main()
    
    
    

    

   
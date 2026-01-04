#author: LIMFENGZHI
import os
from dotenv import load_dotenv
from de_classes.producer_classes.producer import Producer
load_dotenv()


def main():
    CO2_CSV = os.getenv('RAW_CO2_CSV') 
    p = Producer("localhost:9092")
    co2_df = p.load_csv(CO2_CSV)
    p._send_df(co2_df, topic="co2_data")
    p.close()
    
if __name__ == "__main__":
    main()
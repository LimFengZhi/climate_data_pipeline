#author THO HUI YEE
import json, time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

DEFAULT_BATCH_SIZE = 1000
DEFAULT_BATCH_DELAY_SEC = 0.2

class Producer:
    def __init__(self, bootstrap_servers="localhost:9092") -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1, retries=3, batch_size=32768, linger_ms=10,
            compression_type="gzip", buffer_memory=67108864
        )

    def load_csv(self, path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(path)
            print(f"[INFO] Loaded {len(df)} rows from {path}")
            return df
        except Exception as e:
            print(f"[ERROR] reading {path}: {e}")
            return pd.DataFrame()

    def _send_df(self, df: pd.DataFrame, topic: str,
                 batch_size=DEFAULT_BATCH_SIZE,
                 batch_delay_sec=DEFAULT_BATCH_DELAY_SEC,
                 add_city_none: bool = False) -> None:
        
        if df.empty:
            print(f"[WARN] No data for topic '{topic}'.")
            return

        print(f"[INFO] Sending {len(df)} records to '{topic}'…")

        for start in range(0, len(df), batch_size):
            chunk = df.iloc[start:start+batch_size]
            for _, row in chunk.iterrows():
                payload = {k: (None if pd.isna(v) else v) for k, v in row.items()}

                if topic == "weather_data":
                    who = payload.get("city_name") or "?"
                elif topic == "co2_data":
                    who = payload.get("country") or "?"
                else:
                    who = payload.get("city_name") or payload.get("country") or "?"

                date_str = payload.get("date") or payload.get("Date") or "?"

                try:
                    fut = self.producer.send(topic, value=payload)
                    fut.add_errback(lambda exc, t=topic, w=who, d=date_str:
                                    print(f"[ERROR] Send failed ({t}) for {w} — {d}: {exc}"))
                    print(f"[SENT] ({topic}) {who} — {date_str}")
                except (KafkaError, Exception) as exc:
                    print(f"[ERROR] ({topic}) send error for {who} — {date_str}: {exc}")

            self.producer.flush()
            print(f"[INFO] Sent {len(chunk)} to '{topic}' (batch).")
            if batch_delay_sec:
                time.sleep(batch_delay_sec)

        print(f"[INFO] All records to '{topic}' sent.")

    def close(self):
        self.producer.close()
        print("[INFO] Producer closed.")
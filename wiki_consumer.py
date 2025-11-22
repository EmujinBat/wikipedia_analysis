from quixstreams import Application
import json
import duckdb
from datetime import datetime

# ----------------------------
# Connect to DuckDB and create table if it doesn't exist
# ----------------------------
con = duckdb.connect("wiki.db")
con.execute("""
CREATE TABLE IF NOT EXISTS wikipedia_events (
    event_id VARCHAR,
    wiki VARCHAR,
    type VARCHAR,
    user VARCHAR,
    title VARCHAR,
    comment VARCHAR,
    timestamp BIGINT
)
""")

# ----------------------------
# Kafka consumer
# ----------------------------
def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="wikipedia-consumer-group",
        auto_offset_reset="earliest",
        consumer_extra_config={
            "broker.address.family": "v4",
        }
    )
    
    with app.get_consumer() as consumer:
        consumer.subscribe(["wikipedia-edits"])

        while True:
            msg = consumer.poll(1)
            if msg is None:
                continue
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                # Extract relevant fields
                event_id = value.get("id")
                wiki = value.get("wiki")               # language
                change_type = value.get("type")        # edit type
                user = value.get("user")
                title = value.get("title")
                comment = value.get("comment")
                timestamp = value.get("timestamp")

                # Only store if type exists
                if change_type is not None:
                    # Insert into DuckDB
                    con.execute("""
                        INSERT INTO wikipedia_events
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [event_id, wiki, change_type, user, title, comment, timestamp])

                    print(f"{offset} --> {wiki} | {change_type}")

                consumer.store_offsets(msg)

# ----------------------------
# Run consumer
# ----------------------------
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        con.close()  # Ensure DuckDB writes WAL to disk

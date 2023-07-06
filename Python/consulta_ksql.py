from confluent_kafka import Consumer, KafkaError
import requests

KSQL_URL = "http://3.249.15.30:8088"

def run_query():
    query = f"SELECT * FROM A EMIT CHANGES;"
    headers = {
        "Content-Type": "application/vnd.ksql.v1+json",
        "Accept": "application/vnd.ksql.v1+json"
    }
    payload = {
        "ksql": query,
        "streamsProperties": {}
    }
    r = requests.post(KSQL_URL + "/query", json=payload, headers=headers, stream=True)
    for line in r.iter_lines():
        if line:
            print(line.decode('utf-8'))

if __name__ == "__main__":
    run_query()
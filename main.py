from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.conversion.log import converter as log_converter
from paho.mqtt.client import Client, MQTTMessageInfo
from pathlib import Path
import arrow
import json
import sys
import time


def setup_mqtt_client(broker: str, port: int) -> Client:
    client = Client()
    client.connect(broker, port, 60)
    return client


if __name__ == '__main__':
    mqtt_client = setup_mqtt_client(sys.argv[2], int(sys.argv[3]))
    base_topic = sys.argv[4]
    delay = float(sys.argv[5])

    file = sys.argv[1]
    variant = xes_importer.Variants.ITERPARSE
    parameters = {variant.value.Parameters.TIMESTAMP_SORT: True}
    log = xes_importer.apply(file, variant=variant, parameters=parameters)
    df = log_converter.apply(log, variant=log_converter.TO_DATA_FRAME)

    file_name = Path(file).stem

    for index, row in df.iterrows():
        row_d = row.to_dict()
        topic = f'{base_topic}/{file_name}/{row_d["case:concept:name"]}/{row_d["concept:name"]}'
        payload = dict()
        if 'time:timestamp' in row_d:
            timestamp = arrow.get(row_d['time:timestamp'])
            payload = {'timestamp': timestamp.timestamp()}
        else:
            payload = {'timestamp': arrow.utcnow().timestamp()}
        result: MQTTMessageInfo = mqtt_client.publish(topic=topic, payload=json.dumps(payload), qos=0, retain=False)
        print(f'Published to "{topic}" with result code {result.rc}')
        time.sleep(delay)

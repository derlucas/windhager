#!/usr/bin/env python3
import time
import json
import argparse
import logging
from python_settings import settings
import settings as my_local_settings
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import paho.mqtt.client as paho
from Windhager import Windhager

MQTT_BASE_TOPIC = "windhager"
MQTT_STATE = "state"
MQTT_LWT = "lwt"

parser = argparse.ArgumentParser()
parser.add_argument('--debug', action='store_true', help='Activate Debug')
parser.add_argument('--use-influx', action='store_true', help='Activate InfluxDB')
parser.add_argument('--use-mqtt', action='store_true', help='Activate MQTT')
parser.add_argument('--use-ta', action='store_true', help='Activate Technische Alternative Mode (no XML)')
args = parser.parse_args()


class WindhagerMQTTClient:

    def __init__(self, windhager, host='localhost', port='1883', user='', passwd=''):
        self.foo = ""
        self.windhager = windhager
        self.mqtt = paho.Client("windhager-proxy")
        self.mqtt.on_message = self.mqtt_on_message
        self.mqtt.on_connect = self.mqtt_on_connect
        self.mqtt.username_pw_set(user, passwd)
        self.mqtt.will_set(f"{MQTT_BASE_TOPIC}/{MQTT_LWT}", payload="offline", retain=True, qos=0)
        self.mqtt.connect(host, port, keepalive=60)
        self.mqtt.subscribe(f"{MQTT_BASE_TOPIC}/put/")
        self.mqtt.loop_start()

    def mqtt_push_values(self, points):
        values = {}
        for p in points:
            text = p[1].lower().replace(".", "").replace("-", "_").replace("ö", "oe").replace("ä", "ae").replace("ü",
                                                                                                                 "ue").replace(
                "ß", "s")
            oid = p[0].lower().replace("-", "_")
            name = f"{text}_{oid}"
            values[name] = p[2]

        self.mqtt.publish(f"{MQTT_BASE_TOPIC}/{MQTT_STATE}", json.dumps(values))

    def mqtt_push_discovery(self, oids):
        for k, oidarr in oids.items():
            text = (oidarr["text"].lower().replace(".", "")
                    .replace("-", "_").replace("ö", "oe").replace("ä", "ae")
                    .replace("ü", "ue").replace("ß", "s"))
            oid = oidarr["name"].lower().replace("-", "_")
            name = f"windhager_{text}_{oid}"
            if "einheit" in oidarr:
                einheit = oidarr["einheit"]
            else:
                einheit = ""

            config = {
                "~": MQTT_BASE_TOPIC,
                "name": name,
                "device": {
                    "model": "BioWin2",
                    "identifiers": MQTT_BASE_TOPIC,
                    "name": "Windhager"
                },
                "unique_id": f"{MQTT_BASE_TOPIC}" + text,
                "state_topic": f"~/{MQTT_STATE}",
                "value_template": "{{ value_json['" + f"{text}_{oid}" + "'] }}"
            }

            if einheit == "Binary":
                config["device_class"] = "power"
                config["payload_on"] = "1.0"
                config["payload_off"] = "0.0"
                self.mqtt.publish(f'homeassistant/binary_sensor/{name}/config', json.dumps(config))
            elif einheit == "State":
                if text == "betriebsphasen":
                    config["value_template"] = "\
                        {% set mapper = {'0': 'Brenner gesperrt', '1': 'Selbsttest', '2': 'WE ausschalten', '3': 'Standby', '4': 'Brenner AUS', '5': 'Vorspülen', '6': 'Zündphase', '7': 'Flammenstabilisierung', '8': 'Modulationsbetrieb', '9': 'Kessel gesperrt', '10': 'Standby Sperrzeit', '11': 'Gebläse AUS', '12': 'Verkleidungstür offen', '13': 'Zündung bereit', '14': 'Abbruch Zündphase', '15': 'Anheizvorgang', '16': 'Schichtladung', '17': 'Ausbrand'} %} \
                        {% set state = int(value_json['" + f"{text}_{oid}" + "']) %} \
                        {{ mapper[state] if state in mapper else 'Unknown' }}"
                elif text == "betriebswahl":
                    config["value_template"] = "\
                        {% set mapper = {'0': 'Standby', '1': 'Heizprogramm 1', '2': 'Heizprogramm 2', '3': 'Heizprogramm 3', '4': 'Heizbetrieb', '5': 'Absenkbetrieb', '6': 'Warmwasserbetrieb'} %} \
                        {% set state = int(value_json['" + f"{text}_{oid}" + "']) %} \
                        {{ mapper[state] if state in mapper else 'Unknown' }}"
                else:
                    logging.warning(f"Unknwon state variable: {oidarr}")
                    continue
                self.mqtt.publish(f'homeassistant/sensor/{name}/config', json.dumps(config))
            else:
                config["unit_of_measurement"] = einheit
                self.mqtt.publish(f'homeassistant/sensor/{name}/config', json.dumps(config))

            logging.debug(config)

    def mqtt_on_message(self, userdata, message):
        try:
            data = json.loads(message.payload)
        except Exception as e:
            logging.error(f"Malformed JSON data {message.payload}")
            return

        if message.topic == "windhager/put/datapoint":
            if not 'OID' in data:
                logging.error(f"No OID in data {data}")
                return
            if not 'value' in data:
                logging.error(f"No value in data {data}")
                return
            oid = data['OID']
            value = data['value']
            if len(oid.split('/')) < 5:
                logging.error(f"Malformed OID '{oid}'")
                return
            try:
                logging.info(f"old value {oid} = {self.windhager.get_datapoint(oid)['value']}")
            except:
                logging.error(f"Failed to get datapoint for OID {oid}")
                return

            try:
                self.windhager.set_datapoint(oid, value)
            except:
                logging.error(f"Failed to set datapoint for OID {oid} to {value}")
                return

            logging.info(f"new value {oid} = {value}")

    def mqtt_on_connect(self, userdata, flags, rc):
        logging.info("Connected to MQTT broker")
        self.mqtt.publish(f"{MQTT_BASE_TOPIC}/{MQTT_LWT}", payload="Online", qos=0, retain=True)


class WindhagerInfluxClient:
    def __init__(self, windhager, url='localhost', token='', org='', bucket=''):
        self.windhager = windhager
        self.bucket = bucket
        self.org = org
        self.influx = InfluxDBClient(url, token)
        self.write_api = self.influx.write_api(write_options=SYNCHRONOUS)

    def influx_push(self, points):
        bodies = []
        for p in points:
            bodies.append({
                "measurement": f"windhager-{p[0]}-{p[1]}",
                "fields": {'value': p[2]}
            })

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=bodies)
        except:
            logging.warning("could not push data to InfluxDB")



def poll_windhager_values(w, oids):
    points = []
    for oid, entry in oids.items():
        if 'name' not in entry:
            w.log.warning(f"Invalid config for {oid} : {entry}")
            continue
        key = entry['name']

        try:
            d = w.get_datapoint(oid)
            if 'value' not in d:
                w.log.warning(f"Invalid data for {oid}: {d}")
                continue
        except:
            w.log.error(f"could not fetch data for {oid}")
            continue

        # name = w.id_to_string(key.split('-')[0], key.split('-')[1])
        if 'text' in entry:
            name = entry['text'].replace(".", "").replace(" ", "_")
        else:
            name = "unknown"

        # TODO: float vs int vs string vs date?
        #  can not change as most value already as float in influxdb
        try:
            if 'minValue' in d and 'maxValue' in d:
                if d['minValue'] == 0 and d['maxValue'] == 1:
                    value = int(d['value'])
                else:
                    value = float(d['value'])
            else:
                value = float(d['value'])
        except Exception as exc:
            w.log.warning(f"can not convert datapoint {oid}:{name} value {d['value']} {type(d['value'])}")
            # w.log.warning(f"entry: {d}")
            # w.log.warning(f"exception: {exc}")
            # continue
            value = str(d['value'])

        w.log.debug(f"got datapoint {name} {key} with {value}")
        points.append((key, name, value))

    return points


def loop(windhager, influx, mqtt, oids):
    last_discovery = 0

    while True:
        points = poll_windhager_values(windhager, oids)
        if args.use_influx:
            influx.influx_push(points)

        if args.use_mqtt:
            if time.time() - last_discovery > 900:
                logging.info("Pushing discovery data")
                mqtt.mqtt_push_discovery(windhager, mqtt, oids)
                last_discovery = time.time()

            mqtt.mqtt_push_values(mqtt, points)

        # Wait for next 1/4 minute
        time.sleep(1)
        while int(time.time()) % 15:
            time.sleep(1)


def main():
    level = 'INFO'
    if args.debug:
        level = 'DEBUG'

    logging.basicConfig(level=level)
    settings.configure(my_local_settings)

    windhager = Windhager(settings.WINDHAGER_HOST, settings.WINDHAGER_USER,
                          settings.WINDHAGER_PASS, level, args.use_ta)
    windhager_influx = WindhagerInfluxClient(windhager, url=settings.INFLUX_URL,
                                             token=settings.INFLUX_TOKEN, bucket=settings.INFLUX_BUCKET,
                                             org=settings.INFLUX_ORG) if args.use_influx else None
    windhager_mqtt = WindhagerMQTTClient(windhager) if args.use_mqtt else None

    oids = {}
    with open(settings.OIDS_FILE, 'r') as fd:
        lines = fd.readlines()
        for line in lines:
            line = line.split('#', 1)[0].rstrip()
            split = line.split(',')
            if len(split) < 2:
                continue
            oids[split[0]] = {}
            if len(split) > 1:
                oids[split[0]]['name'] = split[1].rstrip()
            if len(split) > 2:
                oids[split[0]]['text'] = split[2].rstrip()
            if len(split) > 4:
                oids[split[0]]['einheit'] = split[4].rstrip()

    logging.info("Initialization done")

    loop(windhager, windhager_influx, windhager_mqtt, oids)


if __name__ == '__main__':
    main()

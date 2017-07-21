#!/usr/bin/env python3

import logging
from time import sleep
import json
from datetime import datetime
from collections import Counter, OrderedDict

import stomp

ut_now = lambda: int(datetime.utcnow().timestamp())
iso_now = lambda: datetime.now().isoformat().split(".")[0]
def f_timestamp(timestamp):
    if not timestamp:
        return (0, None)
    timestamp = int(int(timestamp)/1000)
    return (
        timestamp,
        datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S")
        )

# I present to you... logging in Python! (TOO MANY LINES)
fh = logging.FileHandler('koala.log')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh.setLevel(logging.DEBUG)

log = logging.getLogger("Koala")
log.setLevel(logging.DEBUG)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", '%Y-%m-%dT%H:%M:%S')
ch.setFormatter(format)
fh.setFormatter(format)
log.addHandler(fh)
log.addHandler(ch)

batch = []
batch_last_ut = ut_now()
batch_last_iso = iso_now()

MESSAGES = {
"0001" : "activation",
"0002" : "cancellation",
"0003" : "movement",
"0004" : "_unidentified",
"0005" : "reinstatement",
"0006" : "origin change",
"0007" : "identity change",
"0008" : "location change"
}

with open("config.json") as f:
    CONFIG = json.load(f)

with open("tocs.json") as f:
    TOCS = json.load(f)
TOCS = {k:v["atoc"] for k,v in TOCS.items()}

def connect_and_subscribe(mq):
    for n in range(1,31):
        try:
            log.info("Connecting... (attempt %s)" % n)
            mq.start()
            mq.connect(**{
                "username": CONFIG["username"],
                "passcode": CONFIG["password"],
                "wait": True,
                "client-id": CONFIG["username"],
                })
            mq.subscribe(**{
                "destination": CONFIG["subscribe"],
                "id": 1,
                "ack": "client-individual",
                "activemq.subscriptionName": CONFIG["identifier"],
                })
            log.info("Connected!")
            return
        except:
            log.error("Failed to connect")
            sleep(2)
    log.error("Connection attempts exhausted")

class Listener(object):
    def __init__(self, mq):
        self._mq = mq

    def on_message(self, headers, message):
        global batch, batch_last_ut, batch_last_iso

        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])
        parsed = json.loads(message)
        for train in parsed:
            try:
                head = train["header"]
                body = train["body"]
                type = head["msg_type"]
                message = OrderedDict([
                    ("type_id", type), 
                    ("type_name", MESSAGES.get(type, None)),
                    ("source_device", head["source_dev_id"]),
                    ("original_source", head["original_data_source"]),
                    ("koala_formed_time", iso_now()),
                    ("id", body.get("current_train_id", None) or body.get("train_id", None)),
                    ("toc_id", body.get("toc_id", None)),
                    ("toc_atoc", TOCS.get(body.get("toc_id", None), None)),
                    ])
                message["headcode"] = message["id"][2:6]
                message["tspeed"] = message["id"][6]
                if type=="0001": # Activation
                    message["timestamp"], message["datetime"] = f_timestamp(body["creation_timestamp"])
                    message["stanox"] = body["tp_origin_stanox"] or body["sched_origin_stanox"]
                    message["uid"] = body["train_uid"]
                elif type=="0002": # Cancellation
                    message["timestamp"], message["datetime"] = f_timestamp(body["canx_timestamp"])
                    message["reason_code"] = body["canx_reason_code"]
                elif type=="0003": # Movement
                    message["timestamp"], message["datetime"] = f_timestamp(body["actual_timestamp"])
                    message["timestamp_planned"], message["datetime_planned"] = f_timestamp(body["planned_timestamp"])
                    message["stanox"] = body["loc_stanox"]
                    message["original_stanox"] = body["original_loc_stanox"]
                    message["is_amendment"] = body["correction_ind"]=="true"
                    message["platform"] = body["platform"].strip()
                    message["variation"] = body["variation_status"]
                    message["is_off_route"] = body["offroute_ind"]=="true"
                    message["is_terminating"] = body["train_terminated"]=="true"
                    message["movement_type"] = body["planned_event_type"]
                    message["next_report_time"] = body["next_report_run_time"]
                    message["next_report_stanox"] = body["next_report_stanox"]
                elif type=="0005": # Reinstatement
                    message["timestamp"], message["datetime"] = f_timestamp(body["reinstatement_timestamp"])
                    message["stanox"] = body["loc_stanox"]
                elif type=="0006": # Origin change
                    message["timestamp"], message["datetime"] = f_timestamp(body["coo_timestamp"])
                    message["origin_stanox"] = body["loc_stanox"] # New origin STANOX
                    message["reason_code"] = body["reason_code"]
                elif type=="0007": # Identity change
                    message["timestamp"], message["datetime"] = f_timestamp(body["event_timestamp"])
                    message["revised_id"] = body["revised_train_id"]
                elif type=="0008":
                    pass
                else:
                    log.warning("Unknown message type: " + type)
                    continue
                batch.append(message)
            except Exception as e:
                log.error("Error handling individual record")
                raise e
        if ut_now() - batch_last_ut >= 300:
            log.info("Dumping messages: %s" % Counter([a["type_name"] for a in batch]))
            try:
                with open("records/" + batch_last_iso + ".json", "w") as f:
                    f.write(json.dumps(batch, indent=2))
                batch = []
                batch_last_iso, batch_last_ut = iso_now(), ut_now()
            except Exception as e:
                log.error("Error saving file %s" % batch_last_iso)

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_heartbeat_timeout(self):
        log.error("Heartbeat timeout")

    def on_disconnected(self):
        log.error("Disconnected")
        self._mq.set_listener("koala", self)
        connect_and_subscribe(self._mq)

mq = stomp.Connection([('datafeeds.networkrail.co.uk', 61618)],
    keepalive=True, heartbeats=(10000, 5000))

mq.set_listener('koala', Listener(mq))
connect_and_subscribe(mq)

while True:
    sleep(2)

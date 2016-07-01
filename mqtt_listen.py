import paho.mqtt.client as mqtt
import json
import bson
import os.path
import datetime
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("CR6/#")
    
def on_message(client, userdata, msg):
    print("rcv: "+msg.topic+": "+str(msg.payload))
    
    fname = msg.topic+'.json'
    
    os.makedirs(os.path.split(fname)[0], exist_ok =True)
    s = msg.payload
    js = bson.loads(s)
    js['received_at'] = datetime.datetime.now().isoformat()
    #s = msg.payload.decode('utf-8', errors='replace')
    
    mode = 'w'
    if os.path.exists(fname):
        mode = 'a'
    
    with open(fname, mode) as f:
        json.dump(js, f)          

if __name__=="__main__":
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    HOST = 'localhost'
    PORT = 1883
    if (client.connect(HOST, PORT, 60) != mqtt.MQTT_ERR_SUCCESS):
        print("Failed to connect.")
        exit(-1)
        
    client.loop_forever()


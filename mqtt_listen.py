import paho.mqtt.client as mqtt

import os.path
import os
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("CR6/#")
    
def on_message(client, userdata, msg):
    print("rcv: "+msg.topic+": "+str(msg.payload))
    
    fname = msg.topic+'.json'
    
    os.makedirs(os.path.split(fname)[0], exist_ok =True)
    s = msg.payload
    #s = msg.payload.decode('utf-8', errors='replace')
    
    if os.path.exists(fname):
        with open(fname, 'ab') as f:
            f.write(s)    
    else:                
        with open(fname, 'wb') as f:
            f.write(s)
    

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

import paho.mqtt.client as mqtt
import time
import logging
import logging.handlers
import json
import pidfile
import datetime
import dateutil.parser
import os.path
import threading
from PyCampbellCR1000.pycampbellcr1000.exceptions import NoDeviceException
import sys
from paho.mqtt.client import MQTT_LOG_INFO

logfile =  os.path.splitext(sys.argv[0])[0] + ".log"

logging.basicConfig(level=logging.INFO,
                    handlers=(logging.StreamHandler(stream=sys.stdout),
                              logging.handlers.RotatingFileHandler(logfile,
                                                                    maxBytes = 256*1024,
                                                                    backupCount = 6), ),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logging.getLogger('pycampbellcr1000').setLevel(logging.WARN)
logging.getLogger('pylink').setLevel(logging.WARN)

LOG = logging.getLogger(__name__)
mqLOG = logging.getLogger("MQTT")

# mqtt logging levels to python levels
logmap = {mqtt.MQTT_LOG_INFO: logging.INFO,
    mqtt.MQTT_LOG_NOTICE: logging.INFO,
    mqtt.MQTT_LOG_WARNING : logging.WARN,
    mqtt.MQTT_LOG_ERR : logging.ERROR,
    mqtt.MQTT_LOG_DEBUG: logging.DEBUG}
TABLE_FILE = "tables.json"
MQHOST = "localhost"
MQPORT = 9999
mqconnect = threading.Event()
client = None

from PyCampbellCR1000.pycampbellcr1000 import CR1000

def load_tables():
    'load tables parsing datetimes'
    if os.path.exists(TABLE_FILE):
        with open(TABLE_FILE, 'r') as fp:
            tables = json.load(fp)            
        
        return {k:dateutil.parser.parse(v) for k,v in tables.items()}
    
    return None
def save_tables(tables):
    'save tables fixing datatimes to strings'
    # serialize first so if that breaks, we don't overwrite the file
    data = json.dumps({k:v.isoformat() for k,v in tables.items()})
    with open(TABLE_FILE, 'w') as fp:
        fp.write(data)
def emit_record(topic, rec):
    global client
    if client== None:
        client = get_connected_client()
        
    LOG.info("emit to {}: {}".format(topic, rec))
    # make it serializable
    d = rec['Datetime']
    rec['Datetime'] = rec['Datetime'].isoformat()
    result, mid = client.publish(topic,json.dumps(rec), qos=2)

    if result != mqtt.MQTT_ERR_SUCCESS:
        LOG.warn("MQTT publish failed with {}, restarting connection.".format(mqtt.error_string(result)))
        try:
            shutdown_client()
        except:
            pass
    rec['Datetime'] = d
    #for x,v in rec.items():
        #print (x,v)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    LOG.info("MQTT Connected with result code "+str(rc))
    mqconnect.set()

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    LOG.info("rcv: "+msg.topic+" "+str(msg.payload))

def on_log(client, userdata, level, buf):
    # called when the client has log information. Define
    # to allow debugging. The level variable gives the severity of the message
    # and will be one of MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING,
    #MQTT_LOG_ERR, and MQTT_LOG_DEBUG. The message itself is in buf.
    

    
    mqLOG.log(logmap[level], buf)

def get_connected_client():
    global client
    if client != None:
        return client
        
    client = mqtt.Client()

    if (client.connect(MQHOST, MQPORT, 60) != mqtt.MQTT_ERR_SUCCESS):
        print("Failed to connect to MQTT server.")
        return None
        
    client.on_connect = on_connect
    client.on_message = on_message    
    client.on_log = on_log
    
    client.loop_start()
    if not mqconnect.wait(timeout=30):
        LOG.fatal("Failed to connect to MQ server")
        return None
    
    return client
def shutdown_client():
    global client
    if client != None:
        client.on_connect = None
        client.on_message = None
        client.on_log = None
             
        client.disconnect()
        time.sleep(1)
        client.loop_stop()        
        client = None
    
def connect_and_download():    
    
    LOG.debug("creating device.")
    
    tables = load_tables()
    
    try:
        
        device = CR1000.from_url('serial:/dev/ttyACM0:57600',
                             src_addr=4004,
                             #src_addr=4003,                         
                             dest_addr=1235,
                             #dest_addr=1234,
                             timeout=1)
        
        LOG.debug ('have device: {}'.format(device))                      
        LOG.info ("device time: {}".format(device.gettime()))
                        
        if tables == None or len(tables) == 0:  
            tlist = device.list_tables()
            tables ={x: datetime.datetime.now() for x in tlist if not x in ['Status', 'Public']}        
            save_tables(tables)
        mqroot = 'CR6/{}'.format(device.serialNo)
        
        #make the mqtt connection if needed
        client = get_connected_client()
        
        
        for tablename, lastcollect in tables.items():              
            #if tablename != 'WO209060_PBM':
                #continue
            
            LOG.info("Download {} from {}".format(tablename, lastcollect))
            
            for items in device.get_data_generator(tablename, 
                                                   start_date = lastcollect):            
                for record in items:
                    LOG.debug("got record: {}".format(record))
                    emit_record(mqroot+'/'+tablename, record)
                    time.sleep(0.1)
                tables[tablename] = items[-1]['Datetime'] + datetime.timedelta(seconds=1)
                
        LOG.info("Collection complete.")
        return True
        
    finally:
        shutdown_client()
        save_tables(tables)
        

    return False

if __name__ == "__main__":
    pidfile.write()
    while True:
        try:
            if connect_and_download():
                time.sleep(27)
            
        except NoDeviceException:
            LOG.critical("No response from datalogger.")       
        except Exception as x:
            LOG.critical("Failed with: {}".format(x))
        
        time.sleep(3)
    
        

        
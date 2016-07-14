#!/usr/bin/python3
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
import traceback


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
mqLOG.setLevel(logging.DEBUG)


TABLE_FILE = "/home/alan/cr_logger/tables.json"


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
          
   
    
    # make it serializable
    d = rec['Datetime']
    rec['Datetime'] = rec['Datetime'].isoformat()
 
    #os.makedirs(os.path.dirname(topic), exist_ok=True)
    with open( os.path.join('/home/alan/cr_logger/',topic) + '.json', 'a') as f:
        f.write(json.dumps(rec))
        f.write("\n")
    rec['Datetime'] = d
   
def connect_and_download():    
    
    LOG.debug("creating device.")
    
    tables = load_tables()
   
    try:
        dname = '/dev/ttyACM0'    
        device = CR1000.from_url('serial:{}:57600'.format(dname),
                             src_addr=4094,
                             #src_addr=4003,                         
                             dest_addr=1235,
                             #dest_addr=1234,
                             timeout=1)
        
        LOG.debug ('have device: {}'.format(device))                      
        LOG.info ("device time: {}".format(device.gettime()))
                        
        if tables == None or len(tables) == 0:  
            LOG.info("getting table list.")
            tlist = device.list_tables()
            LOG.info("got table list: {}".format(tlist))
            tables ={x: datetime.datetime.now() for x in tlist if not x in ['Status', 'Public']}        
            save_tables(tables)
        mqroot = 'CR6/{}'.format(device.serialNo)
        
        #LOG.info("device tables are: {}".format(device.table_def))
        devtables = device.list_tables()
        LOG.info("my tables: {}".format(tables.keys()))
        LOG.info("device tables: {}".format(devtables))
        #exit()

        for tablename, lastcollect in tables.items():              
            #if tablename != 'WO209060_PBM':
                #continue
            if tablename not in devtables:
                LOG.error("table {} not found in device {}".format(tablename, devtables))
            LOG.info("Download {} from {}".format(tablename, lastcollect))
            
            for items in device.get_data_generator(tablename, 
                                                   start_date = lastcollect):            
                for record in items:
                    LOG.debug("got record: {}".format(record))
                    emit_record(mqroot+'/'+tablename, record)
#                    time.sleep(0.1)
                tables[tablename] = items[-1]['Datetime'] + datetime.timedelta(seconds=1)
                
        
        return True
        
    finally:
        save_tables(tables)
        LOG.info("Collection complete.")
        

    return False

if __name__ == "__main__":
    pidfile.write()
            
    s= False 
    while s == False:    
      try:
          s = connect_and_download() 
            
      except NoDeviceException:
          LOG.critical("No response from datalogger.")       
      except Exception as x:
          LOG.critical("Failed with: {}".format(x))
          LOG.critical(traceback.format_exc())
 
        

        

import os
import sys
import os.path
import logging
import atexit

pidfile = None

def write():
    global pidfile
    pid = str(os.getpid())
    pidfile = "/tmp/"+ os.path.splitext(sys.argv[0])[0] + ".pid"
    logging.info('wrote pid to {}'.format(pidfile))
    with open(pidfile,'w')as f:
        f.write(pid)
        
    atexit.register(cleanup)
def cleanup():
    global pidfile
    if pidfile != None:
        os.unlink(pidfile)
        pidfile = None
import os
import sys
import os.path
import logging
import atexit

pidfile = None

def write(reset = False):
    global pidfile
    pid = str(os.getpid())

    pname = os.path.splitext(os.path.split(sys.argv[0])[-1])[0]
    pidfile = os.path.join("/tmp/", pname + ".pid")

    if reset == False and os.path.exists(pidfile):
        logging.error("pid file exists, kill running procs and remove {}".format(pidfile))
        exit(-1)

    logging.info('wrote pid to {}'.format(pidfile))
    with open(pidfile,'w')as f:
        f.write(pid)
        
    atexit.register(cleanup)
def cleanup():
    global pidfile
    logging.info('cleanup pidfile: {}'.format(pidfile))
    if pidfile != None:
        os.unlink(pidfile)
        pidfile = None

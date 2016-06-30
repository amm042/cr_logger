import serial
import time
import io


def at_msg(s, cmd):
    print("tx: {}".format(cmd))
    s.write(cmd)#.encode())
    s.flush()
    line = s.readline()
    print("got: {}".format(line))
    return line[:-1]
    

ser = serial.Serial('/dev/ttyUSB0', 
                    baudrate = 38400, bytesize = serial.EIGHTBITS, parity = serial.PARITY_NONE, stopbits = serial.STOPBITS_ONE, 
                    timeout = 4.0, rtscts = True, dsrdtr = True, xonxoff = False )

#sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser, 1), newline = '\r', write_through = True)
sio = io.TextIOWrapper(ser, newline = '\r', write_through = True)
sio._CHUNK_SIZE = 1

print ("opened {}".format(ser.port))

time.sleep(1)
sio.write("+++")
sio.flush()
time.sleep(1)
print("--waiting for OK--")
line = sio.readline()
print("got: {}".format(line))

# source address
while (at_msg(sio, "ATMY\r") == 'FFFF'):
    # autoset address
    at_msg(sio, "ATAM\r")
    
#modem VID
at_msg(sio, "ATID\r")

#hopping pattern
at_msg(sio, "ATHP\r")

# wireless mode
at_msg(sio, "ATMD0\r")

#address mask
#at_msg(sio, "ATMK7FFF\r")
at_msg(sio, "ATMK0\r")

#destination address
#at_msg(sio, "ATDT74d2\r")
at_msg(sio, "ATDTFFFF\r")
#at_msg(sio, "ATDT\r")

# number of retries
at_msg(sio, "ATRR3\r")

#delay slots
at_msg(sio, "ATRN1\r")

#sleep mode 0.5sec
at_msg(sio, "ATSM3\r")

#wake up length
at_msg(sio, "ATLH7\r")


ser.close()
print ("done.")
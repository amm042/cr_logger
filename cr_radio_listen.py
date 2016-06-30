
import serial

device = serial.Serial('/dev/ttyACM0', 9600, timeout=3)
device.write(b'\xBD\xBD\xBD\xBD\xBD\xBD')

#hello
#device.write(b'\xBD\x09\x01\x00\x01\x00\x01\xBD')


#hello req
device.write(b'\xBD\x0e\x00\xBD')

print( device.readline() )
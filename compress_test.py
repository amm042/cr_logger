
import json
import gzip
import hexdump
import bson
from _io import StringIO

rawjson = b"""{"Datetime": "2016-06-30T15:45:00", "RecNbr": 1444, "battV": 13.09, "pTemp": 24.65, "pbmPitch_2": 0.0, "pbmPitch_4": NaN, "pbmVWPkPa": NaN, "pbmTEPCkPa": NaN, "pbmRoll_1": 0.0, "pbmRoll_2": 0.0, "barokPa": 99.7, "pbmHeading_3": 0.0, "pbmRoll_4": NaN, "pbmVWPbUnits": NaN, "pbmRoll_3": 0.0, "pbmHeading_4": NaN, "pbmHeading_1": 0.0, "pbmHeading_2": 0.0, "pbmTEPCbUnits": NaN, "pbmPitch_1": 0.0, "pbmVWPtherm": -143.15200805664062, "pbmPitch_3": 0.0}"""
rec = json.loads(rawjson.decode())

bson = bson.dumps(rec)

compressed = gzip.compress(rawjson)
    
print ("raw: ", len(rawjson))
print ("gzip: ", len(compressed))
print ("    : ", hexdump.dump(compressed))

uncomp = gzip.decompress(compressed)

assert (len(uncomp) == len(rawjson))
for i in range(len(rawjson)):
    assert rawjson[i] == uncomp[i]


print ("bson: ", len(bson))
print ("    : ", hexdump.dump(bson))

gzbson = gzip.compress(bson)

print ("gzbson: ", len(gzbson))
print ("    : ", hexdump.dump(gzbson))


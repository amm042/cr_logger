import json

def pop_dicts(s):
    "pop dicts from a string counting braces"    
    front = 0
    pos = 0
    blevel = 0
    inbrace = False
    while pos < len(s):
        if s[pos] == '{':
            blevel += 1
            inbrace = True
        if s[pos] == '}':
            blevel -= 1
            
        if inbrace and blevel == 0:
            yield json.loads(s[front:pos+1])
            front = pos+1
            inbrace = False
        
        pos += 1    
    
def count_gaps(fname):
    with open(fname,"rb") as f:
        s = f.read().decode('utf-8', errors='replace')
    
    at = None
    gaps = []
    first = None
    last = None
    cnt = 0
    for rec in pop_dicts(s):
        if first == None:
            first = rec
        print (rec) 
        if at == None:
            at = rec['RecNbr']
        else:
            if at+1 != rec['RecNbr']:
                gaps+=[(at, rec['RecNbr'])]            
        at = rec['RecNbr']
        cnt += 1
    last = rec
    
    print ("Gaps: {}".format(gaps))
    print ("Expected: {}, got {}".format(last['RecNbr']-first['RecNbr']+1, cnt))

count_gaps('CR6/2700/WO209060_PBM.json')
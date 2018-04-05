# Python 3.6
# If need this on CentOS (python 2.x), add #!/usr/bin/python on the top, and revise print()/socket statement.

import socket, string, json, time
from argparse import ArgumentParser
from random import randrange

# Prepare your test data. 
def Packer(randomIsON=True):
    if randomIsON:            # if need some random value. Can design yours.
        def rrange(length, min_i=0, max_i=10):
            return ''.join([str(randrange(min_i,max_i)) for i in range(length)])

        customerID = string.ascii_uppercase[randrange(0,26)]+str(rrange(1,min_i=1))+'*****'+str(rrange(3))
        message = {
            'LoginID':randrange(1000,2000),
            'CustomerID':customerID,
            'RoleID':rrange(1,max_i=2)+rrange(1),
            'SourceIP':'10.'+str(randrange(1,256))+'.'+str(randrange(1,256))+'.'+str(randrange(1,256)),
            'Code':'0'+rrange(3),
            'SystemID':'L0'+rrange(1,max_i=6)
        }
    else:
        message = {
            'LoginID': 1988,
            'CustomerID':'A123456789',
            'RoleID':'02',
            'SourceIP':'10.12.34.56',
            'Code':'8787',
            'SystemID':'02x3'
        }

    return json.dumps(message,ensure_ascii=False) + "\n"     # return a json Object. Add '\n' to let logstash get message.

# Sending data to Logstash via TCP
def sendMSG(tHost, tPort, messageToSend):
    
    # with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:   # UDP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((tHost, int(tPort)))
        s.sendall(str.encode(messageToSend))


def main():

    # Create command line arguments.
    parser = ArgumentParser(description='Parameters of DataSender(Python) for sending data to Logstash.', add_help=False)
    parser.add_argument('-h', action="store", dest='ls_host', default='127.0.0.1')     # Logstash Host
    parser.add_argument('-p', action="store", dest='ls_port', default=8877)            # the Port Logstash is listening.
    parser.add_argument('-c', action="store", dest='count', default=1, type=int)       # events
    parser.add_argument('-r', action="store", dest='repeat', default=1, type=int)      # repeat sending times
    parser.add_argument('-i', action="store", dest='interval', type=int)               # wait seconds before next sending

    res = parser.parse_args()
    
    # You can remove this restriction
    if res.count > 1024:
        print("Count [-c] parameters can NOT exceeds 1024, we will send 1024 times.")
        res.count = 1024
    
    count = res.count      # store res.count to count for initializing.
    for i in range(res.repeat):
        res.count = count
        while res.count > 0:
            sendMSG(res.ls_host, res.ls_port, Packer())
            res.count -= 1
        if res.interval is not None:
            time.sleep(res.interval)


if __name__ == '__main__':
    main()

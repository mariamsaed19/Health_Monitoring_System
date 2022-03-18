import socket
import json
import datetime
import psutil

date = datetime.datetime.now()
timestamp = int(datetime.datetime.timestamp(date))
CPU = psutil.cpu_percent(1)
RAM = psutil.virtual_memory()
Disk = psutil.disk_usage('/')
r_total = round(RAM.total/(2**30),2)
r_free = round(RAM.free/(2**30),2)
d_total = round(Disk.total/(2**30),2)
d_free = round(Disk.free/(2**30),2)
UDP_IP = "10.0.6.165"
UDP_PORT = 3500
MESSAGE = {
    "serviceName": "Ticketing Service",
    "Timestamp": timestamp,
    "CPU": CPU,
    "RAM": {
        "Total":r_total ,
        "Free": r_free
    },
    "Disk": {
        "Total": d_total,
        "Free": d_free
    },
}
print(MESSAGE)
# convert into JSON:
MESSAGE = bytes(json.dumps(MESSAGE),'UTF-8')
print("UDP target IP: ", UDP_IP)
print("UDP target port: ", UDP_PORT)
print("message:", MESSAGE)

'''
sock = socket.socket(socket.AF_INET, # Internet
                     socket.SOCK_DGRAM) # UDP
# send many messages
for i in range(1030):
    print("Send message ",i)
    sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))
'''
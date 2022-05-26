import os
from os import path
import random
import socket
import datetime
import time


dirname = os.path.dirname(__file__)
data_path = os.path.join(os.path.dirname(dirname), 'csv_input\\')

files_list = random.choices(os.listdir(data_path), k = 5)
print(files_list)

for filename in files_list:
    path = os.path.join(data_path, filename)
    print(filename)

    with open(path) as f:
        for line in f:
            # remove new line character
            line = line.replace('\n', '')
            # get current timestamp
            date = datetime.datetime.now()
            timestamp = int(datetime.datetime.timestamp(date))
            # modify timestamp
            msg = line.split(',')
            msg[1] = str(timestamp)
            # rejoin array
            modified_msg = ','.join(msg)
            print(modified_msg)
            # send message to health monitor
            UDP_IP = "127.1.0.0"
            UDP_PORT = 3500
            byte_msg = bytes(modified_msg,'UTF-8')

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
            sock.sendto(byte_msg, (UDP_IP, UDP_PORT))

            # time.sleep(3)

                


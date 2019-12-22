import multiprocessing
# import multiprocessing_import_worker
# import worker
import requests
import re
import sys
import string
import csv
import socket
import threading
from collections import defaultdict
import threading
import time
import random
import json

def connect_to_master(id_number):
    IP = "10.128.0.3"
    # IP = "127.0.0.1"
    PORT = 6969
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((IP, PORT))
    connected_string = "Copmleted Reducing Task "  + str(id_number)
    connected= connected_string
    client_socket.send(bytes(connected))

def start_reducer(id_number):
    # IP = "127.0.0.2"
    IP = "10.128.0.7"
    PORT = 7878
    global answer
    global answer1
    global answer2
    wordcount=[]
    wordcount1=[]
    wordcount2=[]
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((IP, PORT))
    
    client_socket.send(bytes("reducer"))
    ack=client_socket.recv(100)
    function_type="wordcount"

    client_socket.send(bytes(str(function_type + " " + str(id_number))))
    
    if function_type=="wordcount":
        length = client_socket.recv(1000)
        length = int(length.decode())
        filename = id_number+"red.csv"
        with open(filename, "a") as f:
            writer = csv.writer(f)
            for i in range(length):
                word = client_socket.recv(100)
                word = word.split()
                client_socket.send(bytes("recieved"))
                wordcount.append([word[0], word[1]])
                writer.writerow([word[0], word[1]])
        
        client_socket.send(str(len(wordcount)))
        ack = client_socket.recv(1000)
        for i in range(len(wordcount)):
            client_socket.send(str(wordcount[i][0]+ " " + str(wordcount[i][1])))
            ack = client_socket.recv(1000)
            time.sleep(0.001)

    connect_to_master(id_number)
        
if __name__ == '__main__':
    hostname = socket.gethostname() 
    a = hostname.split("-")
    start_reducer(a[1])
    answer=0
    answer1=0
    answer2=0      

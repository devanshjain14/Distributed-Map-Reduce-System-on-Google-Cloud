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

def key_val_store(book1, book2, function_type, id_number):
    
    IP = "10.128.0.7"
    # IP = "127.0.0.2"
    PORT = 7878
    
    kv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kv_socket.connect((IP, PORT))
    

    kv_socket.send(bytes("mapper"))
    
    fun_ack=kv_socket.recv(20)

    kv_socket.send(bytes(str(function_type + " " + str(id_number))))
    
    fun_ack=kv_socket.recv(20)
    fun_ack=fun_ack.decode()


    if fun_ack=="recieved":
        if function_type=="wordcount":
            words1= (re.sub('[^A-Za-z0-9]+',' ', book1)).split()
            kv_socket.send(bytes(str(len(words1))))
            ack=kv_socket.recv(20)
            
            for i in range (len(words1)):
                kvp_string = "SET  " + words1[i] + " 1"
                kvpair= kvp_string
                time.sleep(0.01)
                kv_socket.send(bytes(kvpair))
            # kv_socket.close()
        
        elif function_type=="invertedindex":

            words1= (re.sub('[^A-Za-z0-9]+',' ', book1)).split()
            words2= (re.sub('[^A-Za-z0-9]+',' ', book2)).split()
            
            kv_socket.send(bytes(str(len(words1))))
            ack=kv_socket.recv(20)
            
            kv_socket.send(bytes(str(len(words2))))
            ack=kv_socket.recv(20)

            for i in range (len(words1)):
                kvp_string = "SET " + words1[i] + " 1"
                kvpair= kvp_string
                time.sleep(0.01)
                kv_socket.send(bytes(kvpair))
            
            for i in range (len(words2)):
                kvp_string = "SET " + words1[i] + " 1"
                kvpair= kvp_string
                time.sleep(0.01)
                kv_socket.send(bytes(kvpair))
            
            # kv_socket.close()



def start_mapper(id_number):
    IP = "10.128.0.3"
    # IP = "127.0.0.1"
    PORT = 6969
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((IP, PORT))
    connected_string = "Mapping Task Connected "  + str(id_number)
    connected= connected_string
    client_socket.send(bytes(connected))

    function_type=client_socket.recv(100)
    function_type=function_type.decode()

    if function_type=="wordcount":
        input_data1= client_socket.recv(1000000)
        input_data1= input_data1.decode()
        words, value =[], 1
        input_data2= 'NULL'
        mapping_ack = "Mapping Task Completed "  + str(id_number)
        client_socket.send(bytes((mapping_ack)))
        key_val_store(input_data1, input_data2, function_type, id_number)

    elif function_type=="invertedindex":
        input_data1= client_socket.recv(100000)
        input_data1= input_data1.decode()
        client_socket.send(bytes("Ack"))
        input_data2= client_socket.recv(100000)
        input_data2= input_data2.decode()
        words, value =[], 1
        key_val_store(input_data1, input_data2, function_type, id_number)
        
    # client_socket.close()
        
        
if __name__ == '__main__':
    hostname = socket.gethostname() 
    a = hostname.split("-")
    start_mapper(a[1])
    

import multiprocessing
# import mapper1
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
from multiprocessing import Process
# import reducer
# import kv_store   
import json
from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import operator

class key_value_dict(dict):  
    
    def __init__(self):  
        self = dict()  
    
    def add(self, key, value):  
        self.setdefault(key, []).append(value)


def connect_to_kv_stor():
    # IP = "127.0.0.2"
    IP = "10.128.0.7"
    PORT = 7878
    print("i am connected to kv")
    kv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kv_socket.connect((IP, PORT))
    kv_socket.send(bytes("master"))
    fun_ack=kv_socket.recv(20)
    command = ''
    kv_socket.send(bytes(str(function_type + " " + str(num_red))))
    fun_ack=kv_socket.recv(20)
    fun_ack=fun_ack.decode()
    print("waiting")
    if fun_ack=="recieved": 
        print("Task Successfull")
        start_reducers(num_red)

    
class ClientThread(threading.Thread):
    
    def __init__(self,caddr,clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        print ("Connection from Mapper/Reducer", caddr)   
    def run(self):
        
        global strings
        global function_type
        global map_ack_count
        global red_ack_count
        global num_red
        global num_map
        global mydict
        global finalcount
        global function_type

        connection=self.csocket.recv(2048)
        connection = connection.decode()
        print(connection)
        connection = connection.split()
        
        if connection[0]=="Mapping":
            
            number= int(connection[3])
            
            self.csocket.send(bytes(function_type)) 
            if function_type=='wordcount':
                self.csocket.send(bytes(str(strings[number-1])))
            
                map_ack = self.csocket.recv(2048)
                
                map_ack_count += 1
                map_ack = map_ack.decode()
                if map_ack_count==num_map:
                    print("Mapping Task Complete")
                    stop_mappers(num_map)
                    connect_to_kv_stor()
            elif function_type=='invertedindex':

                self.csocket.send(bytes(str(strings[number-1])))
                ack = self.csocket.recv(2048)
                self.csocket.send(bytes(str(strings2[number-1])))
                
                map_ack = self.csocket.recv(2048)
                map_ack_count += 1
                map_ack = map_ack.decode()
                
                if map_ack_count==num_map:
                    print("Mapping Task Complete")
                    connect_to_kv_stor()
        
        elif connection[0]=="Completed":
            number= int(connection[3])
            red_ack_count+=1
            if red_ack_count == num_red:
                stop_reducers(num_red)
                print("Map Reduce Task is now Complete.")


        elif connection[0]=="Reducing":
            number= int(connection[3])
            self.csocket.send(bytes(function_type))
            
            if function_type=="wordcount":
                final_answer= self.csocket.recv(2000000)
                final_answer= final_answer.decode()

                red_ack = self.csocket.recv(20000)
                red_ack = red_ack.decode()
                red_ack_count += 1
                if red_ack_count == num_red:
                    print("Reducing Task Complete")

            elif function_type=="invertedindex":
                com_ack = self.csocket.recv(20000)
                com_ack = com_ack.decode()
                if com_ack=="Combine":
                    with open('count1.csv', 'rt', encoding='utf-8') as master:
                        master_indices = dict((r[0], i) for i, r in enumerate(csv.reader(master)))

                        with open('count2.csv', 'rt', encoding='utf-8') as hosts:
                            with open('result.csv', 'w') as results:    
                                reader = csv.reader(hosts)
                                writer = csv.writer(results)

                                writer.writerow(next(reader, []) + ['RESULTS'])

                                for row in reader:
                                    index = master_indices.get(row[0])
                                    if index is not None:
                                        message = 'FOUND in master list (row {})'.format(index)
                                        writer.writerow(row + [message])

                                    else:
                                        message = 'NOT FOUND in master list'
                                        writer.writerow(row + [message])

                            results.close()


                red_ack = self.csocket.recv(20000)
                red_ack = red_ack.decode()
                red_ack_count += 1
                if red_ack_count == num_red:
                    print("Reducing Task Complete")
                
def start_master_server():
    while True:
        server_socket.listen(1)
        clientsock, caddr = server_socket.accept()
        newthread = ClientThread(caddr, clientsock)
        newthread.start()

def start_kvstore_server():
    print("KV  Started")
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = 'devanshjain'
    zone = 'us-central1-a'
    instance = "kv-store"
    request = service.instances().start(project=project, zone=zone, instance=instance)
    response = request.execute()
    print(instance, "launched")
    # pprint(response)

def start_mappers(num_map):
    print("Mappers Started")
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = 'devanshjain'
    zone = 'us-central1-a'
    instance = ['mapper-1','mapper-2', 'mapper-3','mapper-4','mapper-5','mapper-6']
    for i in range(num_map):
        request = service.instances().start(project=project, zone=zone, instance=instance[i])
        response = request.execute()
        print(instance[i], "launched")
        # pprint(response)

def stop_mappers(num_map):
    print("Mappers Stopped")
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = 'devanshjain'
    zone = 'us-central1-a'
    instance = ['mapper-1','mapper-2', 'mapper-3','mapper-4','mapper-5','mapper-6']
    for i in range(num_map):
        request = service.instances().stop(project=project, zone=zone, instance=instance[i])
        response = request.execute()
        # pprint(response)

def start_reducers(num_red):
    print("Reducers Stopped")
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = 'devanshjain'
    zone = 'us-central1-a'
    instance = ['reducer-1','reducer-2', 'reducer-3','reducer-4','reducer-5','reducer-6']
    for i in range(num_red):
        request = service.instances().start(project=project, zone=zone, instance=instance[i])
        response = request.execute()
        # pprint(response)

def stop_reducers(num_red):
    print("Reducers Started")
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = 'devanshjain'
    zone = 'us-central1-a'
    instance = ['reducer-1','reducer-2', 'reducer-3','reducer-4','reducer-5','reducer-6']
    for i in range(num_red):
        request = service.instances().stop(project=project, zone=zone, instance=instance[i])
        response = request.execute()
        print(instance[i], "stopped")

def splitStringMax( si, limit):
    ls = si.split()
    lo=[]
    st=''
    ln=len(ls)
    if ln==1:
        return [si]
    i=0
    for l in ls:
        st+=l
        i+=1
        if i <ln:
            lk=len(ls[i])
            if (len(st))+1+lk < limit:
                st+=' '
                continue
        lo.append(st);st=''
    return lo

def listToString(s):  
    str1 = " " 
    return (str1.join(s))

if __name__ == '__main__':
    with open ("config.txt", "r") as f:
        text=f.read()
    text=text.split()
    function_type=text[7]
    hostname = socket.gethostname()    
    IP = socket.gethostbyname(hostname)
    # IP = "127.0.0.1"
    PORT= int(text[13])
    input_file=text[15]
    num_map, num_red= 2, 2
    count_mapper, count_reducer= 0, 0
    map_ack_count,red_ack_count=0, 0
    jobs, strings, finalcount=[], [], []

    with open (input_file, "r") as f:
        input_data=f.read()
    # input_data= "Here you can get help of any object by pressing Ctrl+I in front of it, either on the Editor or the Console."
    input_data=re.sub('[^A-Za-z0-9]+',' ', input_data)
    input_data = re.sub ('\n','',  input_data )
    
    if function_type=='invertedindex':
        input_file_2=text[22]
        with open (input_file, "r") as f:
            input_data=f.read()
        with open (input_file_2, "r") as f:
            input_data_2=f.read()

        input_data=re.sub('[^A-Za-z0-9]+',' ', input_data)
        input_data = re.sub ('\n','',  input_data )
        
        input_data_2=re.sub('[^A-Za-z0-9]+',' ', input_data_2)
        input_data_2 = re.sub ('\n','',  input_data_2 )
        limit2=int(len(input_data_2)/num_map)
        strings2=(splitStringMax(input_data_2, limit2))

    mydict=key_value_dict()
    mydict1=key_value_dict()
    mydict2=key_value_dict()
    limit1=int(len(input_data)/num_map)
    
    strings=(splitStringMax(input_data, limit1))
    
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((IP, PORT))
    print("Launching Key Value Store")
    start_kvstore_server()
    print("Successfully Launched Key Value Store")
    
    print("Launching Mappers")
    start_mappers(num_map)
    print("Successfully Launched Mappers")
    
    print("Master Server Listening on ", IP , " " , PORT)
    start_master_server()
    
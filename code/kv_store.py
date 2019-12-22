import socket
import threading
import csv
import io
from collections import defaultdict
from glob import glob
import time

class key_value_dict(dict):  
    
    def __init__(self):  
        self = dict()  
    
    def add(self, key, value):  
        self.setdefault(key, []).append(value)

class ClientThread(threading.Thread):
    
    def __init__(self,caddr,map_socket):
        threading.Thread.__init__(self)
        print ("Connection from Mapper/Reducer", caddr)
        self.csocket = map_socket
        
    def run(self):
        global mydict
        global mydict1
        global mydict2
        command = ''
        id_name = self.csocket.recv(1024)
        id_name = id_name.decode()
        self.csocket.send(bytes("recieved"))
        
        function_type= self.csocket.recv(10000)
        function_type= function_type.decode()
        function_type= function_type.split()
        function_type, id_number= function_type[0], function_type[1]  
    
        if id_name=="master":
            num_red= id_number 
            if function_type=="wordcount":
                filename = "kv_store.csv"
                with open("kv_store.csv", 'a') as singleFile:
                    first_csv = True
                    for i in glob('*.csv'):
                        if i == filename:
                            pass
                        else:
                            header = True
                            for line in open(i, 'r'):
                                if first_csv and header:
                                    singleFile.write(line)
                                    first_csv = False
                                    header = False
                                elif header:
                                    header = False
                                else:
                                    singleFile.write(line)
                    singleFile.close()
                with open ("kv_store.csv", "r") as f2:
                    reader = csv.reader(f2)
                
                    for rows in reader:
                        mydict.add(rows[0], rows[1])

                with open("groubby.csv", "a") as f3:
                    write = csv.writer(f3)

                    for key, value in mydict.items():
                        write.writerow([key]+ value)
                
                csvfile = open('groubby.csv', 'r').readlines()
                filename = 1
                for i in range(len(csvfile)):
                    if i % (len(csvfile)//(int(num_red))) == 0:
                        open(str(filename) + 'red.csv', 'w+').writelines(csvfile[i:i+(len(csvfile)//((int(num_red))))])
                        filename += 1
                        

            elif function_type=="invertedindex":

                with open ("kv_store_book1.csv", "r") as f9:
                    reader = csv.reader(f9)
                
                    for rows in reader:
                        mydict1.add(rows[0], rows[1])

                with open("groubby_book1.csv", "a") as f8:
                    write = csv.writer(f8)

                    for key, value in mydict1.items():
                        write.writerow([key]+ value)
                
                csvfile = open("groubby_book1.csv", 'r').readlines()
                filename = 1
                for i in range(len(csvfile)):
                    if i % (len(csvfile)//(num_red-1)) == 0:
                        open(str(filename) +  'red.csv', 'w+').writelines(csvfile[i:i+(len(csvfile)//((int(num_red)-1)))])
                        filename += 1
                

                with open ("kv_store_book2.csv", "r") as f7:
                    reader = csv.reader(f7)
                
                    for rows in reader:
                        mydict2.add(rows[0], rows[1])

                with open("groubby_book2.csv", "a") as f6:
                    write = csv.writer(f6)

                    for key, value in mydict1.items():
                        write.writerow([key]+ value)
                
                csvfile = open("groubby_book2.csv", 'r').readlines()
                filename = 50
                for i in range(len(csvfile)):
                    if i % (len(csvfile)//(num_red-1)) == 0:
                        open(str(filename) +  'red.csv', 'w+').writelines(csvfile[i:i+(len(csvfile)//((int(num_red)-1)))])
                        filename += 1
        
            self.csocket.send(bytes("recieved"))
            # kv_socket.close()

        elif id_name=="mapper":
            print ("Connection from Mapper")
            self.csocket.send(bytes("recieved"))
            if function_type=='wordcount':
                
                length = self.csocket.recv(2048)
                length = int(length.decode())
                print(length)
                filename = id_number+".csv"
                print(filename)
                self.csocket.send(bytes("recieved"))
                for i in range (length):
                    command = self.csocket.recv(10000)
                    command = command.decode()
                    msg=(command.lower()).split()
                    if len(msg)==3:
                        word, value = msg[1], msg[2]
                        if msg[0]=='set':
                            
                            with open(filename, 'a') as f:
                                writer = csv.writer(f)
                                writer.writerow([word] + [value]) 
                    else:
                        command='NOT STORED'
                    length-=1

            elif function_type=='invertedindex':

                length1 = self.csocket.recv(2048)
                length1 = length1.decode()
                length1 = int(length1)
                self.csocket.send(bytes(ack))

                length2 = self.csocket.recv(2048)
                length2 = length2.decode()
                length2 = int(length2)
                self.csocket.send(bytes(ack))

                for i in range (length1):
                    command = self.csocket.recv(10000)
                    command = command.decode()
                    msg=(command.lower()).split()
                    filename1='kv_store_book1.csv'
                    if len(msg)==4:
                        word, value, book = msg[2], msg[3], msg[1]
                        if msg[0]=='set':
                            with open(filename1, 'a') as f:
                                writer = csv.writer(f)
                                writer.writerow([word] + [value] + [book]) 
                    else:
                        command='NOT STORED'
                    length1-=1
                
                for i in range (length2):
                    command = self.csocket.recv(10000)
                    command = command.decode()
                    msg=(command.lower()).split()
                    filename2='kv_store_book2.csv'
                    if len(msg)==4:
                        word, value, book = msg[2], msg[3], msg[1]
                        if msg[0]=='set':
                            with open(filename2, 'a') as f:
                                writer = csv.writer(f)
                                writer.writerow([word] + [value] + [book]) 
                    else:
                        command='NOT STORED'
                    length2-=1
            
        elif id_name=="reducer": 
            wordcount = []
            
            filename= id_number+"red.csv"
            with open(filename, "r") as f:
                reader = csv.reader(f)
                for rows in reader:
                    count= sum(map(int, rows[1:]))
                    wordcount.append([rows[0], count])
                leng = sum(1 for _ in reader)
            self.csocket.send(str(len(wordcount)))
            for i in range(len(wordcount)):
                self.csocket.send(str(wordcount[i][0]+ " " + str(wordcount[i][1])))
                ack = self.csocket.recv(10000)
                time.sleep(0.001)
            output_length = self.csocket.recv(1024)
            output_length = output_length.decode()
            print(output_length)
            output_length  = int(output_length)
            self.csocket.send(bytes("recieved"))
            with open("wordcount.csv", "a") as f:
                writer = csv.writer(f)
                for i in range(output_length):
                    answer = self.csocket.recv(1024)
                    answer = answer.split()
                    self.csocket.send(bytes("recieved"))
                    writer.writerow([answer[0], answer[1]])
            
def start_kvstore():
    while True:
        kv_socket.listen(1)
        clientsock, caddr = kv_socket.accept()
        newthread = ClientThread(caddr, clientsock)
        newthread.start()

if __name__ == '__main__':
    mydict=key_value_dict()
    mydict1=key_value_dict()
    mydict2=key_value_dict()
    hostname = socket.gethostname()    
    IP = socket.gethostbyname(hostname)
    # print(IP)
    # IP = "127.0.0.2"
    PORT =7878
    kv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    kv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    kv_socket.bind((IP, PORT))
    start_kvstore()
    
# Distributed Map Reduce System From Scratch on Google Cloud
#### This was implemented under the valuable guidance of Prof Prateek Sharma at Indiana University in B516 Engineering Cloud Computing during Fall 2019.

## Background

Google Cloud Platform (GCP), is a suite of cloud computing services that runs on the same infrastructure as Google. In this assignment, map-reduce framework, built from scratch using python programming has been deployed on the google cloud platform. This version of
map-reduce framework has been designed in such a way that, any map-reduce function can be run through this framework, given the function is well defined inside the mappers and reducers. For now, this framework is based on an assumption that user is only looking to get word-count and inverted index.
To explain briefly, MapReduce facilitates concurrent processing by splitting a very large amount of data into smaller chunks and processing them in parallel on different servers(mappers) at the same time. In the end, it aggregates all the data from multiple servers(reducers) to return a consolidated output back to the application (kv_store server in this case).
For example: In this assignment, we have tried to implement a word count for a book. This task is executed by N mapper instances and M instances, whose functioning is coordinated by the Master Instance and Key-Value Store Instance.

## Design Details
This implementation of map-reduce system has four attributes,
#### 1. Master Server:
In a literal sense, this server actually behaves as the master for this system. It is in charge of all the communication taking place between all the servers. It coordinates the exchange of data between the mappers, reducers and the key-value
storage.
#### 2. Mapper Server:
The mapper (say N in number) instances are invoked by the MasterInstance, and then they establish a connection with the Master Instance using socket connection. After being connected to the Master, the mapper instances parallely start accepting input from the Master Instance, in the form of chunks of string. At the master instance the input data in the form of a large document which is equally divided into N strings by the master instance. These mapper instances then concurrently send data to the Key-Value Store Instance.
#### 3. Key-Value Store Server:
At the key-value store, incoming commands in the form of, SET <KEY> <VALUE> are then decoded and the key-value pair is stored in the CSV file. Each mapper writes its own CSV file on the key-value store. After the completion of each mapping process, the master receives an acknowledgment from all the mapping processes, and then the master server, requests the key-value store instance to implement the combiner function. On completion of all the mapping tasks, the mapper instances are stopped. Here, we can see that the data from multiple mappers have been combined, sorted and shuffled by the mappers at the Key-Value Store.
After receiving an acknowledgment from the key-value store instance about the completion of the combiner function, the master server starts the Reducer Instances for further processing.
#### 4. Reducers:
The reducer (say M in number) instances are invoked by the Master Server, and then they establish a connection with the key-value instance using sockets. The reducers function are handed over tokens by the key-value store function, using which these instances can hash values from the output of the combiner function. Reducers than perform computation on data that the receive form the key-value store and send back their results to the key-value store instance, where results from all the reducers is combined and stored. On completion of all the reducing tasks, the reducer instances are stopped.
The most important thing to note here is that there’s no in-memory storage that has been assumed or used for the purpose of the exchange of data between any instances. All the intermediate data is being stored on the key-value store instance.
  
#### Virtual Machines Used on Google Cloud
All the instances used for this assignment have the same configuration which as follows,
● Machine type n1-standard-1 (1 vCPU, 3.75 GB memory)
● Reservation Automatically choose
● CPU platform Intel Haswell
● Zone us-central1-a
● Firewalls Allow HTTP traffic & Allow HTTPS traffic
● Image debian-9-stretch-v20191121
● Size 10 GB
● Image Standard persistent disk
● Image Standard persistent disk Allow full access to all Cloud API

These VMs also run a start-up script which is given as follows,
#! /bin/bash
cd /home/jdevansh99
python kv_store.py

#### Loose Ends
If one sees this assignment from a cloud engineer’s perspective there is a lot that can be changed and improved. The type of instance chosen can be more efficient in terms of cost and CPU utilization, which is turn will directly impact the performance of the program. Owing to a limited knowledge about the types of instances on GCP, it was difficult to predict, which VM Instance would have worked the best in this case, thus this assignment is based on the basic instance, as mentioned above. Map-Reduce as a framework, could have been made more interactive, for example a webpage, that takes all the inputs and then performs computations in the background and comes back
with a response in less than 10-15 seconds.
Though, there’s a massive improvement in the computation strength of the framework from last time, but I still see an opportunity to make it better, in terms of the running time and space utilization. As mentioned earlier, a bunch of other functions that map-reduce i capable of running can be implemented in the current program.

#### List of Files
1. Log Files (Folder- logs)
gcp_generated_logs.pdf | operations_list.txt
3. List of Disks (Folder- resources)
4. Code - master.py | kv_store.py | mapper.py | reducer.py
5. Config Files - config.txt | book1.txt | book2.txt

#### How to run the code?

ssh into the google cloud instance (or any VM/local)

python3 master.py

Disclaimer: IP Addresses are hardcoded in the code, which will require certain changes, depending on the environment. 

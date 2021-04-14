import socket
import pickle
import time

from info import _send_msg, _recv_msg
from info import storage_ip, MSG_SIZE


class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.latest_file_id = 0
        self.dir_tree = dict()
        self.dir_tree[self.client_id] = dict()

    
    def upload(self, file_name): 
        file_id, file_data = self._chunker(file_name)
        
        ## send to storage using sockets
        ## ask IP to monitor
        ## using _write function
        for storage_id in range(1, 3):
            res = self._write(file_id, storage_id, file_data)
            if res == -storage_id:
                print(f"UPLOAD failed with Storage {storage_id}")

            elif res == 0:
                print("File exists, Updated")
            
            else:
                print("Sucessful UPLOAD")

        self.dir_tree[self.client_id][file_id] = file_name
        return 


    def download(self, file_id):
        file_name = self.dir_tree[self.client_id][file_id]

        for storage_id in range(1, 3):
            res, data = self._read(file_id, storage_id)
            if res == -storage_id:
                print(f"DOWNLOAD failed with Storage {storage_id}")

            elif res == 0:
                print("No such file exist")
                return 
            
            else:
                print("Sucessful DOWNLOAD")

                file = open("downloads/"+file_name, "wb")
                file.write(data)
                file.close()
                return 

        print("BOTH STORAGE ARE DOWN !!\n")
        return 


    def _read(self, file_id, storage_id):
        s = socket.socket()  
        # s.settimeout(3)
        #After 3 sec of trying to connect the socket will timeout       
        
        # send monitor to ask for OSD details
        ip = storage_ip[storage_id]["ip"]
        port = storage_ip[storage_id]["port"]                
        
        # try :
        s.connect((ip, port)) 
        # s.settimeout(None)
        #To make it a non blocking assignment

        print(f"Made a connection to recieve file {file_id} from storage {storage_id}")

        msg = {"type":"READ", "client_username":self.client_id, "file":file_id}
        _send_msg(s, msg)

        time.sleep(3)

        response = _recv_msg(s, MSG_SIZE)
        s.close() 

        if response["error"]==False : 
            file_data = response["data"]
            return 1, file_data
        else:
            print(response["error_type"])
            return 0, None

        # except : 
        #     print(f"Unable to connect to storage node {storage_id}")
        #     s.close()
        #     return -storage_id, None
        
    
    def _write(self, file_id, storage_id, file_data):
        s = socket.socket()  
        # s.settimeout(3)
        #After 3 sec of trying to connect the socket will timeout       
        
        # send monitor to ask for OSD details
        ip = storage_ip[storage_id]["ip"]
        port = storage_ip[storage_id]["port"]                
        
        try :
            s.connect((ip, port)) 
            # s.settimeout(None)
            #To make it a non blocking assignment

            print(f"Sending a file {file_id} to storage {storage_id}")
            
            msg = {"type":"WRITE", "client_username":self.client_id, "file":file_id, "data": file_data}
            _send_msg(s, msg)
            time.sleep(3)

            response = _recv_msg(s, MSG_SIZE)
            s.close() 

            if response == None :
                return -storage_id
            elif response["error"]==False:
                return 1
            else:
                print(response["error_type"])
                return 0

        except : 
            print(f"Unable to connect to storage node {storage_id}")
            s.close()
            return -storage_id
    

    def _chunker(self, file_name):
        file = open(file_name, 'rb')
        data = file.read()

        self.latest_file_id += 1
        file_id = self.latest_file_id

        file.close()
        
        return file_id, data
    
    


client = Client("user_name_or_id")


client.upload("1.jpg")
#client.upload("2.jpg")

client.download(1)

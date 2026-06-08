import threading

#todo:
#make it .env
class NodeConfig:
    def __init__(self):
        self.server_url = "http://localhost:5000"
        self.data_directory = "Data"
        self.cache_directory = "Cache"
        self.local = True
        self.hosted = False
        self.chunk_size = (2 ** 19)
        self.public_ip = ""
        self.local_ip = ""
        self.token = ""
        self.decentorage_port = 0
        self.starting_port = 50000
        self.chunk_timeout = 8000
        self.disconnected_timeout = 60 * 60 * 1000
        self.semaphore = threading.Semaphore()
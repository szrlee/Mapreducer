import socket
import asyncore
import asynchat
import shelve
import cPickle as pickel

class peer_client(asynchat.async_chat):
    def __init__(self, addr, worker):
        asynchat.async_chat.__init__(self)
        self.set_terminator("\n\r")
        self.buf= []
        self.worker= worker
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((addr,21111))

    def run(self, key_k_pair):
        pdata= pickel.dumps(key_k_pair)
        length= len(pdata)
        self.push("collect:" + str(length) + ":" + pdata + "\n\r")

    def collect_incoming_data(self, data):
        self.buf.append(data)

    def found_terminator(self):
        command, length, pdata= ''.join(self.buf).split(":", 2)
        self.buf=[]
        self.set_terminator("\n\r")
        data= pickel.loads(pdata)
        commands={
                "collectdone" : self.collectdone
                }
        if command in commands:
            commands[command](data)

    def handle_close(self):
        self.close()

    def handle_error(self):
        self.close()

    def collectdone(self, data):
        key= data[0]
        if key not in self.worker.key_map:
            self.worker.key_map.append(key)
            self.worker.map_result.extend(data[1])
            self.worker.count= self.worker.count - 1
            self.worker.check_full()




class reduceworker(object):
    def __init__(self, data , con_serv):
        self.data= data
        self.conn_to_serv= con_serv
        self.key_map=[]
        self.map_result=[]
        self.peer={}
        self.count= len(self.data[1].keys())

    def work(self):
        k= self.data[0]
        collect_base= self.conn_to_serv.results_base
        for key in self.data[1].keys():
            if str(key) in collect_base.keys():
                self.map_result.extend(collect_base[str(key)][k])
                self.key_map.append(key)
                self.count = self.count -1
                self.check_full()
            else:
                for ip in self.data[1][key]:
                    request = None
                    if ip in self.peer.keys():
                        request = self.peer[ip]
                    else:
                        request= peer_client(ip, self)
                        self.peer[ip] = request
                    request.run((key, k))

    def check_full(self):
        if (self.count==0):
            for peers in self.peer.values():
                peers.close_when_done()
            self.call_reducefn()

    def call_reducefn(self):
        result= self.conn_to_serv.reducefn(self.data[0], self.map_result)
        self.conn_to_serv.send_command("reducedone", (self.data[0], result))




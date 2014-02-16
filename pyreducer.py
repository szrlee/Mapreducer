import socket
import asyncore
import asynchat
import pickle
import marshal
import random
import logging

class Task_control:
    MAPPING= 1
    REDUCING= 2
    DONE=3
    def __init__(self, source, server):
        self.piter=iter(source)
        self.source= source
        self.mapping={}
        self.map_result=[]
        self.result={}
        self.reducing={}
        self.state= Task_control.MAPPING
        self.server= server

    def get_task(self):
        if (self.state== Task_control.MAPPING):
            try:
                key= self.piter.next()
                item= key, self.source[key]
                self.mapping[key]= item[1]
                return ("map", item)
            except StopIteration:
                if (len(self.mapping)> 0):
                    key= random.choice(self.mapping.keys())
                    item= key, self.mapping[key]
                    return ("map", item)
                else:
                    self.piter= iter(self.map_result)
                    self.state= Task_control.REDUCING
        if (self.state== Task_control.REDUCING):
            try:
                key= self.piter.next()
                item= key, self.server.k_key_map[key]
                self.reducing[key]= item[1]
                return ("reduce", item)
            except StopIteration:
                if (len(self.reducing)> 0):
                    key= random.choice(self.reducing.keys())
                    item= key, self.reducing[key]
                    return ("reduce", item)
                if (self.server.resultfn):
                    self.server.resultfn(self.result)
                self.state= Task_control.DONE
        return ("Done", None)

    def complete_a_map(self, data):
        if data[0] in (self.mapping.keys()):
            del(self.mapping[(data[0])])
        for k in data[1]:
            if (k not in self.map_result):
                self.map_result.append(k)
        logging.info("map %s done\n"%str(data[0]))
        return

    def complete_a_reduce(self, item):
        logging.info("reduce %s done\n"%str(item[0]))
        if not(item[0] in self.reducing):
            return
        self.result[item[0]]= item[1]
        del self.reducing[item[0]]
        return




class Server_connection(asynchat.async_chat):
    def __init__(self, connection, server, map):
        asynchat.async_chat.__init__(self, connection, map= map)
        self.buf=[]
        self.server= server
        self.ac_in_buffer_size = 81960
        self.set_terminator("\n\r")

    def collect_incoming_data(self, data):
        self.buf.append(data)

    def found_terminator(self):
        try:
            command, slength, sdata= "".join(self.buf).split(":", 2)
            #if not(int(slength) == len(sdata)):
            #    buf= []
            #    return
            self.buf=[]
            handle= {
            "mapdone": self.map_done,
            "reducedone" : self.reduce_done,
            }
            if (command in handle):
                handle[command](sdata)
            self.set_terminator("\n\r")
        except EOFError:
            if command == "getask":
                self.new_task()

    def start(self):
        pmapfn= pickle.dumps(marshal.dumps(self.server.mapfn.__code__))
        length= len(pmapfn)
        logging.info("Sending mapfn at length %d.\n",length)
        self.push("mapfn:" + str(length) + ":")
        self.push(pmapfn)
        self.push("\n\r")

        preducefn= pickle.dumps(marshal.dumps(self.server.reducefn.__code__))
        length= len(preducefn)
        logging.info("Sending reuducefn at length %d.\n",length)
        self.push("reducefn:"+str(length)+":")
        self.push(preducefn)
        self.push("\n\r")

        self.new_task()

    def new_task(self):
        command, data= self.server.tasks.get_task()
        if data:
            pickled_data= pickle.dumps(data)
            length= len(pickled_data)
            self.push(command+ ":" + str(length) + ":" + pickled_data + "\n\r")
        else:
            length= 0
            self.push(command+ ":" + str(length) + ":" + "\n\r")
        logging.info("sending command %s.\n",command)


    def map_done(self, sdata):
        data= pickle.loads(sdata)
        self.server.tasks.complete_a_map(data)
        if data[0] not in self.server.key_con_map.keys():
            self.server.key_con_map[data[0]]= []
        self.server.key_con_map[data[0]].append(self.addr[0])
        for key_item in data[1]:
            if key_item not in self.server.k_key_map.keys():
                self.server.k_key_map[key_item]={}
            self.server.k_key_map[key_item][data[0]]=self.server.key_con_map[data[0]]

        self.new_task()


    def reduce_done(self, sdata):
        data= pickle.loads(sdata)
        self.server.tasks.complete_a_reduce(data)
        self.new_task()


class Server(asyncore.dispatcher,object):
    def __init__(self, host, port):
        self.socket_map={}
        self.key_con_map={}
        self.k_key_map={}
        asyncore.dispatcher.__init__(self, map=self.socket_map)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)
        self.mapfn= None
        self.reducefn= None
        self.resultfn= None
        self.source= None
        self.tasks= None

    def run_server(self):
        logging.info("Server Start.\n")
        asyncore.loop(map= self.socket_map)

    def handle_accept(self):
        connection, addr= self.accept()
        logging.info("%s connected.\n",addr)
        server_connect= Server_connection(connection, self, self.socket_map)
        server_connect.start()

    def get_tasks(self):
        self.tasks= Task_control(self.source, self)


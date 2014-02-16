import asynchat
import cPickle as pickle
import types
import socket
import asyncore
import sys
import marshal
import shelve
import reduceworker
import os
import thread
import time
import threading

class Client(asynchat.async_chat, object):
    def __init__(self, base):
        asynchat.async_chat.__init__(self)
        self.buffer = []
        self.set_terminator('\n\r')
        self.globemapfn = self.reducefn = None
        self.results_base = base
        self.ac_in_buffer_size = 999999

    def conn(self, server, port):
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect((server, port))

    def collect_incoming_data(self, data):
        self.buffer.append(data)

    def found_terminator(self):
        try:
            command, length, pdata  = (''.join(self.buffer)).split(':',2)
            data = pickle.loads(pdata)
            self.buffer = []
            self.process_command(command, data)
            self.set_terminator('\n\r')
        except EOFError:
            self.done(command, None)

    def process_command(self, command, data=None):
        commands = {
                'mapfn': self.set_mapfn,
                'reducefn': self.set_reducefn,
                'map': self.call_mapfn,
                'reduce': self.call_reducefn,
                'done': self.done
                }
        if command in commands:
            commands[command](command, data)

    def set_mapfn(self, command, data):
        self.mapfn = types.FunctionType(marshal.loads(data), globals(), 'mapfn')

    def set_reducefn(self, command, data):
        self.reducefn = types.FunctionType(marshal.loads(data), globals(), 'reducefn')

    def call_mapfn(self, command, data):
        results = {}
        for k, v in self.mapfn(data[0], data[1]):
            if k not in results:
                results[k] = []
            results[k].append(v)
        K = str(data[0])
        metex.acquire()
        if data[0] in results.keys():
            if not self.results_base[K]== results:
                self.results_base[K] = results
            else :
                pass
        else:
            self.results_base[K] = results
        metex.release()
        self.send_command('mapdone', (data[0], results.keys()))

    def call_reducefn(self, command, data):
        reducer = reduceworker.reduceworker(data, self)
        reducer.work()

    def send_command(self, command, data):
        if data:
            pdata = pickle.dumps(data)
            length = str(len(pdata))
            self.push(command +':'+length+':'+pdata+'\n\r')
        else:
            self.push(command +':'+'0:'+'\n\r')

    def done(self, command, data):
        if os.path.isfile('results.dat'):
            os.remove('results.dat')
        self.close_when_done()
        quit()

class peer_server(asyncore.dispatcher):
    def __init__(self, port, base):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind(('',port))
        self.listen(5)
        self.base = base

    def handle_accept(self):
        acception= self.accept()
        if acception:
            peer_conn, addr = acception
            pconnect = peer_server_conn(peer_conn, self.base)


class peer_server_conn(asynchat.async_chat):
    def __init__(self, conn, base):
        asynchat.async_chat.__init__(self, conn)
        self.buffer = []
        self.set_terminator('\n\r')
        self.base = base

    def collect_incoming_data(self, data):
        self.buffer.append(data)

    def found_terminator(self):
        command, length, pdata  = (''.join(self.buffer)).split(':',2)
        if int(length) == len(pdata):
            data = pickle.loads(pdata)
        self.buffer = []
        if data:
            self.process_command(command, data)
        else:
            print error
        self.set_terminator('\n\r')    

   # def handle_close(self):
   #     self.close()

   # def handle_error(self):
   #     self.close()

    def send_command(self, command, data):
        if data:
            pdata = pickle.dumps(data)
            length = str(len(pdata))
            self.push(command +':'+length+':'+pdata+'\n\r')
        else:
            self.push(command +':'+'\n\r')

    def process_command(self, command, data):
        commands ={
                'collect': self.collect
                }
        if command in commands:
            commands[command](command, data)

    def collect(self, command, data):
        K = str(data[0])
        collect_base = self.base
        collect_values = []
        if K in collect_base.keys():
            collect_values.extend(collect_base[K][data[1]])
        else:
            print 'The key:%s isn\'t here'%K
        self.send_command('collectdone', (data[0], collect_values))

def start_client(base):
    client = Client(base)
    client.conn('127.0.0.1', 20000)

    
metex = threading.RLock()
def main():
    try:
        base = shelve.open('results.dat', 'c')
        peerserver = peer_server(21111, base)
        for i in range(3):
           # map = {}
            thread.start_new_thread(start_client, (base,))
            time.sleep(1)
        asyncore.loop()
    except EOFError:
        quit()


if __name__ == '__main__':
    main()


from queue import Queue
import zmq
import signal
import sys
import select
import time
import traceback
from threading import Thread

signal.signal(signal.SIGINT, signal.SIG_DFL)

def run_thread(func, args):
    my_thread = Thread(target=func, args=args)
    my_thread.daemon = True
    my_thread.start()
    return my_thread

def wait_for_server_startup(ip, port):
    while True:
        try:
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect(f"tcp://localhost:{port}")   #replace localhost with ip
            return socket
        except Exception as e:
            traceback.print_exc(limit=1000)

def send_and_recv_no_retry(msg, ip, port, timeout=-1):
    '''conn = wait_for_server_startup(ip, port)
    print(port)
    resp = None
    try:
        #conn.sendall(msg.encode('utf-8'))
        conn.send(msg.encode('utf-8'))
        if timeout > 0:
            ready = select.select([conn], [], [], timeout)
            if ready[0]:
                resp = conn.recv(2048).decode('utf-8')
        else:
            resp = conn.recv(2048).decode('utf-8')

    except Exception as e:
        traceback.print_exc(limit=1000)
    
    conn.close()
    print("hi",port)
    return resp'''
    try:
        conn = wait_for_server_startup(ip, port)
    except Exception as e:
        traceback.print_exc(limit=1000)
        return None

    print(port)
    resp = 0
    try:
        conn.send(msg.encode('utf-8'))
        if timeout > 0:
            ready = select.select([conn], [], [], timeout)
            if ready[0]:
                resp = conn.recv(2048).decode('utf-8')
        else:
            resp = conn.recv(2048).decode('utf-8')

    except Exception as e:
        traceback.print_exc(limit=1000)
        conn.close()
        return None
    
    conn.close()
    return resp

def send_and_recv(msg, ip, port, res=None, timeout=-1):
    resp = None
    while True:
        resp = send_and_recv_no_retry(msg, ip, port, timeout)
        if resp:
            break

    if res is not None:
        res.put(resp)
    return resp




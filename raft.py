import threading
import time
from random import randint, random
import zmq
import signal
import sys
import time
from threading import Thread, Lock
import os
import utils
import datetime
from queue import Queue
import traceback
from datetime import datetime
import re

signal.signal(signal.SIGINT, signal.SIG_DFL)

term = 1

class RaftDatabase:
    def __init__(self, node_id):
        self.node_id = node_id
        self.logs_file = f"logs_node_{node_id.split(':')[1]}/logs.txt"
        #self.metadata = self.load_metadata()
        self.logs = self.load_logs()
        #self.metadata_dic = self.metadata
        self.map = self.logs
            
    def append_log(self, map):
        with open(self.logs_file, 'w') as f:
            for key, value in map.items():
                f.write(f'{key}: {value}\n')
    
    def load_logs(self):
        if os.path.exists(self.logs_file):
            with open(self.logs_file, 'r') as f:
                logs = f.read()
                logs_dic = {}
                logs = logs.split('\n')
                logs = [log for log in logs if log]
                for log in logs:
                    log = log.split(':')
                    logs_dic[log[0]] = log[1]
                return logs_dic   
        else:
            return {}

    def set(self, key, value):
        self.map[key] = value
        self.append_log(self.map)

    def get(self, key):
        if key in self.map:
            return self.map[key]
        return " "

class CommitLog:
    def __init__(self,file):
        self.file = file
        self.create = self.create_file()
        self.last_index, self.last_term = self.load_last_index_term()
        #self.last_term = 0
        #self.last_index = -1

    def load_last_index_term(self):
        if os.path.exists(self.file+"-metadata.txt"):
            with open(self.file+"-metadata.txt", 'r') as f:
                lines = f.readlines()
                if lines:
                    last_line = lines[-1]
                    last_line = last_line.strip().split(",")
                    self.last_term = int(last_line[0])
                    self.last_index = int(last_line[1])
                    term = self.last_term
                else:
                    self.last_term = 1
                    self.last_index = -1
        else:
            with open(self.file+"-metadata.txt", 'w') as f:
                f.write(f"1,-1")
            self.last_term = 1
            self.last_index = -1
        
        return self.last_index, self.last_term
    
    def create_file(self):
        with open(self.file, 'a') as f:
            f.write('')
    
    def get_last_index_term(self):
        return self.last_index, self.last_term
    
    def log(self, term, command):
        with open(self.file, 'a') as f:
            now = "SET"
            message = f"{now},{term},{command}"
            f.write(f"{message}\n")
            self.last_term = term
            self.last_index += 1
            with open(self.file+"-metadata.txt", 'w') as f:
                f.write(f"{self.last_term},{self.last_index}")
        return self.last_index, self.last_term

    def log_replace(self, term, commands, start):
        with open(self.file, 'r') as f:
            x = []
            for line in f:
                line = line.strip().split(",")
                x.append(line)
        index = 0
        i = 0
        with open(self.file, 'a') as f1:
            if len(commands) > 0:
                while i < len(commands):
                    if index >= start:
                        command = commands[i]
                        i += 1
                        now = "SET"
                        command = command.split(" ")
                        command = ",".join(command)
                        message = f"{now},{term},{command}"
                        flag = 0
                        for j in range(len(x)):
                            if str(x[j][2]) == str(command.split(",")[0]) and str(x[j][3]) == str(command.split(",")[1]):
                                flag = 1
                                break
                        if flag == 0:
                            f1.write(f"{message}\n")
                            print(f"{message} committed to the log.")
                            with open(f"logs_node_{(((self.file).split(':')[1]).split('.')[0])}/dump.txt", 'a') as f3:
                                f3.write(f"{message} committed to the log.\n")
                            f3.close()
                            if index > self.last_index:
                                self.last_term = term
                                self.last_index = index
                                with open(self.file+"-metadata.txt", 'w') as f2:
                                    f2.write(f"{self.last_term},{self.last_index}")                    
                    index += 1
        return self.last_index, self.last_term
    
    def read_log(self):
        output = []
        with open(self.file, 'r') as f:
            for line in f:
                _, term, command = line.strip().split(",")
                output += [(term, command)]
        return output

    def read_logs_start_end(self, start, end=None):
        output = []
        index = 0
        with open(self.file, 'r') as f:
            for line in f:
                if index >= start:
                    x = line.strip().split(",")
                    term = x[1]
                    command = x[2]+" "+x[3]
                    output += [(term, command)]
                index += 1
                if end and index > end:
                    break
        return output
 
class RaftNode:
    def __init__(self, ip, port, partitions):
        self.ip = ip
        self.port = port
        self.partitions = eval(partitions)
        self.conns = [[None]*len(self.partitions[i]) for i in range(len(self.partitions))]
        self.cluster_index = -1
        self.server_index = -1
        self.node_id = f'{ip}:{port}'
        self.lease_duration = 10    #check 
        self.lease_expiration_time = 0
        self.heartbeat_interval=1
        for i in range(len(self.partitions)):
            cluster = self.partitions[i]
            for j in range(len(cluster)):
                ip, port = cluster[j].split(':')
                port = int(port)
                if(ip == self.ip and port == self.port):
                    self.cluster_index = i
                    self.server_index = j
                else:
                    self.conns[i][j] = (ip, port)
        
        self.logs_dir = f"logs_node_{self.node_id.split(':')[1]}"
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

        self.dump_file = f"logs_node_{self.node_id.split(':')[1]}/dump.txt"
        with open(self.dump_file, 'a') as f:
            f.write('')

        self.database = RaftDatabase(self.node_id)
        self.commit_log = CommitLog(f'commit_log-{self.node_id}.txt')
        self.current_term = self.commit_log.last_term
        self.voted_for = -1
        self.votes = set()

        u = len(self.partitions[self.cluster_index])
        self.state = 'FOLLOWER' if len(self.partitions[self.cluster_index]) > 1 else 'LEADER'
        self.leader_id = -1
        self.commit_index = 0
        self.next_index = [0]*u
        self.match_index = [0]*u
        self.election_period_ms = randint(5000, 10000)
        self.rpc_period_ms = 1000
        self.election_timeout = -1
        self.rpc_timeout = [-1]*u

        print("Ready....")

    def init(self):
        self.set_election_timeout()
        utils.run_thread(func=self.on_election_timeout, args=())
        utils.run_thread(func=self.leader_send_append_entries, args=())
        utils.run_thread(func=self.lease_expiry_monitor, args=())

    
    def get_leader(self):
        return self.leader_id

    def set_election_timeout(self, timeout=None):
        print("election timeout set....")
        if timeout:
            self.election_timeout = timeout
        else:
            self.election_timeout = time.time() + randint(self.election_period_ms, 2*self.election_period_ms)/1000
    def lease_expiry_monitor(self):
        while True:
            self.check_lease_and_step_down()
            time.sleep(1)  
    def on_election_timeout(self):
        while True:
            if time.time() > self.election_timeout and (self.state == 'FOLLOWER' or self.state == 'CANDIDATE'):
                print(f"Election timeout for Node {self.server_index}...")
                with open(self.dump_file, 'a') as f:
                    f.write(f"Election timeout for Node {self.server_index}...\n")
                f.close()
                self.set_election_timeout()
                self.start_election()
    
    def start_election(self):
        print(f"Node {self.server_index} Starting election...")
        with open(self.dump_file, 'a') as f:
            f.write(f"Node {self.server_index} Starting election...\n")
        f.close()
        self.state = 'CANDIDATE'
        self.voted_for = self.server_index
        self.current_term += 1
        self.votes.add(self.server_index)
        threads = []
        for i in range(len(self.partitions[self.cluster_index])):
            if i != self.server_index:
                t = utils.run_thread(func=self.request_vote, args=(i,))
                threads.append(t)
        for t in threads:
            t.join()
        
        return True 
    
    def request_vote(self, server):
        last_index, last_term = self.commit_log.get_last_index_term()
        while True:
            print(f"Requesting vote from {server}...")
            if(self.state == 'CANDIDATE' and time.time() < self.election_timeout):
                ip, port = self.conns[self.cluster_index][server]
                msg = f"RequestVote,{self.current_term},{self.server_index},{last_index},{last_term}"
                resp = utils.send_and_recv_no_retry(msg, ip, port, timeout=self.rpc_period_ms/1000)
                if resp:
                    resp = resp.split(',')
                    server = int(resp[1])
                    curr_term = int(resp[2])
                    voted_for = int(resp[3])
                    self.process_vote_reply(server, curr_term, voted_for)
                    break
            else:
                break
    
    def step_down(self, term):
        print(f"Node {self.server_index} Stepping down....")
        with open(self.dump_file, 'a') as f:
            f.write(f"Node {self.server_index} Stepping down....\n")
        f.close()
        self.state = 'FOLLOWER'
        self.current_term = term
        self.voted_for = -1
        self.votes = set()
        self.set_election_timeout()
        self.lease_expiration_time=0

    def process_vote_request(self, server, term, last_term, last_index):
        print(f"Processing vote request from {server}...")
        if (term > self.current_term):
            self.step_down(term)

        self_last_index, self_last_term = self.commit_log.get_last_index_term()
        
        if(term == self.current_term) and (self.voted_for == server or self.voted_for == -1) and (last_term > self_last_term or (last_term == self_last_term and last_index >= self_last_index)):
            print(f"Vote granted for Node {server} in term {self_last_term}.")
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote granted for Node {server} in term {self_last_term}.\n")
            f.close()
            self.voted_for = server
            self.state = 'FOLLOWER'
            self.set_election_timeout()
        else:
            print(f"Vote denied for Node {server} in term {self_last_term}.")
            with open(self.dump_file, 'a') as f:
                f.write(f"Vote denied for Node {server} in term {self_last_term}.\n")
            f.close()

        return f'VOTE-REP,{self.server_index},{self.current_term},{self.voted_for}'
    
    def process_vote_reply(self, server, term, voted_for):
        print(f"Processing vote reply from {server}...")
        if term > self.current_term:
            self.step_down(term)
        if term == self.current_term and self.state == 'CANDIDATE':
            if voted_for == self.server_index:
                self.votes.add(server)
        if (len(self.votes) > len(self.partitions[self.cluster_index])//2):
            self.state = 'LEADER'
            self.leader_id = self.server_index
            print(f"New leader {self.server_index} waiting for the lease timer to run out....")
            with open(self.dump_file, 'a') as f:
                f.write(f"New leader {self.server_index} waiting for the lease timer to run out....\n")
            f.close()
            self.renew_lease()
            print(f"Node {self.server_index} is the leader....")
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.server_index} is the leader....\n")
            f.close()
            print(f"{self.votes}-{self.current_term}")

    # def leader_send_append_entries(self):
    #     print("Sending append entries....")
    #     while True:
    #         if self.state == 'LEADER':
    #             self.append_entries()
    #             last_index, _ = self.commit_log.get_last_index_term()
    #             self.commit_index = last_index
    #             #wait for 1 second before sending the next append entries
    #             time.sleep(1)
            
    def leader_send_append_entries(self):
        while True:
            if self.state == 'LEADER':
                ack_count = 1
                print(f"Leader {self.server_index} sending heartbeats and trying to renew lease...")  
                with open(self.dump_file, 'a') as f:
                    f.write(f"Leader {self.server_index} sending heartbeats and trying to renew lease...\n")
                f.close()
                res = Queue()
                for i, server in enumerate(self.partitions[self.cluster_index]):
                    if i != self.server_index:
                        utils.run_thread(func=self.send_append_entries_request, args=(i, res,))
                
                while True:
                    res.get(block=True)
                    ack_count += 1
                    if ack_count > len(self.partitions[self.cluster_index])//2:
                        self.renew_lease()
                        break
                time.sleep(self.heartbeat_interval)

    def renew_lease(self):
        print(f"{self.server_index} renewing lease....")
        with open(self.dump_file, 'a') as f:
            f.write(f"Node {self.server_index} renewing lease....\n")
        f.close()
        self.lease_expiration_time = time.time() + self.lease_duration

    def redirect_to_leader(self, msg, conn):
        if self.leader_id is not None:
            for i in range(len(self.partitions)):
                cluster = self.partitions[i]
                for j in range(len(cluster)):
                    leader_ip, leader_port = cluster[j].split(':')
                    leader_port=int(leader_port)
        else:
            print("Leader ID is not known.")
            return "Error: No Active Leader."

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, 1000)
        socket.setsockopt(zmq.SNDTIMEO, 1000)
        try:
            socket.connect(f"tcp://localhost:{leader_port}")  #replace localhost with ip
            message = ','.join(msg)
            socket.send_string(message)
            response = socket.recv_string()
            print(response)
            return response
        except zmq.error.Again:
            print("Node is not available.")
            return "Error: Node is not available."
        finally:
            socket.close()
    def is_lease_valid(self):
        return time.time() < self.lease_expiration_time
    def append_entries(self):
        res = Queue()
        for i in range(len(self.partitions[self.cluster_index])):
            if i != self.server_index:
                utils.run_thread(func=self.send_append_entries_request, args=(i, res,))

        if len(self.partitions[self.cluster_index]) > 1:
            cnts = 0
            while True:
                res.get(block=True)
                cnts += 1
                if cnts >= len(self.partitions[self.cluster_index])//2:
                    return
        else:
            return

    def send_append_entries_request(self, server, res=None):
        print(f"Sending append entries request to {server}....")
        prev_idx = self.next_index[server] - 1
        log_slice = self.commit_log.read_logs_start_end(prev_idx)

        if prev_idx == -1:
            prev_term = 0
        else:
            if(len(log_slice)>0):
                prev_term = log_slice[0][0]
                log_slice = log_slice[1:] if len(log_slice) > 1 else []
            else:
                prev_term = 0
                log_slice = []

        log_slice = [{x[0]: x[1]} for x in log_slice]
        msg = f"APPEND-REQ,{self.server_index},{self.current_term},{prev_idx},{prev_term},{str(log_slice)}, {self.commit_index}"
        succ = 0
        while True:
            if self.state == 'LEADER':
                ip, port = self.conns[self.cluster_index][server]
                resp = utils.send_and_recv_no_retry(msg, ip, port, timeout=self.rpc_period_ms/1000)
                if resp:
                    resp = resp.split(',')
                    server = int(resp[1])
                    curr_term = int(resp[2])
                    success = bool(resp[3])
                    succ = success
                    index = int(resp[4])
                    self.process_append_reply(server, curr_term, success, index)
                    break
            else:
                break

        if res:
            res.put('OK')
        
        return succ
    
    def process_append_requests(self, server, term, prev_idx, prev_term, logs, commit_index):
        print(f"Processing append request from {server}....")
        self.set_election_timeout()
        flag, index = 0, 0

        if term > self.current_term:
            self.step_down(term)

        if term == self.current_term:
            self.leader_id = server
            self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) if prev_idx != -1 else []

            success = prev_idx == -1 or (len(self_logs) > 0 and int(self_logs[0][0]) == prev_term)

            if success:
                last_index, last_term = self.commit_log.get_last_index_term()
                if len(logs) > 0:
                    for key in logs[-1].keys():
                        a = key
                if len(logs)>0 and last_term == a and last_index == self.commit_index:
                    index = self.commit_index
                else:
                    index = self.store_entries(prev_idx,logs)
            
            flag = 1 if success else 0
        
        if flag == 1:
            print(f"Node {self.server_index} accepted AppendEntries RPC from Node {server}.")
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.server_index} accepted AppendEntries RPC from Node {server}.\n")
            f.close()
        else:
            print(f"Node {self.server_index} rejected AppendEntries RPC from Node {server}.")
            with open(self.dump_file, 'a') as f:
                f.write(f"Node {self.server_index} rejected AppendEntries RPC from Node {server}.\n")
            f.close()

        return f"APPEND-REP,{self.server_index},{self.current_term},{flag},{index}"
    
    def process_append_reply(self, server, term, success, index):
        print(f"Processing append reply from {server}....")
        if term > self.current_term:
            self.step_down(term)
        
        if term == self.current_term and self.state == 'LEADER':
            if success:
                self.next_index[server] = index + 1
            else:
                self.next_index[server] = max(0, self.next_index[server] - 1)
                self.send_append_entries_request(server)
    def store_entries(self, prev_idx, leader_logs):
        commands = []
        for i in range(len(leader_logs)):
            for key, value in leader_logs[i].items():
                commands.append(f"{value}")
        #commands = [f"{leader_logs[i][0]}" for i in range(len(leader_logs))]
        last_index, _ = self.commit_log.log_replace(self.current_term, commands, prev_idx+1)
        self.commit_index = last_index

        for command in commands:
            self.update_state_machine(command)
        return last_index
    
    def check_lease_and_step_down(self):
        if self.state == 'LEADER' and not self.is_lease_valid():
            print(f"Leader lease for {self.server_index} expired. Stepping down.")
            with open(self.dump_file, 'a') as f:
                f.write(f"Leader lease for {self.server_index} expired. Stepping down.\n")
            f.close()
            self.state = 'FOLLOWER'
            self.leader_id = None
            self.voted_for = None
            self.votes = set()
            self.set_election_timeout()
            self.start_election()

    def update_state_machine(self, command):     #correct this
        if ',' in command:
            command = command.split(',')
            key = command[0]
            value = command[1]
        else:
            command = command.split(' ')
            key = command[0]
            value = command[1]
        self.database.set(key, value)

    def handle_requests(self, msg, conn, socket):
        print(f"Handling requests....")
        msg1 = msg
        msg = msg.split(',')
        if msg[0] == 'RequestVote':
            term = int(msg[1])
            server = int(msg[2])
            last_index = int(msg[3])
            last_term = int(msg[4])
            output = self.process_vote_request(server, term, last_term, last_index)
            socket.send_multipart([output.encode('utf-8')])
        elif msg[0] == 'VOTE-REP':
            server = int(msg[1])
            term = int(msg[2])
            voted_for = bool(msg[3])
            self.process_vote_reply(server, term, voted_for)
        elif msg[0] == 'APPEND-REQ':
            server = int(msg[1])
            term = int(msg[2])
            prev_idx = int(msg[3])
            prev_term = int(msg[4])
            match = re.search(r'\[.*?\]', msg1)
            if match:
                extracted_part = match.group(0)
            logs = eval(extracted_part)
            commit_index = int(msg[-1])
            output = self.process_append_requests(server, term, prev_idx, prev_term, logs, commit_index)
            socket.send_multipart([output.encode('utf-8')])
        elif msg[0] == 'APPEND-REP':
            server = int(msg[1])
            term = int(msg[2])
            success = bool(msg[3])
            index = int(msg[4])
            self.process_append_reply(server, term, success, index)
        elif msg[0] == 'LEADER':
            leader = self.get_leader()
            #socket.send_multipart([int(leader)])
            socket.send(str(leader).encode('utf-8'))
        elif msg[0] == 'SET':
            if self.state == 'LEADER':
                print(f"Node {self.server_index} processing SET request....")
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.server_index} processing SET request....\n")
                f.close()
                key = conn[1].decode('utf-8')
                value = conn[2].decode('utf-8')
                command = f"{key},{value}"
                _, _ = self.commit_log.log(self.current_term, command)
                print(f"Node {self.server_index} committed SET request to the state machine")
                self.update_state_machine(command)
                self.append_entries()
                socket.send_multipart([b'Successfully set key-value pair.'])
            else:
                socket.send_multipart([b'Error: No Active Leader.'])
        elif msg[0] == 'GET':
            if self.state == 'LEADER' and self.is_lease_valid():
                print(f"Node {self.server_index} processing GET request....")
                with open(self.dump_file, 'a') as f:
                    f.write(f"Node {self.server_index} processing GET request....\n")
                f.close()
                with open('data2.txt', 'w') as f:
                    f.write('LEADER')
                key = conn[1].decode('utf-8')
                value = self.database.get(key)
                socket.send_multipart([value.encode('utf-8')])
            else:
                with open('data2.txt', 'w') as f:
                    f.write(f'abe bhai{self.state}')
                response = self.redirect_to_leader(msg, conn)
                socket.send_string(response)
            #key = conn[1].decode('utf-8')
            #value = self.database.get(key)
            #socket.send_multipart([value.encode('utf-8')])

            # else:
            #     socket.send_multipart([b'Leader cannot process GET request.'])


    def process_requests(self, conn, socket):
        while True:
            try:
                msg = conn[0].decode('utf-8')
                if msg:
                    print(f"Received {msg}....")
                    output = self.handle_requests(msg, conn, socket)
                    #conn.send(output.encode('utf-8'))
                    break
            except Exception as e:
                traceback.print_exc(limit=1000)
                print("Error processing request....")
                conn.close()
                break

    def listen_to_client(self):
        print("Listening to client....")
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{port}")
        while True:
            conn = socket.recv_multipart()
            print(f"Received request from {conn}")
            my_thread = Thread(target=self.process_requests, args=(conn,socket,))
            my_thread.daemon = True
            my_thread.start()
            my_thread.join()

if __name__ == "__main__":
    #ip, port, partitions = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    ip, port = sys.argv[1], int(sys.argv[2])
    partitions = "[['127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003','127.0.0.1:5004','127.0.0.1:5005']]"
    raft = RaftNode(ip, port, partitions)
    utils.run_thread(func=raft.init, args=())
    raft.listen_to_client()



    

    









    













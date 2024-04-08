import random
import threading
from concurrent import futures
import time
import grpc
import os
import signal
import sys
import raft_pb2
import raft_pb2_grpc


nodes = {1: "localhost:50051", 2: "localhost:50052", 3: "localhost:50053", 4: "localhost:50054", 5: "localhost:50055"}       #global list of all nodes where keys are node id and values are node addresses

class Node(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, id, address, client_address):
        self.id = id
        self.address = address
        self.clientAddress = client_address
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitLength = 0
        self.currentRole = "Follower"
        self.currentLeader = None
        self.votesReceived = []
        self.sentLength = {node_id: 0 for node_id in nodes}
        self.ackLength = {node_id: 0 for node_id in nodes}
        self.electionTimer = None
        self.leaseDuration = 10
        self.leaseTimer = self.election_timer = threading.Timer(0, lambda: None)
        self.oldLease = 0
        self.start_time_lease=0

        self.dumpPath = os.path.join(os.getcwd() + "/logs_node_" + str(self.id), "dump.txt")   #dump file path
        self.dump = open(self.dumpPath, "a")

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(address)        
        self.server.start()

        self.metadata = os.path.join(os.getcwd() + "/logs_node_" + str(self.id), "metadata.txt")        #Load metadata if it exists
        if os.path.exists(self.metadata):
            self.crashRecovery()

        self.startElectionTimer()

        self.replication_interval = 5        # Adjust the interval as needed (in seconds)
        self.replication_thread = threading.Thread(target=self.periodicHeartbeats)
        self.replication_thread.daemon = True          # Daemonize the thread so it exits when the main program exits
        self.replication_thread.start()

        signal.signal(signal.SIGINT, self.signalHandler)       #On Ctrl+C, store metadata and exit


        
    def crashRecovery(self):
        f = open(self.metadata, "r")
        lines = f.readlines()
        f.close()
        for line in lines:
            key_value = line.strip().split(": ")
            if len(key_value) == 2:
                k, v = key_value
                if k == "currentTerm":
                    self.currentTerm = int(v)
                elif k == "votedFor":
                    if v == "None":
                        self.votedFor = None
                    else:
                        self.votedFor = int(v)
                elif k == "commitLength":
                    self.commitLength = int(v)
                elif k == "log":
                    self.log = [entry.strip() for entry in v.split(",")]

            elif len(key_value) == 1:       #log is empty
                self.log = []



    def startElectionTimer(self):
        def timeForElectionTimeout():
            if self.election_timer.is_alive():
                self.electionTimeout()
        
        timeout = random.uniform(5, 10)
        self.election_timer = threading.Timer(timeout, timeForElectionTimeout)
        self.election_timer.daemon = True  # Set as daemon thread
        self.election_timer.start()

        #print(f"Election timer started for node {self.id}. Timeout: {timeout} seconds")

    
    def startLeaseTimer(self):
        def timeForLeaseTimeout():
            self.LeaseTimeout()

        timeout = self.leaseDuration
        self.leaseTimer = threading.Timer(timeout, timeForLeaseTimeout)
        self.leaseTimer.daemon = True  # Set as daemon thread
        self.leaseTimer.start()
        #print(f"Lease timer started for node {self.id}. Timeout: {timeout} seconds")

    
    def stopElectionTimer(self):
        if self.election_timer.is_alive():
            self.election_timer.cancel()
            #print(f"Election timer canceled for node {self.id}")
        else:
            pass
            #print(f"No active election timer to cancel for node {self.id}")
        
        self.startElectionTimer()


    def stopLeaseTimer(self):
        if self.leaseTimer.is_alive():
            self.leaseTimer.cancel()
            #print(f"Lease timer canceled for node {self.id}")
        else:
            pass
            #print(f"No active lease timer to cancel for node {self.id}")
        self.oldLease=0
        self.startLeaseTimer()


    def LeaseTimeout(self):
        #print(f"Lease timeout for node {self.id}")
        if self.currentRole == "Leader": 
            self.currentRole = 'Follower'
            self.votedFor = None  
            self.dump.write(f"Leader {self.id} lease renewal failed. Stepping Down.\n")


    def electionTimeout(self):
        #print(f"Election timeout for node {self.id}")
        self.dump.write(f"Node {self.id} election timer timed out, Starting election.\n")
        #self.startElectionTimer()

        self.currentTerm += 1
        self.currentRole = "Candidate"
        self.votedFor = self.id
        self.votesReceived = [self.id]
        
        if len(self.log) > 0:
            s = self.log[-1]
            t = s.split()[-1]
            lastTerm = int(t)
        else:
            lastTerm = 0

        msg = ["VoteRequest", self.id, self.currentTerm, len(self.log), lastTerm]

        for node in nodes:
            if node != self.id:
                print()
                #print("follower node is ", node)
                '''request = voteRequest(      
                    address = nodes[node],
                    msg = msg
                )  '''
                
                try:
                    channel = grpc.insecure_channel(nodes[node])  
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    request = raft_pb2.VoteRequest(candidate_id = msg[1], term = msg[2], log_length = msg[3], last_log_term = msg[4])  # Example request
                    response = stub.receiveVoteRequest(request)   #rpc to receive vote request response

                    '''print("vote_granted: ", response.vote_granted)
                    print("term: ", response.term)
                    print("id: ", response.id)
                    print()'''

                    self.receiveVoteResponse(response)

                except grpc.RpcError as e:
                    print(f"Node {node} is down")
                    self.dump.write(f"Error occurred while sending RequestVote RPC to Node {node}. It has crashed.\n")
                    continue
                except Exception as e:
                    continue
            
        self.startElectionTimer()
        
        

    def receiveVoteRequest(self, request, context):
        #print("im voting for ", request.candidate_id)
        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.currentRole = "Follower"
            self.votedFor = None
            
        if len(self.log) > 0:
            s = self.log[-1]
            t = s.split()[-1]
            lastTerm = int(t)
        else:
            lastTerm = 0

        if (request.last_log_term > lastTerm) or (request.last_log_term == lastTerm and request.log_length >= len(self.log)):
            logOK = True
        else:
            logOK = False
        
        # Added to update old lease remaining of current node
        self.oldLease=max(0,int( self.start_time_lease+ self.oldLease- time.time()))
        if request.term == self.currentTerm and (self.votedFor == None or self.votedFor == request.candidate_id) and logOK:
            self.votedFor = request.candidate_id
            msg = ["VoteResponse", self.id, self.currentTerm, True, self.oldLease]
            self.dump.write(f"Vote granted for Node {request.candidate_id} in term {self.currentTerm}.\n")
            self.stopElectionTimer()
        else:
            msg =  ["VoteResponse", self.id, self.currentTerm, False, self.oldLease]
            self.dump.write(f"Vote denied for Node {request.candidate_id} in term {self.currentTerm}.\n")
        
        response = raft_pb2.VoteResponse(vote_granted = msg[3], term = msg[2], id = msg[1], currentLease = msg[4])  # Example response
        return response
        
    
    def receiveVoteResponse(self, response):
        if self.currentRole == 'Candidate' and response.term == self.currentTerm and response.vote_granted == True:
            if response.id not in self.votesReceived:
                self.votesReceived.append(response.id)
    # Added to update old lease remaining from all the nodes
                self.oldLease=max(self.oldLease,response.currentLease)

            if (len(self.votesReceived) > len(nodes)/2):
                print(f"Node {self.id} is the leader")
                self.dump.write(f"Node {self.id} became the leader for term {self.currentTerm}\n")
                self.currentRole = "Leader"
                self.currentLeader = self.id
                self.stopElectionTimer()

                self.dump.write( "New Leader waiting for Old Leader Lease to timeout.\n")
                time.sleep(self.oldLease) 
                self.startLeaseTimer()
                print(f"NO-OP {self.currentTerm}")
                #self.log.append(f"NO-OP {self.currentTerm}")     # Add a NO-OP entry to the log
                #self.commitLogEntries()
                for node in nodes:                               #NEED TO CHECK IF THIS IS FOR ALL NODES OR ONLY FOLLOWERS
                    if node != self.id:
                        self.sentLength[node] = len(self.log)
                        self.ackLength[node] = 0
    
            
        elif response.term > self.currentTerm:
            #print(f"Node {self.id} is reverting to follower")
            self.currentTerm = response.term
            self.currentRole = "Follower"
            self.votedFor = None
            self.stopElectionTimer()
        else:
            pass
            #print(f"DENIED")


    def broadcastMessages(self, msg):      #msg is of the form either SET k v or GET k
        if self.currentRole == "Leader":
            new_msg = msg + " " + str(self.currentTerm)
            self.log.append(new_msg)
            self.ackLength[self.id] = len(self.log)
            for node in nodes:
                if node != self.id:
                    self.replicateLog(node)
            
            return {"Data": msg, "LeaderID": self.id, "Success": True}          #return back to client

    
    def get_latest_value(self, key):
        curr_dir = os.getcwd() + "/logs_node_" + str(self.id)
        log_name = "logs.txt"
        log_path = os.path.join(curr_dir, log_name)

        with open(log_path, 'r') as f:
            for line in reversed(list(f)):  # Read file backwards for the latest value
                parts = line.strip().split()
                if parts[0] == 'SET' and parts[1] == key:
                    return parts[2]  # Found latest value
        return ""  # Key not found
    
    
    def serveClient(self, request, context):
        msg = request.Request
        #print(msg)
        if (self.currentRole == "Leader" and self.leaseTimer.is_alive()):
            self.dump.write(f"Node {self.id} (leader) received an {msg} request\n")
            arr=msg.split(" ")
            val=arr[1]
            response={"Data":val, "LeaderID": self.id, "Success": True}
            if arr[0]=="GET":
                key = arr[1]
                value = self.get_latest_value(key)
                response = {"Data": value, "LeaderID": self.id, "Success": True} 
            else:
                response = self.broadcastMessages(msg)         #return client request as success
        else:
            response = {"Data": msg, "LeaderID": self.currentLeader, "Success": False}      #client request failed; either update the client's leader id or there is no leader
        
        return raft_pb2.ServeClientReply(Data = response["Data"], LeaderID = response["LeaderID"], Success = response["Success"])
    

    def periodicHeartbeats(self):
        while True:
            if self.currentRole == "Leader" and self.leaseTimer.is_alive():
                #print("Heartbeat")
                for node in nodes:
                    if node != self.id:
                        #print("sending heartbeat to node: ", node)
                        self.replicateLog(node)
            time.sleep(self.replication_interval)  # Sleep for the specified interval


    
    def replicateLog(self, node):
        try:
            self.dump.write(f"Leader {self.id} sending heartbeat & Renewing Lease.\n")  
            prefixLen = self.sentLength[node]
            suffix = []
            for i in range(prefixLen, len(self.log)):
                suffix.append(self.log[i])
            
            prefixTerm = 0
            if prefixLen > 0:
                s = self.log[prefixLen - 1]
                t = s.split()[-1]
                prefixTerm = int(t)
    
            channel = grpc.insecure_channel(nodes[node])  
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            request = raft_pb2.LogRequest(leaderId = self.id, currentTerm = self.currentTerm, prefixLen = prefixLen, prefixTerm = prefixTerm, leaderCommit = self.commitLength, suffix = suffix, leaderLease = self.leaseDuration)  
            response = stub.receiveLogRequest(request)   #rpc to receive log request response
            #print("there")

            path = os.getcwd() + "/logs_node_" + str(self.id)
            location = os.path.join(path, "test.txt")
            f = open(location, "w")
            f.write("Node: " + str(node))
            f.write("Current term: " +  str(response.currentTerm))
            f.write("Ack: " + str(response.ack))
            f.write("Success: " + str(response.Success))

            self.receiveLogResponse(response)

        except grpc.RpcError as e:
            #print(f"Node {node} is down. Cannot replicate log.")
            self.dump.write(f"Error occurred while sending AppendEntries RPC to Node {node}. It has crashed.\n")
            return
        
        except Exception as e:
            return

    
    def receiveLogRequest(self, request, context):
        if request.currentTerm > self.currentTerm:
            self.currentTerm = request.currentTerm
            self.votedFor = None
        
        if request.currentTerm == self.currentTerm:
            if self.currentRole == "Leader":
                self.dump.write(f"{self.id} Stepping down.\n")

            self.currentRole = "Follower"
            self.currentLeader = request.leaderId
        
        if (len(self.log) >= request.prefixLen) and (request.prefixLen == 0 or int(self.log[request.prefixLen - 1].split()[-1]) == request.prefixTerm):
            logOK = True
        else:
            logOK = False


        '''print(self.log, request.prefixLen, request.prefixTerm, int(self.log[request.prefixLen - 1].split()[-1]))
        print(request.prefixLen)
        print(request.prefixTerm)
        print(int(self.log[request.prefixLen - 1].split()[-1]))
        print(request.currentTerm, self.currentTerm, logOK)'''
        
        if request.currentTerm == self.currentTerm and logOK:
            self.appendEntries(request.prefixLen, request.leaderCommit, request.suffix)
            ack = request.prefixLen + len(request.suffix)
            response = raft_pb2.LogResponse(nodeId = self.id, currentTerm = self.currentTerm, ack = ack, Success = True)
            self.dump.write(f"Node {self.id} accepted AppendEntries RPC from {request.leaderId}.\n")
            self.stopElectionTimer()

            self.stopLeaseTimer()
            self.oldLease=request.leaderLease
            self.start_time_lease=time.time()
        else:
            response = raft_pb2.LogResponse(nodeId = self.id, currentTerm = self.currentTerm, ack = 0, Success = False)
            self.dump.write(f"Node {self.id} rejected AppendEntries RPC from {request.leaderId}.\n")
        
        return response

        
    def receiveLogResponse(self, response):
        if response.currentTerm == self.currentTerm and self.currentRole == "Leader":
            if response.Success == True and response.ack >= self.ackLength[response.nodeId]:
                self.sentLength[response.nodeId] = response.ack
                self.ackLength[response.nodeId] = response.ack
                self.commitLogEntries()
                self.stopElectionTimer()
                self.stopLeaseTimer()
            elif self.sentLength[response.nodeId] > 0:
                self.sentLength[response.nodeId] -= 1
                self.replicateLog(response.nodeId)
        
        elif response.currentTerm > self.currentTerm:
            self.currentTerm = response.currentTerm
            self.currentRole = "Follower"
            self.votedFor = None
            self.stopElectionTimer()
            self.dump.write(f"{self.id} Stepping down.\n")
        

    def appendEntries(self, prefixLen, leaderCommit, suffix):
        if len(suffix) > 0 and len(self.log) > prefixLen:
            index = min(len(self.log), prefixLen + len(suffix)) - 1
            if int(self.log[index].split()[-1]) != int(suffix[index - prefixLen].split()[-1]):
                self.log = self.log[:prefixLen]

        if prefixLen + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefixLen, len(suffix)):
                self.log.append(suffix[i])
            
        if leaderCommit > self.commitLength:
            curr_dir = os.getcwd() + "/logs_node_" + str(self.id)
            log_name = "logs.txt"
            log_path = os.path.join(curr_dir, log_name)
            f = open(log_path, "a")
            for i in range(self.commitLength, leaderCommit):
                print("Committing FOLLOWER log entry: ", self.log[i])        #store in logs.txt
                f.write(self.log[i])
                f.write("\n")
                self.dump.write(f"Node {self.id} (follower) committed the entry {self.log[i]} to the state machine\n")      #MIGHT HAVE TO SLICE THE TERM NUMBER OUT
            f.close()

            self.commitLength = leaderCommit
        

    def commitLogEntries(self):
        while self.commitLength < len(self.log):
            acks = 0
            for node in nodes:
                if self.ackLength[node] > self.commitLength:
                    acks += 1
            
            curr_dir = os.getcwd() + "/logs_node_" + str(self.id)
            log_name = "logs.txt"
            log_path = os.path.join(curr_dir, log_name)
            f = open(log_path, "a")
            if acks > len(nodes)/2:
                print(" Committing LEADER log entry: ", self.log[self.commitLength])        #store in logs.txt
                f.write(self.log[self.commitLength])
                f.write("\n")
                f.close()
                self.dump.write(f"Node {self.id} (leader) committed the entry {self.log[self.commitLength]} to the state machine.\n")      #MIGHT HAVE TO SLICE THE TERM NUMBER OUT
                self.commitLength += 1
            else:
                break

        
    def storeMetadata(self):
        f = open(self.metadata, "w")
        f.write(f"currentTerm: {self.currentTerm}\n")
        f.write(f"votedFor: {self.votedFor}\n")
        f.write(f"commitLength: {self.commitLength}\n")
        f.write("log: ")
        f.write(",".join(self.log))
        f.write("\n")
        f.close()


    def signalHandler(self, sig, frame):
        print("Exiting...")
        self.storeMetadata()
        self.dump.close()
        sys.exit(0)

        



if __name__ == "__main__":
    node_id = int(input("Enter node ID: "))  # Example node id
    print()
    current_directory = os.getcwd()
    new_folder = "logs_node_" + str(node_id)
    final_directory = os.path.join(current_directory, new_folder)
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)

    node_address = nodes[node_id]  # Example node address
    client_address = "localhost:8080"  # Example client address

    node = Node(node_id, node_address, client_address)
    
    while True:
        pass



        

        






        
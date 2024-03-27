import random
import threading
from concurrent import futures
import time
import grpc
import os
import raft_pb2
import raft_pb2_grpc


nodes = {1: "localhost:50051", 2: "localhost:50052", 3: "localhost:50053"}     #global list of all nodes where keys are node id and values are node addresses

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
        self.sentLength = {}
        self.ackLength = {}
        self.electionTimer = None

        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, self.server)
        self.server.add_insecure_port(address)  # Assuming port 50051, adjust as needed
        self.server.start()

        self.replication_interval = 1  # Adjust the interval as needed (in seconds)
        self.replication_thread = threading.Thread(target=self.periodicHeartbeats)
        self.replication_thread.daemon = True  # Daemonize the thread so it exits when the main program exits
        self.replication_thread.start()


        

    def crashRecovery(self):
        self.currentRole = "Follower"
        self.currentLeader = None
        self.votesReceived = []
        self.sentLength = {}
        self.ackLength = {}


    def startElectionTimer(self):
        def timeForElectionTimeout():
            self.electionTimeout()
        
        timeout = random.uniform(5, 10)
        self.election_timer = threading.Timer(timeout, timeForElectionTimeout)
        self.election_timer.daemon = True  # Set as daemon thread
        self.election_timer.start()

        print(f"Election timer started for node {self.id}. Timeout: {timeout} seconds")

    
    def stopElectionTimer(self):
        if self.election_timer and self.election_timer.is_alive():
            self.election_timer.cancel()
            print(f"Election timer canceled for node {self.id}")
        else:
            print(f"No active election timer to cancel for node {self.id}")
            self.startElectionTimer()


    def electionTimeout(self):
        print(f"Election timeout for node {self.id}")
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
                print("follower node is ", node)
                '''request = voteRequest(      
                    address = nodes[node],
                    msg = msg
                )  '''
                
                try:
                    channel = grpc.insecure_channel(nodes[node])  
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    request = raft_pb2.VoteRequest(candidate_id = msg[1], term = msg[2], log_length = msg[3], last_log_term = msg[4])  # Example request
                    response = stub.receiveVoteRequest(request)   #rpc to receive vote request response

                    print("vote_granted: ", response.vote_granted)
                    print("term: ", response.term)
                    print("id: ", response.id)
                    print()

                    self.receiveVoteResponse(response)

                except Exception as e:
                    print(f"Node {node} is down")
                    continue
        
        self.startElectionTimer()
        

    def receiveVoteRequest(self, request, context):
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
        
        if request.term == self.currentTerm and (self.votedFor == None or self.votedFor == request.candidate_id) and logOK:
            self.votedFor = request.candidate_id
            msg = ["VoteResponse", self.id, self.currentTerm, True]
        else:
            msg =  ["VoteResponse", self.id, self.currentTerm, False]
        
        response = raft_pb2.VoteResponse(vote_granted = msg[3], term = msg[2], id = msg[1])  # Example response
        return response
        
    
    def receiveVoteResponse(self, response):
        if self.currentRole == 'Candidate' and response.term == self.currentTerm and response.vote_granted == True:
            if response.id not in self.votesReceived:
                self.votesReceived.append(response.id)

            if (len(self.votesReceived) > len(nodes)/2):
                print(f"Node {self.id} is the leader")
                self.currentRole = "Leader"
                self.currentLeader = self.id
                self.stopElectionTimer()
                for node in nodes:                               #NEED TO CHECK IF THIS IS FOR ALL NODES OR ONLY FOLLOWERS
                    if node != self.id:
                        self.sentLength[node] = len(self.log)
                        self.ackLength[node] = 0
                        self.replicateLog(node)
            
        elif response.term > self.currentTerm:
            print(f"Node {self.id} is reverting to follower")
            self.currentTerm = response.term
            self.currentRole = "Follower"
            self.votedFor = None
            self.stopElectionTimer()
        else:
            print(f"DENIED")


    def broadcastMessages(self, msg):      #msg is of the form either SET k v or GET k
        if self.currentRole == "Leader":
            new_msg = {"msg": msg, "term": self.currentTerm}
            self.log.append(new_msg)
            self.ackLength[self.id] = len(self.log)
            for node in nodes:
                if node != self.id:
                    self.replicateLog(node)
            
            return {"Data": msg, "LeaderID": self.id, "Success": True}          #return back to client

    
    def serveClient(self, request, context):
        msg = request.Request
        if (self.currentRole == "Leader"):
            response = self.broadcastMessages(msg)         #return client request as success
        else:
            response = {"Data": msg, "LeaderID": self.currentLeader, "Success": False}      #client request failed; either update the client's leader id or there is no leader
        
        return raft_pb2.ServeClientReply(data = response["Data"], LeaderID = response["LeaderID"], Success = response["Success"])
    

    def periodicHeartbeats(self):
        print("Heartbeat")
        while True:
            if self.currentRole == "Leader":
                for node in nodes:
                    if node != self.id:
                        self.replicateLog(node)
            time.sleep(self.replication_interval)  # Sleep for the specified interval

    
    def replicateLog(self, node):
        try:
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
            request = raft_pb2.LogRequest(leaderId = self.id, currentTerm = self.currentTerm, prefixLen = prefixLen, prefixTerm = prefixTerm, leaderCommit = self.commitLength, suffix = suffix)  
            response = stub.receiveLogRequest(request)   #rpc to receive log request response

            print()
            print("Node: ", node)
            print("Current term: ", response.currentTerm)
            print("Ack: ", response.ack)
            print("Success: ", response.Success)
            print()

            self.receiveLogResponse(response)

        except Exception as e:
            print(f"Node {node} is down. Cannot replicate log.")
            return

    
    def receiveLogRequest(self, request, context):
        if request.currentTerm > self.currentTerm:
            self.currentTerm = request.currentTerm
            self.votedFor = None
            self.stopElectionTimer()
        
        if request.currentTerm == self.currentTerm:
            self.currentRole = "Follower"
            self.currentLeader = request.leaderId
        
        if (len(self.log) >= request.prefixLen) and (request.prefixLen == 0 or int(self.log[request.prefixLen - 1].split()[-1]) == request.prefixTerm):
            logOK = True
        else:
            logOK = False
        
        if request.currentTerm == self.currentTerm and logOK:
            self.appendEntries(request.prefixLen, request.leaderCommit, request.suffix)
            ack = request.prefixLen + len(request.suffix)
            response = raft_pb2.LogResponse(nodeId = self.id, currentTerm = self.currentTerm, ack = ack, Success = True)
        else:
            response = raft_pb2.LogResponse(nodeId = self.id, currentTerm = self.currentTerm, ack = 0, Success = False)
        
        return response

        
    def receiveLogResponse(self, response):
        if response.currentTerm == self.currentTerm and self.currentRole == "Leader":
            if response.Success == True and response.ack >= self.ackLength[response.nodeId]:
                self.sentLength[response.nodeId] = response.ack
                self.ackLength[response.nodeId] = response.ack
                self.commitLogEntries()
            elif self.sentLength[response.nodeId] > 0:
                self.sentLength[response.nodeId] -= 1
                self.replicateLog(response.nodeId)
        
        elif response.currentTerm > self.currentTerm:
            self.currentTerm = response.currentTerm
            self.currentRole = "Follower"
            self.votedFor = None
            self.stopElectionTimer()
        

    def appendEntries(self, prefixLen, leaderCommit, suffix):
        if len(suffix) > 0 and len(self.log) > prefixLen:
            index = min(len(self.log), prefixLen + len(suffix)) - 1
            if int(self.log[index].split()[-1]) != int(suffix[index - prefixLen].split()[-1]):
                self.log = self.log[:prefixLen]

        if prefixLen + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefixLen, len(suffix) - 1):
                self.log.append(suffix[i])
            
        if leaderCommit > self.commitLength:
            curr_dir = os.getcwd() + "/logs_node_" + str(self.id)
            log_name = "logs.txt"
            log_path = os.path.join(curr_dir, log_name)
            f = open(log_path, "a")
            for i in range(self.commitLength, leaderCommit - 1):
                print("Committing FOLLOWER log entry: ", self.log[i])        #store in logs.txt
                f.write(self.log[i])
                f.write("\n")
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
                self.commitLength += 1
            else:
                break
        

                
                






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
    

    node.startElectionTimer()
    while True:
        pass



        

        






        
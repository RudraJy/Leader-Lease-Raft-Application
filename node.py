
nodes = {} #global list of all nodes where keys are node id and values are node addresses

class Node:
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

    def crashRecovery(self):
        self.currentRole = "Follower"
        self.currentLeader = None
        self.votesReceived = []
        self.sentLength = {}
        self.ackLength = {}


    
    def electionTimeout(self):
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
                request = voteRequest(       #rpc to send vote request
                    address = nodes[node],
                    msg = msg
                )  

                response = VoteRequestResponse(request)   #rpc to receive vote request response

                self.receiveVoteResponse(response)



        

    def receiveVoteRequest(self, msg):
        if msg[2] > self.currentTerm:
            self.currentTerm = msg[2]
            self.currentRole = "Follower"
            self.votedFor = None
            
        if len(self.log) > 0:
            s = self.log[-1]
            t = s.split()[-1]
            lastTerm = int(t)
        else:
            lastTerm = 0

        if (msg[4] > lastTerm) or (msg[4] == lastTerm and msg[3] >= len(self.log)):
            logOK = True
        else:
            logOk = False
        
        if msg[2] == self.currentTerm and (self.votedFor == None or self.votedFor == msg[1]) and logOK:
            self.votedFor = msg[1]
            return ["VoteResponse", self.id, self.currentTerm, True]
        else:
            return ["VoteResponse", self.id, self.currentTerm, False]
        
    
    def receiveVoteResponse(self, msg):
        if self.currentRole == 'Candidate' and msg[2] == self.currentTerm and msg[3] == True:
            if msg[1] not in self.votesReceived:
                self.votesReceived.append(msg[1])

            if (len(self.votesReceived) > len(nodes)/2):
                self.currentRole = "Leader"
                self.currentLeader = self.id
                for node in nodes:
                    if node != self.id:
                        self.sentLength[node] = len(self.log)
                        self.ackLength[node] = 0
                        self.replicateLog(node)
            
        elif msg[2] > self.currentTerm:
            self.currentTerm = msg[2]
            self.currentRole = "Follower"
            self.votedFor = None


    def broadcastMessages(self, msg):      #msg is of the form either SET k v or GET k
        if self.currentRole == "Leader":
            new_msg = msg + " " + str(self.currentTerm)
            self.ackLength[self.id] = len(self.log)
            for node in nodes:
                if node != self.id:
                    self.replicateLog(node)

        #incomplete
    

    def replicateLog(self, node):


        

        






        

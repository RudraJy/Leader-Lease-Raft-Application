import grpc  
import raft_pb2  
import raft_pb2_grpc
import random  

nodes = {1: "localhost:50051", 2: "localhost:50052", 3: "localhost:50053", 4: "localhost:50054", 5: "localhost:50055"}  

class RaftClient:
    def __init__(self):
        self.current_leader_id = None

    def send_request(self, request):
        while True:
            node = self._get_node() 
            print(node)
            try:
                channel = grpc.insecure_channel(nodes[node])
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                serve_client_args = raft_pb2.ServeClientArgs(Request=request) 
                response = stub.serveClient(serve_client_args)
    
                if response.Success == True:
                    data = f"SUCCESS: Request succeeded on node {node}, Response: {response.Data}"
                    return data
                    

                # Update leader information if redirection occurred
                elif response.LeaderID:
                    print(f"FAILURE MESSAGE: NOT THE LEADER/DOES NOT HAVE LEADER LEASE - Redirecting to node {response.LeaderID}")
                    for node in nodes: 
                        if node == response.LeaderID:
                            self.current_leader_id = node
                            break 
                
                # No leader available
                elif response.LeaderID == None:
                    data = "FAILURE MESSAGE: NO LEADER AVAILABLE"
                    self.current_leader_id = None
                    return data

            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    self.current_leader_id = None
                    continue  # Try the next node
                else:
                    print(f"SUCCESS: Request succeeded on node {node}, Response: {response.Data}")
                    break
            

    def _get_node(self):
        if self.current_leader_id:
            for node in nodes:
                if node == self.current_leader_id: 
                    return node
        else:
            index = random.randrange(len(nodes))
            return (index+1)
        
# Usage
if(__name__=="__main__"):
    print("------------------Welcome Client------------------")
    client = RaftClient()
    try:
        while(True):
            request = input("Enter request: ")
            response = client.send_request(request)
            print(response)
    except KeyboardInterrupt:
        print("Exiting...")
    except Exception as e:
        print(f"Exiting due to incorrect input or error...")
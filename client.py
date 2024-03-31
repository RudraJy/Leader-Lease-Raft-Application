import grpc  
import raft_pb2  
import raft_pb2_grpc
import random  

class Node:  # Simple data structure
    def __init__(self, id, address):
        self.id = id
        self.address = address

class RaftClient:
    def __init__(self):
        self.nodes = []  # Will be populated by user input
        self.current_leader_id = None

    def add_node(self, node_id, node_address):
        self.nodes.append(Node(node_id, node_address))
        self.channels = [grpc.insecure_channel(node.address) for node in self.nodes]
        self.stubs = [raft_pb2_grpc.RaftServiceStub(channel) for channel in self.channels] 
    def send_request(self, request):
        while True:
            node, stub = self._get_node_and_stub() 
            try:
                serve_client_args = raft_pb2.ServeClientArgs(Request=request) 
                response = stub.serveClient(serve_client_args)
    
                if response.Success == True:
                    print(f"Request succeeded on node {node.id}, Response: {response.Data}")
                    return response  # Request succeeded, we're done

                # Update leader information if redirection occurred
                if response.LeaderID:
                    print(f"Redirecting to node {response.LeaderID}")
                    for node in self.nodes: 
                        if node.id == response.LeaderID:
                            self.current_leader_id = node.id 
                            break 

            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print(f"Error contacting node: {e}")
                    self.current_leader_id = None
                    continue  # Try the next node
                else:
                    raise e

    def _get_node_and_stub(self):
        if self.current_leader_id:
            for node, stub in zip(self.nodes, self.stubs):
                if node.id == self.current_leader_id: 
                    return node, stub
        else:
            index = random.randrange(len(self.nodes))
            return self.nodes[index], self.stubs[index]
        
# Usage
if(__name__=="__main__"):
    print("------------------Welcome Client------------------")
    client = RaftClient()
    try:
        num_nodes = int(input("Enter number of nodes: "))
        for i in range(num_nodes):
            node_id = int(input(f"Enter node {i+1} ID: "))
            node_address = input(f"Enter node {i+1} address: ")
            client.add_node(node_id, node_address)
        while(True):
            request = input("Enter request: ")
            response = client.send_request(request)
            print(f"Response: {response.Response}")
    except KeyboardInterrupt:
        print("Exiting...")
    except Exception as e:
        print(f"Error: {e}, Exiting due to incorrect input or error...")
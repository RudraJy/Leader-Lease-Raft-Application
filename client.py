class RaftClient:
    def __init__(self, nodes):
        self.nodes = nodes

    def send_request(self, request):
        leader = self.get_current_leader()
        if leader:
            response = leader.handle_client_request(request)
            return response
        else:
            return "No leader available"

    def get_current_leader(self):
        for node in self.nodes:
            if node.leader_id:
                return node
        return None

import os

from src.server.server import RaftServer

if __name__ == "__main__":
    node_name = os.environ.get("NODE_NAME", "node_1")
    server = RaftServer(node_name)
    server.start()

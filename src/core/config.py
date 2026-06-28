import os

CLUSTER_SIZE = 4
MAJORITY = (CLUSTER_SIZE // 2) + 1

GRPC_PORT = 50051

PEER_ADDRESSES: dict[str, str] = {
    "node_1": "node_1:50051",
    "node_2": "node_2:50051",
    "node_3": "node_3:50051",
    "node_4": "node_4:50051",
}

DATA_DIR = os.getenv("RAFT_DATA_DIR", "data")

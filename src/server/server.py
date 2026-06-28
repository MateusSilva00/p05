import threading
import time
from concurrent import futures

import grpc

from src.core.config import GRPC_PORT
from src.core.logging import logger
from src.core.models import LogEntry, NodeRole
from src.generated import raft_pb2, raft_pb2_grpc
from src.server.election import ElectionManager
from src.server.raft_node import RaftNode
from src.server.replication import ReplicationManager


# ── gRPC Servicers ──────────────────────────────────────────────────


class RaftServiceServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node: RaftNode) -> None:
        self._node = node

    def RequestVote(self, request, context):  # noqa: N802
        with self._node.lock:
            term, granted = self._node.handle_vote_request(
                request.candidate_id,
                request.term,
                request.last_log_index,
                request.last_log_term,
            )
        return raft_pb2.VoteResponse(term=term, vote_granted=granted)

    def AppendEntries(self, request, context):  # noqa: N802
        entries = [
            LogEntry(term=e.term, index=e.index, command=e.command)
            for e in request.entries
        ]
        with self._node.lock:
            term, success, conflict_index = self._node.handle_append_entries(
                request.leader_id,
                request.term,
                request.prev_log_index,
                request.prev_log_term,
                entries,
                request.leader_commit,
            )
        return raft_pb2.AppendEntriesResponse(
            term=term, success=success, conflict_index=conflict_index
        )


class KVServiceServicer(raft_pb2_grpc.KVServiceServicer):
    def __init__(self, node: RaftNode, replication: ReplicationManager) -> None:
        self._node = node
        self._replication = replication

    def Publish(self, request, context):  # noqa: N802
        with self._node.lock:
            if self._node.role != NodeRole.LEADER:
                return raft_pb2.PublishResponse(
                    success=False,
                    leader_id=self._node.leader_id or "",
                    error="not_leader",
                )
            entry = self._node.append_command(request.data)

        self._replication.replicate()

        with self._node.lock:
            if self._node.volatile.commit_index >= entry.index:
                return raft_pb2.PublishResponse(success=True)
            return raft_pb2.PublishResponse(success=False, error="no_quorum")

    def Consume(self, request, context):  # noqa: N802
        with self._node.lock:
            entries = self._node.committed_entries()
            return raft_pb2.ConsumeResponse(
                success=True,
                data=[e.command for e in entries],
                leader_id=self._node.leader_id or "",
            )


# ── Server Orchestrator ─────────────────────────────────────────────


class RaftServer:
    _TICK_INTERVAL: float = 0.1
    _STARTUP_DELAY: float = 3.0

    def __init__(self, node_name: str) -> None:
        self._node = RaftNode(node_name)
        self._election = ElectionManager(self._node)
        self._replication = ReplicationManager(self._node)

    def start(self) -> None:
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

        raft_pb2_grpc.add_RaftServiceServicer_to_server(
            RaftServiceServicer(self._node), grpc_server
        )
        raft_pb2_grpc.add_KVServiceServicer_to_server(
            KVServiceServicer(self._node, self._replication), grpc_server
        )

        grpc_server.add_insecure_port(f"0.0.0.0:{GRPC_PORT}")
        grpc_server.start()

        logger.success(f"[{self._node.node_name}] gRPC server on port {GRPC_PORT}")

        tick = threading.Thread(target=self._tick_loop, daemon=True)
        tick.start()

        grpc_server.wait_for_termination()

    def _tick_loop(self) -> None:
        time.sleep(self._STARTUP_DELAY)
        logger.info(f"[{self._node.node_name}] tick loop started")

        while True:
            time.sleep(self._TICK_INTERVAL)
            self._check_election()
            self._check_heartbeat()

    def _check_election(self) -> None:
        with self._node.lock:
            role = self._node.role
            expired = self._node.is_election_expired

        if role in (NodeRole.FOLLOWER, NodeRole.CANDIDATE) and expired:
            self._election.run()

    def _check_heartbeat(self) -> None:
        with self._node.lock:
            role = self._node.role
            expired = self._node.is_heartbeat_expired

        if role == NodeRole.LEADER and expired:
            self._replication.replicate()

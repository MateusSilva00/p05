import threading

import grpc

from src.core.config import PEER_ADDRESSES
from src.core.logging import logger
from src.core.models import LogEntry, NodeRole
from src.generated import raft_pb2, raft_pb2_grpc
from src.server.raft_node import RaftNode


class ReplicationManager:
    def __init__(self, node: RaftNode) -> None:
        self._node = node

    def replicate(self) -> None:
        with self._node.lock:
            if self._node.role != NodeRole.LEADER or self._node.leader_state is None:
                return
            term = self._node.persistent.current_term
            commit_index = self._node.volatile.commit_index
            self._node.reset_heartbeat_timeout()

        peers = [p for p in PEER_ADDRESSES if p != self._node.node_name]
        threads = []
        for peer in peers:
            t = threading.Thread(
                target=self._send_to_peer, args=(peer, term, commit_index), daemon=True
            )
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=3)

        with self._node.lock:
            self._node.try_advance_commit()

    def _send_to_peer(
        self, peer_name: str, leader_term: int, commit_index: int
    ) -> None:
        with self._node.lock:
            if self._node.leader_state is None:
                return
            next_idx = self._node.leader_state.next_index[peer_name]
            prev_log_index = next_idx - 1
            prev_log_term = 0
            if 0 < prev_log_index <= len(self._node.persistent.log):
                prev_log_term = self._node.persistent.log[prev_log_index - 1].term
            entries = self._node.persistent.log[next_idx - 1 :]

        pb_entries = [
            raft_pb2.LogEntry(term=e.term, index=e.index, command=e.command)
            for e in entries
        ]

        request = raft_pb2.AppendEntriesRequest(
            term=leader_term,
            leader_id=self._node.node_name,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=pb_entries,
            leader_commit=commit_index,
        )

        try:
            addr = PEER_ADDRESSES[peer_name]
            with grpc.insecure_channel(addr) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                response = stub.AppendEntries(request, timeout=2)

            with self._node.lock:
                if response.term > self._node.persistent.current_term:
                    self._node.become_follower(response.term)
                    return
                if self._node.leader_state is None:
                    return

                if response.success:
                    if entries:
                        self._node.leader_state.next_index[peer_name] = (
                            entries[-1].index + 1
                        )
                        self._node.leader_state.match_index[peer_name] = entries[
                            -1
                        ].index
                else:
                    if response.conflict_index > 0:
                        self._node.leader_state.next_index[peer_name] = (
                            response.conflict_index
                        )
                    else:
                        self._node.leader_state.next_index[peer_name] = max(
                            1, next_idx - 1
                        )

        except grpc.RpcError:
            logger.warning(
                f"[{self._node.node_name}] AppendEntries failed → {peer_name}"
            )

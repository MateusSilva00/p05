import threading

import grpc

from src.core.config import CLUSTER_SIZE, MAJORITY, PEER_ADDRESSES
from src.core.logging import logger
from src.core.models import NodeRole
from src.generated import raft_pb2, raft_pb2_grpc
from src.server.raft_node import RaftNode


class ElectionManager:
    def __init__(self, node: RaftNode) -> None:
        self._node = node

    def run(self) -> bool:
        with self._node.lock:
            self._node.become_candidate()
            term = self._node.persistent.current_term
            last_log_index = self._node.persistent.last_log_index
            last_log_term = self._node.persistent.last_log_term

        votes = self._collect_votes(term, last_log_index, last_log_term)

        with self._node.lock:
            if (
                self._node.role != NodeRole.CANDIDATE
                or self._node.persistent.current_term != term
            ):
                return False

            if votes >= MAJORITY:
                self._node.become_leader()
                return True

            logger.warning(
                f"[{self._node.node_name}] election failed | "
                f"votes={votes}/{CLUSTER_SIZE} (need {MAJORITY})"
            )
            return False

    def _collect_votes(self, term: int, last_log_index: int, last_log_term: int) -> int:
        votes = 1
        votes_lock = threading.Lock()

        def request_from(peer_name: str) -> None:
            nonlocal votes
            try:
                addr = PEER_ADDRESSES[peer_name]
                with grpc.insecure_channel(addr) as channel:
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    response = stub.RequestVote(
                        raft_pb2.VoteRequest(  # ignore=E501
                            term=term,
                            candidate_id=self._node.node_name,
                            last_log_index=last_log_index,
                            last_log_term=last_log_term,
                        ),
                        timeout=3,
                    )

                with self._node.lock:
                    if response.term > self._node.persistent.current_term:
                        self._node.become_follower(response.term)
                        return

                if response.vote_granted:
                    with votes_lock:
                        votes += 1
                    logger.info(
                        f"[{self._node.node_name}] got vote from {peer_name} | "
                        f"total={votes}/{CLUSTER_SIZE}"
                    )

            except grpc.RpcError:
                logger.warning(
                    f"[{self._node.node_name}] couldn't reach {peer_name} for vote"
                )

        threads = []
        for peer_name in PEER_ADDRESSES:
            if peer_name == self._node.node_name:
                continue
            t = threading.Thread(target=request_from, args=(peer_name,), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=3)

        return votes

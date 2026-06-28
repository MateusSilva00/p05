import threading
from random import uniform
from time import monotonic

from src.core.config import MAJORITY, PEER_ADDRESSES
from src.core.logging import logger
from src.core.models import (
    ELECTION_TIMEOUT_RANGE,
    HEARTBEAT_INTERVAL,
    LeaderState,
    LogEntry,
    NodeRole,
    PersistentState,
    VolatileState,
)
from src.core.persistence import StatePersistence


class RaftNode:
    def __init__(self, node_name: str) -> None:
        self.node_name = node_name
        self.lock = threading.Lock()

        self._persistence = StatePersistence(node_name)
        self.persistent = self._persistence.load()
        self.volatile = VolatileState()
        self.leader_state: LeaderState | None = None

        self.role = NodeRole.FOLLOWER
        self.leader_id: str | None = None
        self.votes_received = 0

        self._election_deadline = self._new_election_deadline()
        self._heartbeat_deadline = 0.0

        if self.persistent.current_term > 0:
            logger.info(
                f"[{node_name}] recovered | term={self.persistent.current_term} "
                f"log_len={len(self.persistent.log)}"
            )

    # ── Timeouts ────────────────────────────────────────────────────────

    def _now_ms(self) -> float:
        return monotonic() * 1000

    def _new_election_deadline(self) -> float:
        return self._now_ms() + uniform(*ELECTION_TIMEOUT_RANGE)

    def reset_election_timeout(self) -> None:
        self._election_deadline = self._new_election_deadline()

    def reset_heartbeat_timeout(self) -> None:
        self._heartbeat_deadline = self._now_ms() + HEARTBEAT_INTERVAL

    @property
    def is_election_expired(self) -> bool:
        return self._now_ms() >= self._election_deadline

    @property
    def is_heartbeat_expired(self) -> bool:
        return self._now_ms() >= self._heartbeat_deadline

    # ── State transitions ───────────────────────────────────────────────

    def become_candidate(self) -> None:
        self.role = NodeRole.CANDIDATE
        self.persistent.current_term += 1
        self.persistent.voted_for = self.node_name
        self.votes_received = 1
        self.leader_id = None
        self.reset_election_timeout()
        self._save()
        logger.info(f"[{self.node_name}] → CANDIDATE | term={self.persistent.current_term}")

    def become_follower(self, new_term: int) -> None:
        self.role = NodeRole.FOLLOWER
        self.persistent.current_term = new_term
        self.persistent.voted_for = None
        self.votes_received = 0
        self.leader_state = None
        self.reset_election_timeout()
        self._save()
        logger.info(f"[{self.node_name}] → FOLLOWER | term={new_term}")

    def become_leader(self) -> None:
        self.role = NodeRole.LEADER
        self.leader_id = self.node_name
        self.votes_received = 0

        peer_ids = [p for p in PEER_ADDRESSES if p != self.node_name]
        self.leader_state = LeaderState()
        self.leader_state.initialize(peer_ids, self.persistent.last_log_index)

        self.reset_heartbeat_timeout()
        logger.success(f"[{self.node_name}] → LEADER | term={self.persistent.current_term}")

    # ── Vote logic (§5.2, §5.4.1) ──────────────────────────────────────

    def handle_vote_request(
        self,
        candidate_id: str,
        candidate_term: int,
        last_log_index: int,
        last_log_term: int,
    ) -> tuple[int, bool]:
        if candidate_term < self.persistent.current_term:
            return (self.persistent.current_term, False)

        if candidate_term > self.persistent.current_term:
            self.become_follower(candidate_term)

        if (
            self.persistent.voted_for is not None
            and self.persistent.voted_for != candidate_id
        ):
            return (self.persistent.current_term, False)

        # Log up-to-date check (§5.4.1)
        my_last_term = self.persistent.last_log_term
        my_last_index = self.persistent.last_log_index

        candidate_up_to_date = last_log_term > my_last_term or (
            last_log_term == my_last_term and last_log_index >= my_last_index
        )

        if not candidate_up_to_date:
            logger.warning(f"[{self.node_name}] rejected {candidate_id} | log outdated")
            return (self.persistent.current_term, False)

        self.persistent.voted_for = candidate_id
        self.reset_election_timeout()
        self._save()
        logger.info(f"[{self.node_name}] voted for {candidate_id} | term={self.persistent.current_term}")
        return (self.persistent.current_term, True)

    # ── AppendEntries logic (§5.3) ──────────────────────────────────────

    def handle_append_entries(
        self,
        leader_id: str,
        leader_term: int,
        prev_log_index: int,
        prev_log_term: int,
        entries: list[LogEntry],
        leader_commit: int,
    ) -> tuple[int, bool, int]:
        """Returns (term, success, conflict_index)."""
        if leader_term < self.persistent.current_term:
            return (self.persistent.current_term, False, 0)

        if self.role != NodeRole.FOLLOWER or self.persistent.current_term != leader_term:
            self.become_follower(leader_term)
        else:
            self.reset_election_timeout()

        self.leader_id = leader_id

        # Consistency check
        if prev_log_index > 0:
            if prev_log_index > len(self.persistent.log):
                return (self.persistent.current_term, False, len(self.persistent.log) + 1)

            existing = self.persistent.log[prev_log_index - 1]
            if existing.term != prev_log_term:
                conflict_term = existing.term
                conflict_idx = prev_log_index
                for i in range(prev_log_index - 1, 0, -1):
                    if self.persistent.log[i - 1].term != conflict_term:
                        break
                    conflict_idx = i
                self.persistent.log = self.persistent.log[: prev_log_index - 1]
                self._save()
                return (self.persistent.current_term, False, conflict_idx)

        # Append new entries
        if entries:
            insert_from = prev_log_index
            self.persistent.log = self.persistent.log[:insert_from] + entries
            self._save()
            logger.info(f"[{self.node_name}] replicated {len(entries)} entries from {leader_id}")

        # Update commit index
        if leader_commit > self.volatile.commit_index:
            old = self.volatile.commit_index
            self.volatile.commit_index = min(leader_commit, len(self.persistent.log))
            if self.volatile.commit_index > old:
                logger.success(f"[{self.node_name}] commit_index {old} → {self.volatile.commit_index}")

        return (self.persistent.current_term, True, 0)

    # ── Log operations ──────────────────────────────────────────────────

    def append_command(self, command: str) -> LogEntry:
        entry = LogEntry(
            term=self.persistent.current_term,
            index=self.persistent.last_log_index + 1,
            command=command,
        )
        self.persistent.log.append(entry)
        self._save()
        return entry

    def committed_entries(self) -> list[LogEntry]:
        return self.persistent.log[: self.volatile.commit_index]

    def try_advance_commit(self) -> None:
        if self.role != NodeRole.LEADER or self.leader_state is None:
            return

        for n in range(len(self.persistent.log), self.volatile.commit_index, -1):
            if self.persistent.log[n - 1].term != self.persistent.current_term:
                continue

            replication_count = 1
            for peer_id in self.leader_state.match_index:
                if self.leader_state.match_index[peer_id] >= n:
                    replication_count += 1

            if replication_count >= MAJORITY:
                old = self.volatile.commit_index
                self.volatile.commit_index = n
                logger.success(f"[{self.node_name}] commit_index {old} → {n}")
                break

    # ── Persistence ─────────────────────────────────────────────────────

    def _save(self) -> None:
        self._persistence.save(self.persistent)

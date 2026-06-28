from enum import StrEnum

from pydantic import BaseModel, Field

ELECTION_TIMEOUT_RANGE = (2000, 5000)
HEARTBEAT_INTERVAL = 500


class NodeRole(StrEnum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"


class LogEntry(BaseModel):
    term: int
    index: int
    command: str


class PersistentState(BaseModel):
    """State that MUST survive crashes — saved to disk on every mutation."""

    current_term: int = 0
    voted_for: str | None = None
    log: list[LogEntry] = Field(default_factory=list)

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0


class VolatileState(BaseModel):
    commit_index: int = 0
    last_applied: int = 0


class LeaderState(BaseModel):
    """Per-follower replication tracking — re-initialized after each election win."""

    next_index: dict[str, int] = Field(default_factory=dict)
    match_index: dict[str, int] = Field(default_factory=dict)

    def initialize(self, peer_ids: list[str], last_log_index: int) -> None:
        for peer_id in peer_ids:
            self.next_index[peer_id] = last_log_index + 1
            self.match_index[peer_id] = 0

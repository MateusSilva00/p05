import json
from pathlib import Path

from src.core.config import DATA_DIR
from src.core.models import PersistentState


class StatePersistence:
    def __init__(self, node_id: str) -> None:
        self._path = Path(DATA_DIR) / f"{node_id}.json"
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, state: PersistentState) -> None:
        self._path.write_text(state.model_dump_json(indent=2))

    def load(self) -> PersistentState:
        if not self._path.exists():
            return PersistentState()
        raw = json.loads(self._path.read_text())
        return PersistentState.model_validate(raw)

from typing import List, Set
import uuid
from typing_extensions import Literal

from .accern_xyme import PipelineHandle, NodeHandle
from .types import PipelineDef, NodeDef


ColType = Literal[  # pylint: disable=invalid-name
    "seq",
    "num",
    "cat",
    "date",
    "text",
    "target_clf",
    "target_reg",
]
VALID_COL_TYPES: Set[ColType] = {
    "seq",
    "num",
    "cat",
    "date",
    "text",
    "target_clf",
    "target_reg",
}
VALID_PRED_TYPES: Set[ColType] = {
    "num",
    "cat",
}


class GenericNode:
    def __init__(self, pipe: 'DataPipeline', node_id: str) -> None:
        if node_id[0] != "n":
            raise ValueError(f"{node_id} is not valid")
        self._node_id = node_id
        self._pipe = pipe

    def get_data_pipe(self) -> 'DataPipeline':
        return self._pipe

    def get_node_hnd(self) -> NodeHandle:
        data_pipe = self.get_data_pipe()
        data_pipe.update_def(force=False)
        pipe = data_pipe.get_pipeline()
        return pipe.get_node(self.get_id())

    def as_node_def(self) -> List[NodeDef]:
        raise NotImplementedError()

    def get_valid_inputs(self) -> List[str]:
        raise NotImplementedError()

    def get_valid_outputs(self) -> List[str]:
        raise NotImplementedError()

    def get_id(self) -> str:
        return self._node_id

    def __hash__(self) -> int:
        return hash(self._node_id)

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.get_id() < other.get_id()

    def __le__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.get_id() <= other.get_id()

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.get_id() > other.get_id()

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.get_id() >= other.get_id()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.get_id() == other.get_id()

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self._node_id

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}[{self._node_id}]"


# class CSVReader(GenericNode):

#     def as_node_def(self) -> List[NodeDef]:
#         return [{
#             "id": self.get_id(),
#             "kind": "csv_reader",
#             "params": {
#                 "columns": columns,
#                 "column_types": column_types,
#             },
#         }]

#     def get_valid_inputs(self) -> List[str]:
#         return []

#     def get_valid_outputs(self) -> List[str]:
#         return ["csv"]


class EmptyReader(GenericNode):
    def as_node_def(self) -> List[NodeDef]:
        return [{
            "id": self.get_id(),
            "kind": "empty",
        }]

    def get_valid_inputs(self) -> List[str]:
        return []

    def get_valid_outputs(self) -> List[str]:
        return ["data"]


class DataPipeline:
    def __init__(self, pipe: PipelineHandle) -> None:
        self._pipe = pipe
        self._dirty = True
        self._nodes: Set[GenericNode] = set()

    def create_node_id(self) -> str:
        pipe = self._pipe
        nid = uuid.uuid5(
            uuid.UUID(pipe.get_id()[1:]), f"node_{len(self._nodes)}")
        return f"n{nid.hex}"

    @staticmethod
    def derive_node_id(node_id: str, prefix: str, number: int) -> str:
        if node_id[0] != "n":
            raise ValueError(f"{node_id} is not valid")
        nid = uuid.uuid5(uuid.UUID(node_id[1:]), f"{prefix}_{number}")
        return f"n{nid.hex}"

    def add_node(self, node: GenericNode) -> None:
        if node in self._nodes:
            raise ValueError(f"{node} already in {self._nodes}")
        self._nodes.add(node)
        self._dirty = True

    def update_def(self, force: bool) -> None:
        if not force and not self._dirty:
            return
        pipe = self.get_pipeline()
        node_defs: List[NodeDef] = [
            node_def
            for node in sorted(self._nodes)
            for node_def in node.as_node_def()
        ]
        pipe_def: PipelineDef = {
            "name": pipe.get_name(),
            "company": pipe.get_company(),
            "nodes": node_defs,
            "state": pipe.get_state_type(),
            "high_priority": pipe.is_high_priority(),
        }
        pipe.set_pipeline(pipe_def)
        self._dirty = False

    def get_pipeline(self) -> PipelineHandle:
        return self._pipe

    # def read_csv(self) -> CSVReader:
    #     pass

    def read_empty(self) -> EmptyReader:
        pass

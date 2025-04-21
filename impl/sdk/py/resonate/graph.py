from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


class Graph[T]:
    def __init__(self, id: str, root: T) -> None:
        self.id = id
        self.root = Node(id, root)

    def find(self, func: Callable[[Node[T]], bool], edge: str = "default") -> Node[T] | None:
        return self.root.find(func, edge)

    def filter(self, func: Callable[[Node[T]], bool], edge: str = "default") -> Generator[Node[T], None, None]:
        return self.root.filter(func, edge)

    def traverse(self, edge: str = "default") -> Generator[Node[T], None, None]:
        return self.root.traverse(edge)

    def traverse_with_level(self, edge: str = "default") -> Generator[tuple[Node[T], int], None, None]:
        return self.root.traverse_with_level(edge)


class Node[T]:
    def __init__(self, id: str, value: T) -> None:
        self.id = id
        self._value = value
        self._edges: dict[str, list[Node[T]]] = {}

    def __repr__(self) -> str:
        edges = {e: [v.id for v in v] for e, v in self._edges.items()}
        return f"Node({self.id}, {self.value}, {edges})"

    @property
    def value(self) -> T:
        return self._value

    def transition(self, value: T) -> None:
        self._value = value

    def add_edge(self, node: Node[T], edge: str = "default") -> None:
        self._edges.setdefault(edge, []).append(node)

    def rmv_edge(self, node: Node[T], edge: str = "default") -> None:
        self._edges[edge].remove(node)

    def has_edge(self, node: Node[T], edge: str = "default") -> bool:
        return node in self._edges.get(edge, [])

    def get_edge(self, edge: str = "default") -> list[Node]:
        return self._edges.get(edge, [])

    def find(self, func: Callable[[Node[T]], bool], edge: str = "default") -> Node[T] | None:
        for node in self.traverse(edge):
            if func(node):
                return node

        return None

    def filter(self, func: Callable[[Node[T]], bool], edge: str = "default") -> Generator[Node[T], None, None]:
        for node in self.traverse(edge):
            if func(node):
                yield node

    def traverse(self, edge: str = "default") -> Generator[Node[T], None, None]:
        for node, _ in self._traverse(edge):
            yield node

    def traverse_with_level(self, edge: str = "default") -> Generator[tuple[Node[T], int], None, None]:
        return self._traverse(edge)

    def _traverse(self, edge: str, visited: set[str] | None = None, level: int = 0) -> Generator[tuple[Node[T], int], None, None]:
        if visited is None:
            visited = set()

        if self.id in visited:
            return

        visited.add(self.id)
        yield self, level

        for node in self._edges.get(edge, []):
            yield from node._traverse(edge, visited, level + 1)  # noqa: SLF001

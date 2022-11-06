from abc import ABC, abstractmethod


class DatabaseController(ABC):
    @abstractmethod
    def insert(self, *args):
        pass

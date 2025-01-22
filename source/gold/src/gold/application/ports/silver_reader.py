from abc import ABC, abstractmethod


class SilverReader(ABC):
    @abstractmethod
    def read(self):
        """Read records from the Silver Delta table."""
        pass

from abc import ABC, abstractmethod


class BasePipeline(ABC):
    """Abstract class for implementing pipelines"""

    def __init__(self):
        super(BasePipeline, self).__init__()
        self._pipeline = []

    def add(self, name, function):
        """Add a function to the pipeline"""
        self._pipeline.append((name, function))

    @abstractmethod
    def apply_pipe(self, input):
        """Apply functions in the pipeline to input"""

        raise NotImplementedError

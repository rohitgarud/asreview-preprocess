from abc import ABC, abstractmethod


class BasePipeline(ABC):
    """Abstract class for implementing pipelines"""

    def __init__(self):
        super(BasePipeline, self).__init__()
        self.pipeline = []

    def add(self, name, function):
        """Add a function to the pipeline"""
        self.pipeline.append((name, function))

    @abstractmethod
    def pipe(self, input):
        """Apply functions in the pipeline to input"""

        raise NotImplementedError

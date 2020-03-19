from abc import ABC, abstractmethod


class RecorderBase(ABC):
    """日志记录抽象"""

    @abstractmethod
    def record_simple_info(self, content):
        """简单信息日志"""
        NotImplementedError("Not Implemented")

    @abstractmethod
    def record_procedure(self, content):
        """过程日志"""
        NotImplementedError("Not Implemented")

    @abstractmethod
    def record_exception(self, content):
        """异常日志"""
        NotImplementedError("Not Implemented")


class SimpleRecorder(RecorderBase):
    """简单打印"""

    def record_simple_info(self, content):
        print(content, end='\n\n')

    def record_procedure(self, content):
        print(content, end='\n\n')

    def record_exception(self, content):
        print(content, end='\n\n')

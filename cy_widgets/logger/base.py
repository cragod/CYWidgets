from abc import ABC, abstractmethod


class RecorderBase(ABC):
    """日志记录抽象"""

    _summary_log = ""

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

    @abstractmethod
    def _record_summary_log(self):
        NotImplementedError("Not Implemented")

    def append_summary_log(self, content):
        # 打印并加入到最终日志
        self.record_simple_info(content)
        self._summary_log = self._summary_log + content + '\n\n'

    def record_summary_log(self, content=None):
        """记录整体日志，后清空"""
        if content is not None:
            self.append_summary_log(content)
        self._record_summary_log()
        self._summary_log = ""


class SimpleRecorder(RecorderBase):
    """简单打印"""

    def record_simple_info(self, content):
        print(content, end='\n\n')

    def record_procedure(self, content):
        print(content, end='\n\n')

    def record_exception(self, content):
        print(content, end='\n\n')

    def _record_summary_log(self):
        print(self._summary_log, end='\n\n')

class BaseLogger:
    """日志类抽象"""

    def log_simple_info(self, content):
        """简单信息日志"""
        print(content)

    def log_procedure(self, content):
        """过程日志"""
        print(content, end='\n\n')

    def log_exception(self, content):
        """异常日志"""
        print(content, end='\n\n')

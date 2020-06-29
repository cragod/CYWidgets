import traceback
from .base import RecorderBase


class TraderLogger:
    """交易类日志记录"""

    def __init__(self, exchange_name, coin_pair_name, trade_type_name, recorder: RecorderBase):
        """交易类 Logger 记录类

        Parameters
        ----------
        exchange_name : str
            交易所名
        coin_pair_name : str
            交易币种描述
        trade_type_name : str
            交易类型描述
        recorder : RecorderBase
            记录对象
        """
        super().__init__()
        self.exchange_name = exchange_name
        self.coin_pair_name = coin_pair_name
        self.trade_type_name = trade_type_name
        self.recorder = recorder

    def log_exception(self, phase_name):
        """交易异常记录

        Parameters
        ----------
        phase_name : str
            当前阶段描述
        """
        msg = """**[{}]** \n
**Exchange**: {} \n
**Coin Pair**: {} \n
**Exception**: {}""".format(phase_name, self.exchange_name, self.coin_pair_name, traceback.format_exc())
        self.recorder.record_exception(msg)

    def log_phase_info(self, phase_name, content):
        """交易过程记录

        Parameters
        ----------
        phase_name : str
            当前阶段描述
        content : str
            当前交易信息描述
        """
        msg = """**[{}]** \n
**Exchange**: {} \n
**Coin Pair**: {} \n
**Information**: {}""".format(phase_name, self.exchange_name, self.coin_pair_name, content)
        self.recorder.record_procedure(msg)

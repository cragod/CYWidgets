from ..strategy.exchange.base import BaseExchangeStrategy


class StrategyHelper:
    @staticmethod
    def formatted_identifier(strategy: BaseExchangeStrategy):
        """格式化策略标识，用于存储"""
        identifier = strategy.identifier
        return strategy.name.lower() + identifier.replace(': ', ':').replace(' ', '')

# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
import ccxt


class ExchangeBase(ABC):
    """交易所最基础类，仅抽象 CCXT 的初始流程"""

    def __init__(self, apiKey="", apiSecret="", params={}):
        """
        params:
            'proxies': CCXT Proxies
            'one_token_cfgs': [{
                'key': 'abc',
                'secret': '123',
            }, {
                'key': 'abc',
                'secret': '123',
            }, ...]
        """
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        # 1. setup
        ccxt_objects = self._setup_ccxt_objects(params)
        # 2. process ccxt setting
        self.__process_extra_params(ccxt_objects, params)

    @abstractmethod
    def _setup_ccxt_objects(self, params={}) -> []:
        """根据配置初始化 CCXT 对象，返回一组对象"""
        raise NotImplementedError

    # Default Implementation

    @property
    def display_name(self):
        """Exchange display name"""
        return getattr(self.ccxt_object_for_fetching, 'name')

    @staticmethod
    def __process_extra_params(ccxt_objects=[], params={}):
        """all ccxt objects make configure from here"""
        # 设置代理参数
        if 'proxies' in params:
            for objc in ccxt_objects:
                objc.proxies = params['proxies']

        # 加载基本数据
        for obj in ccxt_objects:
            obj.load_markets()
            obj.enableRateLimit = True

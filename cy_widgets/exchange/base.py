# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
import ccxt


class ExchangeCCXTFactory:
    """ CCXT 对象工厂类 """

    @staticmethod
    def _config_ccxt_object(ccxt_object, api_key, api_secret):
        ccxt_object.apiKey = api_key
        ccxt_object.secret = api_secret
        return ccxt_object

    @staticmethod
    def binance_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.binance()
        return ExchangeCCXTFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def huobipro_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.huobipro()
        return ExchangeCCXTFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def okex_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.okex3()
        return ExchangeCCXTFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def bitfinex_v1_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.bitfinex()
        return ExchangeCCXTFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def bitfinex_v2_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.bitfinex2()
        return ExchangeCCXTFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)


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

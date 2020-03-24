import ccxt
import enum


class ExchangeType(enum.Enum):
    """交易所类型"""
    Bitfinex = 0
    HuobiPro = 1
    Okex = 2
    Binance = 3


class CCXTObjectFactory:
    """ CCXT 对象工厂类 """

    @staticmethod
    def _config_ccxt_object(ccxt_object, api_key, api_secret):
        ccxt_object.apiKey = api_key
        ccxt_object.secret = api_secret
        return ccxt_object

    @staticmethod
    def binance_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.binance()
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def huobipro_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.huobipro()
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def okex_ccxt_object(apiKey, apiSecret, param):
        ccxt_object = ccxt.okex3()
        password = param['password']
        if password is None:
            raise ValueError(str)
        ccxt_object.password = password
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def bitfinex_v1_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.bitfinex()
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)

    @staticmethod
    def bitfinex_v2_ccxt_object(apiKey, apiSecret):
        ccxt_object = ccxt.bitfinex2()
        return CCXTObjectFactory._config_ccxt_object(ccxt_object, apiKey, apiSecret)


class CCXTProvider:
    """CCXT 提供类，负责为外提供对应的 CCXT 对象"""

    def __init__(self, api_key, secret, params, exg_type: ExchangeType):
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
            'password': '...."
        """
        self.__setup_ccxt_objects(api_key, secret, exg_type, params)
        self.__process_extra_params(params)

    @property
    def ccxt_object_for_fetching(self):
        """用于抓取数据"""
        return self.__object_4_fetching

    @property
    def ccxt_object_for_query(self):
        """用于查询"""
        return self.__object_4_query

    @property
    def ccxt_object_for_order(self):
        """用于下单"""
        return self.__object_4_order

    @property
    def display_name(self):
        """display name"""
        return getattr(self.ccxt_object_for_fetching, 'name')

    def __process_extra_params(self, params={}):
        """ccxt object 公共配置流程"""
        # 所有 obj 都设置
        ccxt_objects = [self.__object_4_fetching, self.__object_4_query, self.__object_4_order]
        # 代理参数
        for obj in ccxt_objects:
            if 'proxies' in params:
                obj.proxies = params['proxies']
            # 加载基本数据
            obj.load_markets()
            obj.enableRateLimit = True

    def __setup_ccxt_objects(self, api_key, secret, exg_type: ExchangeType, param={}):
        """初始化内部 Objects"""
        if exg_type == ExchangeType.Binance:
            self.__object_4_fetching = self.__object_4_query = self.__object_4_order = CCXTObjectFactory.binance_ccxt_object(
                api_key, secret)
        elif exg_type == ExchangeType.HuobiPro:
            self.__object_4_fetching = self.__object_4_query = self.__object_4_order = CCXTObjectFactory.huobipro_ccxt_object(
                api_key, secret)
        elif exg_type == ExchangeType.Okex:
            self.__object_4_fetching = self.__object_4_query = self.__object_4_order = CCXTObjectFactory.okex_ccxt_object(
                api_key, secret, param)
        elif exg_type == ExchangeType.Bitfinex:
            self.__object_4_fetching = self.__object_4_order = CCXTObjectFactory.bitfinex_v1_ccxt_object(
                api_key, secret)
            self.__object_4_query = CCXTObjectFactory.bitfinex_v2_ccxt_object(api_key, secret)

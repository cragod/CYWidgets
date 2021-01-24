from pyecharts.charts import Line
from pyecharts.globals import CurrentConfig, NotebookType
from pyecharts import options as opts
from pyecharts.charts import Kline, Bar, Grid, Line


def pyecharts_float_values_data(df, column_name, digits=2):
    """取某一列的值数组"""
    return df.copy()[column_name].apply(lambda x: str(round(x, digits))).tolist()  # 2位小数


def pyecharts_time_data(df):
    """取出所有的 CandleBeginTime"""
    return df.copy()['candle_begin_time'].apply(lambda x: str(x)).tolist()  # k线的x轴


def pyecharts_ohlc_data(df):
    """取出所有的 OHLC"""
    return df.copy().apply(lambda record: [record['open'], record['close'], record['low'], record['high']], axis=1).tolist()

# 基础 K 线


def ohlc_kline_chart(df, x_axis_count=2, signal_infos=[]):
    """创建一个 OHLC K线图，并返回
    :time_series 时间数组
    :ohlc_data   K线数据
    :x_axis_count 控制要缩放的 X 轴数量
    """
    time_series = pyecharts_time_data(df)
    ohlc_data = pyecharts_ohlc_data(df)
    kline = (
        Kline().add_xaxis(xaxis_data=time_series)
        .add_yaxis(
            series_name="K线",
            y_axis=ohlc_data,
            yaxis_index=2,
            itemstyle_opts=opts.ItemStyleOpts(color="#314555", color0="#ec0000", border_color="#314555", border_color0="#ec0000", ),
            markpoint_opts=opts.MarkPointOpts(
                data=[{'value': info[2], "coord": [info[0], info[1]],
                       "itemStyle": {"color": info[3]}} for info in signal_infos]
            ),
        )
        .set_global_opts(
            legend_opts=opts.LegendOpts(
                is_show=True, pos_top=10, pos_left="center",
            ),
            title_opts=opts.TitleOpts(title=""),
            datazoom_opts=[
                opts.DataZoomOpts(
                    is_show=True,
                    xaxis_index=list(range(0, x_axis_count)),
                    type_="slider",
                    pos_top="90%",
                    range_start=50,
                    range_end=100,
                ),
            ],
            xaxis_opts=opts.AxisOpts(
                is_scale=True,
                splitline_opts=opts.SplitLineOpts(
                    is_show=True,
                    linestyle_opts=opts.LineStyleOpts(type_='dotted')),
            ),
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True,
                    areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
                background_color="rgba(245, 245, 245, 0.8)",
                border_width=1,
                border_color="#ccc",
                textstyle_opts=opts.TextStyleOpts(color="#000"),
            ),
            visualmap_opts=opts.VisualMapOpts(
                is_show=False,
                dimension=2,
                series_index=5,
                is_piecewise=True,
                pieces=[
                    {"value": 1, "color": "#314555"},
                    {"value": -1, "color": "#ec0000"},
                ],
            ),
            axispointer_opts=opts.AxisPointerOpts(
                is_show=True,
                link=[{"xAxisIndex": "all"}],
                label=opts.LabelOpts(background_color="#777"),
            ),
            brush_opts=opts.BrushOpts(
                x_axis_index="all",
                brush_link="all",
                out_of_brush={"colorAlpha": 0.1},
                brush_type="lineX",
            ),
        ))
    return kline


def volume_bar_chart(df):
    """创建一个交易量柱状图并返回"""
    time_series = pyecharts_time_data(df)
    volume_series = pyecharts_float_values_data(df, 'volume')
    bar = (
        Bar().add_xaxis(xaxis_data=time_series)
        .add_yaxis(
            series_name="交易量",
            y_axis=volume_series,
            yaxis_index=0,
            label_opts=opts.LabelOpts(is_show=False),
        ).set_global_opts(
            xaxis_opts=opts.AxisOpts(
                is_scale=True,
                axislabel_opts=opts.LabelOpts(is_show=False),
                split_number=20,
            ),
            yaxis_opts=opts.AxisOpts(
                name="成交量",
                is_scale=True,
                split_number=2,
                axislabel_opts=opts.LabelOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        ))
    return bar


def equity_line_chart(df):
    """资金曲线图，需要有 equity_curve 字段"""
    time_series = pyecharts_time_data(df)
    equity_series = pyecharts_float_values_data(df, 'equity_curve')

    line_chart = Line().add_xaxis(
        xaxis_data=time_series
    ).add_yaxis(
        series_name='Equity Curve',
        y_axis=equity_series,
        yaxis_index=0,
        is_symbol_show=False,
        label_opts=None,
    ).set_global_opts(
        xaxis_opts=opts.AxisOpts(
            is_scale=True,
            axislabel_opts=opts.LabelOpts(is_show=False),
            split_number=20,
        ),
        yaxis_opts=opts.AxisOpts(
            name="资金曲线",
            is_scale=True,
            split_number=2,
            axislabel_opts=opts.LabelOpts(is_show=True),
            splitline_opts=opts.SplitLineOpts(is_show=False),
        ),
        legend_opts=opts.LegendOpts(is_show=False),
    )
    return line_chart


def base_kline_grid_chart(df, signal_infos=[]):
    """ 最最最基础的 K 线图，传入的 df 只需要有 open high low close volume 即可绘制 """
    # OHLC
    kline_chart = ohlc_kline_chart(df, signal_infos=signal_infos)
    # Volume
    volume_chart = volume_bar_chart(df)
    # 把 ohlc 和 volume 图组合起来
    grid_chart = Grid(init_opts=opts.InitOpts(
        width="1000px",
        height="800px",
        bg_color="#ffffff",
        animation_opts=opts.AnimationOpts(animation=False),
    ))
    grid_chart.add(
        kline_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="55%"),
    )
    grid_chart.add(
        volume_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", pos_top="70%", height="15%"),
    )
    return grid_chart


# ========= 画布林线相关


def bolling_signals_data(df):
    """取出开平仓点数据"""
    signal_df = df.copy()
    if 'signal' in signal_df.columns.tolist():
        signal_df = signal_df[signal_df.signal.notnull()]
        x = list(signal_df.candle_begin_time)
        y = list(signal_df.high)
        s = list(signal_df.signal)

        def map_signal_title(s):
            if s > 0:
                return "多"
            elif s < 0:
                return "空"
            else:
                return "平"

        def map_signal_color(s):
            if s > 0:
                return "#8B3522"
            elif s < 0:
                return "#6E615E"
            else:
                return "#F09883"
        s_titles = map(map_signal_title, s)
        s_colors = map(map_signal_color, s)
        return [(str(i[0]), i[1], i[2], i[3]) for i in zip(x, y, s_titles, s_colors)]
    return list()


def bolling_kline_chart(df, x_axis_count):
    """在 OHLC 的基础上添加三轨数据"""
    # 先画一个基础的 OHLC 图
    kline_chart = ohlc_kline_chart(df, x_axis_count)
    # 获取上轨、中轨、下轨数据
    uppers = pyecharts_float_values_data(df, 'upper')
    lowers = pyecharts_float_values_data(df, 'lower')
    medians = pyecharts_float_values_data(df, 'median')
    # 信号信息
    signal_infos = bolling_signals_data(df)
    # 三轨数据绘制成线图
    line_chart = Line().add_xaxis(
        xaxis_data=pyecharts_time_data(df)
    ).add_yaxis(
        series_name='Upper',
        y_axis=uppers,
        is_smooth=True,
        is_symbol_show=False,
        label_opts=None,
        is_hover_animation=False
    ).add_yaxis(
        series_name='Lower',
        y_axis=lowers,
        is_smooth=True,
        is_symbol_show=False,
        label_opts=None,
        is_hover_animation=False
    ).add_yaxis(
        series_name='Meian',
        y_axis=medians,
        is_smooth=True,
        is_symbol_show=False,
        label_opts=None,
        is_hover_animation=False,
        markpoint_opts=opts.MarkPointOpts(
            data=[{'value': info[2], "coord": [info[0], info[1]], "itemStyle": {"color": info[3]}} for info in signal_infos]
        )
    )
    # 把线图覆加到基础的 OHLC 图上
    kline_chart.overlap(line_chart)
    return kline_chart


def bolling_backtest_grid_chart(df):
    """完整布林线回测图，传入的 df 需要有 open high low close volume upper median lower signal equity_curve 即可绘制 """

    # 布林线的 OHLC 图
    kline_chart = bolling_kline_chart(df, 3)  # 这里要控制3个轴同时缩放
    # Volume
    volume_chart = volume_bar_chart(df)
    # 资金曲线
    equity_chart = equity_line_chart(df)
    # 组合起来
    grid_chart = Grid(init_opts=opts.InitOpts(
        width="1000px",
        height="800px",
        bg_color="#ffffff",
        animation_opts=opts.AnimationOpts(animation=False),
    ))
    grid_chart.add(
        kline_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="40%"),
    )
    grid_chart.add(
        volume_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", pos_top="55%", height="10%"),
    )
    grid_chart.add(
        equity_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", pos_top="75%", height="10%"),
    )
    return grid_chart


# ======= 动量因子自适应子母布林线

def god_j_mtm_adapt_bolling_chart(df):
    """子母布林自适应完整图"""

    # 母布林线上轨、下轨数据
    p_uppers = pyecharts_float_values_data(df, 'upper')
    p_lower = pyecharts_float_values_data(df, 'lower')

    # 子布林线上轨、中轨、下轨数据
    uppers = pyecharts_float_values_data(df, 'up', int(1e15))
    lowers = pyecharts_float_values_data(df, 'dn', int(1e15))
    medians = pyecharts_float_values_data(df, 'mtm_mean', int(1e15))
    # 信号信息
    signal_infos = bolling_signals_data(df)

    # k 线
    p_bolling_line = Line().add_xaxis(
        xaxis_data=pyecharts_time_data(df)
    ).add_yaxis(
        y_axis=p_uppers,
        series_name='pUpper',
        markpoint_opts=opts.MarkPointOpts(
            data=[{'value': info[2], "coord": [info[0], info[1]],
                   "itemStyle": {"color": info[3]}} for info in signal_infos]
        ),
        is_symbol_show=False,
        label_opts=None,
    ).add_yaxis(
        y_axis=p_lower,
        series_name='pLower',
        is_symbol_show=False,
        label_opts=None,
    )
    ohlc_chart = ohlc_kline_chart(
        df, x_axis_count=3
    )
    ohlc_chart.overlap(p_bolling_line)

    # 因子通道
    indicator_line_chart = Line().add_xaxis(
        xaxis_data=pyecharts_time_data(df)
    ).add_yaxis(
        series_name='Upper',
        y_axis=uppers,
        is_smooth=True,
        is_symbol_show=False,
        is_hover_animation=False,
        label_opts=opts.LabelOpts(is_show=False),
    ).add_yaxis(
        series_name='Lower',
        y_axis=lowers,
        is_smooth=True,
        is_symbol_show=False,
        is_hover_animation=False,
        label_opts=opts.LabelOpts(is_show=False),
    ).add_yaxis(
        series_name='Meian',
        y_axis=medians,
        is_smooth=True,
        is_symbol_show=False,
        is_hover_animation=False,
        label_opts=opts.LabelOpts(is_show=False),
    ).set_global_opts(
        legend_opts=opts.LegendOpts(is_show=False),
    )

    # 资金曲线
    equity_chart = equity_line_chart(df)

    grid_chart = Grid(init_opts=opts.InitOpts(
        width="1000px",
        height="800px",
        bg_color="#ffffff",
        animation_opts=opts.AnimationOpts(animation=False),
    ))
    grid_chart.add(
        ohlc_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%", height="30%"),
    )
    grid_chart.add(
        indicator_line_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="43%", height="30%"),
    )
    grid_chart.add(
        equity_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="80%", height="10%"),
    )
    return grid_chart

# ==================== Aberration + J.Flash


def ab_flash_bolling_chart(df):
    """ab+j.flash chart"""

    # 母布林线上轨、下轨数据
    uppers = pyecharts_float_values_data(df, 'upper')
    lowers = pyecharts_float_values_data(df, 'lower')
    medians = pyecharts_float_values_data(df, 'median')
    flash_stop_wins = pyecharts_float_values_data(df, 'flash_stop_win')
    # 信号信息
    signal_infos = bolling_signals_data(df)

    # k 线
    p_bolling_line = Line().add_xaxis(
        xaxis_data=pyecharts_time_data(df)
    ).add_yaxis(
        y_axis=uppers,
        series_name='Upper',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    ).add_yaxis(
        y_axis=lowers,
        series_name='Lower',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    ).add_yaxis(
        y_axis=medians,
        series_name='Median',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    ).add_yaxis(
        y_axis=flash_stop_wins,
        series_name='Flash Stop Wins',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    )

    ohlc_chart = ohlc_kline_chart(
        df, x_axis_count=2, signal_infos=signal_infos
    )
    ohlc_chart.overlap(p_bolling_line)

    # 资金曲线
    equity_chart = equity_line_chart(df)

    grid_chart = Grid(init_opts=opts.InitOpts(
        width="1000px",
        height="600px",
        bg_color="#ffffff",
        animation_opts=opts.AnimationOpts(animation=False),
    ))
    grid_chart.add(
        ohlc_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="5%", height="50%"),
    )
    grid_chart.add(
        equity_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="70%", height="18%"),
    )
    return grid_chart

# ==================== Vix Bolling


def vix_bolling_chart(df):
    """vix bolling"""

    # 母布林线上轨、下轨数据
    uppers = pyecharts_float_values_data(df, 'up', 6)
    lowers = pyecharts_float_values_data(df, 'down', 6)
    medians = pyecharts_float_values_data(df, 'mean', 6)
    vix = pyecharts_float_values_data(df, 'vix', 6)
    # 信号信息
    signal_infos = bolling_signals_data(df)

    # k 线
    p_bolling_line = Line().add_xaxis(
        xaxis_data=pyecharts_time_data(df)
    ).add_yaxis(
        y_axis=uppers,
        series_name='Upper',
        markpoint_opts=opts.MarkPointOpts(
            data=[{'value': info[2], "coord": [info[0], info[1]],
                   "itemStyle": {"color": info[3]}} for info in signal_infos]
        ),
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    ).add_yaxis(
        y_axis=lowers,
        series_name='Lower',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    ).add_yaxis(
        y_axis=medians,
        series_name='Median',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    ).add_yaxis(
        y_axis=vix,
        series_name='Vix',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    )

    ohlc_chart = ohlc_kline_chart(
        df, x_axis_count=3, signal_infos=signal_infos
    )
    # ohlc_chart.overlap(p_bolling_line)

    # 资金曲线
    equity_chart = equity_line_chart(df)

    grid_chart = Grid(init_opts=opts.InitOpts(
        width="1000px",
        height="600px",
        bg_color="#ffffff",
        animation_opts=opts.AnimationOpts(animation=False),
    ))
    grid_chart.add(
        ohlc_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="5%", height="30%"),
    )
    grid_chart.add(
        p_bolling_line,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="40%", height="20%"),
    )
    grid_chart.add(
        equity_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="70%", height="18%"),
    )
    return grid_chart

# ============== ICE MA SHRINKAGE


def ice_ga_shrinkage_chart(df):
    """ice ga shrinkage"""

    os_ma = pyecharts_float_values_data(df, 'os_ma', 6)
    # 信号信息
    signal_infos = bolling_signals_data(df)

    # k 线
    ohlc_chart = ohlc_kline_chart(
        df, x_axis_count=3, signal_infos=signal_infos
    )
    # os_ma
    os_ma_line = Line().add_xaxis(
        xaxis_data=pyecharts_time_data(df)
    ).add_yaxis(
        y_axis=os_ma,
        series_name='os_ma',
        is_symbol_show=False,
        label_opts=None,
        is_smooth=True,
    )
    # 资金曲线
    equity_chart = equity_line_chart(df)

    grid_chart = Grid(init_opts=opts.InitOpts(
        width="1000px",
        height="600px",
        bg_color="#ffffff",
        animation_opts=opts.AnimationOpts(animation=False),
    ))
    grid_chart.add(
        ohlc_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="5%", height="30%"),
    )
    grid_chart.add(
        os_ma_line,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="40%", height="20%"),
    )
    grid_chart.add(
        equity_chart,
        grid_opts=opts.GridOpts(pos_left="10%", pos_right="8%",
                                pos_top="70%", height="18%"),
    )
    return grid_chart

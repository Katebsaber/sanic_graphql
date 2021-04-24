import graphene
import pytz

from datetime import datetime
from graphene_sqlalchemy import SQLAlchemyObjectType

from models import create_candle_model
from helpers import get_fields


def candle_shema_factory(table_name):
    class Candle(SQLAlchemyObjectType):
        class Meta:
            model = create_candle_model(table_name=table_name)

    return Candle


class High(graphene.ObjectType):
    mts = graphene.String()
    price = graphene.String()


class Low(graphene.ObjectType):
    mts = graphene.String()
    price = graphene.String()


class Open(graphene.ObjectType):
    mts = graphene.String()
    price = graphene.String()


class Volume(graphene.ObjectType):
    mts = graphene.String()
    amount = graphene.String()


class Interval1m(graphene.ObjectType):
    high = graphene.List(High)
    low = graphene.List(Low)
    open = graphene.List(Open)
    volume = graphene.List(Volume)


class Interval5m(graphene.ObjectType):
    high = graphene.List(High)
    low = graphene.List(Low)
    open = graphene.List(Open)
    volume = graphene.List(Volume)


class BTCUSDT(graphene.ObjectType):
    interval_1m = graphene.Field(Interval1m)
    interval_5m = graphene.Field(Interval5m)


class Candle(graphene.ObjectType):
    btcusdt = graphene.Field(BTCUSDT)


def get_interval_customized_obj(query_param, interval_obj, query) -> object:
    for key in query_param:
        if key == 'high':
            high_obj = [High(query_obj.open_time, query_obj.high_price) for query_obj in query]
            interval_obj.high = high_obj
        elif key == 'low':
            low_obj = [Low(query_obj.open_time, query_obj.low_price) for query_obj in query]
            interval_obj.low = low_obj
        elif key == 'open':
            open_obj = [Open(query_obj.open_time, query_obj.open_price) for query_obj in query]
            interval_obj.open = open_obj
        elif key == 'volume':
            volume_obj = [Volume(query_obj.open_time, query_obj.volume) for query_obj in query]
            interval_obj.volume = volume_obj
    return interval_obj


def get_candle_customized_obj(symbol: str, interval: str, fields: dict, candle_arg_obj, query) -> object:
    if symbol == 'btcusdt' and symbol in fields.keys():
        assert f'interval{interval}' not in fields[symbol].keys(), f"Must pass query for interval{interval}"
        query_param = fields[symbol][f'interval{interval}'].keys()
        btcusdt_obj = BTCUSDT()
        if candle_arg_obj.btcusdt:
            btcusdt_obj = candle_arg_obj.btcusdt
        if interval == '1m':
            _1m_obj = Interval1m()
            interval_obj = get_interval_customized_obj(query_param, _1m_obj, query)
            btcusdt_obj.interval_1m = interval_obj

        if interval == '5m':
            _5m_obj = Interval5m()
            interval_obj = get_interval_customized_obj(query_param, _5m_obj, query)
            btcusdt_obj.interval_5m = interval_obj
        candle_arg_obj.btcusdt = btcusdt_obj
    return candle_arg_obj


def convert_date_to_mts(date: str):
    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S %Z')
    utc_date = date.astimezone(pytz.utc)
    return utc_date.timestamp() * 1000


class AllBinance1mQuery(graphene.ObjectType):
    candle = graphene.Field(Candle, symbol=graphene.List(graphene.String), interval=graphene.List(graphene.String),
                            limit=graphene.Int(), start_date=graphene.String(), end_date=graphene.String())

    def resolve_candle(self, info, **kwargs):
        fields = get_fields(info)
        symbols = kwargs.get('symbol')
        intervals = kwargs.get('interval')
        limit = kwargs.get('limit')
        if kwargs.get('start_date'):
            start_date = int(convert_date_to_mts(kwargs.get('start_date')))
        if kwargs.get('end_date'):
            end_date = int(convert_date_to_mts(kwargs.get('end_date')))
        candle_arg_obj = Candle()
        for symbol in symbols:
            for interval in intervals:
                symbol = symbol.lower()
                interval = interval.lower()
                query = candle_shema_factory(f"binance_{symbol}_{interval}").get_query(
                    info=info).filter().limit(limit)
                print(f'start job {interval}')
                candle_arg_obj = get_candle_customized_obj(symbol, interval, fields, candle_arg_obj, query)
                print(f'finish job {interval}')
        return candle_arg_obj


binance_1m_schema = graphene.Schema(query=AllBinance1mQuery)

"""
bagel-tushare style database connection
"""

from pandas import DataFrame, read_sql
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import create_engine, text

from sqlalchemy import Engine
from typing import Any, Iterable


@dataclass(slots=True)
class Database:

    host: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str

    engine: Engine = field(init=False, repr=False)

    def __post_init__(self):
        self.engine = create_engine(
                f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
                )

    def query(self, sql: str) -> Any:
        with self.engine.begin() as connection:
            return connection.execute(text(sql)).fetchall()

    def query_price_df(self,
                       table: str,
                       col_name: str,
                       symbol: str,
                       start_date: datetime | None = None,
                       end_date: datetime | None = None) -> DataFrame:
        """
        Select price data from {table} between {start_date} and {end_date}
        :param table: str, ex: daily, us_daily, us_daily_adj
        :param col_name: str, ex: open, high, low, close
        :param symbol: str, ex: 000001.SZ
        :param start_date: datetime | None = None
        :param end_date: datetime | None = None
        """
        if start_date is None:
            start_date = datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.today()

        sql = f"""
        SELECT DISTINCT trade_date, {col_name}
        FROM {table}
        WHERE ts_code = '{symbol}' 
        AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
        """
        return read_sql(sql,
                        self.engine,
                        index_col='trade_date',
                        parse_dates=['trade_date']
                        ).pipe(lambda df: df.rename(columns={col_name: symbol}))

    def query_price_df_multi_symbols(self,
                                     table: str,
                                     col_name: str,
                                     symbols: Iterable[str],
                                     start_date: datetime | None = None,
                                     end_date: datetime | None = None) -> DataFrame:
        """
        Select price data from {table} between {start_date} and {end_date}
        :param table: str, ex: daily, us_daily, us_daily_adj
        :param col_name: str, ex: open, high, low, close
        :param symbols: Iterable[str], a list of symbols
        :param start_date: datetime | None = None
        :param end_date: datetime | None = None
        """
        if start_date is None:
            start_date = datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.today()

        sql = f"""
        SELECT DISTINCT trade_date, {col_name}, ts_code
        FROM {table}
        WHERE ts_code in {tuple(symbols)}
        AND trade_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY trade_date
        """
        return read_sql(sql,
                        self.engine,
                        index_col='trade_date',
                        parse_dates=['trade_date']
                        ).pipe(lambda df: df.pivot(columns='ts_code', 
                                                   values=col_name))
    
    def query_cn_fundamental(self,
                             symbol: str,
                             table: str,
                             col_name: str,
                             start_date: datetime | None = None,
                             end_date: datetime | None = None) -> DataFrame:
        """
        Select fundamental data between {start_date} and {end_date}
        :param symbol: str, ex: 000001.SZ
        :param table: str, ex: cn_fundamental
        :param col_name: str, ex: revenue, profit, assets
        :param start_date: datetime | None = None
        :param end_date: datetime | None = None
        :return: DataFrame
        """
        if start_date is None:
            start_date = datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.today()
        sql = f"""
        SELECT DISTINCT f_ann_date, {col_name}
        FROM {table}
        WHERE ts_code = '{symbol}'
        AND ann_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY f_ann_date
        """
        return read_sql(sql,
                        self.engine,
                        index_col='f_ann_date',
                        parse_dates=['f_ann_date']
                        ).pipe(lambda df: df.rename(columns={col_name: symbol}))
    
    def query_cn_fundamental_multi_symbols(self,
                                           symbols: Iterable[str],
                                           table: str,
                                           col_name: str,
                                           start_date: datetime | None = None,
                                           end_date: datetime | None = None) -> DataFrame:
        """
        Select fundamental data between {start_date} and {end_date}
        :param symbols: Iterable[str], a list of symbols
        :param table: str, ex: cn_fundamental
        :param col_name: str, ex: revenue, profit, assets
        :param start_date: datetime | None = None
        :param end_date: datetime | None = None
        :return: DataFrame
        """
        if start_date is None:
            start_date = datetime(1970, 1, 1)
        if end_date is None:
            end_date = datetime.today()
        sql = f"""
        SELECT DISTINCT f_ann_date, {col_name}, ts_code
        FROM {table}
        WHERE ts_code in {tuple(symbols)}
        AND f_ann_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY f_ann_date
        """
        return read_sql(sql,
                        self.engine,
                        index_col='f_ann_date',
                        parse_dates=['f_ann_date']
                        ).pipe(lambda df: df.pivot(columns='ts_code',
                                                   values=col_name))

    """===close price and adj_close==="""
    def cn_close(self, 
                 symbol: str, 
                 start_date: datetime, 
                 end_date: datetime) -> DataFrame:
        return self.query_price_df(table='daily', 
                                   col_name='close', 
                                   symbol=symbol, 
                                   start_date=start_date, 
                                   end_date=end_date)

    def us_close(self,
                 symbol: str,
                 start_date: datetime,
                 end_date: datetime) -> DataFrame:
        return self.query_price_df(table='us_daily',
                                   col_name='close',
                                   symbol=symbol,
                                   start_date=start_date,
                                   end_date=end_date)

    def us_adj_close(self,
                     symbol: str,
                     start_date: datetime,
                     end_date: datetime) -> DataFrame:
        return self.query_price_df(table='us_daily_adj',
                                   col_name='close',
                                   symbol=symbol,
                                   start_date=start_date,
                                   end_date=end_date)

    """===multi symbols close price and adj_close==="""
    def cn_close_symbols(self,
                         symbols: Iterable[str],
                         start_date: datetime,
                         end_date: datetime) -> DataFrame:
        return self.query_price_df_multi_symbols(table='daily',
                                                 col_name='close',
                                                 symbols=symbols,
                                                 start_date=start_date,
                                                 end_date=end_date)

    def us_close_symbols(self,
                         symbols: Iterable[str],
                         start_date: datetime,
                         end_date: datetime) -> DataFrame:
        return self.query_price_df_multi_symbols(table='us_daily',
                                                 col_name='close',
                                                 symbols=symbols,
                                                 start_date=start_date,
                                                 end_date=end_date)

    def us_adj_close_symbols(self,
                             symbols: Iterable[str],
                             start_date: datetime,
                             end_date: datetime) -> DataFrame:
        return self.query_price_df_multi_symbols(table='us_daily_adj',
                                                 col_name='close',
                                                 symbols=symbols,
                                                 start_date=start_date,
                                                 end_date=end_date)



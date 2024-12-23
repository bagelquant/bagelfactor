"""
Tests for database module
"""

import unittest
import json
from pathlib import Path

from src.bagelfactor.database import *

from unittest import TestCase
from datetime import datetime


class TestDatabase(TestCase):

    def setUp(self):
        with Path('tests/test_database_config.json').open() as f:
            config = json.load(f)
        self.db = Database(**config)
        self.cn_symbol: str = '000001.SZ'
        self.cn_symbol_2: str = '000002.SZ'
        self.us_symbols: list[str] = ['AAPL', 'MSFT', 'TSLA']
        self.start_date: datetime = datetime(2020, 1, 1)
        self.end_date: datetime = datetime(2021, 1, 1)

    def test_db_info(self):
        print('\nTestDatabase.test_db_info')
        print(self.db)

    def test_query(self):
        print('\nTestDatabase.test_query')
        result = [_[0] for _ in self.db.query('SHOW TABLES')]
        print(result)

    def test_cn_close(self):
        print('\nTestDatabase.test_cn_close')
        df = self.db.cn_close(symbol=self.cn_symbol, 
                              start_date=self.start_date, 
                              end_date=self.end_date)
        print(df)

    def test_us_close(self):
        print('\nTestDatabase.test_us_close')
        df = self.db.us_close(symbol=self.us_symbols[0],
                              start_date=self.start_date, 
                              end_date=self.end_date)
        print(df)

    def test_us_adj_close(self):
        print('\nTestDatabase.test_us_adj_close')
        df = self.db.us_adj_close(symbol=self.us_symbols[0],
                                  start_date=self.start_date, 
                                  end_date=self.end_date)
        print(df)

    def test_cn_close_symbols(self):
        print('\nTestDatabase.test_cn_close_symbols')
        df = self.db.cn_close_symbols(symbols=[self.cn_symbol, self.cn_symbol_2],
                                      start_date=self.start_date, 
                                      end_date=self.end_date)
        print(df)

    def test_us_close_symbols(self):
        print('\nTestDatabase.test_us_close_symbols')
        df = self.db.us_close_symbols(symbols=self.us_symbols,
                                      start_date=self.start_date, 
                                      end_date=self.end_date)
        print(df)

    def test_us_adj_close_symbols(self):
        print('\nTestDatabase.test_us_adj_close_symbols')
        df = self.db.us_adj_close_symbols(symbols=self.us_symbols,
                                          start_date=self.start_date, 
                                          end_date=self.end_date)
        print(df)

    def test_query_cn_fundamental(self):
        print('\nTestDatabase.test_cn_fundamental')
        df = self.db.query_cn_fundamental(symbol=self.cn_symbol,
                                          col_name='net_profit',
                                          table='cashflow',
                                          start_date=self.start_date,
                                          end_date=self.end_date)
        print(df)

    def test_query_cn_fundamental_multi_symbols(self):
        print('\nTestDatabase.test_us_fundamental')
        df = self.db.query_cn_fundamental_multi_symbols(symbols=[self.cn_symbol, self.cn_symbol_2],
                                                        col_name='net_profit',
                                                        table='cashflow',
                                                        start_date=self.start_date,
                                                        end_date=self.end_date)
        print(df)

if __name__ == '__main__':
    unittest.main()


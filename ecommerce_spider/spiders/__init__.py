"""电商爬虫模块初始化"""
from .taobao_spider import TaoBaoSpider
from .jd_spider import JDSpider
from .pdd_spider import PDDSpider

__all__ = ['TaoBaoSpider', 'JDSpider', 'PDDSpider']
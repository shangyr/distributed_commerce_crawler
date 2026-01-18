"""电商爬虫项目主包"""
import logging
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

def run_spiders(platforms=None):
    """运行指定平台的爬虫"""
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    
    # 默认可运行的爬虫
    available_spiders = {
        'taobao': 'ecommerce_spider.spiders.taobao_spider.TaoBaoSpider',
        'jd': 'ecommerce_spider.spiders.jd_spider.JDSpider',
        'pdd': 'ecommerce_spider.spiders.pdd_spider.PDDSpider'
    }
    
    # 确定要运行的爬虫
    target_spiders = platforms or available_spiders.keys()
    for spider in target_spiders:
        if spider in available_spiders:
            process.crawl(available_spiders[spider])
    
    process.start()

if __name__ == '__main__':
    run_spiders()
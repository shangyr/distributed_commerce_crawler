"""定义爬虫数据结构"""
import scrapy
from  itemloaders.processors import MapCompose, TakeFirst, Join
import re

def clean_price(value):
    """清理价格字符串"""
    if not value:
        return ''
    # 移除货币符号和逗号
    return re.sub(r'[^\d.]', '', str(value))

def extract_numeric(value):
    """提取数值"""
    if not value:
        return 0
    try:
        return float(value)
    except ValueError:
        return 0

def clean_number(value):
    """清理数字字符串（处理带单位的情况）"""
    if not value:
        return ''
    # 处理"1.2万"之类的格式
    value = str(value).strip()
    if '万' in value:
        return str(float(value.replace('万', '')) * 10000)
    return re.sub(r'[^\d]', '', value)

class ProductItem(scrapy.Item):
    """商品基本信息"""
    platform = scrapy.Field()  # 平台
    product_id = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    name = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    price = scrapy.Field(
        input_processor=MapCompose(clean_price, extract_numeric),
        output_processor=TakeFirst()
    )
    original_price = scrapy.Field(
        input_processor=MapCompose(clean_price, extract_numeric),
        output_processor=TakeFirst()
    )
    sales = scrapy.Field(
        input_processor=MapCompose(clean_number, extract_numeric),
        output_processor=TakeFirst()
    )
    comments_count = scrapy.Field(
        input_processor=MapCompose(clean_number, extract_numeric),
        output_processor=TakeFirst()
    )
    shop_name = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    category = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=Join(' > ')
    )
    url = scrapy.Field(
        output_processor=TakeFirst()
    )
    crawl_time = scrapy.Field(
        output_processor=TakeFirst()
    )


class CommentItem(scrapy.Item):
    """商品评论信息"""
    product_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    comment_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    user_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    user_name = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    content = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    rating = scrapy.Field(
        output_processor=TakeFirst()
    )
    comment_time = scrapy.Field(
        output_processor=TakeFirst()
    )
    useful_votes = scrapy.Field(
        input_processor=MapCompose(clean_number, extract_numeric),
        output_processor=TakeFirst()
    )
    reply_count = scrapy.Field(
        input_processor=MapCompose(clean_number, extract_numeric),
        output_processor=TakeFirst()
    )
    crawl_time = scrapy.Field(
        output_processor=TakeFirst()
    )


class ShopItem(scrapy.Item):
    """店铺信息"""
    shop_id = scrapy.Field(
        output_processor=TakeFirst()
    )
    shop_name = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    shop_type = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    score_service = scrapy.Field(
        input_processor=MapCompose(clean_price, extract_numeric),
        output_processor=TakeFirst()
    )
    score_delivery = scrapy.Field(
        input_processor=MapCompose(clean_price, extract_numeric),
        output_processor=TakeFirst()
    )
    score_description = scrapy.Field(
        input_processor=MapCompose(clean_price, extract_numeric),
        output_processor=TakeFirst()
    )
    location = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    registered_time = scrapy.Field(
        input_processor=MapCompose(str.strip),
        output_processor=TakeFirst()
    )
    crawl_time = scrapy.Field(
        output_processor=TakeFirst()
    )
import os
import csv
import json
import logging
from datetime import datetime
from itemadapter import ItemAdapter
from utils.db_helper import DBHelper

logger = logging.getLogger(__name__)

class CSVPipeline:
    """CSV文件存储管道"""
    def __init__(self):
        self.files = {}  # 存储不同类型文件句柄
        self.writers = {}  # 存储不同类型CSV写入器
        self.data_dir = None
        self.date_suffix = datetime.now().strftime('%Y%m%d')  # 按日期分文件

    def open_spider(self, spider):
        """初始化文件和写入器"""
        self.data_dir = spider.settings.get('STORAGE', {}).get('data_dir', './data')
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 定义各数据类型的CSV表头
        self.schemas = {
            'ProductItem': [
                'platform', 'product_id', 'name', 'price', 'original_price', 
                'sales', 'comments_count', 'shop_name', 'category', 'url', 'crawl_time'
            ],
            'CommentItem': [
                'product_id', 'comment_id', 'user_id', 'user_name', 'content', 
                'rating', 'comment_time', 'useful_votes', 'reply_count', 'crawl_time'
            ],
            'ShopItem': [
                'shop_id', 'shop_name', 'shop_type', 'score_service', 
                'score_delivery', 'score_description', 'location', 'registered_time', 'crawl_time'
            ]
        }
        
        # 创建文件和写入器
        for item_type, fields in self.schemas.items():
            filename = f"{item_type.lower().replace('item', '')}s_{self.date_suffix}.csv"
            file_path = os.path.join(self.data_dir, filename)
            file = open(file_path, 'w', newline='', encoding='utf-8-sig')
            writer = csv.writer(file)
            writer.writerow(fields)
            
            self.files[item_type] = file
            self.writers[item_type] = writer
            logger.info(f"CSV文件初始化: {file_path}")

    def close_spider(self, spider):
        """关闭所有文件句柄"""
        for file in self.files.values():
            file.close()
        logger.info("所有CSV文件已关闭")

    def process_item(self, item, spider):
        """根据item类型写入对应CSV文件"""
        item_type = item.__class__.__name__
        if item_type in self.writers:
            adapter = ItemAdapter(item)
            # 按表头顺序提取数据
            row = [adapter.get(field, '') for field in self.schemas[item_type]]
            self.writers[item_type].writerow(row)
        return item


class SQLitePipeline:
    """SQLite数据库存储管道"""
    def __init__(self):
        self.db_helper = None
        self.table_schemas = {
            'products': '''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    platform TEXT NOT NULL,
                    product_id TEXT NOT NULL UNIQUE,
                    name TEXT,
                    price REAL,
                    original_price REAL,
                    sales INTEGER,
                    comments_count INTEGER,
                    shop_name TEXT,
                    category TEXT,
                    url TEXT,
                    crawl_time DATETIME,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'comments': '''
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT NOT NULL,
                    comment_id TEXT NOT NULL UNIQUE,
                    user_id TEXT,
                    user_name TEXT,
                    content TEXT,
                    rating INTEGER,
                    comment_time DATETIME,
                    useful_votes INTEGER,
                    reply_count INTEGER,
                    crawl_time DATETIME,
                    FOREIGN KEY (product_id) REFERENCES products(product_id)
                )
            ''',
            'shops': '''
                CREATE TABLE IF NOT EXISTS shops (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shop_id TEXT NOT NULL UNIQUE,
                    shop_name TEXT,
                    shop_type TEXT,
                    score_service REAL,
                    score_delivery REAL,
                    score_description REAL,
                    location TEXT,
                    registered_time TEXT,
                    crawl_time DATETIME,
                    update_time DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            '''
        }

    def open_spider(self, spider):
        """初始化数据库连接和表结构"""
        db_config = {
            'type': 'sqlite',
            'path': os.path.join(
                spider.settings.get('STORAGE', {}).get('data_dir', './data'),
                'ecommerce.db'
            )
        }
        self.db_helper = DBHelper(db_config)
        self.db_helper.create_tables(self.table_schemas)
        logger.info("SQLite数据库管道初始化完成")

    def process_item(self, item, spider):
        """将item写入数据库"""
        adapter = ItemAdapter(item)
        item_type = item.__class__.__name__
        
        try:
            if item_type == 'ProductItem':
                # 处理商品数据（存在则更新，不存在则插入）
                data = {
                    'platform': adapter.get('platform'),
                    'product_id': adapter.get('product_id'),
                    'name': adapter.get('name'),
                    'price': adapter.get('price'),
                    'original_price': adapter.get('original_price'),
                    'sales': adapter.get('sales'),
                    'comments_count': adapter.get('comments_count'),
                    'shop_name': adapter.get('shop_name'),
                    'category': adapter.get('category'),
                    'url': adapter.get('url'),
                    'crawl_time': adapter.get('crawl_time'),
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 检查是否已存在
                existing = self.db_helper.execute_query(
                    "SELECT id FROM products WHERE product_id = %s",
                    (data['product_id'],),
                    fetch_one=True
                )
                
                if existing:
                    # 更新现有记录
                    self.db_helper.execute_update('''
                        UPDATE products SET 
                            platform = %s, name = %s, price = %s, original_price = %s,
                            sales = %s, comments_count = %s, shop_name = %s, category = %s,
                            url = %s, crawl_time = %s, update_time = %s
                        WHERE product_id = %s
                    ''', (
                        data['platform'], data['name'], data['price'], data['original_price'],
                        data['sales'], data['comments_count'], data['shop_name'], data['category'],
                        data['url'], data['crawl_time'], data['update_time'], data['product_id']
                    ))
                else:
                    # 插入新记录
                    self.db_helper.execute_update('''
                        INSERT INTO products (
                            platform, product_id, name, price, original_price, sales,
                            comments_count, shop_name, category, url, crawl_time, update_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        data['platform'], data['product_id'], data['name'], data['price'],
                        data['original_price'], data['sales'], data['comments_count'],
                        data['shop_name'], data['category'], data['url'], data['crawl_time'],
                        data['update_time']
                    ))

            elif item_type == 'CommentItem':
                # 处理评论数据
                data = {
                    'product_id': adapter.get('product_id'),
                    'comment_id': adapter.get('comment_id'),
                    'user_id': adapter.get('user_id'),
                    'user_name': adapter.get('user_name'),
                    'content': adapter.get('content'),
                    'rating': adapter.get('rating'),
                    'comment_time': adapter.get('comment_time'),
                    'useful_votes': adapter.get('useful_votes'),
                    'reply_count': adapter.get('reply_count'),
                    'crawl_time': adapter.get('crawl_time')
                }
                
                # 避免重复评论
                if not self.db_helper.execute_query(
                    "SELECT id FROM comments WHERE comment_id = %s",
                    (data['comment_id'],),
                    fetch_one=True
                ):
                    self.db_helper.execute_update('''
                        INSERT INTO comments (
                            product_id, comment_id, user_id, user_name, content, rating,
                            comment_time, useful_votes, reply_count, crawl_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        data['product_id'], data['comment_id'], data['user_id'],
                        data['user_name'], data['content'], data['rating'],
                        data['comment_time'], data['useful_votes'], data['reply_count'],
                        data['crawl_time']
                    ))

            elif item_type == 'ShopItem':
                # 处理店铺数据
                data = {
                    'shop_id': adapter.get('shop_id'),
                    'shop_name': adapter.get('shop_name'),
                    'shop_type': adapter.get('shop_type'),
                    'score_service': adapter.get('score_service'),
                    'score_delivery': adapter.get('score_delivery'),
                    'score_description': adapter.get('score_description'),
                    'location': adapter.get('location'),
                    'registered_time': adapter.get('registered_time'),
                    'crawl_time': adapter.get('crawl_time'),
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                existing = self.db_helper.execute_query(
                    "SELECT id FROM shops WHERE shop_id = %s",
                    (data['shop_id'],),
                    fetch_one=True
                )
                
                if existing:
                    self.db_helper.execute_update('''
                        UPDATE shops SET 
                            shop_name = %s, shop_type = %s, score_service = %s,
                            score_delivery = %s, score_description = %s, location = %s,
                            registered_time = %s, crawl_time = %s, update_time = %s
                        WHERE shop_id = %s
                    ''', (
                        data['shop_name'], data['shop_type'], data['score_service'],
                        data['score_delivery'], data['score_description'], data['location'],
                        data['registered_time'], data['crawl_time'], data['update_time'],
                        data['shop_id']
                    ))
                else:
                    self.db_helper.execute_update('''
                        INSERT INTO shops (
                            shop_id, shop_name, shop_type, score_service, score_delivery,
                            score_description, location, registered_time, crawl_time, update_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        data['shop_id'], data['shop_name'], data['shop_type'],
                        data['score_service'], data['score_delivery'], data['score_description'],
                        data['location'], data['registered_time'], data['crawl_time'],
                        data['update_time']
                    ))
                    
        except Exception as e:
            logger.error(f"数据库存储错误: {str(e)}", exc_info=True)
            
        return item


class JSONPipeline:
    """JSON文件存储管道（按天分割）"""
    def __init__(self):
        self.files = {}
        self.data_dir = None
        self.date_suffix = datetime.now().strftime('%Y%m%d')

    def open_spider(self, spider):
        """初始化JSON文件"""
        self.data_dir = spider.settings.get('STORAGE', {}).get('data_dir', './data')
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 为每种数据类型创建JSON文件
        for item_type in ['products', 'comments', 'shops']:
            file_path = os.path.join(self.data_dir, f"{item_type}_{self.date_suffix}.json")
            # 写入JSON数组开始符
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write('[\n')
            self.files[item_type] = open(file_path, 'a', encoding='utf-8')
            logger.info(f"JSON文件初始化: {file_path}")

    def close_spider(self, spider):
        """关闭JSON文件并补全格式"""
        for item_type, file in self.files.items():
            # 移除最后一个逗号并写入数组结束符
            file.seek(file.tell() - 2, os.SEEK_SET)  # 回退删除最后一个逗号和换行
            file.write('\n]')
            file.close()
        logger.info("所有JSON文件已关闭")

    def process_item(self, item, spider):
        """将item写入JSON文件"""
        adapter = ItemAdapter(item)
        item_type = item.__class__.__name__.lower().replace('item', '') + 's'
        
        if item_type in self.files:
            # 转换为字典并写入
            json.dump(
                adapter.asdict(),
                self.files[item_type],
                ensure_ascii=False,
                indent=2
            )
            self.files[item_type].write(',\n')  # 逗号分隔
        return item
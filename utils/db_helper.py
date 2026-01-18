import sqlite3
import pymysql
from contextlib import contextmanager
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DBHelper:
    """支持多数据库的操作工具类"""
    def __init__(self, db_config):
        """初始化数据库连接配置"""
        self.db_type = db_config.get('type', 'sqlite')
        self.config = db_config
        self._test_connection()  # 初始化时测试连接

    @contextmanager
    def get_connection(self):
        """数据库连接上下文管理器（自动处理连接生命周期）"""
        conn = None
        try:
            if self.db_type == 'sqlite':
                conn = sqlite3.connect(
                    self.config.get('path', 'ecommerce.db'),
                    check_same_thread=False
                )
                conn.row_factory = sqlite3.Row  # 支持按列名访问
            elif self.db_type == 'mysql':
                conn = pymysql.connect(
                    host=self.config.get('host', 'localhost'),
                    port=self.config.get('port', 3306),
                    user=self.config.get('user', 'root'),
                    password=self.config.get('password', ''),
                    db=self.config.get('db', 'ecommerce'),
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
            else:
                raise ValueError(f"不支持的数据库类型: {self.db_type}")
            
            yield conn
            conn.commit()  # 自动提交事务
        except Exception as e:
            if conn:
                conn.rollback()  # 出错时回滚
            logger.error(f"数据库连接错误: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()  # 确保连接关闭

    def execute_update(self, query, params=None):
        """执行更新操作（INSERT/UPDATE/DELETE）"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                affected = cursor.rowcount
                logger.debug(f"执行SQL: {query}，影响行数: {affected}")
                return affected
            except Exception as e:
                logger.error(f"SQL执行错误: {str(e)}，SQL: {query}，参数: {params}")
                raise

    def execute_query(self, query, params=None, fetch_one=False):
        """执行查询操作"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or ())
                if fetch_one:
                    result = cursor.fetchone()
                    logger.debug(f"查询结果: {result}")
                    return result
                else:
                    result = cursor.fetchall()
                    logger.debug(f"查询到 {len(result)} 条记录")
                    return result
            except Exception as e:
                logger.error(f"查询错误: {str(e)}，SQL: {query}，参数: {params}")
                raise

    def batch_insert(self, table, data_list, batch_size=100):
        """批量插入数据（优化大量数据插入性能）"""
        if not data_list:
            return 0
            
        # 提取字段名
        fields = list(data_list[0].keys())
        placeholders = ', '.join(['%s'] * len(fields)) if self.db_type == 'mysql' else ', '.join(['?'] * len(fields))
        query = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"
        
        total = 0
        # 分批插入
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i+batch_size]
            params = [tuple(item.values()) for item in batch]
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.executemany(query, params)
                    affected = cursor.rowcount
                    total += affected
                    logger.debug(f"批量插入 {table} 表 {affected} 条记录，累计: {total}")
                except Exception as e:
                    logger.error(f"批量插入错误: {str(e)}")
                    raise
        return total

    def create_tables(self, schema):
        """根据 schema 创建数据表"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for table, sql in schema.items():
                try:
                    cursor.execute(sql)
                    logger.info(f"数据表 {table} 创建成功")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.warning(f"数据表 {table} 已存在")
                    else:
                        logger.error(f"创建数据表 {table} 失败: {str(e)}")
                        raise

    def _test_connection(self):
        """测试数据库连接是否正常"""
        try:
            with self.get_connection():
                logger.info(f"{self.db_type} 数据库连接成功")
        except Exception as e:
            logger.error(f"{self.db_type} 数据库连接失败: {str(e)}")
            raise
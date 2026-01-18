"""Cookie池管理工具"""
import time
import random
import logging
from threading import Lock

logger = logging.getLogger(__name__)

class CookiePool:
    """Cookie池管理类（支持自动刷新与健康检查）"""
    def __init__(self, pool_size=10, refresh_interval=3600, platform='', generate_func=None):
        self.pool_size = pool_size  # Cookie池大小
        self.refresh_interval = refresh_interval  # 刷新间隔（秒）
        self.platform = platform  # 目标平台
        self.generate_func = generate_func  # Cookie生成函数
        self.cookies = []  # 可用Cookie列表
        self.failed_cookies = set()  # 失败Cookie标记
        self.lock = Lock()  # 线程锁
        self._init_pool()

    def _init_pool(self):
        """初始化Cookie池"""
        if not self.generate_func:
            logger.warning("未设置Cookie生成函数，Cookie池初始化失败")
            return
        
        # 填充Cookie池
        for _ in range(self.pool_size):
            try:
                cookie = self.generate_func()
                if cookie:
                    self.cookies.append(cookie)
            except Exception as e:
                logger.error(f"生成Cookie失败: {str(e)}")
        
        logger.info(f"{self.platform} Cookie池初始化完成，可用Cookie: {len(self.cookies)}/{self.pool_size}")

    def get_cookie(self):
        """获取一个可用Cookie（随机选择）"""
        with self.lock:
            if not self.cookies:
                self._init_pool()  # 池为空时重新初始化
            
            if self.cookies:
                # 优先选择最近生成的Cookie
                self.cookies.sort(key=lambda x: x.get('created_at', 0), reverse=True)
                return random.choice(self.cookies[:max(1, len(self.cookies)//2)])  # 选择前半部分较新的
            return None

    def mark_cookie_failed(self, cookie):
        """标记失败的Cookie"""
        if not cookie:
            return
        
        with self.lock:
            # 生成Cookie唯一标识
            cookie_key = self._get_cookie_key(cookie)
            self.failed_cookies.add(cookie_key)
            logger.debug(f"标记失败Cookie: {cookie_key}，当前失败数: {len(self.failed_cookies)}")

    def refresh_expired(self):
        """刷新过期或失败的Cookie"""
        with self.lock:
            current_time = time.time()
            valid_cookies = []
            expired_count = 0
            failed_count = 0

            for cookie in self.cookies:
                cookie_key = self._get_cookie_key(cookie)
                created_at = cookie.get('created_at', 0)
                
                # 检查是否过期或失败
                if (current_time - created_at > self.refresh_interval) or (cookie_key in self.failed_cookies):
                    if current_time - created_at > self.refresh_interval:
                        expired_count += 1
                    else:
                        failed_count += 1
                    continue
                valid_cookies.append(cookie)

            # 补充新Cookie
            need补充 = self.pool_size - len(valid_cookies)
            if need补充 > 0:
                logger.info(f"{self.platform} Cookie池需要补充 {need补充} 个Cookie（过期: {expired_count}, 失败: {failed_count}）")
                for _ in range(need补充):
                    try:
                        new_cookie = self.generate_func()
                        if new_cookie:
                            valid_cookies.append(new_cookie)
                    except Exception as e:
                        logger.error(f"补充Cookie失败: {str(e)}")

            # 更新Cookie池
            self.cookies = valid_cookies[:self.pool_size]  # 截断到池大小
            self.failed_cookies.clear()  # 清空失败标记
            logger.info(f"{self.platform} Cookie池刷新完成，当前可用: {len(self.cookies)}/{self.pool_size}")

    def _get_cookie_key(self, cookie):
        """生成Cookie的唯一标识"""
        # 基于关键字段生成哈希
        key_fields = ['pin', 'pt_pin', 'uuid', 'sid']  # 各平台关键Cookie字段
        key_parts = []
        for field in key_fields:
            if field in cookie:
                key_parts.append(str(cookie[field]))
        return '_'.join(key_parts) if key_parts else str(hash(frozenset(cookie.items())))
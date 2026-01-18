import time
import random
import threading
import requests
import logging
from datetime import datetime
from scrapy.utils.project import get_project_settings

logger = logging.getLogger(__name__)

class ProxyPool:
    """代理池管理类，负责代理的加载、验证和动态维护"""
    def __init__(self):
        self.settings = get_project_settings()
        self.proxy_list_path = self.settings.get('PROXY_LIST_PATH')
        self.working_proxies = []  # 可用代理列表，格式: [{'proxy': 'http://ip:port', 'score': 100, 'last_used': None}, ...]
        self.lock = threading.Lock()  # 线程锁
        self.check_interval = self.settings.get('PROXY_CHECK_INTERVAL', 300)  # 代理检查间隔(秒)
        self.min_working_proxies = 3  # 最小可用代理数量
        self.timeout = 10  # 代理验证超时时间
        self.test_urls = [  # 用于验证代理的URL
            'https://mobile.yangkeduo.com/',
            'https://www.taobao.com/',
            'https://www.jd.com/'
        ]
        
        # 初始化代理池
        self._load_proxies_from_file()
        # 启动定期检查线程
        self._start_check_thread()
        # 初始验证
        self._check_all_proxies()

    def _load_proxies_from_file(self):
        """从文件加载代理列表"""
        try:
            with open(self.proxy_list_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        # 确保代理格式正确
                        if not line.startswith(('http://', 'https://')):
                            line = f'http://{line}'
                        self.working_proxies.append({
                            'proxy': line,
                            'score': 100,
                            'last_used': None,
                            'fail_count': 0,
                            'success_count': 0
                        })
            logger.info(f"从文件加载代理 {len(self.working_proxies)} 个")
        except Exception as e:
            logger.error(f"加载代理列表失败: {e}")

    def _start_check_thread(self):
        """启动定期检查代理的线程"""
        def check_loop():
            while True:
                self._check_all_proxies()
                time.sleep(self.check_interval)
        
        thread = threading.Thread(target=check_loop, daemon=True)
        thread.start()
        logger.info("代理定期检查线程已启动")

    def _check_all_proxies(self):
        """检查所有代理的可用性"""
        with self.lock:
            proxies_to_check = self.working_proxies.copy()
        
        # 对每个代理进行验证
        for proxy_info in proxies_to_check:
            proxy = proxy_info['proxy']
            is_working = self._check_proxy(proxy)
            
            with self.lock:
                # 找到代理在列表中的位置
                for p in self.working_proxies:
                    if p['proxy'] == proxy:
                        if is_working:
                            # 验证成功，恢复分数
                            p['score'] = min(p['score'] + 10, 100)
                            p['success_count'] += 1
                            p['fail_count'] = 0
                            logger.debug(f"代理 {proxy} 验证成功，当前分数: {p['score']}")
                        else:
                            # 验证失败，降低分数
                            p['score'] -= 20
                            p['fail_count'] += 1
                            logger.debug(f"代理 {proxy} 验证失败，当前分数: {p['score']}")
                            
                            # 分数过低则移除
                            if p['score'] <= 0:
                                self.working_proxies.remove(p)
                                logger.warning(f"代理 {proxy} 分数过低，已移除")
                        break
        
        # 如果可用代理不足，尝试重新加载
        if len(self.working_proxies) < self.min_working_proxies:
            logger.warning(f"可用代理不足({len(self.working_proxies)}个)，尝试重新加载")
            self._load_proxies_from_file()
            # 再次检查
            self._check_all_proxies()

    def _check_proxy(self, proxy):
        """检查单个代理是否可用"""
        try:
            test_url = random.choice(self.test_urls)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
            }
            proxies = {
                'http': proxy,
                'https': proxy
            }
            
            response = requests.get(
                test_url,
                headers=headers,
                proxies=proxies,
                timeout=self.timeout,
                allow_redirects=False
            )
            
            # 200或302状态码视为可用
            return response.status_code in [200, 302]
        except Exception as e:
            logger.debug(f"代理 {proxy} 验证失败: {e}")
            return False

    def get_working_proxy(self):
        """获取一个可用代理（基于分数加权随机选择）"""
        with self.lock:
            if not self.working_proxies:
                logger.warning("没有可用代理，将使用本地IP")
                return None
            
            # 基于分数加权随机选择（分数越高，被选中概率越大）
            total_score = sum(p['score'] for p in self.working_proxies)
            if total_score <= 0:
                return random.choice(self.working_proxies)['proxy']
            
            # 加权随机选择
            rand = random.uniform(0, total_score)
            current = 0
            for p in self.working_proxies:
                current += p['score']
                if current >= rand:
                    p['last_used'] = datetime.now()
                    return p['proxy']
        
        return random.choice(self.working_proxies)['proxy']

    def report_failure(self, proxy):
        """报告代理使用失败"""
        if not proxy:
            return
            
        with self.lock:
            for p in self.working_proxies:
                if p['proxy'] == proxy:
                    p['fail_count'] += 1
                    p['score'] = max(p['score'] - 15, 0)  # 失败一次扣15分
                    logger.debug(f"代理 {proxy} 报告失败，当前分数: {p['score']}，失败次数: {p['fail_count']}")
                    
                    # 失败次数过多则标记为不可用
                    if p['fail_count'] > 5:
                        self.working_proxies.remove(p)
                        logger.warning(f"代理 {proxy} 失败次数过多，已移除")
                    break

    def report_success(self, proxy):
        """报告代理使用成功"""
        if not proxy:
            return
            
        with self.lock:
            for p in self.working_proxies:
                if p['proxy'] == proxy:
                    p['success_count'] += 1
                    p['score'] = min(p['score'] + 5, 100)  # 成功一次加5分
                    logger.debug(f"代理 {proxy} 报告成功，当前分数: {p['score']}")
                    break

    def get_stats(self):
        """获取代理池统计信息"""
        with self.lock:
            return {
                'total': len(self.working_proxies),
                'working': len([p for p in self.working_proxies if p['score'] > 0]),
                'average_score': sum(p['score'] for p in self.working_proxies) / max(len(self.working_proxies), 1),
                'top_proxies': [p['proxy'] for p in sorted(self.working_proxies, key=lambda x: x['score'], reverse=True)[:3]]
            }
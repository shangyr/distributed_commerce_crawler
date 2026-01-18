import time
import random
import pickle
import os
import hashlib
from datetime import datetime, timedelta
from scrapy.utils.project import get_project_settings

class AntiCrawler:
    """反爬工具类，处理Cookie管理、User-Agent轮换、签名生成等反爬机制"""
    def __init__(self):
        # 初始化配置
        self.settings = get_project_settings()
        self.ua_list = self._load_user_agents()
        self.proxy_list = []
        self.working_proxies = []
        
        # Cookie存储结构（增强版，支持多账户轮换）
        self.cookie_store = {
            'taobao': {'cookies_pool': [], 'current_idx': 0, 'expire_time': 0},
            'jd': {'cookies_pool': [], 'current_idx': 0, 'expire_time': 0},
            'pdd': {'cookies_pool': [], 'current_idx': 0, 'expire_time': 0}
        }
        self.cookie_file = 'cookie_store.pkl'
        self.load_cookies()
        
        # 初始化代理池
        self.load_proxies()

    def _load_user_agents(self):
        """从配置文件加载User-Agent列表"""
        ua_path = self.settings.get('USER_AGENTS_PATH')
        try:
            with open(ua_path, 'r', encoding='utf-8') as f:
                return [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"加载User-Agent失败: {e}")
            #  fallback默认UA列表
            return [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15'
            ]

    def get_random_ua(self, mobile=False):
        """获取随机User-Agent（支持区分移动端）"""
        if not self.ua_list:
            return self._load_user_agents()[0]
            
        if mobile:
            # 筛选移动端UA
            mobile_keywords = ['Mobile', 'Android', 'iPhone', 'iPad']
            mobile_uas = [ua for ua in self.ua_list if any(k in ua for k in mobile_keywords)]
            return random.choice(mobile_uas) if mobile_uas else random.choice(self.ua_list)
        return random.choice(self.ua_list)

    def add_ua(self, ua):
        """添加新的User-Agent到列表"""
        if ua not in self.ua_list:
            self.ua_list.append(ua)

    def load_proxies(self):
        """加载代理列表（从配置或文件）"""
        # 实际应用中可从文件或API加载代理
        proxy_config = self.settings.get('PROXY_CONFIG')
        if proxy_config.get('enable', False):
            # 这里仅为示例，实际应从代理源加载
            self.proxy_list = [
                'http://127.0.0.1:7890',
                'http://127.0.0.1:7891'
            ]
            self.check_proxy_health()

    def check_proxy_health(self):
        """检查代理健康状态"""
        # 实际应用中应通过测试请求验证代理有效性
        self.working_proxies = [p for p in self.proxy_list if random.random() > 0.3]  # 模拟健康检查
        return self.working_proxies

    def get_random_proxy(self):
        """获取随机可用代理"""
        if not self.working_proxies:
            self.check_proxy_health()
        return random.choice(self.working_proxies) if self.working_proxies else None

    def load_cookies(self):
        """加载Cookie存储（支持多账户池）"""
        try:
            if os.path.exists(self.cookie_file) and os.path.getsize(self.cookie_file) > 0:
                with open(self.cookie_file, 'rb') as f:
                    self.cookie_store = pickle.load(f)
        except (FileNotFoundError, EOFError, pickle.UnpicklingError) as e:
            print(f"加载Cookie失败: {e}，使用默认配置")
            # 初始化多账户Cookie池
            for platform in self.cookie_store:
                self.cookie_store[platform]['cookies_pool'] = [
                    self._generate_platform_cookies(platform) for _ in range(3)  # 每个平台3个Cookie账户
                ]
                self.cookie_store[platform]['expire_time'] = time.time() + 3600 * 8  # 8小时有效期

    def save_cookies(self):
        """保存Cookie到文件"""
        try:
            with open(self.cookie_file, 'wb') as f:
                pickle.dump(self.cookie_store, f)
        except Exception as e:
            print(f"保存Cookie错误: {e}")

    def _get_valid_cookies(self, platform):
        """获取有效的Cookie（支持账户轮换）"""
        current_time = time.time()
        cookies_info = self.cookie_store.get(platform, {})
        
        # 检查是否需要刷新Cookie池
        if (not cookies_info['cookies_pool'] or 
            current_time > cookies_info.get('expire_time', 0)):
            # 生成新的Cookie池
            cookies_info['cookies_pool'] = [
                self._generate_platform_cookies(platform) for _ in range(3)
            ]
            cookies_info['expire_time'] = current_time + 3600 * 8  # 8小时有效期
            cookies_info['current_idx'] = 0
            self.save_cookies()
        
        # 实现Cookie轮换（轮询策略）
        cookies = cookies_info['cookies_pool'][cookies_info['current_idx']]
        cookies_info['current_idx'] = (cookies_info['current_idx'] + 1) % len(cookies_info['cookies_pool'])
        return cookies

    def _generate_platform_cookies(self, platform):
        """根据平台生成Cookie"""
        if platform == 'taobao':
            return self.generate_taobao_cookies()
        elif platform == 'jd':
            return self.generate_jd_cookies()
        elif platform == 'pdd':
            return self.generate_pdd_cookies()
        return []

    def get_taobao_cookie_string(self):
        """获取淘宝Cookie字符串（用于请求头）"""
        cookies = self._get_valid_cookies('taobao')
        return '; '.join([f"{c['name']}={c['value']}" for c in cookies])

    def get_jd_cookie_string(self):
        """获取京东Cookie字符串"""
        cookies = self._get_valid_cookies('jd')
        return '; '.join([f"{c['name']}={c['value']}" for c in cookies])

    def get_pdd_cookie_string(self):
        """获取拼多多Cookie字符串"""
        cookies = self._get_valid_cookies('pdd')
        return '; '.join([f"{c['name']}={c['value']}" for c in cookies])

    def save_response_cookies(self, platform, response):
        """从响应中提取并保存Cookie"""
        if not response.headers.getlist('Set-Cookie'):
            return
            
        cookies = []
        for cookie_str in response.headers.getlist('Set-Cookie'):
            parts = cookie_str.decode().split(';')
            if not parts:
                continue
                
            name_value = parts[0].strip().split('=', 1)
            if len(name_value) != 2:
                continue
                
            name, value = name_value
            cookie = {'name': name, 'value': value}
            
            # 解析过期时间
            for part in parts[1:]:
                part = part.strip()
                if part.lower().startswith('expires='):
                    try:
                        expire_str = part[8:]
                        expire_time = datetime.strptime(
                            expire_str, '%a, %d-%b-%Y %H:%M:%S GMT'
                        ).timestamp()
                        cookie['expire_time'] = expire_time
                    except Exception:
                        pass
            cookies.append(cookie)
            
        if cookies:
            # 添加到Cookie池
            self.cookie_store[platform]['cookies_pool'].append(cookies)
            # 限制池大小为5
            if len(self.cookie_store[platform]['cookies_pool']) > 5:
                self.cookie_store[platform]['cookies_pool'].pop(0)
            self.save_cookies()

    def generate_taobao_cookies(self):
        """生成模拟的淘宝Cookie"""
        return [
            {'name': 'cna', 'value': self.generate_random_string(16)},
            {'name': 'cookie2', 'value': self.generate_random_string(32)},
            {'name': 't', 'value': self.generate_random_string(40)},
            {'name': 'thw', 'value': 'cn'},
            {'name': 'tracknick', 'value': f'test_{self.generate_random_string(6)}'},
            {'name': 'l', 'value': f'0{self.generate_random_string(18)}'},
            {'name': 'isg', 'value': self.generate_random_string(32)},
            {'name': 'tfstk', 'value': self.generate_random_string(43)},
        ]

    def generate_jd_cookies(self):
        """生成模拟的京东Cookie"""
        return [
            {'name': 'pin', 'value': f'test_{self.generate_random_string(8)}'},
            {'name': 'jdv', 'value': f'76161171|baidu-pinzhuan|t_{self.generate_random_string(10)}'},
            {'name': 'jdw', 'value': self.generate_random_string(32)},
            {'name': 'unick', 'value': f'test_{self.generate_random_string(6)}'},
            {'name': 'wlfstk_smdl', 'value': self.generate_random_string(32)},
            {'name': 'shshshfpa', 'value': self.generate_random_string(24)},
            {'name': 'shshshfpb', 'value': self.generate_random_string(40)},
        ]

    def generate_pdd_cookies(self):
        """生成模拟的拼多多Cookie"""
        return [
            {'name': 'api_uid', 'value': f'{self.generate_random_string(16)}=='},
            {'name': 'pd_user_id', 'value': self.generate_random_string(16)},
            {'name': 'pd_session_id', 'value': f'{self.generate_random_string(32)}%7C{int(time.time())}'},
            {'name': 'pdd_sso_token', 'value': self.generate_random_string(40)},
            {'name': 'mobile_uuid', 'value': self.generate_random_string(32)},
        ]

    def generate_random_string(self, length):
        """生成指定长度的随机字符串"""
        chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        return ''.join(random.choice(chars) for _ in range(length))

    def get_random_delay(self, base_range=None):
        """生成随机延迟时间（模拟人类行为）"""
        if base_range:
            min_delay, max_delay = base_range
        else:
            # 从配置获取默认延迟范围
            min_delay, max_delay = self.settings.get('ANTI_CRAWLER', {}).get('random_delay', [0.5, 1.5])
        return random.uniform(min_delay, max_delay)

    def generate_taobao_sign(self, keyword, page, timestamp):
        """生成淘宝搜索签名"""
        salt = 'tb_search_' + self.generate_random_string(8)
        sign_str = f"keyword={keyword}&page={page}&ts={timestamp}&salt={salt}"
        return hashlib.md5(sign_str.encode()).hexdigest()

    def get_random_ip(self):
        """生成随机IP地址（用于X-Forwarded-For头）"""
        return f"{random.randint(10, 250)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
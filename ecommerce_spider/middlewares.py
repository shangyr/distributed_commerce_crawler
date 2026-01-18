import time
import random
import logging
from scrapy import signals
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.downloadermiddlewares.httpproxy import HttpProxyMiddleware
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from scrapy.http import HtmlResponse

logger = logging.getLogger(__name__)

class UserAgentMiddleware:
    """用户代理中间件"""
    def __init__(self, settings):
        self.user_agents = []
        self.mobile_user_agents = []
        self.desktop_user_agents = []
        self.ua_file = settings.get('USER_AGENTS_PATH')
        self._load_user_agents()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def _load_user_agents(self):
        """从文件加载用户代理并分类"""
        try:
            with open(self.ua_file, 'r') as f:
                for line in f:
                    ua = line.strip()
                    if ua:
                        self.user_agents.append(ua)
                        if 'Mobile' in ua or 'Android' in ua or 'iPhone' in ua or 'iPad' in ua:
                            self.mobile_user_agents.append(ua)
                        else:
                            self.desktop_user_agents.append(ua)
            logger.info(f"加载用户代理 {len(self.user_agents)} 个，移动端 {len(self.mobile_user_agents)} 个")
        except Exception as e:
            logger.error(f"加载用户代理失败: {e}")
            # 使用默认UA
            self.user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1'
            ]
            self.mobile_user_agents = [self.user_agents[1]]
            self.desktop_user_agents = [self.user_agents[0]]

    def process_request(self, request, spider):
        """为请求设置用户代理（支持平台差异化）"""
        if hasattr(spider, 'name'):
            # 拼多多优先使用移动端UA
            if spider.name == 'pdd':
                ua = random.choice(self.mobile_user_agents)
            # 淘宝和京东混合使用
            else:
                ua = random.choice(self.mobile_user_agents) if random.random() > 0.3 else random.choice(self.desktop_user_agents)
        else:
            ua = random.choice(self.user_agents)
            
        request.headers['User-Agent'] = ua
        return None

class ProxyMiddleware:
    """代理中间件"""
    def __init__(self, settings):
        from utils.proxy_pool import ProxyPool
        self.proxy_pool = ProxyPool()
        self.proxy_stats = {}  # 记录代理使用情况: {proxy: {'success': 0, 'fail': 0}}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        """为请求设置代理"""
        # 如果请求已指定代理则使用
        if 'proxy' in request.meta:
            proxy = request.meta['proxy']
        else:
            # 从代理池获取
            proxy = self.proxy_pool.get_working_proxy()
            if proxy:
                request.meta['proxy'] = proxy
        
        # 记录代理使用
        if proxy:
            if proxy not in self.proxy_stats:
                self.proxy_stats[proxy] = {'success': 0, 'fail': 0}
            logger.debug(f"使用代理 {proxy} 访问 {request.url}")

    def process_response(self, request, response, spider):
        """处理响应，记录代理成功情况"""
        proxy = request.meta.get('proxy')
        if proxy:
            # 对403/429等反爬状态码视为失败
            if response.status in [403, 429, 503]:
                self.proxy_stats[proxy]['fail'] += 1
                self.proxy_pool.report_failure(proxy)
                logger.warning(f"代理 {proxy} 访问 {request.url} 失败，状态码: {response.status}")
            else:
                self.proxy_stats[proxy]['success'] += 1
                self.proxy_pool.report_success(proxy)
        return response

    def process_exception(self, request, exception, spider):
        """处理异常，记录代理失败情况"""
        proxy = request.meta.get('proxy')
        if proxy:
            self.proxy_stats[proxy]['fail'] += 1
            self.proxy_pool.report_failure(proxy)
            logger.error(f"代理 {proxy} 访问 {request.url} 发生异常: {exception}")
        return None

class JavaScriptMiddleware:
    """JavaScript渲染中间件"""
    def __init__(self, settings):
        self.js_render_enabled = settings.get('ANTI_CRAWLER', {}).get('js_render', True)
        self.timeout = settings.get('JS_RENDER_TIMEOUT', 30)
        self.driver = self._init_driver()
        self.render_count = 0  # 渲染计数器
        self.max_render_per_driver = 50  # 每个driver最大渲染次数

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def _init_driver(self):
        """初始化Chrome驱动（增强反检测）"""
        if not self.js_render_enabled:
            return None
            
        chrome_options = Options()
        # 基础反检测配置
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--disable-infobars')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--ignore-certificate-errors')
        
        # 随机用户代理
        from ecommerce_spider.middlewares import UserAgentMiddleware
        ua_middleware = UserAgentMiddleware(self.timeout)  # 临时实例获取UA
        chrome_options.add_argument(f'user-agent={random.choice(ua_middleware.user_agents)}')
        
        # 随机窗口大小
        window_sizes = ['1920,1080', '1366,768', '1536,864', '1280,720']
        chrome_options.add_argument(f'--window-size={random.choice(window_sizes)}')
        
        # 禁用自动化特征检测
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            
            # 进一步隐藏自动化特征
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['zh-CN', 'zh', 'en-US', 'en']
                    });
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3]
                    });
                """
            })
            
            # 随机设置时区
            time_zones = [
                "Asia/Shanghai", "Asia/Hong_Kong", "Asia/Taipei",
                "America/New_York", "Europe/London"
            ]
            driver.execute_cdp_cmd("Emulation.setTimezoneOverride", {
                "timezoneId": random.choice(time_zones)
            })
            
            driver.set_page_load_timeout(self.timeout)
            logger.info("Chrome驱动初始化成功")
            return driver
        except Exception as e:
            logger.error(f"Chrome驱动初始化失败: {str(e)}")
            self.js_render_enabled = False
            return None

    def _refresh_driver(self):
        """定期刷新驱动以避免被检测"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("定期刷新Chrome驱动")
            except:
                pass
        self.driver = self._init_driver()
        self.render_count = 0

    def process_request(self, request, spider):
        """处理需要JS渲染的请求（增强版）"""
        if not self.js_render_enabled or not self.driver or not request.meta.get('render_js', False):
            return
            
        # 每渲染一定次数刷新驱动
        if self.render_count >= self.max_render_per_driver:
            self._refresh_driver()
            
        try:
            # 随机设置页面加载策略
            load_strategies = ['normal', 'eager', 'none']
            self.driver.execute_cdp_cmd("Page.setLoadStrategy", {
                "strategy": random.choice(load_strategies)
            })
            
            self.driver.get(request.url)
            
            # 随机等待一段时间（模拟人类浏览）
            base_sleep = random.uniform(1, 3)
            # 根据页面类型调整等待时间
            if request.meta.get('task_type') == 'product':
                base_sleep += random.uniform(1, 2)  # 商品页多等一会
            time.sleep(base_sleep)
            
            # 模拟滚动行为
            if random.random() > 0.3:
                scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                scroll_steps = random.randint(2, 5)
                for i in range(scroll_steps):
                    scroll_to = int(scroll_height * (i + 1) / scroll_steps)
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                    time.sleep(random.uniform(0.3, 0.8))
            
            # 平台差异化等待元素
            if hasattr(spider, 'name'):
                wait_time = random.uniform(5, 10)  # 随机等待时间
                try:
                    if spider.name == 'taobao':
                        WebDriverWait(self.driver, wait_time).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '.grid-item, .item.J_ItemList'))
                        )
                    elif spider.name == 'pdd':
                        WebDriverWait(self.driver, wait_time).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '.goods-list, .goods-item'))
                        )
                    elif spider.name == 'jd':
                        WebDriverWait(self.driver, wait_time).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, '#J_goodsList .gl-item'))
                        )
                except TimeoutException:
                    logger.warning(f"{spider.name} 页面元素加载超时，继续尝试获取内容")
            
            self.render_count += 1
            return self._get_partial_response(request)
            
        except WebDriverException as e:
            logger.error(f"JS渲染失败: {str(e)}")
            # 渲染失败时刷新驱动
            self._refresh_driver()
            return self._get_partial_response(request)

    def _get_partial_response(self, request):
        """获取部分加载的页面内容"""
        try:
            body = self.driver.page_source
            return HtmlResponse(
                self.driver.current_url,
                body=body.encode('utf-8'),
                encoding='utf-8',
                request=request
            )
        except Exception as e:
            logger.error(f"获取部分响应错误: {e}")
            return None

    def spider_closed(self, spider):
        """关闭浏览器驱动"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Chrome驱动已关闭")
            except Exception as e:
                logger.error(f"关闭Chrome驱动失败: {e}")
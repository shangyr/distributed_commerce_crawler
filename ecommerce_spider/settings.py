import os
import yaml
import random
from datetime import datetime
from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).parent.parent.resolve()

# 加载配置文件
with open(BASE_DIR / 'config' / 'config.yaml', 'r') as f:
    config = yaml.safe_load(f)

BOT_NAME = 'ecommerce_spider'

SPIDER_MODULES = ['ecommerce_spider.spiders']
NEWSPIDER_MODULE = 'ecommerce_spider.spiders'

# 遵守robots协议（平台差异化）
ROBOTSTXT_OBEY = config['robots_obey']

# 并发设置（按平台动态调整）
CONCURRENT_REQUESTS = config['concurrent_requests']
CONCURRENT_REQUESTS_PER_DOMAIN = config['concurrent_requests_per_domain']
CONCURRENT_REQUESTS_PER_IP = config['concurrent_requests_per_ip']
DOWNLOAD_DELAY = config['download_delay']

# 分布式配置
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER_PERSIST = True
SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.PriorityQueue'

# Redis配置
redis_config = config.get('redis', {})
REDIS_URL = f"redis://{redis_config.get('host', 'localhost')}:{redis_config.get('port', 6379)}/{redis_config.get('db', 0)}"
if redis_config.get('password'):
    REDIS_URL = f"redis://:{redis_config['password']}@{redis_config['host']}:{redis_config['port']}/{redis_config.get('db', 0)}"

# 下载中间件（优先级优化）
DOWNLOADER_MIDDLEWARES = {
    'ecommerce_spider.middlewares.UserAgentMiddleware': 540,  # UA优先设置
    'ecommerce_spider.middlewares.ProxyMiddleware': 541,      # 代理次之
    'ecommerce_spider.middlewares.CookieMiddleware': 542,     # Cookie在UA/代理之后
    'ecommerce_spider.middlewares.JavaScriptMiddleware': 543, # JS渲染最后
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
}

# 数据管道
ITEM_PIPELINES = {}
storage_config = config.get('storage', {})
if storage_config.get('sqlite', True):
    ITEM_PIPELINES['ecommerce_spider.pipelines.SQLitePipeline'] = 300
if storage_config.get('csv', True):
    ITEM_PIPELINES['ecommerce_spider.pipelines.CSVPipeline'] = 301
if storage_config.get('json', True):
    ITEM_PIPELINES['ecommerce_spider.pipelines.JSONPipeline'] = 302
ITEM_PIPELINES['scrapy_redis.pipelines.RedisPipeline'] = 400

# 去重配置
DUPEFILTER_DEBUG = False

# 日志配置（按平台分日志文件）
log_dir = storage_config.get('log_dir', './logs')
os.makedirs(log_dir, exist_ok=True)
LOG_ENABLED = True
LOG_ENCODING = 'utf-8'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FILE = os.path.join(log_dir, f'scrapy_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
LOG_LEVEL = 'INFO'

# 扩展配置
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,  # 禁用telnet
    'scrapy.extensions.closespider.CloseSpider': 500,
    'ecommerce_spider.extensions.ProxyHealthExtension': 550,  # 代理健康检查扩展
    'ecommerce_spider.extensions.SpiderStatsExtension': 560,  # 爬虫统计扩展
}

# 自动限速（平台差异化）
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 5
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False  # 调试模式下可开启

# 重试设置（增强版）
RETRY_ENABLED = True
RETRY_TIMES = config.get('anti_crawler', {}).get('retry_times', 3)
RETRY_HTTP_CODES = [403, 408, 500, 502, 503, 504, 429]
RETRY_PRIORITY_ADJUST = -1  # 重试请求优先级降低

# 超时设置（平台差异化）
DOWNLOAD_TIMEOUT = 15
DOWNLOAD_TIMEOUT_PER_PLATFORM = {
    'pdd': 20,   # 拼多多页面加载慢，超时更长
    'taobao': 18,
    'jd': 15
}

# 资源控制
REACTOR_THREADPOOL_MAXSIZE = 20
MEMUSAGE_LIMIT_MB = 512  # 内存限制
MEMUSAGE_NOTIFY_MAIL = []  # 内存超限通知邮箱

# 禁用默认UA
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
}

# 反爬增强配置
USER_AGENTS_PATH = BASE_DIR / 'config' / 'user_agents.txt'  # UA文件路径
PROXY_LIST_PATH = BASE_DIR / 'config' / 'proxy_list.txt'    # 代理列表路径
JS_RENDER_TIMEOUT = 30  # JS渲染超时时间
PROXY_HEALTH_CHECK_INTERVAL = 300  # 代理健康检查间隔（秒）

# 站点特定配置
SPIDERS = config.get('spiders', {})
ANTI_CRAWLER = config.get('anti_crawler', {})
PROXY_CONFIG = config.get('proxy', {})

# 爬虫关闭条件（防止无限运行）
CLOSESPIDER_TIMEOUT = 3600 * 24  # 24小时后自动关闭
CLOSESPIDER_ITEMCOUNT = 100000   # 抓取10万条数据后关闭
CLOSESPIDER_PAGECOUNT = 50000    # 抓取5万页后关闭

# 自定义设置（供扩展使用）
CUSTOM_SETTINGS = {
    'RANDOMIZE_DOWNLOAD_DELAY': True,  # 随机化下载延迟
    'DOWNLOAD_FAIL_ON_DATALOSS': False,  # 数据不完整时不失败
    'HTTPERROR_ALLOW_ALL': False,  # 不允许所有HTTP错误
    'HTTPERROR_ALLOWED_CODES': [404],  # 允许404错误
}
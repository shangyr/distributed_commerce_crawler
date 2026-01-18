import time
import json
import random
import hashlib
import re
from urllib.parse import quote, urlparse, parse_qs
from datetime import datetime, timedelta
import scrapy
from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from ecommerce_spider.items import ProductItem, CommentItem, ShopItem
from utils.anti_crawler import AntiCrawler
from utils.monitor import SpiderMonitor
from utils.proxy_pool import ProxyPool  # 新增代理池工具

class PDDSpider(RedisSpider):
    """拼多多分布式爬虫"""
    name = "pdd"
    allowed_domains = ["pinduoduo.com", "yangkeduo.com", "pddapi.com"]  # 补充域名
    redis_key = "pdd:start_urls"
    redis_batch_size = 5
    redis_encoding = "utf-8"

    # 增强反爬配置
    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.3,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 400, 408],  # 补充常见错误码
        'DUPEFILTER_KEY': 'pdd:dupefilter:%(timestamp)s',
        'SCHEDULER_PERSIST': True,
        'PROXY_CHECK_INTERVAL': 300,
        # 新增下载超时配置
        'DOWNLOAD_TIMEOUT': 20,
        # 启用Cookie持久化
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': False,
        # 增加请求间隔随机化
        'RANDOMIZE_DOWNLOAD_DELAY': True,
    }

    def __init__(self, role='worker', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.role = role
        self.settings = get_project_settings()
        self.max_pages = self.settings.get('SPIDERS', {}).get('pdd', {}).get('max_pages', 10)
        self.max_comments = self.settings.get('SPIDERS', {}).get('pdd', {}).get('max_comments', 50)
        self.anti_crawler = AntiCrawler()
        self.monitor = SpiderMonitor()
        self.proxy_pool = ProxyPool()  # 初始化代理池
        self.worker_id = hashlib.md5(f"{time.time()}-{random.randint(1, 1000000)}".encode()).hexdigest()[:12]
        self._register_worker()
        self.session_id = self._generate_session_id()
        self.device_id = self._generate_device_id()
        self.anti_content_version = 'v2'
        self.task_counter = 0
        self.error_history = []  # 错误历史记录，用于动态调整策略
        self.last_proxy_switch_time = time.time()
        self.proxy = None  # 当前使用的代理
        self.user_agent_rotation = self._load_user_agents()  # 加载UA池

    def _load_user_agents(self):
        """从文件加载用户代理池（移动端优先）"""
        ua_list = []
        try:
            with open(self.settings.get('USER_AGENTS_PATH'), 'r') as f:
                for line in f:
                    ua = line.strip()
                    if 'Mobile' in ua or 'Android' in ua or 'iPhone' in ua or 'iPad' in ua:
                        ua_list.append(ua)
            return ua_list if ua_list else [self.anti_crawler.get_random_ua(mobile=True)]
        except Exception as e:
            self.logger.warning(f"加载用户代理失败: {e}，使用默认UA")
            return [self.anti_crawler.get_random_ua(mobile=True)]

    def _register_worker(self):
        self.monitor.register_worker(self.worker_id)
        self.logger.info(f"PDD Worker节点注册: {self.worker_id}")
        # 启动定期健康检查
        self._start_health_check()

    def _start_health_check(self):
        """启动工作节点健康检查线程"""
        from threading import Thread
        def check():
            while True:
                # 每30秒更新一次活动时间
                self.monitor.redis_conn.set(
                    f"worker:{self.worker_id}:last_active", 
                    datetime.now().timestamp()
                )
                # 每5分钟检查一次代理健康
                if time.time() - self.last_proxy_switch_time > 300:
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.last_proxy_switch_time = time.time()
                time.sleep(30)
        
        Thread(target=check, daemon=True).start()

    def _generate_session_id(self):
        """生成会话ID（模拟APP会话）"""
        base = f"pdd_{int(time.time())}_{random.getrandbits(32)}_android"
        return hashlib.md5(base.encode()).hexdigest()

    def _generate_device_id(self):
        """设备ID（模拟真实设备指纹）"""
        manufacturers = ['xiaomi', 'huawei', 'oppo', 'vivo', 'apple']
        models = ['mi11', 'p50', 'reno6', 'x70', 'iphone13']
        manufacturer = random.choice(manufacturers)
        model = random.choice(models)
        serial = ''.join(random.sample('0123456789ABCDEF', 16))
        return f"{manufacturer}-{model}-{serial}"

    def start_requests(self):
        """Master节点生成初始任务，Worker节点从Redis消费"""
        if self.role == 'master':
            self.logger.info("PDD Master节点启动，生成初始任务...")
            start_urls = self.settings.get('SPIDERS', {}).get('pdd', {}).get('start_urls', [])
            keywords = []
            if start_urls:
                for url in start_urls:
                    parsed = urlparse(url)
                    keywords.append(parse_qs(parsed.query).get('search_key', [''])[0])
            else:
                keywords = ["手机", "电脑", "家电", "服装", "化妆品", "零食", "图书"]  # 关键词根据任务
            
            # 任务去重处理
            seen_keywords = set()
            unique_keywords = []
            for kw in keywords:
                if kw and kw not in seen_keywords:
                    seen_keywords.add(kw)
                    unique_keywords.append(kw)
            
            # 随机化任务生成
            random.shuffle(unique_keywords)
            for idx, keyword in enumerate(unique_keywords):
                # 模拟人类浏览间隔，递增延迟避免规律
                delay = random.uniform(1.5, 3.0) + idx * 0.1
                time.sleep(delay)
                for page in range(1, self.max_pages + 1):
                    # 随机跳过部分初始页，避免全量爬取被识别
                    if random.random() > 0.1:
                        yield self._build_search_request(keyword, page)
        else:
            self.logger.info("PDD Worker节点启动，等待Redis任务...")
            # 初始化代理
            self.proxy = self.proxy_pool.get_working_proxy()
            return super().start_requests()

    def _build_search_request(self, keyword, page):
        """构建搜索请求（增强反爬参数）"""
        anti_content = self.generate_anti_content(keyword, page)
        url = f"https://mobile.yangkeduo.com/search_result.html?search_key={quote(keyword)}&page={page}&anti_content={anti_content}"
        
        # 随机选择不同的搜索路径，增加随机性
        path_choices = [
            "search_result.html", 
            "search.html", 
            "search_list.html"
        ]
        if random.random() > 0.5:
            selected_path = random.choice(path_choices)
            url = url.replace("search_result.html", selected_path)
        
        # 随机添加冗余参数
        redundant_params = [
            f"t={int(time.time())}",
            f"req_id={hashlib.md5(str(random.random()).encode()).hexdigest()[:16]}",
            f"source={random.choice(['app', 'h5', 'wx'])}"
        ]
        url += "&" + "&".join(random.sample(redundant_params, random.randint(1, 2)))
        
        return scrapy.Request(
            url,
            callback=self.parse_search,
            meta={
                'keyword': keyword,
                'page': page,
                'task_type': 'search',
                'render_js': True,
                'retry_times': 0,
                'proxy': self.proxy,  # 绑定当前代理
                'start_time': time.time()  # 记录开始时间用于超时判断
            },
            headers=self._get_headers(),
            errback=self.handle_error,
            dont_filter=True
        )

    def _get_headers(self):
        """生成更真实的请求头（动态调整策略）"""
        # 基础 headers
        headers = {
            'Referer': 'https://mobile.yangkeduo.com/',
            'X-Requested-With': 'XMLHttpRequest',
            'PDD-User-ID': '',
            'PDD-Session-ID': self.session_id,
            'PDD-Device-ID': self.device_id,
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'User-Agent': random.choice(self.user_agent_rotation)  # 从UA池随机选择
        }

        # 动态调整策略：每处理8个任务更换会话ID（比原来更频繁）
        if self.task_counter % 8 == 0 and self.task_counter > 0:
            self.session_id = self._generate_session_id()
            headers['PDD-Session-ID'] = self.session_id
            # 同时更换设备ID
            if random.random() > 0.3:
                self.device_id = self._generate_device_id()
                headers['PDD-Device-ID'] = self.device_id
        
        self.task_counter += 1
        
        # 随机增加可选头，增强真实性
        optional_headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://mobile.yangkeduo.com',
            'X-Forwarded-For': self.anti_crawler.get_random_ip(),
            'DNT': '1',  # 防追踪标识
            'Upgrade-Insecure-Requests': '1'
        }
        # 随机选择2-4个可选头
        selected_headers = random.sample(list(optional_headers.items()), random.randint(2, 4))
        for k, v in selected_headers:
            headers[k] = v
            
        return headers

    def generate_anti_content(self, keyword, page):
        """增强版anti_content生成（更接近真实加密逻辑）"""
        timestamp = int(time.time() * 1000)
        nonce = ''.join(random.sample('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', 16))  # 更长更复杂
        app_version = random.choice(['6.34.0', '6.35.1', '6.36.2'])  # 模拟不同APP版本
        channel = random.choice(['appstore', 'huawei', 'xiaomi', 'oppo'])  # 模拟不同渠道
        
        # 增加更多真实参数
        data = (
            f"keyword={quote(keyword)}&page={page}&ts={timestamp}&nonce={nonce}"
            f"&device={self.device_id}&version={app_version}&channel={channel}"
            f"&screen=1080x2340&network={random.choice(['wifi', '4g', '5g'])}"
            f"&os={random.choice(['android_12', 'android_13', 'ios_16'])}"
        )
        
        # 多层加密模拟
        salt1 = hashlib.md5(str(timestamp % 3600).encode()).hexdigest()[:8]
        temp = hashlib.sha1((data + salt1).encode()).hexdigest()
        salt2 = 'pdd_' + hashlib.md5(app_version.encode()).hexdigest()[:6]
        sign = hashlib.md5((temp + salt2).encode()).hexdigest()
        
        return f"{sign}_{timestamp}_{nonce}_{self.anti_content_version}_{channel}"

    def parse_search(self, response):
        """解析搜索结果并生成商品任务（增强错误处理）"""
        # 检查请求耗时，异常则更换代理
        request_time = time.time() - response.meta['start_time']
        if request_time > 15:  # 超过15秒视为异常
            self.logger.warning(f"请求耗时过长: {request_time}s，更换代理")
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()

        self.monitor.update_request_stats(success=True)
        keyword = response.meta['keyword']
        current_page = response.meta['page']
        
        try:
            # 多路径提取商品数据脚本
            product_script = None
            script_selectors = [
                'script:contains(window.rawData)',
                'script:contains(initialState)',
                'script:contains(goodsListData)',
                'script[type="application/json"]'
            ]
            for selector in script_selectors:
                product_script = response.css(selector).get()
                if product_script:
                    break
            
            if not product_script:
                self.logger.warning(f"未找到商品数据，重试页面 {current_page}")
                self._handle_parse_failure(response, "未找到商品数据脚本")
                return
            
            # 解析JSON数据（增强容错）
            try:
                # 提取JSON部分（处理各种格式异常）
                start = product_script.find('{')
                end = product_script.rfind('}') + 1
                if start == -1 or end == 0:
                    raise ValueError("未找到JSON结构")
                
                raw_data = product_script[start:end]
                # 替换非法JSON值
                raw_data = re.sub(r'undefined', 'null', raw_data)
                raw_data = re.sub(r'NaN', '0', raw_data)
                raw_data = re.sub(r'inf', '1000000', raw_data)
                data = json.loads(raw_data)
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.error(f"解析数据失败: {e}，尝试备用解析方式")
                self._handle_parse_failure(response, f"数据解析错误: {str(e)}")
                return
            
            # 多路径提取商品列表
            products = []
            data_paths = [
                'props.pageProps.search.goods',
                'goodsList',
                'data.goodsList',
                'searchResult.goods',
                'props.goodsList'
            ]
            for path in data_paths:
                current = data
                for key in path.split('.'):
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        current = None
                        break
                if current and isinstance(current, list):
                    products = current
                    break
            
            if not products:
                self.logger.warning(f"商品列表为空，页面 {current_page}")
                # 检查是否被反爬拦截
                if '验证' in response.text or '安全' in response.text or '请登录' in response.text:
                    self.logger.warning("检测到反爬拦截，强制更换代理和会话")
                    self.proxy_pool.report_failure(response.meta.get('proxy'))
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.session_id = self._generate_session_id()
                return
            
            # 处理商品数据
            for product in products:
                item = ProductItem()
                item['platform'] = 'pdd'
                
                # 提取商品ID（多来源）
                goods_id = product.get('goods_id') or product.get('id') or product.get('item_id')
                if not goods_id:
                    continue
                item['product_id'] = str(goods_id)
                
                # 提取商品名称（清理特殊字符）
                name = product.get('goods_name', '').strip() or product.get('name', '').strip()
                item['name'] = re.sub(r'[\r\n\t]+', ' ', name)
                
                # 价格处理增强
                price_fields = [
                    ('min_group_price', 'formatted_price'),
                    ('max_group_price', 'formatted_price'),
                    ('price', ''),
                    ('sales_price', '')
                ]
                prices = []
                for main_key, sub_key in price_fields:
                    if main_key in product:
                        if sub_key and isinstance(product[main_key], dict) and sub_key in product[main_key]:
                            prices.append(product[main_key][sub_key].strip())
                        else:
                            prices.append(str(product[main_key]).strip())
                
                if prices:
                    unique_prices = list(filter(None, list(set(prices))))
                    if len(unique_prices) >= 2:
                        item['price'] = f"{unique_prices[0]}-{unique_prices[1]}"
                    else:
                        item['price'] = unique_prices[0] if unique_prices else ''
                    item['original_price'] = item['price']
                else:
                    item['price'] = ''
                    item['original_price'] = ''
                
                # 销量处理增强
                sales = product.get('sales', '') or product.get('sales_tip', '').replace('已售', '').replace('件', '').strip()
                # 转换销量格式（如"10万+" -> "100000+"）
                if '万' in sales:
                    sales = sales.replace('万', '0000').replace('+', '')
                item['sales'] = sales
                
                # 店铺信息
                item['shop_name'] = product.get('mall_name', '').strip() or product.get('shop_name', '').strip()
                item['url'] = f"https://mobile.yangkeduo.com/goods.html?goods_id={goods_id}"
                item['category'] = keyword
                item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                self.monitor.update_item_stats('product')
                yield item
                
                # 跟进商品详情页（动态调整概率）
                if goods_id:
                    # 根据错误历史调整爬取概率
                    error_rate = len(self.error_history) / max(self.task_counter, 1)
                    crawl_prob = 0.8 - min(error_rate * 2, 0.5)  # 错误率高则降低爬取概率
                    
                    if random.random() < crawl_prob:
                        # 随机延迟，模拟人类浏览
                        time.sleep(random.uniform(0.3, 1.2))
                        yield scrapy.Request(
                            item['url'],
                            callback=self.parse_product,
                            meta={
                                'item': item, 
                                'render_js': True, 
                                'goods_id': goods_id, 
                                'task_type': 'product',
                                'proxy': self.proxy,
                                'start_time': time.time()
                            },
                            headers=self._get_headers(),
                            priority=2,
                            errback=self.handle_error
                        )
            
            # 处理分页（智能调整）
            if current_page < self.max_pages:
                next_page = current_page + 1
                # 根据当前页商品数量动态调整下一页爬取概率
                page_quality = len(products) / 20  # 预期20个商品
                next_page_prob = 0.7 + min(page_quality - 0.5, 0.3)  # 质量高则提高概率
                
                if random.random() < next_page_prob:
                    # 随机延迟后再生成下一页任务
                    time.sleep(random.uniform(0.5, 1.5))
                    yield self._build_search_request(keyword, next_page)
                
        except Exception as e:
            self.logger.error(f"解析搜索页错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            self.monitor.log_error(str(e), self.name)
            self.error_history.append({
                'time': datetime.now(),
                'type': 'search_parse',
                'message': str(e)
            })
            # 限制错误历史长度
            if len(self.error_history) > 100:
                self.error_history.pop(0)
            
            # 智能重试
            retry_times = response.meta.get('retry_times', 0)
            if retry_times < 3:
                # 指数退避重试
                retry_delay = (2 ** retry_times) + random.random()
                self.logger.info(f"{retry_times+1}次重试页面 {current_page}，延迟 {retry_delay:.2f}s")
                time.sleep(retry_delay)
                yield self._build_search_request(
                    keyword,
                    current_page
                ).replace(meta={** response.meta, 'retry_times': retry_times + 1})

    def _handle_parse_failure(self, response, reason):
        """统一处理解析失败逻辑"""
        self.monitor.update_request_stats(success=False)
        self.monitor.log_error(reason, self.name)
        self.error_history.append({
            'time': datetime.now(),
            'type': 'parse_failure',
            'message': reason
        })
        
        retry_times = response.meta.get('retry_times', 0)
        if retry_times < 3:
            # 更换代理后重试
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()
            self.session_id = self._generate_session_id()  # 同时更换会话
            self.logger.info(f"因{reason}，更换代理并重试第{retry_times+1}次")
            yield self._build_search_request(
                response.meta['keyword'],
                response.meta['page']
            ).replace(
                meta={**response.meta, 'retry_times': retry_times + 1, 'proxy': self.proxy},
                headers=self._get_headers()  # 使用新会话的headers
            )

    def parse_product(self, response):
        """解析商品详情页（增强信息提取和容错）"""
        # 检查请求耗时
        request_time = time.time() - response.meta['start_time']
        if request_time > 15:
            self.logger.warning(f"商品详情请求耗时过长: {request_time}s，更换代理")
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()

        self.monitor.update_request_stats(success=True)
        item = response.meta['item']
        goods_id = response.meta.get('goods_id')
        
        try:
            # 提取价格信息（多来源）
            price_info = None
            price_selectors = [
                'script:contains(initialData)',
                'script:contains(goodsDetail)',
                'script:contains(priceInfo)',
                'div.price-container::text'
            ]
            for selector in price_selectors:
                price_info = response.css(selector).get()
                if price_info:
                    break
            
            if price_info and '<script' in price_info:
                try:
                    start = price_info.find('{')
                    end = price_info.rfind('}') + 1
                    initial_data = json.loads(price_info[start:end].replace('undefined', 'null'))
                    
                    # 多路径提取原价
                    original_price_paths = [
                        'goods.goods_detail.original_price',
                        'goodsDetail.originalPrice',
                        'priceInfo.originalPrice',
                        'data.originalPrice'
                    ]
                    for path in original_price_paths:
                        current = initial_data
                        for key in path.split('.'):
                            if isinstance(current, dict) and key in current:
                                current = current[key]
                            else:
                                current = None
                                break
                        if current:
                            item['original_price'] = str(current).strip()
                            break
                except (json.JSONDecodeError, ValueError) as e:
                    self.logger.error(f"解析价格信息失败: {e}")
            
            # 提取评论数量（多来源）
            comment_count = None
            comment_selectors = [
                '.comment-count::text',
                '.comment-total::text',
                'span:contains(评价)::text',
                'script:contains(commentCount)'
            ]
            for selector in comment_selectors:
                comment_text = response.css(selector).get()
                if comment_text:
                    comment_count = re.search(r'(\d+)', comment_text)
                    if comment_count:
                        comment_count = comment_count.group(1)
                        break
            
            item['comments_count'] = comment_count if comment_count else ''
            
            # 提取店铺ID（增强版）
            shop_id = None
            # 从链接提取
            shop_links = response.css('a[href*="mall_id"]::attr(href)').getall()
            for link in shop_links:
                parsed = urlparse(link)
                shop_id = parse_qs(parsed.query).get('mall_id', [None])[0]
                if shop_id:
                    break
            # 从脚本提取
            if not shop_id:
                shop_script = response.css('script:contains(mall_id)::text').get()
                if shop_script:
                    match = re.search(r'mall_id\s*=\s*(\d+)', shop_script)
                    if match:
                        shop_id = match.group(1)
            
            if shop_id:
                # 生成店铺信息任务
                shop_item = ShopItem()
                shop_item['shop_id'] = shop_id
                shop_item['shop_name'] = item['shop_name']
                shop_item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                yield shop_item
                
                # 店铺评分任务
                yield scrapy.Request(
                    f"https://mobile.yangkeduo.com/mall_page.html?mall_id={shop_id}",
                    callback=self.parse_shop_score,
                    meta={
                        'shop_item': shop_item, 
                        'task_type': 'shop',
                        'proxy': self.proxy,
                        'start_time': time.time()
                    },
                    headers=self._get_headers(),
                    priority=4,
                    errback=self.handle_error
                )
            
            # 生成评论任务
            if goods_id and comment_count and int(comment_count) > 0:
                pages_needed = min(int(comment_count) // 20 + 1, self.max_comments // 20)
                for page in range(1, pages_needed + 1):
                    # 评论URL随机化，增加更多变体
                    comment_urls = [
                        f"https://mobile.yangkeduo.com/proxy/api/comments/list?goods_id={goods_id}&page={page}&size=20",
                        f"https://apiv2.yangkeduo.com/api/turing/v2/comments/list?goods_id={goods_id}&page={page}&size=20",
                        f"https://pddapi.com/api/v1/comments/goods?goods_id={goods_id}&page={page}&limit=20",
                        f"https://mobile.yangkeduo.com/proxy/api/v2/comments/list?goods_id={goods_id}&page={page}&size=20"
                    ]
                    comment_url = random.choice(comment_urls)
                    
                    # 随机延迟，模拟人类行为
                    time.sleep(random.uniform(0.5, 1.5))
                    yield scrapy.Request(
                        comment_url,
                        callback=self.parse_comments,
                        meta={
                            'item': item, 
                            'page': page, 
                            'goods_id': goods_id, 
                            'task_type': 'comment',
                            'proxy': self.proxy,
                            'start_time': time.time()
                        },
                        headers=self._get_headers(),
                        priority=3,
                        errback=self.handle_error
                    )
            
            item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            yield item
            
        except Exception as e:
            self.logger.error(f"解析商品页错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            self.error_history.append({
                'time': datetime.now(),
                'type': 'product_parse',
                'message': str(e)
            })
            if 'item' in response.meta:
                yield response.meta['item']

    def parse_shop_score(self, response):
        """新增店铺评分解析方法"""
        self.monitor.update_request_stats(success=True)
        shop_item = response.meta['shop_item']
        
        try:
            # 提取店铺评分
            score_selectors = {
                'score_service': '.service-score::text',
                'score_delivery': '.delivery-score::text',
                'score_description': '.description-score::text'
            }
            for key, selector in score_selectors.items():
                score_text = response.css(selector).get() or ''
                score = re.search(r'(\d+\.\d+)', score_text)
                if score:
                    shop_item[key] = score.group(1)
            
            # 提取店铺类型和位置
            shop_item['shop_type'] = response.css('.shop-type::text').get(default='').strip()
            shop_item['location'] = response.css('.location::text').get(default='').strip()
            
            # 提取开店时间
            open_time_text = response.css('.open-time::text').get(default='').strip()
            match = re.search(r'(\d{4}-\d{2}-\d{2})', open_time_text)
            if match:
                shop_item['registered_time'] = match.group(1)
            
            yield shop_item
            
        except Exception as e:
            self.logger.error(f"解析店铺评分错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            yield shop_item

    def parse_comments(self, response):
        """完整解析拼多多评论（增强版）"""
        # 检查请求耗时
        request_time = time.time() - response.meta['start_time']
        if request_time > 15:
            self.logger.warning(f"评论请求耗时过长: {request_time}s，更换代理")
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()

        self.monitor.update_request_stats(success=True)
        try:
            item = response.meta['item']
            current_page = response.meta.get('page', 1)
            goods_id = response.meta.get('goods_id')
            
            try:
                # 处理可能的JSONP格式
                response_text = response.text
                if response_text.startswith('jsonp'):
                    response_text = re.sub(r'^jsonp\d+\(', '', response_text).rstrip(')')
                comment_data = json.loads(response_text)
            except json.JSONDecodeError as e:
                self.logger.error(f"解析评论失败: {e}，响应内容: {response.text[:100]}")
                # 重试评论请求（带退避）
                if current_page < 3:
                    retry_delay = 2 **current_page + random.random()
                    time.sleep(retry_delay)
                    yield scrapy.Request(
                        response.url,
                        callback=self.parse_comments,
                        meta=response.meta,
                        headers=self._get_headers(),
                        priority=3,
                        errback=self.handle_error,
                        dont_filter=True
                    )
                return
            
            # 多路径提取评论数据
            comments = []
            comment_paths = [
                'data.comments',
                'comments',
                'data.items',
                'result.comments'
            ]
            for path in comment_paths:
                current = comment_data
                for key in path.split('.'):
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        current = None
                        break
                if current and isinstance(current, list):
                    comments = current
                    break
            
            for comment in comments:
                comment_item = CommentItem()
                comment_item['product_id'] = item['product_id']
                
                # 提取评论ID（多来源）
                comment_id = comment.get('comment_id') or comment.get('id') or comment.get('commentId')
                comment_item['comment_id'] = str(comment_id) if comment_id else ''
                if not comment_item['comment_id']:
                    continue  # 跳过无ID的评论
                
                # 提取用户ID
                user_id = comment.get('user_id') or comment.get('buyer_id') or comment.get('userId')
                comment_item['user_id'] = str(user_id) if user_id else ''
                
                # 提取用户名（处理匿名情况）
                user_name = comment.get('user_name') or comment.get('buyer_name', '').strip() or comment.get('userName', '').strip()
                if not user_name or '匿名' in user_name:
                    user_name = f"匿名用户_{hashlib.md5(str(user_id).encode()).hexdigest()[:6]}"
                comment_item['user_name'] = user_name
                
                # 提取评论内容（清理格式）
                content = comment.get('content') or comment.get('comment', '').strip() or comment.get('contentText', '').strip()
                content = re.sub(r'[\r\n\t]+', ' ', content)
                comment_item['content'] = content
                
                # 提取评分（处理星级转换）
                rating = comment.get('rating') or comment.get('score', '') or comment.get('star', '')
                if isinstance(rating, str) and '星' in rating:
                    rating = re.search(r'(\d+)', rating).group(1) if re.search(r'(\d+)', rating) else ''
                comment_item['rating'] = str(rating) if rating else ''
                
                # 提取评论时间（多格式处理）
                comment_time = comment.get('comment_time') or comment.get('create_time', '') or comment.get('time', '')
                if comment_time:
                    # 处理时间戳
                    if str(comment_time).isdigit():
                        try:
                            # 处理毫秒级时间戳
                            if len(str(comment_time)) >= 10:
                                ts = int(comment_time) // 1000 if len(str(comment_time)) > 10 else int(comment_time)
                                comment_time = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            pass
                    # 处理其他时间格式
                    else:
                        time_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m-%d %H:%M']
                        for fmt in time_formats:
                            try:
                                dt = datetime.strptime(comment_time, fmt)
                                comment_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                                break
                            except ValueError:
                                continue
                comment_item['comment_time'] = comment_time
                
                # 提取有用投票数
                useful_votes = comment.get('useful_votes') or comment.get('like_count', 0) or comment.get('useful', 0)
                comment_item['useful_votes'] = str(useful_votes) if useful_votes else '0'
                
                # 提取回复数
                reply_count = comment.get('reply_count') or comment.get('replyNum', 0) or 0
                comment_item['reply_count'] = str(reply_count) if reply_count else '0'
                
                comment_item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.monitor.update_item_stats('comment')
                yield comment_item
            
            # 自动检测是否有下一页（根据返回数据）
            has_more = False
            more_paths = [
                'data.has_more',
                'has_more',
                'data.hasMore',
                'result.has_more'
            ]
            for path in more_paths:
                current = comment_data
                for key in path.split('.'):
                    if isinstance(current, dict) and key in current:
                        current = current[key]
                    else:
                        current = None
                        break
                if current is not None:
                    has_more = bool(current)
                    break
            
            if has_more and current_page < (self.max_comments // 20):
                next_page = current_page + 1
                # 随机延迟后请求下一页
                time.sleep(random.uniform(0.8, 2.0))
                yield scrapy.Request(
                    response.url.replace(f'page={current_page}', f'page={next_page}'),
                    callback=self.parse_comments,
                    meta={** response.meta, 'page': next_page},
                    headers=self._get_headers(),
                    priority=3,
                    errback=self.handle_error
                )
                
        except Exception as e:
            self.logger.error(f"解析评论错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            self.error_history.append({
                'time': datetime.now(),
                'type': 'comment_parse',
                'message': str(e)
            })

    def handle_error(self, failure):
        """统一错误处理回调（增强版）"""
        request = failure.request
        self.logger.error(f"请求错误: {failure.value}, URL: {request.url}")
        self.monitor.update_request_stats(success=False)
        self.monitor.log_error(str(failure.value), self.name)
        
        # 记录错误历史
        self.error_history.append({
            'time': datetime.now(),
            'type': 'request_error',
            'message': str(failure.value),
            'url': request.url
        })
        
        # 错误处理策略
        retry_times = request.meta.get('retry_times', 0)
        if retry_times < self.settings.get('RETRY_TIMES', 5):
            # 根据错误类型调整策略
            if hasattr(failure.value, 'response'):
                status = failure.value.response.status
                # 403/429通常是反爬，强制更换代理和会话
                if status in [403, 429]:
                    self.logger.warning(f"收到{status}错误，强制更换代理和会话")
                    self.proxy_pool.report_failure(request.meta.get('proxy'))
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.session_id = self._generate_session_id()
                    self.device_id = self._generate_device_id()
                    
                    # 指数退避重试
                    retry_delay = (2 **retry_times) + random.uniform(1, 3)
                    self.logger.info(f"因{status}错误，延迟{retry_delay:.2f}s后重试第{retry_times+1}次")
                    
                    # 构建新请求
                    new_request = request.copy()
                    new_request.meta['retry_times'] = retry_times + 1
                    new_request.meta['proxy'] = self.proxy
                    new_request.headers = self._get_headers()  # 使用新会话headers
                    new_request.dont_filter = True
                    
                    # 使用scrapy的重试中间件调度重试
                    from twisted.internet import reactor
                    from scrapy.utils.reactor import CallLaterOnce
                    CallLaterOnce(new_request.callback, new_request)
            else:
                # 网络错误，简单重试
                self.logger.info(f"网络错误，重试第{retry_times+1}次")
                new_request = request.copy()
                new_request.meta['retry_times'] = retry_times + 1
                new_request.dont_filter = True
                yield new_request
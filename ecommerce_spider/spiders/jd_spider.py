import time
import json
import random
import hashlib
import re
from urllib.parse import urlparse, parse_qs, quote
from datetime import datetime, timedelta

import scrapy
from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings

from ecommerce_spider.items import ProductItem, ShopItem, CommentItem
from utils.anti_crawler import AntiCrawler
from utils.monitor import SpiderMonitor
from utils.proxy_pool import ProxyPool


class JDSpider(RedisSpider):
    """京东分布式爬虫"""
    name = "jd"
    allowed_domains = ["jd.com", "club.jd.com", "shop.jd.com", "list.jd.com", "item.jd.com"]
    redis_key = "jd:start_urls"
    redis_batch_size = 8
    redis_encoding = "utf-8"

    # 增强反爬配置
    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.5,
        'RETRY_TIMES': 6,
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 400, 408, 401],
        'DUPEFILTER_KEY': 'jd:dupefilter:%(timestamp)s',
        'SCHEDULER_PERSIST': True,
        'PROXY_CHECK_INTERVAL': 180,  # 京东代理失效较快，检查间隔缩短
        'DOWNLOAD_TIMEOUT': 20,
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': False,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Referer': 'https://www.jd.com/',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Upgrade-Insecure-Requests': '1'
        }
    }

    def __init__(self, role='worker', *args, **kwargs):
        """初始化京东爬虫"""
        super().__init__(*args, **kwargs)
        self.role = role
        self.settings = get_project_settings()
        self.max_pages = self.settings.get('SPIDERS', {}).get('jd', {}).get('max_pages', 25)
        self.max_comments = self.settings.get('SPIDERS', {}).get('jd', {}).get('max_comments', 100)
        self.anti_crawler = AntiCrawler()
        self.monitor = SpiderMonitor()
        self.proxy_pool = ProxyPool()
        self.worker_id = hashlib.md5(f"{time.time()}-{random.randint(1, 1000000)}".encode()).hexdigest()[:12]
        self._register_worker()
        self.session_id = self._generate_session_id()
        self.device_id = self._generate_device_id()
        self.task_counter = 0
        self.error_history = []
        self.last_proxy_switch_time = time.time()
        self.proxy = None
        self.user_agent_rotation = self._load_user_agents()
        self.jd_cookie_pool = self._load_jd_cookies()  # 京东Cookie池

    def _load_user_agents(self):
        """加载京东专用UA池（PC端+移动端）"""
        ua_list = []
        try:
            with open(self.settings.get('USER_AGENTS_PATH'), 'r') as f:
                for line in f:
                    ua = line.strip()
                    # 优先选择京东相关UA
                    if 'jd' in ua.lower() or '京东' in ua or 'JD' in ua.upper():
                        ua_list.insert(0, ua)  # 京东UA放前面
                    elif 'Mozilla' in ua:
                        ua_list.append(ua)
            return ua_list if ua_list else [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
                'jdapp;android;10.0.2;11;network/wifi;Mozilla/5.0 (Linux; Android 11; SM-G9910 Build/RP1A.200720.012; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/89.0.4389.72 MQQBrowser/6.2 TBS/045913 Mobile Safari/537.36'
            ]
        except Exception as e:
            self.logger.warning(f"加载UA失败: {e}，使用默认UA")
            return [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
            ]

    def _load_jd_cookies(self):
        """加载京东Cookie池（用于轮换）"""
        cookies = []
        try:
            cookie_path = self.settings.get('COOKIES_PATH', 'cookies/jd_cookies.txt')
            with open(cookie_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        cookie_dict = {}
                        for item in line.split(';'):
                            if '=' in item:
                                key, value = item.strip().split('=', 1)
                                cookie_dict[key] = value
                        if cookie_dict:
                            cookies.append(cookie_dict)
            return cookies
        except Exception as e:
            self.logger.warning(f"加载Cookie失败: {e}")
            return []

    def _register_worker(self):
        """注册Worker节点并启动健康检查"""
        self.monitor.register_worker(self.worker_id)
        self.logger.info(f"JD Worker节点注册: {self.worker_id}")
        self._start_health_check()

    def _start_health_check(self):
        """启动健康检查线程（包含代理检查和Cookie更新）"""
        from threading import Thread
        def check():
            while True:
                # 更新Worker活动时间
                self.monitor.redis_conn.set(
                    f"worker:{self.worker_id}:last_active",
                    datetime.now().timestamp()
                )
                
                # 定期切换代理（京东代理失效较快，3分钟切换）
                if time.time() - self.last_proxy_switch_time > 180:
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.last_proxy_switch_time = time.time()
                    self.logger.debug(f"定期切换代理: {self.proxy}")
                
                # 定期清理错误历史（保持最近50条）
                if len(self.error_history) > 50:
                    self.error_history = self.error_history[-50:]
                
                time.sleep(30)
        Thread(target=check, daemon=True).start()

    def _generate_session_id(self):
        """生成京东风格会话ID"""
        timestamp = int(time.time() * 1000)
        random_str = ''.join(random.sample('0123456789abcdef', 16))
        return f"jd_{timestamp}_{random_str}"

    def _generate_device_id(self):
        """生成京东设备指纹（模拟Android/iOS设备）"""
        device_types = ['android', 'ios', 'pc']
        device_type = random.choice(device_types)
        
        if device_type == 'android':
            brands = ['xiaomi', 'huawei', 'oppo', 'vivo', 'samsung']
            models = ['MI 14', 'Mate 60', 'Reno 10', 'X100', 'Galaxy S23']
            brand = random.choice(brands)
            model = random.choice(models)
            imei = ''.join(random.sample('0123456789', 15))
            return f"{brand}_{model}_{imei}"
        elif device_type == 'ios':
            models = ['iPhone15,2', 'iPhone14,5', 'iPhone13,1']
            model = random.choice(models)
            udid = ''.join(random.sample('0123456789ABCDEF', 40))
            return f"Apple_{model}_{udid}"
        else:
            return f"PC_{hashlib.md5(str(random.random()).encode()).hexdigest()[:16]}"

    def start_requests(self):
        """Master/Worker启动逻辑"""
        if self.role == 'master':
            self.logger.info("JD Master节点启动，生成初始任务...")
            start_urls = self.settings.get('SPIDERS', {}).get('jd', {}).get('start_urls', [])
            keywords = []
            if start_urls:
                for url in start_urls:
                    parsed = urlparse(url)
                    query_params = parse_qs(parsed.query)
                    keywords.extend(query_params.get('keyword', []))
                    keywords.extend(query_params.get('key', []))
                    keywords.extend(query_params.get('q', []))
            else:
                keywords = ["手机", "笔记本电脑", "数码相机", "家电", "服装", "美妆", "食品", "家居", "运动户外"]
            
            # 去重和随机化
            seen_keywords = set()
            unique_keywords = []
            for kw in keywords:
                if kw and kw not in seen_keywords:
                    seen_keywords.add(kw)
                    unique_keywords.append(kw)
            
            random.shuffle(unique_keywords)
            for idx, keyword in enumerate(unique_keywords):
                # 模拟人类浏览间隔，逐渐增加延迟
                delay = random.uniform(1.5, 3.0) + idx * 0.15
                time.sleep(delay)
                
                # 计算需要爬取的页数（基于错误率动态调整）
                error_rate = len(self.error_history) / max(self.task_counter, 1)
                adjusted_max_pages = max(3, self.max_pages - int(error_rate * 15))
                
                for page in range(1, min(adjusted_max_pages, self.max_pages) + 1):
                    # 基于页面质量的爬取概率
                    if page == 1:
                        page_prob = 1.0
                    else:
                        page_prob = 0.85 - min((page - 2) * 0.05, 0.35)
                    
                    if random.random() < page_prob:
                        yield self._build_search_request(keyword, page)
        else:
            self.logger.info("JD Worker节点启动，等待Redis任务...")
            self.proxy = self.proxy_pool.get_working_proxy()
            return super().start_requests()

    def _build_search_request(self, keyword, page):
        """构建京东搜索请求（增强反爬参数）"""
        # 京东搜索基础参数
        s = (page - 1) * 30 + 1  # 京东特有的s参数
        
        # 生成京东签名（模拟京东App签名机制）
        sign_data = self._generate_jd_sign(keyword, page)
        
        # 基础URL
        base_urls = [
            f"https://search.jd.com/Search?keyword={quote(keyword)}&page={page}&s={s}",
            f"https://list.jd.com/list.html?keyword={quote(keyword)}&page={page}",
            f"https://so.jd.com/search?keyword={quote(keyword)}&page={page}"
        ]
        url = random.choice(base_urls)
        
        # 添加签名参数
        if sign_data:
            url += f"&_={sign_data['timestamp']}&sign={sign_data['sign']}"
        
        # 添加冗余参数（模拟正常浏览器行为）
        redundant_params = [
            f"psort={random.randint(1, 5)}",  # 排序方式
            f"t={int(time.time())}",
            f"ev={hashlib.md5(str(random.random()).encode()).hexdigest()[:8]}",
            f"uc={random.randint(0, 1)}",
            f"stock={random.randint(0, 1)}",
            f"delivery={random.randint(0, 1)}"
        ]
        url += "&" + "&".join(random.sample(redundant_params, random.randint(2, 4)))
        
        # 添加随机路径参数
        if random.random() > 0.7:
            url += f"&callback=jQuery{random.randint(1000000, 9999999)}"
        
        return scrapy.Request(
            url,
            callback=self.parse_search,
            meta={
                'keyword': keyword,
                'page': page,
                'task_type': 'search',
                'render_js': False,
                'retry_times': 0,
                'proxy': self.proxy,
                'start_time': time.time(),
                'jd_sign': sign_data
            },
            headers=self._get_headers(),
            errback=self.handle_error,
            dont_filter=True,
            cookies=self._get_cookies()  # 随机使用Cookie
        )

    def _generate_jd_sign(self, keyword, page):
        """生成京东签名（模拟京东签名机制）"""
        timestamp = int(time.time() * 1000)
        salt = random.choice(['jd_2018', 'android_jd', 'ios_jd_mobile'])
        
        # 模拟京东签名算法
        sign_str = f"keyword={keyword}&page={page}&timestamp={timestamp}&salt={salt}"
        sign = hashlib.md5(sign_str.encode()).hexdigest()
        
        return {
            'sign': sign,
            'timestamp': timestamp,
            'salt': salt
        }

    def _get_headers(self):
        """生成京东专用请求头（增强反爬）"""
        # 基础头
        headers = {
            'Referer': 'https://www.jd.com/',
            'User-Agent': random.choice(self.user_agent_rotation),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # 京东特有头
        jd_specific_headers = {
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'X-Requested-With': 'XMLHttpRequest' if random.random() > 0.5 else '',
            'X-Forwarded-For': self.anti_crawler.get_random_ip(),
            'Client-IP': self.anti_crawler.get_random_ip(),
            'JD-Device-Id': self.device_id,
            'JD-Session-Id': self.session_id
        }
        
        # 随机添加部分京东特有头
        selected_headers = random.sample(list(jd_specific_headers.items()), random.randint(4, 7))
        for k, v in selected_headers:
            if v:  # 只添加非空值
                headers[k] = v
        
        # 每5个任务更换会话和设备ID
        if self.task_counter % 5 == 0 and self.task_counter > 0:
            self.session_id = self._generate_session_id()
            headers['JD-Session-Id'] = self.session_id
            
            if random.random() > 0.3:
                self.device_id = self._generate_device_id()
                headers['JD-Device-Id'] = self.device_id
        
        self.task_counter += 1
        
        return headers

    def _get_cookies(self):
        """获取京东Cookie（从Cookie池中随机选择）"""
        if self.jd_cookie_pool:
            return random.choice(self.jd_cookie_pool)
        else:
            # 生成基础Cookie
            return {
                '__jda': f'122270672.{int(time.time())}.1.1.{hashlib.md5(str(random.random()).encode()).hexdigest()[:8]}',
                '__jdv': '122270672|direct|-|none|-|' + str(int(time.time())),
                '__jdc': '122270672',
                'areaId': str(random.randint(1, 30)),
                'ipLoc-djd': str(random.randint(1, 9999))
            }

    def parse_search(self, response):
        """解析京东搜索结果（增强容错性）"""
        request_time = time.time() - response.meta['start_time']
        if request_time > 15:
            self.logger.warning(f"请求耗时过长: {request_time}s，更换代理")
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()

        self.monitor.update_request_stats(success=True)
        keyword = response.meta['keyword']
        current_page = response.meta['page']
        
        # 检测京东反爬页面
        if self._detect_anti_crawler(response):
            self.logger.warning(f"检测到京东反爬页面，页面 {current_page}")
            self._handle_anti_crawler(response)
            return
        
        try:
            # 多路径提取商品数据
            products = []
            
            # 方法1: CSS选择器提取
            css_products = response.css('#J_goodsList .gl-item, .gl-i-wrap, .p-img a')
            if css_products:
                products = css_products
            
            # 方法2: 从JSON数据提取（京东有时会内嵌JSON）
            if not products:
                script_data = response.css('script:contains(window.pageConfig)::text, script:contains(pageConfig)::text').get()
                if script_data:
                    try:
                        # 提取JSON数据
                        json_start = script_data.find('{')
                        json_end = script_data.rfind('}') + 1
                        if json_start >= 0 and json_end > json_start:
                            page_config = json.loads(script_data[json_start:json_end].replace("'", '"'))
                            products = page_config.get('wareList', []) or page_config.get('productList', [])
                    except (json.JSONDecodeError, ValueError) as e:
                        self.logger.debug(f"解析内嵌JSON失败: {e}")
            
            # 方法3: 正则匹配商品数据
            if not products:
                product_patterns = [
                    r'data-sku="(\d+)"',
                    r'"wareId":"(\d+)"',
                    r'"sku":"(\d+)"'
                ]
                for pattern in product_patterns:
                    matches = re.findall(pattern, response.text)
                    if matches:
                        products = matches
                        break
            
            if not products:
                self.logger.warning(f"未找到商品数据，页面 {current_page}")
                self._handle_parse_failure(response, "未找到商品数据")
                return
            
            self.logger.info(f"页面 {current_page} 找到 {len(products) if isinstance(products, list) else '多个'} 个商品")
            
            # 处理商品数据
            processed_count = 0
            for product in (products if isinstance(products, list) else [products]):
                try:
                    item = ProductItem()
                    item['platform'] = 'jd'
                    
                    # 提取商品ID
                    if isinstance(product, scrapy.Selector):
                        sku_id = product.css('::attr(data-sku)').get() or \
                                 product.css('::attr(data-id)').get() or \
                                 re.search(r'(\d+)', product.css('::attr(href)').get(default=''))
                        if sku_id:
                            item['product_id'] = sku_id if isinstance(sku_id, str) else sku_id.group(1)
                        else:
                            continue
                    else:
                        item['product_id'] = str(product)
                    
                    # 提取商品名称
                    if isinstance(product, scrapy.Selector):
                        name_selectors = [
                            '.p-name em::text',
                            '.p-name a::attr(title)',
                            '.p-name a em::text',
                            'a::attr(title)'
                        ]
                        for selector in name_selectors:
                            name = product.css(selector).get()
                            if name:
                                item['name'] = name.strip()
                                break
                        else:
                            item['name'] = ''
                    else:
                        item['name'] = ''
                    
                    # 提取价格
                    if isinstance(product, scrapy.Selector):
                        price_selectors = [
                            '.p-price i::text',
                            '.p-price strong i::text',
                            '.price.J_price::text',
                            '.J_price::text'
                        ]
                        for selector in price_selectors:
                            price = product.css(selector).get()
                            if price:
                                item['price'] = price.strip()
                                break
                        else:
                            item['price'] = ''
                    else:
                        item['price'] = ''
                    
                    # 提取原价
                    item['original_price'] = ''
                    if isinstance(product, scrapy.Selector):
                        original_price = product.css('.p-price del::text, .p-price-old i::text').get()
                        if original_price:
                            item['original_price'] = original_price.strip()
                    
                    # 提取销量
                    if isinstance(product, scrapy.Selector):
                        sales_selectors = [
                            '.p-commit strong::text',
                            '.p-commit a::text',
                            '.deal-cnt::text'
                        ]
                        for selector in sales_selectors:
                            sales = product.css(selector).get()
                            if sales:
                                sales_clean = sales.strip().replace('条评价', '').replace('+', '')
                                item['sales'] = sales_clean
                                break
                        else:
                            item['sales'] = ''
                    else:
                        item['sales'] = ''
                    
                    # 店铺信息
                    if isinstance(product, scrapy.Selector):
                        shop_selectors = [
                            '.p-shop a::text',
                            '.p-shop span a::text',
                            '.shop a::text'
                        ]
                        for selector in shop_selectors:
                            shop_name = product.css(selector).get()
                            if shop_name:
                                item['shop_name'] = shop_name.strip()
                                break
                        else:
                            item['shop_name'] = '京东自营' if random.random() > 0.5 else ''
                    else:
                        item['shop_name'] = ''
                    
                    # 其他字段
                    item['url'] = f"https://item.jd.com/{item['product_id']}.html"
                    item['category'] = keyword
                    item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    yield item
                    self.monitor.update_item_stats('product')
                    processed_count += 1
                    
                    # 生成商品详情任务（基于错误率动态控制）
                    error_rate = len(self.error_history) / max(self.task_counter, 1)
                    detail_prob = 0.8 - min(error_rate * 1.5, 0.5)
                    
                    if random.random() < detail_prob:
                        time.sleep(random.uniform(0.5, 1.2))
                        yield scrapy.Request(
                            item['url'],
                            callback=self.parse_product,
                            meta={
                                'item': item,
                                'task_type': 'product',
                                'render_js': False,
                                'proxy': self.proxy,
                                'start_time': time.time(),
                                'retry_times': 0
                            },
                            headers=self._get_headers(),
                            priority=3,
                            errback=self.handle_error,
                            cookies=self._get_cookies()
                        )
                    
                    # 每处理5个商品短暂休息
                    if processed_count % 5 == 0:
                        time.sleep(random.uniform(0.3, 0.7))
                
                except Exception as e:
                    self.logger.debug(f"处理单个商品失败: {e}")
                    continue
            
            # 分页处理（智能判断是否有下一页）
            if current_page < self.max_pages:
                # 计算页面质量（商品数量/预期数量）
                page_quality = processed_count / 30  # 京东每页约30个商品
                
                # 基于页面质量的翻页概率
                if page_quality > 0.7:
                    next_page_prob = 0.9
                elif page_quality > 0.4:
                    next_page_prob = 0.7
                else:
                    next_page_prob = 0.4
                
                # 基于错误率的调整
                error_rate = len(self.error_history) / max(self.task_counter, 1)
                next_page_prob -= min(error_rate * 0.5, 0.3)
                
                if random.random() < next_page_prob:
                    next_page = current_page + 1
                    time.sleep(random.uniform(0.8, 1.8))
                    yield self._build_search_request(keyword, next_page)
            
        except Exception as e:
            self.logger.error(f"解析搜索页错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            self.monitor.log_error(str(e), self.name)
            self.error_history.append({
                'time': datetime.now(),
                'type': 'search_parse',
                'message': str(e),
                'page': current_page
            })
            
            retry_times = response.meta.get('retry_times', 0)
            if retry_times < 3:
                retry_delay = (1.5 ** retry_times) + random.random()
                self.logger.info(f"第{retry_times+1}次重试页面 {current_page}，延迟 {retry_delay:.2f}s")
                time.sleep(retry_delay)
                yield self._build_search_request(
                    keyword,
                    current_page
                ).replace(meta={** response.meta, 'retry_times': retry_times + 1})

    def _detect_anti_crawler(self, response):
        """检测京东反爬机制"""
        anti_crawler_indicators = [
            '验证码',
            '安全验证',
            '访问过于频繁',
            '人机验证',
            '异常访问',
            '滑块验证',
            '请稍后再试',
            'refuse',
            'denied',
            'block'
        ]
        
        for indicator in anti_crawler_indicators:
            if indicator in response.text:
                return True
        
        # 检查HTTP状态码
        if response.status in [403, 429, 503]:
            return True
        
        return False

    def _handle_anti_crawler(self, response):
        """处理京东反爬"""
        self.logger.warning("触发京东反爬机制，执行应对措施")
        
        # 1. 标记代理失败
        if response.meta.get('proxy'):
            self.proxy_pool.report_failure(response.meta['proxy'])
        
        # 2. 更换代理
        self.proxy = self.proxy_pool.get_working_proxy()
        
        # 3. 更换会话和设备ID
        self.session_id = self._generate_session_id()
        self.device_id = self._generate_device_id()
        
        # 4. 记录错误
        self.error_history.append({
            'time': datetime.now(),
            'type': 'anti_crawler',
            'message': '触发京东反爬机制',
            'url': response.url
        })
        
        # 5. 较长延迟后重试
        retry_times = response.meta.get('retry_times', 0)
        if retry_times < 2:
            self.logger.info(f"反爬触发，延迟后重试第{retry_times+1}次")
            
            # 构建新的请求
            new_meta = response.meta.copy()
            new_meta.update({
                'retry_times': retry_times + 1,
                'proxy': self.proxy,
                'start_time': time.time()
            })
            
            # 延迟重试（反爬触发时延迟更长）
            retry_delay = (2 ** retry_times) * 3 + random.uniform(2, 5)
            
            from twisted.internet import reactor
            from scrapy.utils.reactor import CallLaterOnce
            
            def retry_request():
                request = response.request.copy()
                request.meta = new_meta
                request.headers = self._get_headers()
                request.dont_filter = True
                request.callback = self.parse_search
                return request
            
            # 这里简化处理，实际应该使用调度器
            self.logger.info(f"反爬触发，等待{retry_delay:.1f}s后重试")

    def _handle_parse_failure(self, response, reason):
        """处理解析失败"""
        self.monitor.update_request_stats(success=False)
        self.monitor.log_error(reason, self.name)
        self.error_history.append({
            'time': datetime.now(),
            'type': 'parse_failure',
            'message': reason,
            'url': response.url
        })
        
        retry_times = response.meta.get('retry_times', 0)
        if retry_times < 2:
            # 更换代理重试
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()
            self.logger.info(f"因{reason}，更换代理并重试第{retry_times+1}次")
            
            yield self._build_search_request(
                response.meta['keyword'],
                response.meta['page']
            ).replace(
                meta={**response.meta, 'retry_times': retry_times + 1, 'proxy': self.proxy},
                headers=self._get_headers()
            )

    def parse_product(self, response):
        """解析京东商品详情页（增强版）"""
        request_time = time.time() - response.meta['start_time']
        if request_time > 18:
            self.logger.warning(f"商品详情请求耗时过长: {request_time}s，更换代理")
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()

        self.monitor.update_request_stats(success=True)
        item = response.meta['item']
        
        try:
            # 检测反爬
            if self._detect_anti_crawler(response):
                self._handle_anti_crawler(response)
                yield item  # 返回已有数据
                return
            
            # 补充商品核心信息
            
            # 1. 评论数量
            comment_selectors = [
                '#comment-count::text',
                '.comment-count::text',
                'span:contains(评价)::text',
                'a:contains(评价)::text',
                'script:contains(commentCount)::text'
            ]
            
            for selector in comment_selectors:
                comment_text = response.css(selector).get()
                if comment_text:
                    comment_match = re.search(r'(\d+\.?\d*[\u4e00-\u9fa5]?\d*)', comment_text)
                    if comment_match:
                        comment_count = comment_match.group(1)
                        # 处理"万"等单位
                        if '万' in comment_count:
                            comment_count = str(float(comment_count.replace('万', '')) * 10000)
                        item['comments_count'] = comment_count
                        break
            else:
                item['comments_count'] = ''
            
            # 2. 销量信息（如果搜索页没有获取到）
            if not item.get('sales') or item['sales'] == '':
                sales_selectors = [
                    '.sale-num::text',
                    '.sales-volume::text',
                    'span:contains(销量)::text',
                    'div:contains(销量)::text'
                ]
                for selector in sales_selectors:
                    sales_text = response.css(selector).get()
                    if sales_text:
                        sales_match = re.search(r'(\d+\.?\d*[\u4e00-\u9fa5]?\d*)', sales_text)
                        if sales_match:
                            sales = sales_match.group(1)
                            if '万' in sales:
                                sales = str(float(sales.replace('万', '')) * 10000)
                            item['sales'] = sales
                            break
            
            # 3. 规格参数
            spec_items = []
            spec_selectors = [
                '#parameter2 li',
                '.p-parameter li',
                '.parameter-list li',
                '.product-detail-attr li'
            ]
            
            for selector in spec_selectors:
                specs = response.css(selector)
                if specs:
                    for spec in specs:
                        spec_text = ''.join(spec.css('::text').getall()).strip()
                        if spec_text and '：' in spec_text:
                            spec_items.append(spec_text)
                    break
            
            item['specs'] = spec_items if spec_items else []
            
            # 4. 促销信息
            promotion_selectors = [
                '.prom-gifts',
                '.promotion-list',
                '.sales-promotion',
                '.J-promotion'
            ]
            
            promotions = []
            for selector in promotion_selectors:
                promotion_elements = response.css(selector)
                if promotion_elements:
                    for elem in promotion_elements:
                        promo_text = elem.css('::text').get(default='').strip()
                        if promo_text:
                            promotions.append(promo_text)
                    break
            
            item['promotions'] = promotions
            
            # 5. 库存状态
            stock_selectors = [
                '.store-prompt::text',
                '.stock::text',
                '#store-delivery::text',
                '.J_Stock::text'
            ]
            
            for selector in stock_selectors:
                stock_text = response.css(selector).get()
                if stock_text:
                    item['stock'] = stock_text.strip()
                    break
            else:
                item['stock'] = '有货'  # 默认值
            
            # 6. 提取店铺ID和店铺信息
            shop_id = None
            shop_link_selectors = [
                '.seller-info a::attr(href)',
                '.shop-name a::attr(href)',
                '#crumb-wrap a::attr(href)',
                'a[href*="shop.jd.com"]::attr(href)'
            ]
            
            for selector in shop_link_selectors:
                shop_link = response.css(selector).get()
                if shop_link:
                    shop_id = self._extract_shop_id(shop_link)
                    if shop_id:
                        break
            
            # 如果没有提取到店铺ID，尝试从URL参数提取
            if not shop_id:
                parsed_url = urlparse(response.url)
                shop_id = parse_qs(parsed_url.query).get('shopId', [None])[0]
            
            # 创建店铺信息
            if shop_id:
                shop_item = ShopItem()
                shop_item['shop_id'] = str(shop_id)
                shop_item['shop_name'] = item.get('shop_name', '')
                shop_item['platform'] = 'jd'
                shop_item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield shop_item
                
                # 店铺评分任务
                shop_score_urls = [
                    f"https://shop.m.jd.com/rest/shop/scores/{shop_id}",
                    f"https://shop.jd.com/{shop_id}.html",
                    f"https://mall.jd.com/view_search-{shop_id}.html"
                ]
                
                shop_score_url = random.choice(shop_score_urls)
                
                time.sleep(random.uniform(0.5, 1.2))
                yield scrapy.Request(
                    shop_score_url,
                    callback=self.parse_shop_score,
                    meta={
                        'shop_item': shop_item,
                        'task_type': 'shop',
                        'proxy': self.proxy,
                        'start_time': time.time()
                    },
                    headers=self._get_headers(),
                    priority=4,
                    errback=self.handle_error,
                    cookies=self._get_cookies()
                )
            
            # 7. 评论任务（如果评论数大于0）
            if item.get('comments_count') and item['comments_count'] != '':
                try:
                    comment_count = int(float(item['comments_count']))
                    if comment_count > 0:
                        # 计算需要爬取的评论页数
                        pages_needed = min(comment_count // 10, self.max_comments // 10)
                        if pages_needed > 0:
                            # 基于错误率动态控制评论爬取
                            error_rate = len(self.error_history) / max(self.task_counter, 1)
                            comment_page_prob = 0.7 - min(error_rate * 0.8, 0.4)
                            
                            # 随机选择部分评论页（避免全量爬取）
                            selected_pages = random.sample(
                                range(1, pages_needed + 1),
                                max(1, int(pages_needed * comment_page_prob))
                            )
                            
                            for page in selected_pages:
                                comment_url = self._build_comment_url(item['product_id'], page)
                                
                                time.sleep(random.uniform(0.8, 1.5))
                                yield scrapy.Request(
                                    comment_url,
                                    callback=self.parse_comments,
                                    meta={
                                        'item': item,
                                        'page': page,
                                        'task_type': 'comment',
                                        'proxy': self.proxy,
                                        'start_time': time.time()
                                    },
                                    headers=self._get_headers(),
                                    priority=5,
                                    errback=self.handle_error,
                                    cookies=self._get_cookies()
                                )
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"解析评论数失败: {e}")
            
            # 更新商品爬取时间
            item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            yield item
            
        except Exception as e:
            self.logger.error(f"解析商品页错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            self.error_history.append({
                'time': datetime.now(),
                'type': 'product_parse',
                'message': str(e),
                'product_id': item.get('product_id', '')
            })
            
            # 返回已有数据
            yield item

    def _build_comment_url(self, product_id, page):
        """构建评论URL（京东评论接口）"""
        comment_apis = [
            f"https://club.jd.com/comment/productPageComments.action?productId={product_id}&score=0&sortType=5&page={page}&pageSize=10&isShadowSku=0",
            f"https://sclub.jd.com/comment/productPageComments.action?productId={product_id}&score=0&sortType=5&page={page}&pageSize=10",
            f"https://club.jd.com/discussion/getProductPageImageCommentList.action?productId={product_id}&page={page}&pageSize=10"
        ]
        
        # 随机添加冗余参数
        base_url = random.choice(comment_apis)
        redundant_params = [
            f"callback=jQuery{random.randint(1000000, 9999999)}",
            f"_{int(time.time())}",
            f"t={random.randint(0, 1)}",
            f"pageFlag={random.randint(0, 1)}"
        ]
        
        return base_url + "&" + "&".join(random.sample(redundant_params, random.randint(1, 2)))

    def parse_shop_score(self, response):
        """解析京东店铺评分（增强版）"""
        self.monitor.update_request_stats(success=True)
        shop_item = response.meta['shop_item']
        
        try:
            # 检测反爬
            if self._detect_anti_crawler(response):
                self.logger.warning("店铺评分页面触发反爬")
                yield shop_item
                return
            
            # 尝试解析JSON格式的评分数据
            if response.url.endswith('.html'):
                # HTML页面解析
                score_selectors = {
                    'score_service': '.score-service .score-num::text',
                    'score_delivery': '.score-delivery .score-num::text',
                    'score_description': '.score-description .score-num::text'
                }
                
                for key, selector in score_selectors.items():
                    score_text = response.css(selector).get(default='')
                    if score_text:
                        score_match = re.search(r'(\d+\.?\d*)', score_text)
                        if score_match:
                            shop_item[key] = score_match.group(1)
                
                # 店铺其他信息
                shop_item['location'] = response.css('.shop-location::text').get(default='').strip()
                shop_item['shop_type'] = response.css('.shop-auth a::text').get(default='').strip()
                shop_item['registered_time'] = response.css('.open-time::text').get(default='').strip()
                
            else:
                # JSON数据解析
                try:
                    data = json.loads(response.text)
                    
                    # 不同接口的数据结构
                    if 'shopScore' in data:
                        score_data = data['shopScore']
                        shop_item['score_service'] = str(score_data.get('serviceScore', ''))
                        shop_item['score_delivery'] = str(score_data.get('deliveryScore', ''))
                        shop_item['score_description'] = str(score_data.get('descriptionScore', ''))
                    elif 'data' in data:
                        score_data = data['data']
                        shop_item['score_service'] = str(score_data.get('serviceScore', ''))
                        shop_item['score_delivery'] = str(score_data.get('deliveryScore', ''))
                        shop_item['score_description'] = str(score_data.get('itemScore', ''))
                except json.JSONDecodeError:
                    self.logger.debug(f"店铺评分JSON解析失败: {response.text[:100]}")
            
            # 默认值处理
            for key in ['score_service', 'score_delivery', 'score_description']:
                if key not in shop_item or not shop_item[key]:
                    shop_item[key] = '4.8'  # 京东店铺平均分
            
            yield shop_item
            
        except Exception as e:
            self.logger.error(f"解析店铺评分错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            yield shop_item

    def parse_comments(self, response):
        """完整解析京东评论（修复版）"""
        self.monitor.update_request_stats(success=True)
        item = response.meta['item']
        current_page = response.meta['page']
        
        try:
            # 检测反爬
            if self._detect_anti_crawler(response):
                self.logger.warning("评论页面触发反爬")
                return
            
            # 提取原始数据（京东评论可能是JSONP格式）
            raw_data = response.text.strip()
            if not raw_data:
                self.logger.warning(f"评论页面为空，商品ID: {item['product_id']}, 页码: {current_page}")
                return
            
            # 处理JSONP格式
            if raw_data.startswith('jQuery') or raw_data.startswith('fetchJSON'):
                # 提取JSON部分
                json_start = raw_data.find('(') + 1
                json_end = raw_data.rfind(')')
                if json_start > 0 and json_end > json_start:
                    raw_data = raw_data[json_start:json_end]
            
            # 解析JSON数据
            try:
                comment_data = json.loads(raw_data)
            except json.JSONDecodeError as e:
                self.logger.error(f"评论JSON解析失败: {e}, 原始数据: {raw_data[:200]}")
                return
            
            # 提取评论列表
            comments = []
            if 'comments' in comment_data:
                comments = comment_data['comments']
            elif 'data' in comment_data and 'comments' in comment_data['data']:
                comments = comment_data['data']['comments']
            elif 'productCommentSummary' in comment_data:
                # 可能是评论摘要
                pass
            
            if not comments:
                self.logger.info(f"该页无评论数据，商品ID: {item['product_id']}, 页码: {current_page}")
                return
            
            # 解析每条评论
            for comment in comments:
                comment_item = CommentItem()
                comment_item['product_id'] = item['product_id']
                comment_item['comment_id'] = str(comment.get('id', ''))
                comment_item['user_id'] = str(comment.get('userId', ''))
                comment_item['user_name'] = comment.get('nickname', '匿名用户').strip()
                
                # 评论内容
                content = comment.get('content', '').strip()
                if not content:
                    content = comment.get('commentData', {}).get('content', '').strip()
                
                # 处理图片
                images = comment.get('images', []) or comment.get('imgUrls', [])
                if images:
                    image_count = len(images) if isinstance(images, list) else 0
                    content += f" [图片:{image_count}张]"
                
                # 处理视频
                videos = comment.get('videos', [])
                if videos:
                    video_count = len(videos) if isinstance(videos, list) else 0
                    content += f" [视频:{video_count}个]"
                
                comment_item['content'] = content
                
                # 评分
                rating = comment.get('score', '') or comment.get('grade', '')
                comment_item['rating'] = str(rating) if rating else ''
                
                # 评论时间
                comment_time = comment.get('creationTime', '') or comment.get('commentTime', '')
                comment_item['comment_time'] = comment_time.strip()
                
                # 有用数
                useful_votes = comment.get('usefulVoteCount', 0) or comment.get('useful', 0)
                comment_item['useful_votes'] = self._clean_number(useful_votes)
                
                # 回复数
                reply_count = comment.get('replyCount', 0) or comment.get('reply', 0)
                comment_item['reply_count'] = self._clean_number(reply_count)
                
                # 其他信息
                comment_item['user_level'] = comment.get('userLevel', '')
                comment_item['user_client'] = comment.get('userClient', '')
                comment_item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield comment_item
                self.monitor.update_item_stats('comment')
            
            # 生成下一页评论任务
            total_pages = comment_data.get('maxPage', 0) or \
                         comment_data.get('productCommentSummary', {}).get('commentCount', 0) // 10
            
            if current_page < total_pages and current_page < self.max_comments // 10:
                next_page = current_page + 1
                error_rate = len(self.error_history) / max(self.task_counter, 1)
                next_page_prob = 0.6 - min(error_rate * 0.5, 0.3)
                
                if random.random() < next_page_prob:
                    next_comment_url = self._build_comment_url(item['product_id'], next_page)
                    
                    time.sleep(random.uniform(1.0, 2.0))
                    yield scrapy.Request(
                        next_comment_url,
                        callback=self.parse_comments,
                        meta={
                            'item': item,
                            'page': next_page,
                            'task_type': 'comment',
                            'proxy': self.proxy,
                            'start_time': time.time()
                        },
                        headers=self._get_headers(),
                        priority=6,
                        errback=self.handle_error,
                        cookies=self._get_cookies()
                    )
                    
        except json.JSONDecodeError as e:
            self.logger.error(f"评论JSON解析失败: {e}, 响应内容: {response.text[:200]}")
            self.monitor.log_error(f"评论解析JSON错误: {str(e)}", self.name)
        except Exception as e:
            self.logger.error(f"解析评论页错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)
            self.error_history.append({
                'time': datetime.now(),
                'type': 'comment_parse',
                'message': str(e)
            })

    def _extract_shop_id(self, shop_link):
        """从店铺链接提取店铺ID（增强版）"""
        if not shop_link:
            return None
        
        try:
            parsed = urlparse(shop_link)
            
            # 处理不同格式的店铺链接
            if 'shop.jd.com' in parsed.netloc:
                # https://shop.jd.com/10000001.html
                path_parts = parsed.path.strip('/').split('.')
                if path_parts:
                    return path_parts[0]
            
            elif 'mall.jd.com' in parsed.netloc:
                # https://mall.jd.com/index-10000001.html
                match = re.search(r'index-(\d+)', parsed.path)
                if match:
                    return match.group(1)
                
                # 从查询参数提取
                shop_id = parse_qs(parsed.query).get('shopId', [None])[0]
                if shop_id:
                    return shop_id
            
            elif parsed.path.startswith('/shop/viewShop'):
                # 旧版店铺链接
                shop_id = parse_qs(parsed.query).get('shopId', [None])[0]
                if shop_id:
                    return shop_id
            
            # 从路径中提取数字ID
            match = re.search(r'/(\d+)(?:\.html|/|$)', parsed.path)
            if match:
                return match.group(1)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"提取店铺ID失败: {e}")
            return None

    def _clean_number(self, value):
        """清洗数字格式（处理万/千等单位）"""
        if value is None:
            return '0'
        
        if isinstance(value, str):
            value = value.strip()
            # 处理中文单位
            if '万' in value:
                try:
                    num = float(value.replace('万', ''))
                    return str(int(num * 10000))
                except ValueError:
                    return value.replace('万', '0000')
            elif '千' in value:
                try:
                    num = float(value.replace('千', ''))
                    return str(int(num * 1000))
                except ValueError:
                    return value.replace('千', '000')
            elif '亿' in value:
                try:
                    num = float(value.replace('亿', ''))
                    return str(int(num * 100000000))
                except ValueError:
                    return value.replace('亿', '00000000')
            else:
                # 移除非数字字符
                cleaned = re.sub(r'[^\d\.]', '', value)
                return cleaned if cleaned else '0'
        else:
            return str(value)

    def handle_error(self, failure):
        """统一错误处理（增强版）"""
        request = failure.request
        error_msg = str(failure.value)
        
        self.logger.error(f"请求错误: {error_msg}, URL: {request.url}")
        self.monitor.update_request_stats(success=False)
        self.monitor.log_error(error_msg, self.name)
        
        self.error_history.append({
            'time': datetime.now(),
            'type': 'request_error',
            'message': error_msg,
            'url': request.url
        })
        
        # 代理失败处理
        proxy = request.meta.get('proxy')
        if proxy:
            self.proxy_pool.report_failure(proxy)
        
        # 重试逻辑
        retry_times = request.meta.get('retry_times', 0)
        max_retries = self.settings.get('RETRY_TIMES', 6)
        
        if retry_times < max_retries:
            # 分析错误类型
            if hasattr(failure.value, 'response'):
                status_code = failure.value.response.status
                
                if status_code in [403, 429, 401]:
                    self.logger.warning(f"收到{status_code}错误，强制更换代理和会话")
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.session_id = self._generate_session_id()
                    self.device_id = self._generate_device_id()
                    
                    retry_delay = (2 ** retry_times) * 2 + random.uniform(1, 3)
                    self.logger.info(f"因{status_code}错误，延迟{retry_delay:.2f}s后重试第{retry_times+1}次")
                    
                elif status_code in [500, 502, 503, 408]:
                    retry_delay = (1.5 ** retry_times) + random.random()
                    self.logger.info(f"因{status_code}错误，延迟{retry_delay:.2f}s后重试第{retry_times+1}次")
                
                else:
                    retry_delay = random.uniform(1, 2)
            else:
                # 网络错误
                retry_delay = (1.5 ** retry_times) + random.uniform(0.5, 1.5)
                self.logger.info(f"网络错误，延迟{retry_delay:.2f}s后重试第{retry_times+1}次")
            
            # 构建重试请求
            new_request = request.copy()
            new_meta = request.meta.copy()
            new_meta.update({
                'retry_times': retry_times + 1,
                'proxy': self.proxy,
                'start_time': time.time()
            })
            
            new_request.meta = new_meta
            new_request.headers = self._get_headers()
            new_request.dont_filter = True
            
            # 延迟执行
            from twisted.internet import reactor
            reactor.callLater(retry_delay, lambda: self.crawler.engine.crawl(new_request, spider=self))
            
        else:
            self.logger.warning(f"达到最大重试次数{max_retries}，放弃请求: {request.url}")
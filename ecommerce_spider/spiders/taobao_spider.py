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
from utils.proxy_pool import ProxyPool

class TaobaoSpider(RedisSpider):
    """淘宝分布式爬虫"""
    name = "taobao"
    allowed_domains = ["taobao.com", "tmall.com", "rate.tmall.com"]
    redis_key = "taobao:start_urls"
    redis_batch_size = 5
    redis_encoding = "utf-8"

    # 增强反爬配置
    custom_settings = {
        'DOWNLOAD_DELAY': 3.0,  # 淘宝反爬严格，延迟更高
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.2,
        'RETRY_TIMES': 6,
        'RETRY_HTTP_CODES': [403, 429, 500, 502, 503, 400, 408, 401],
        'DUPEFILTER_KEY': 'taobao:dupefilter:%(timestamp)s',
        'SCHEDULER_PERSIST': True,
        'PROXY_CHECK_INTERVAL': 200,
        'DOWNLOAD_TIMEOUT': 25,
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': False,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Referer': 'https://www.taobao.com/',
        }
    }

    def __init__(self, role='worker', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.role = role
        self.settings = get_project_settings()
        self.max_pages = self.settings.get('SPIDERS', {}).get('taobao', {}).get('max_pages', 8)
        self.max_comments = self.settings.get('SPIDERS', {}).get('taobao', {}).get('max_comments', 40)
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

    def _load_user_agents(self):
        """加载淘宝专用UA池（移动端+PC端）"""
        ua_list = []
        try:
            with open(self.settings.get('USER_AGENTS_PATH'), 'r') as f:
                for line in f:
                    ua = line.strip()
                    if 'Taobao' in ua or 'Tmall' in ua or 'Alipay' in ua or 'Mobile' in ua:
                        ua_list.append(ua)
            return ua_list if ua_list else [
                'Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Taobao/10.20.0',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 TmallBrowser/9.0'
            ]
        except Exception as e:
            self.logger.warning(f"加载UA失败: {e}，使用默认UA")
            return [
                'Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Taobao/10.20.0'
            ]

    def _register_worker(self):
        """注册Worker节点并启动健康检查"""
        self.monitor.register_worker(self.worker_id)
        self.logger.info(f"Taobao Worker节点注册: {self.worker_id}")
        self._start_health_check()

    def _start_health_check(self):
        """启动健康检查线程"""
        from threading import Thread
        def check():
            while True:
                self.monitor.redis_conn.set(f"worker:{self.worker_id}:last_active", datetime.now().timestamp())
                if time.time() - self.last_proxy_switch_time > 200:
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.last_proxy_switch_time = time.time()
                time.sleep(25)
        Thread(target=check, daemon=True).start()

    def _generate_session_id(self):
        """生成淘宝风格会话ID"""
        base = f"tb_{int(time.time())}_{random.getrandbits(32)}_ios"
        return hashlib.md5(base.encode()).hexdigest()

    def _generate_device_id(self):
        """生成淘宝设备指纹"""
        manufacturers = ['apple', 'huawei', 'xiaomi', 'oppo', 'vivo']
        models = ['iphone14', 'mate60', 'mi14', 'reno10', 'x100']
        manufacturer = random.choice(manufacturers)
        model = random.choice(models)
        serial = ''.join(random.sample('0123456789ABCDEF', 16))
        return f"{manufacturer}-{model}-{serial}"

    def start_requests(self):
        """Master/Worker启动逻辑"""
        if self.role == 'master':
            self.logger.info("Taobao Master节点启动，生成初始任务...")
            start_urls = self.settings.get('SPIDERS', {}).get('taobao', {}).get('start_urls', [])
            keywords = []
            if start_urls:
                for url in start_urls:
                    parsed = urlparse(url)
                    keywords.append(parse_qs(parsed.query).get('q', [''])[0])
            else:
                keywords = ["手机", "电脑", "服装", "美妆", "家居", "零食", "数码"]
            
            seen_keywords = set()
            unique_keywords = []
            for kw in keywords:
                if kw and kw not in seen_keywords:
                    seen_keywords.add(kw)
                    unique_keywords.append(kw)
            
            random.shuffle(unique_keywords)
            for idx, keyword in enumerate(unique_keywords):
                delay = random.uniform(2.0, 4.0) + idx * 0.2
                time.sleep(delay)
                for page in range(1, self.max_pages + 1):
                    if random.random() > 0.15:
                        yield self._build_search_request(keyword, page)
        else:
            self.logger.info("Taobao Worker节点启动，等待Redis任务...")
            self.proxy = self.proxy_pool.get_working_proxy()
            return super().start_requests()

    def _build_search_request(self, keyword, page):
        """构建淘宝搜索请求（增强反爬参数）"""
        sign = self.generate_sign(keyword, page)
        url = f"https://s.taobao.com/search?q={quote(keyword)}&page={page}&sign={sign}"
        
        # 淘宝搜索路径随机化
        path_choices = ["search", "s", "list"]
        selected_path = random.choice(path_choices)
        url = url.replace("s.taobao.com/search", f"s.taobao.com/{selected_path}")
        
        # 冗余参数
        redundant_params = [
            f"t={int(time.time())}",
            f"traceid={hashlib.md5(str(random.random()).encode()).hexdigest()[:16]}",
            f"imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index"
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
                'proxy': self.proxy,
                'start_time': time.time()
            },
            headers=self._get_headers(),
            errback=self.handle_error,
            dont_filter=True
        )

    def _get_headers(self):
        """生成淘宝专用请求头"""
        headers = {
            'Referer': 'https://www.taobao.com/',
            'X-Requested-With': 'XMLHttpRequest',
            'tb-session-id': self.session_id,
            'tb-device-id': self.device_id,
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'User-Agent': random.choice(self.user_agent_rotation)
        }

        # 每6个任务更换会话ID
        if self.task_counter % 6 == 0 and self.task_counter > 0:
            self.session_id = self._generate_session_id()
            headers['tb-session-id'] = self.session_id
            if random.random() > 0.25:
                self.device_id = self._generate_device_id()
                headers['tb-device-id'] = self.device_id
        
        self.task_counter += 1
        
        # 淘宝特有头
        optional_headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Origin': 'https://www.taobao.com',
            'X-Forwarded-For': self.anti_crawler.get_random_ip(),
            'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        }
        selected_headers = random.sample(list(optional_headers.items()), random.randint(3, 5))
        for k, v in selected_headers:
            headers[k] = v
            
        return headers

    def generate_sign(self, keyword, page):
        """淘宝签名生成（增强版）"""
        timestamp = int(time.time() * 1000)
        nonce = ''.join(random.sample('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', 20))
        app_version = random.choice(['10.20.0', '10.21.1', '10.22.2'])
        
        data = (
            f"q={quote(keyword)}&page={page}&ts={timestamp}&nonce={nonce}"
            f"&device={self.device_id}&version={app_version}&platform=ios"
            f"&network={random.choice(['wifi', '4g', '5g'])}&channel=appstore"
        )
        
        salt1 = hashlib.md5(str(timestamp % 3600).encode()).hexdigest()[:10]
        temp = hashlib.sha256((data + salt1).encode()).hexdigest()
        salt2 = 'tb_' + hashlib.md5(app_version.encode()).hexdigest()[:8]
        sign = hashlib.md5((temp + salt2).encode()).hexdigest()
        
        return f"{sign}_{timestamp}_{nonce}"

    def parse_search(self, response):
        """解析淘宝搜索结果"""
        request_time = time.time() - response.meta['start_time']
        if request_time > 20:
            self.logger.warning(f"请求耗时过长: {request_time}s，更换代理")
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()

        self.monitor.update_request_stats(success=True)
        keyword = response.meta['keyword']
        current_page = response.meta['page']
        
        try:
            # 多路径提取商品数据
            product_script = None
            script_selectors = [
                'script:contains(g_page_config)',
                'script:contains(initialData)',
                'script:contains(itemList)',
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
            
            # 解析淘宝复杂的JSON结构
            try:
                # 提取g_page_config
                start = product_script.find('g_page_config = ') + len('g_page_config = ')
                end = product_script.find('};', start) + 1
                raw_data = product_script[start:end]
                raw_data = re.sub(r'undefined', 'null', raw_data)
                raw_data = re.sub(r'NaN', '0', raw_data)
                data = json.loads(raw_data)
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.error(f"解析数据失败: {e}，尝试备用解析方式")
                self._handle_parse_failure(response, f"数据解析错误: {str(e)}")
                return
            
            # 多路径提取商品列表
            products = []
            data_paths = [
                'mods.itemlist.data.auctions',
                'data.items',
                'auctions',
                'itemList'
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
                if '验证' in response.text or '安全中心' in response.text:
                    self.logger.warning("检测到反爬拦截，强制更换代理和会话")
                    self.proxy_pool.report_failure(response.meta.get('proxy'))
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.session_id = self._generate_session_id()
                return
            
            # 处理商品数据
            for product in products:
                item = ProductItem()
                item['platform'] = 'taobao'
                
                # 提取商品ID
                goods_id = product.get('nid') or product.get('item_id') or product.get('id')
                if not goods_id:
                    continue
                item['product_id'] = str(goods_id)
                
                # 商品名称处理
                name = product.get('title', '').strip() or product.get('raw_title', '').strip()
                item['name'] = re.sub(r'[\r\n\t]+', ' ', name)
                
                # 价格处理
                price_fields = [
                    'view_price', 'price', 'real_price', 'zk_price'
                ]
                prices = []
                for field in price_fields:
                    if field in product and product[field]:
                        prices.append(str(product[field]).strip())
                
                if prices:
                    unique_prices = list(filter(None, list(set(prices))))
                    if len(unique_prices) >= 2:
                        item['price'] = f"{unique_prices[0]}-{unique_prices[1]}"
                    else:
                        item['price'] = unique_prices[0] if unique_prices else ''
                    item['original_price'] = product.get('reserve_price', item['price'])
                else:
                    item['price'] = ''
                    item['original_price'] = ''
                
                # 销量处理
                sales = product.get('sales', '') or product.get('deal_cnt', '').strip()
                if '万' in sales:
                    sales = sales.replace('万', '0000').replace('+', '')
                item['sales'] = sales
                
                # 店铺信息
                item['shop_name'] = product.get('nick', '').strip() or product.get('shop_name', '').strip()
                item['url'] = f"https://item.taobao.com/item.htm?id={goods_id}"
                item['category'] = keyword
                item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                self.monitor.update_item_stats('product')
                yield item
                
                # 跟进商品详情页
                seller_id = product.get('seller_id') or product.get('user_id')
                if goods_id and seller_id:
                    error_rate = len(self.error_history) / max(self.task_counter, 1)
                    crawl_prob = 0.75 - min(error_rate * 2, 0.45)
                    
                    if random.random() < crawl_prob:
                        time.sleep(random.uniform(0.5, 1.5))
                        yield scrapy.Request(
                            item['url'],
                            callback=self.parse_product,
                            meta={
                                'item': item, 
                                'render_js': True, 
                                'goods_id': goods_id,
                                'seller_id': seller_id,
                                'task_type': 'product',
                                'proxy': self.proxy,
                                'start_time': time.time()
                            },
                            headers=self._get_headers(),
                            priority=2,
                            errback=self.handle_error
                        )
            
            # 分页处理
            if current_page < self.max_pages:
                next_page = current_page + 1
                page_quality = len(products) / 40  # 淘宝每页约40个商品
                next_page_prob = 0.65 + min(page_quality - 0.5, 0.35)
                
                if random.random() < next_page_prob:
                    time.sleep(random.uniform(0.8, 1.8))
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
            if len(self.error_history) > 100:
                self.error_history.pop(0)
            
            retry_times = response.meta.get('retry_times', 0)
            if retry_times < 4:
                retry_delay = (2 ** retry_times) + random.random()
                self.logger.info(f"{retry_times+1}次重试页面 {current_page}，延迟 {retry_delay:.2f}s")
                time.sleep(retry_delay)
                yield self._build_search_request(
                    keyword,
                    current_page
                ).replace(meta={** response.meta, 'retry_times': retry_times + 1})

    def _handle_parse_failure(self, response, reason):
        """处理解析失败"""
        self.monitor.update_request_stats(success=False)
        self.monitor.log_error(reason, self.name)
        self.error_history.append({
            'time': datetime.now(),
            'type': 'parse_failure',
            'message': reason
        })
        
        retry_times = response.meta.get('retry_times', 0)
        if retry_times < 4:
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()
            self.session_id = self._generate_session_id()
            self.logger.info(f"因{reason}，更换代理并重试第{retry_times+1}次")
            yield self._build_search_request(
                response.meta['keyword'],
                response.meta['page']
            ).replace(
                meta={**response.meta, 'retry_times': retry_times + 1, 'proxy': self.proxy},
                headers=self._get_headers()
            )

    def parse_product(self, response):
        """解析淘宝商品详情页"""
        request_time = time.time() - response.meta['start_time']
        if request_time > 20:
            self.logger.warning(f"商品详情请求耗时过长: {request_time}s，更换代理")
            self.proxy_pool.report_failure(response.meta.get('proxy'))
            self.proxy = self.proxy_pool.get_working_proxy()

        self.monitor.update_request_stats(success=True)
        item = response.meta['item']
        goods_id = response.meta.get('goods_id')
        seller_id = response.meta.get('seller_id')
        
        try:
            # 提取价格和库存信息
            price_info = None
            price_selectors = [
                'script:contains(price)',
                'script:contains(skuPrice)',
                'div.price J_price::text',
                'span.price::text'
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
                    
                    original_price_paths = [
                        'price.originalPrice',
                        'reservePrice',
                        'skuBase.price',
                        'itemInfo.price'
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
            
            # 评论数量
            comment_count = None
            comment_selectors = [
                '.J_RateCounter::text',
                '.comment-count::text',
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
            
            # 店铺信息
            shop_id = seller_id
            if shop_id:
                shop_item = ShopItem()
                shop_item['shop_id'] = str(shop_id)
                shop_item['shop_name'] = item['shop_name']
                shop_item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                yield shop_item
                
                # 店铺评分
                yield scrapy.Request(
                    f"https://shop{shop_id}.taobao.com/",
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
            
            # 评论任务
            if goods_id and seller_id and comment_count and int(comment_count) > 0:
                pages_needed = min(int(comment_count) // 20 + 1, self.max_comments // 20)
                for page in range(1, pages_needed + 1):
                    comment_urls = [
                        f"https://rate.tmall.com/list_detail_rate.htm?itemId={goods_id}&sellerId={seller_id}&currentPage={page}",
                        f"https://comment.taobao.com/feedRateList.htm?auctionNumId={goods_id}&sellerId={seller_id}&currentPage={page}"
                    ]
                    comment_url = random.choice(comment_urls)
                    
                    time.sleep(random.uniform(0.8, 1.8))
                    yield scrapy.Request(
                        comment_url,
                        callback=self.parse_comments,
                        meta={
                            'item': item, 
                            'seller_id': seller_id,
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
        """解析淘宝店铺评分"""
        self.monitor.update_request_stats(success=True)
        shop_item = response.meta['shop_item']
        
        try:
            # 提取店铺评分
            score_selectors = {
                'score_service': '.service-score::text',
                'score_delivery': '.delivery-score::text',
                'score_description': '.desc-score::text'
            }
            for key, selector in score_selectors.items():
                score_text = response.css(selector).get() or ''
                score = re.search(r'(\d+\.\d+)', score_text)
                if score:
                    shop_item[key] = score.group(1)
            
            # 店铺类型和信誉
            shop_item['shop_type'] = response.css('.shop-type::text').get(default='').strip()
            shop_item['credit_level'] = response.css('.credit-level::text').get(default='').strip()
            
            # 开店时间
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
        """完整解析淘宝评论（修复版）"""
        self.monitor.update_request_stats(success=True)
        item = response.meta['item']
        seller_id = response.meta['seller_id']
        current_page = response.meta['page']
        
        try:
            # 提取评论数据（淘宝评论为JSONP格式，需处理）
            raw_data = response.text.strip()
            if not raw_data:
                self.logger.warning(f"评论页面为空，商品ID: {item['product_id']}, 页码: {current_page}")
                return
            
            # 处理JSONP格式（移除前后包裹的函数调用）
            if raw_data.startswith('jsonp'):
                start = raw_data.find('(') + 1
                end = raw_data.rfind(')')
                raw_data = raw_data[start:end]
            
            comment_data = json.loads(raw_data)
            comments = comment_data.get('rateDetail', {}).get('rateList', [])
            
            if not comments:
                self.logger.info(f"该页无评论数据，商品ID: {item['product_id']}, 页码: {current_page}")
                return
            
            # 解析每条评论
            for comment in comments:
                comment_item = CommentItem()
                comment_item['product_id'] = item['product_id']
                comment_item['comment_id'] = str(comment.get('id', ''))
                comment_item['user_id'] = str(comment.get('user', {}).get('id', ''))
                comment_item['user_name'] = comment.get('user', {}).get('nick', '匿名用户').strip()
                
                # 评论内容处理（合并图文）
                content = comment.get('content', '').strip()
                pics = comment.get('pics', [])
                if pics:
                    content += f" [图片数量: {len(pics)}]"
                comment_item['content'] = content
                
                # 评分处理（淘宝评分1-5分）
                comment_item['rating'] = str(comment.get('grade', ''))
                comment_item['comment_time'] = comment.get('date', '').strip()
                comment_item['useful_votes'] = self._clean_number(comment.get('useful', 0))
                comment_item['reply_count'] = self._clean_number(comment.get('replyCount', 0))
                comment_item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                yield comment_item
                self.monitor.update_item_stats('comment')
            
            # 生成下一页评论任务
            total_pages = comment_data.get('rateDetail', {}).get('paginator', {}).get('lastPage', 0)
            if current_page < total_pages and current_page < self.max_comments // 20:
                next_page = current_page + 1
                next_comment_url = (
                    f"https://rate.tmall.com/list_detail_rate.htm?itemId={item['product_id']}"
                    f"&sellerId={seller_id}&currentPage={next_page}"
                )
                # 随机延迟避免反爬
                time.sleep(random.uniform(0.8, 1.5))
                yield scrapy.Request(
                    next_comment_url,
                    callback=self.parse_comments,
                    meta={
                        'item': item,
                        'seller_id': seller_id,
                        'page': next_page,
                        'task_type': 'comment',
                        'render_js': False,
                        'proxy': self.proxy
                    },
                    headers=self._get_headers(),
                    priority=6,
                    errback=self.handle_error
                )
                
        except json.JSONDecodeError as e:
            self.logger.error(f"评论JSON解析失败: {e}, 响应内容: {raw_data[:100]}")
            self.monitor.log_error(f"评论解析JSON错误: {str(e)}", self.name)
        except Exception as e:
            self.logger.error(f"解析评论页错误: {e}", exc_info=True)
            self.monitor.update_request_stats(success=False)

    def _clean_number(self, num):
        """清洗数字（处理万/千单位）"""
        if not num:
            return '0'
        num_str = str(num).strip()
        if '万' in num_str:
            num_str = num_str.replace('万', '0000').replace('+', '')
        elif '千' in num_str:
            num_str = num_str.replace('千', '000').replace('+', '')
        return re.sub(r'[^\d]', '', num_str) or '0'

    def handle_error(self, failure):
        """统一错误处理"""
        request = failure.request
        self.logger.error(f"请求错误: {failure.value}, URL: {request.url}")
        self.monitor.update_request_stats(success=False)
        self.monitor.log_error(str(failure.value), self.name)
        
        self.error_history.append({
            'time': datetime.now(),
            'type': 'request_error',
            'message': str(failure.value),
            'url': request.url
        })
        
        retry_times = request.meta.get('retry_times', 0)
        if retry_times < self.settings.get('RETRY_TIMES', 6):
            if hasattr(failure.value, 'response'):
                status = failure.value.response.status
                if status in [403, 429, 401]:
                    self.logger.warning(f"收到{status}错误，强制更换代理和会话")
                    self.proxy_pool.report_failure(request.meta.get('proxy'))
                    self.proxy = self.proxy_pool.get_working_proxy()
                    self.session_id = self._generate_session_id()
                    self.device_id = self._generate_device_id()
                    
                    retry_delay = (2 ** retry_times) + random.uniform(1, 4)
                    self.logger.info(f"因{status}错误，延迟{retry_delay:.2f}s后重试第{retry_times+1}次")
                    
                    new_request = request.copy()
                    new_request.meta['retry_times'] = retry_times + 1
                    new_request.meta['proxy'] = self.proxy
                    new_request.headers = self._get_headers()
                    new_request.dont_filter = True
                    
                    from twisted.internet import reactor
                    from scrapy.utils.reactor import CallLaterOnce
                    CallLaterOnce(new_request.callback, new_request)
            else:
                self.logger.info(f"网络错误，重试第{retry_times+1}次")
                new_request = request.copy()
                new_request.meta['retry_times'] = retry_times + 1
                new_request.dont_filter = True
                yield new_request
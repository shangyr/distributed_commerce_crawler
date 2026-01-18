import time
import datetime
import redis
import psutil
from scrapy.utils.project import get_project_settings

class SpiderMonitor:
    """爬虫监控器，负责统计请求、节点状态及告警"""
    def __init__(self):
        self.settings = get_project_settings()
        self.redis_conn = redis.Redis(
            host=self.settings.get('REDIS_HOST', 'localhost'),
            port=self.settings.get('REDIS_PORT', 6379),
            db=self.settings.get('REDIS_MONITOR_DB', 2),
            decode_responses=True
        )
        # 初始化统计结构
        self.stats = {
            'total_requests': 0,
            'success_requests': 0,
            'failed_requests': 0,
            'spider_stats': {
                'taobao': {'requests': 0, 'success': 0, 'fail': 0},
                'jd': {'requests': 0, 'success': 0, 'fail': 0},
                'pdd': {'requests': 0, 'success': 0, 'fail': 0}
            },
            'item_stats': {'product': 0, 'comment': 0, 'shop': 0},
            'nodes': {}  # 节点状态: {worker_id: {last_active, cpu, memory}}
        }
        # 告警阈值配置
        self.alert_thresholds = {
            'failure_rate': 0.3,  # 失败率超过30%告警
            'node_timeout': 60,   # 节点60秒未活跃视为离线
            'min_working_nodes': 2  # 最小工作节点数
        }
        # 启动监控线程
        self._start_monitor_thread()

    def _start_monitor_thread(self):
        """启动后台监控线程，定期保存统计和检查告警"""
        import threading
        def monitor_loop():
            while True:
                self._save_stats_to_redis()  # 每30秒保存一次统计
                self._check_alerts()         # 检查告警条件
                self._clean_expired_nodes()  # 清理离线节点
                time.sleep(30)
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()

    def update_request_stats(self, success=True, spider_name=None):
        """更新请求统计（按爬虫区分）"""
        self.stats['total_requests'] += 1
        if success:
            self.stats['success_requests'] += 1
        else:
            self.stats['failed_requests'] += 1
        
        # 更新爬虫单独统计
        if spider_name and spider_name in self.stats['spider_stats']:
            self.stats['spider_stats'][spider_name]['requests'] += 1
            if success:
                self.stats['spider_stats'][spider_name]['success'] += 1
            else:
                self.stats['spider_stats'][spider_name]['fail'] += 1

    def update_item_stats(self, item_type):
        """更新爬取到的Item统计"""
        if item_type in self.stats['item_stats']:
            self.stats['item_stats'][item_type] += 1

    def register_worker(self, worker_id):
        """注册工作节点"""
        self.stats['nodes'][worker_id] = {
            'last_active': datetime.now().timestamp(),
            'cpu_usage': 0.0,
            'memory_usage': 0.0
        }
        self.redis_conn.sadd('active_workers', worker_id)
        self.redis_conn.hset(f"worker:{worker_id}", mapping={
            'status': 'active',
            'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    def update_worker_status(self, worker_id):
        """更新节点状态（CPU/内存）"""
        if worker_id in self.stats['nodes']:
            self.stats['nodes'][worker_id].update({
                'last_active': datetime.now().timestamp(),
                'cpu_usage': psutil.cpu_percent(interval=0.1),
                'memory_usage': psutil.virtual_memory().percent
            })
            # 保存到Redis
            self.redis_conn.hset(f"worker:{worker_id}", mapping={
                'last_active': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'cpu_usage': self.stats['nodes'][worker_id]['cpu_usage'],
                'memory_usage': self.stats['nodes'][worker_id]['memory_usage']
            })

    def _save_stats_to_redis(self):
        """将统计数据保存到Redis（按时间分片）"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M')
        # 保存总体统计
        self.redis_conn.hset(f"stats:total:{timestamp}", mapping={
            'total': self.stats['total_requests'],
            'success': self.stats['success_requests'],
            'failed': self.stats['failed_requests'],
            'failure_rate': round(self.stats['failed_requests'] / max(self.stats['total_requests'], 1), 2)
        })
        # 保存爬虫统计
        for spider, data in self.stats['spider_stats'].items():
            self.redis_conn.hset(f"stats:spider:{spider}:{timestamp}", mapping={
                'requests': data['requests'],
                'success': data['success'],
                'fail': data['fail'],
                'failure_rate': round(data['fail'] / max(data['requests'], 1), 2)
            })
        # 保存Item统计
        self.redis_conn.hset(f"stats:items:{timestamp}", mapping=self.stats['item_stats'])

    def _check_alerts(self):
        """检查告警条件并触发告警"""
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 1. 失败率过高告警
        total_fail_rate = self.stats['failed_requests'] / max(self.stats['total_requests'], 1)
        if total_fail_rate > self.alert_thresholds['failure_rate']:
            alert_msg = (f"[ALERT] 总体失败率过高: {total_fail_rate:.2%} "
                        f"({self.stats['failed_requests']}/{self.stats['total_requests']}) "
                        f"at {current_time}")
            self._send_alert(alert_msg)
        
        # 2. 节点数量不足告警
        active_nodes = len(self.stats['nodes'])
        if active_nodes < self.alert_thresholds['min_working_nodes']:
            alert_msg = (f"[ALERT] 工作节点不足: 当前{active_nodes}个，"
                        f"最低要求{self.alert_thresholds['min_working_nodes']}个 "
                        f"at {current_time}")
            self._send_alert(alert_msg)

    def _clean_expired_nodes(self):
        """清理超过超时时间的离线节点"""
        expired_ids = []
        now = time.time()
        for worker_id, status in self.stats['nodes'].items():
            if now - status['last_active'] > self.alert_thresholds['node_timeout']:
                expired_ids.append(worker_id)
        
        for worker_id in expired_ids:
            del self.stats['nodes'][worker_id]
            self.redis_conn.srem('active_workers', worker_id)
            self.redis_conn.hset(f"worker:{worker_id}", 'status', 'inactive')
            self._send_alert(f"[ALERT] 节点{worker_id}已离线 at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _send_alert(self, message):
        """发送告警（可扩展为邮件/钉钉机器人）"""
        print(f"===== {message} =====")  # 实际应用中替换为告警渠道
        self.redis_conn.rpush('alerts', message)  # 保存告警历史

    def get_recent_stats(self, minutes=10):
        """获取最近N分钟的统计数据"""
        end_time = datetime.now()
        start_time = end_time - datetime.timedelta(minutes=minutes)
        stats = []
        # 从Redis查询时间范围内的数据（简化实现）
        return stats
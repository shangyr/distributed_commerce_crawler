#!/bin/bash
set -euo pipefail  # 增强错误检测：未定义变量报错、管道错误检测

# 配置常量
REDIS_CONF="redis://localhost:6379/0"
LOG_DIR="./logs"
DATA_DIR="./data"
CONFIG_DIR="./config"
VENV_DIR="./venv"
REQUIREMENTS_FILE="./requirements.txt"
MONITOR_SCRIPT="utils/monitor_script.py"

# 颜色输出函数
info() { echo -e "\033[32m[INFO] $*\033[0m"; }
warn() { echo -e "\033[33m[WARN] $*\033[0m"; }
error() { echo -e "\033[31m[ERROR] $*\033[0m"; exit 1; }

# 检查操作系统
check_os() {
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        warn "检测到Windows环境，部分功能可能受限"
        return 1
    fi
    return 0
}

# 检查Python环境
check_python() {
    info "检查Python环境..."
    if ! command -v python3 &> /dev/null; then
        error "未找到Python3，请先安装Python3.8及以上版本"
    fi

    local python_version
    python_version=$(python3 -c 'import sys; print(sys.version_info.minor)' 2>/dev/null)
    if [[ $python_version -lt 8 ]]; then
        error "Python版本过低，需要Python3.8及以上版本"
    fi
}

# 检查并启动Redis
check_redis() {
    info "检查Redis服务..."
    if ! command -v redis-cli &> /dev/null; then
        error "未安装Redis，请先安装Redis（推荐5.0+版本）"
    fi

    if ! redis-cli -u "$REDIS_CONF" ping &> /dev/null; then
        info "Redis服务未运行，尝试启动..."
        if command -v redis-server &> /dev/null; then
            redis-server --daemonize yes --port 6379 --timeout 300
            sleep 3  # 等待服务启动
            if ! redis-cli -u "$REDIS_CONF" ping &> /dev/null; then
                error "Redis启动失败，请手动启动Redis服务"
            fi
            info "Redis服务启动成功"
        else
            error "未找到redis-server，无法启动Redis服务"
        fi
    else
        info "Redis服务已运行"
    fi
}

# 检查浏览器环境（JS渲染）
check_browser() {
    info "检查浏览器环境..."
    local browser_found=0
    for browser in google-chrome chrome chromium; do
        if command -v "$browser" &> /dev/null; then
            browser_found=1
            break
        fi
    done

    if [[ $browser_found -eq 0 ]]; then
        warn "未检测到Chrome/Chromium浏览器，JS渲染功能将不可用"
        read -p "是否继续运行（可能导致部分网站爬取失败）? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        info "检测到可用浏览器，JS渲染功能正常"
    fi
}

# 管理虚拟环境
setup_venv() {
    info "配置虚拟环境..."
    if [[ ! -d "$VENV_DIR" ]]; then
        info "创建虚拟环境..."
        python3 -m venv "$VENV_DIR" || error "虚拟环境创建失败"
    fi

    # 激活虚拟环境
    if [[ -f "$VENV_DIR/bin/activate" ]]; then
        source "$VENV_DIR/bin/activate"
    elif [[ -f "$VENV_DIR/Scripts/activate" ]]; then
        source "$VENV_DIR/Scripts/activate"
    else
        error "虚拟环境激活脚本不存在"
    fi

    # 安装依赖
    info "安装依赖包..."
    if [[ ! -f "$REQUIREMENTS_FILE" ]]; then
        error "依赖文件 $REQUIREMENTS_FILE 不存在"
    fi
    pip install --upgrade pip &> /dev/null
    pip install -r "$REQUIREMENTS_FILE" || error "依赖安装失败"
}

# 初始化目录和配置文件
init_dirs() {
    info "初始化目录结构..."
    # 创建数据和日志目录
    mkdir -p \
        "$DATA_DIR/products" \
        "$DATA_DIR/comments" \
        "$DATA_DIR/shops" \
        "$LOG_DIR" \
        "$CONFIG_DIR" || error "目录创建失败"

    # 检查配置文件
    if [[ ! -f "$CONFIG_DIR/config.yaml" ]]; then
        error "配置文件 $CONFIG_DIR/config.yaml 不存在"
    fi

    # 处理代理列表
    if [[ ! -f "$CONFIG_DIR/proxy_list.txt" ]]; then
        warn "未找到代理列表文件 $CONFIG_DIR/proxy_list.txt"
        read -p "确认使用本地IP爬取（可能触发反爬）? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
        # 创建空代理文件避免后续错误
        touch "$CONFIG_DIR/proxy_list.txt"
    else
        # 统计有效代理数量
        local proxy_count
        proxy_count=$(grep -v '^#' "$CONFIG_DIR/proxy_list.txt" | grep -v '^$' | wc -l)
        if [[ $proxy_count -eq 0 ]]; then
            warn "代理列表文件为空，将使用本地IP爬取"
        elif [[ $proxy_count -lt 3 ]]; then
            warn "代理数量较少（$proxy_count个），可能影响爬取稳定性"
        else
            info "检测到有效代理 $proxy_count 个"
        fi
    fi
}

# 清理历史数据
clean_data() {
    if [[ "$1" == "clean" ]]; then
        info "清理历史数据..."
        # 清理Redis数据
        redis-cli -u "$REDIS_CONF" KEYS "taobao:*" | xargs -r redis-cli -u "$REDIS_CONF" DEL
        redis-cli -u "$REDIS_CONF" KEYS "jd:*" | xargs -r redis-cli -u "$REDIS_CONF" DEL
        redis-cli -u "$REDIS_CONF" KEYS "pdd:*" | xargs -r redis-cli -u "$REDIS_CONF" DEL
        redis-cli -u "$REDIS_CONF" DEL "spider:stats" "worker:*"
        
        # 清理日志（保留最近3天）
        find "$LOG_DIR" -name "*.log" -mtime +3 -delete
        
        info "历史数据清理完成"
    fi
}

# 启动监控脚本
start_monitor() {
    info "启动监控服务..."
    if [[ ! -f "$MONITOR_SCRIPT" ]]; then
        error "监控脚本 $MONITOR_SCRIPT 不存在"
    fi
    # 检查监控进程是否已运行
    if pgrep -f "$MONITOR_SCRIPT" &> /dev/null; then
        warn "监控服务已在运行，跳过启动"
    else
        nohup python3 -m "$(echo "$MONITOR_SCRIPT" | sed 's/\//./g' | sed 's/\.py$//')" \
            > "$LOG_DIR/monitor.log" 2>&1 &
        sleep 2
        if ! pgrep -f "$MONITOR_SCRIPT" &> /dev/null; then
            error "监控服务启动失败"
        fi
    fi
}

# 启动爬虫节点
start_spiders() {
    local worker_count
    worker_count=$(nproc)
    # 限制最大Worker数量（根据CPU核心动态调整）
    if [[ $worker_count -gt 8 ]]; then
        worker_count=8
    elif [[ $worker_count -lt 2 ]]; then
        worker_count=2
    fi
    # 拼多多反爬严格，Worker数量减半
    local pdd_worker_count=$((worker_count / 2))
    if [[ $pdd_worker_count -lt 1 ]]; then
        pdd_worker_count=1
    fi

    info "启动Master节点..."
    # 启动淘宝Master
    nohup scrapy runspider ecommerce_spider/spiders/taobao_spider.py -a role=master \
        > "$LOG_DIR/taobao_master.log" 2>&1 &
    # 启动京东Master
    nohup scrapy runspider ecommerce_spider/spiders/jd_spider.py -a role=master \
        > "$LOG_DIR/jd_master.log" 2>&1 &
    # 启动拼多多Master
    nohup scrapy runspider ecommerce_spider/spiders/pdd_spider.py -a role=master \
        > "$LOG_DIR/pdd_master.log" 2>&1 &

    # 等待Master节点初始化
    info "等待Master节点初始化（8秒）..."
    sleep 8

    # 启动Worker节点
    info "启动Worker节点（淘宝/京东各$worker_count个，拼多多$pdd_worker_count个）..."
    # 淘宝Worker
    for ((i=1; i<=$worker_count; i++)); do
        nohup scrapy runspider ecommerce_spider/spiders/taobao_spider.py -a role=worker \
            > "$LOG_DIR/taobao_worker_$i.log" 2>&1 &
    done
    # 京东Worker
    for ((i=1; i<=$worker_count; i++)); do
        nohup scrapy runspider ecommerce_spider/spiders/jd_spider.py -a role=worker \
            > "$LOG_DIR/jd_worker_$i.log" 2>&1 &
    done
    # 拼多多Worker
    for ((i=1; i<=$pdd_worker_count; i++)); do
        nohup scrapy runspider ecommerce_spider/spiders/pdd_spider.py -a role=worker \
            > "$LOG_DIR/pdd_worker_$i.log" 2>&1 &
    done
}

# 显示启动信息
show_info() {
    info "=============================================="
    info "分布式电商爬虫启动完成！"
    info "日志文件路径: $LOG_DIR"
    info "数据存储路径: $DATA_DIR"
    info "监控统计路径: $LOG_DIR/spider_stats.json"
    info "----------------------------------------------"
    info "查看进程: ps aux | grep scrapy"
    info "查看日志: tail -f $LOG_DIR/taobao_master.log"
    info "实时统计: redis-cli -u $REDIS_CONF get spider:stats"
    info "停止爬虫: pkill -f scrapy; pkill -f $MONITOR_SCRIPT"
    info "=============================================="
}

# 主流程
main() {
    check_os
    check_python
    check_redis
    check_browser
    setup_venv
    init_dirs
    clean_data "${1:-}"
    start_monitor
    start_spiders
    show_info
}

# 执行主流程
main "$@"
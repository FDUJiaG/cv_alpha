from datetime import datetime
import tushare as ts


def get_time(date=False, utc=False, msl=3):
    if date:
        time_fmt = "%Y-%m-%d %H:%M:%S.%f"
    else:
        time_fmt = "%H:%M:%S.%f"

    if utc:
        return datetime.utcnow().strftime(time_fmt)[:(msl - 6)]
    else:
        return datetime.now().strftime(time_fmt)[:(msl - 6)]


def print_info(status="I"):
    return "\033[0;33;1m[{} {}]\033[0m".format(status, get_time())


def get_api(token):
    # 设置 tushare pro 的 token 并获取连接，仅首次和重置时需获取，对于日 K 每分钟最多调取两百次
    ts.set_token(token)
    pro = ts.pro_api()
    return pro


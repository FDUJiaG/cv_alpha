# 获取指数k线数据并存入stock_index_pro数据表
# 以HS300为例, code: 000300.SH

# 导入包
import tushare as ts
import pymysql
import datetime
from src.password import tushare_token, sql_host, sql_username, sql_password, db_name
from src.common_utils import get_api, print_info

# ts_code 同交易所代码, 需增加交易所代码, 如:
# 上证指数(SH): 000001.SH
# 深圳成指(SZ): 399001.SZ
# 上证50(SH50):  000016.SH
# 沪深300(HS300): 000300.SH or 399300.SZ
# 中证500(ZZ500): 000905.SH or 399905.SZ
# 中小板指(ZX): 399005.SZ
# 创业板指(CY): 399006.SZ

# ts_code同交易所代码, 需增加交易所代码, 如:
# 上证指数(SH): 000001.SH
# 深圳成指(SZ): 399001.SZ
# 上证50(SH50):  000016.SH
# 沪深300(HS300): 000300.SH or 399300.SZ
# 中证500(ZZ500): 000905.SH or 399905.SZ
# 中小板指(ZX): 399005.SZ
# 创业板指(CY): 399006.SZ

index_dict = {
    "SH": "000001.SH",
    "SZ": "399001.SZ",
    "SH50":  "000016.SH",
    "HS300": "000300.SH",
    "ZZ500": "000905.SH",
    "ZX": "399005.SZ",
    "CY": "399006.SZ"
}


def get_stock_index(ts_pro, index_n, start_d, end_d):
    # 获取一个回测的参考指标

    try:
        df = ts_pro.index_daily(ts_code=index_dict[index_n], start_date=start_d, end_date=end_d)
        # 统一指数标注并删除原复杂指数代码标注, 换算成交额为万元
        df['stock_code'] = index_n
        df['amount'] = df.amount / 10
        print(print_info(), end=" ")
        print("Get the data from api!")
    except:
        print(print_info("E"), end=" ")
        print("Can not get the data from api!")
        return False

    if "ts_code" in df.keys():
        df.pop("ts_code")

    # 调整 df 顺序, 纯属个人喜好
    order = [
        'trade_date', 'stock_code',
        'close', 'open', 'high', 'low', 'pre_close',
        'change', 'pct_chg', 'vol', 'amount'
    ]
    df = df[order]
    df = df.sort_values(by="trade_date", ascending=True)
    print(print_info(), end=" ")
    print("The DataFrame is:")
    print(df)
    return df


def index_data_to_db(df, host, username, password, db_n):
    # 建立数据库连接，剔除已入库的部分
    try:
        db = pymysql.connect(
            host=host,
            user=username,
            passwd=password,
            db=db_n,
            charset='utf8mb4'
        )
        cursor = db.cursor()
        print(print_info("I"), end=" ")
        print("Connect to the database!")
    except:
        print(print_info("E"), end=" ")
        print("Can not connect to the database!")
        return False

    # 逐条导入指数数据
    df_len = df.shape[0]
    for i in range(df_len):
        resu0 = list(df.iloc[df_len - 1 - i])
        resu = []

        # 逐列准备list
        for k in range(len(resu0)):
            if str(resu0[k]) == 'nan':
                # 如果 Tushare 数据有空值, 则设定一个诡异值用以区分
                resu.append(-1998.0129)
            else:
                resu.append(resu0[k])

        # 日期需要单独导入, 否则会默认增加 %H:%M:%S
        state_dt = (datetime.datetime.strptime(resu[0], "%Y%m%d")).strftime('%Y-%m-%d')
        try:
            # 尝试将list写入数据表
            sql_insert = "INSERT INTO stock_index_pro(" \
                         "state_dt,stock_code,close,open,high,low,vol,amount,p_change,pct_chg" \
                         ") VALUES (" \
                         "'%s', '%s', '%.2f', '%.2f','%.2f', '%.2f', '%d', '%d', '%.2f', '%.2f')" % (
                state_dt, str(resu[1]), float(resu[2]), float(resu[3]), float(resu[4]), float(resu[5]),
                float(resu[9]), float(resu[10]), float(resu[7]), float(resu[8])
            )
            cursor.execute(sql_insert)
            db.commit()
        except Exception as err:
            print(err)
            print(print_info("E"), end=" ")
            print("Unknown Error happened!")
            continue

    # 关闭指针及数据库
    cursor.close()
    db.close()

    print(print_info(), end=" ")
    print('Finish Loading %s Index Data!!!' % df['stock_code'][0])
    return True


if __name__ == '__main__':
    # 设定获取日线行情的初始日期和终止日期，暂时将终止日期设定为今天
    start_dt = '20140101'
    time_temp = datetime.datetime.now()
    end_dt = time_temp.strftime('%Y%m%d')

    pro = get_api(tushare_token)

    index_name = "HS300"
    df_index_data = get_stock_index(pro, index_name, start_dt, end_dt)

    if type(df_index_data) is bool:
        print(print_info("E"), end=" ")
        print("Can not get the index data!")
    else:
        TF = index_data_to_db(df_index_data, sql_host, sql_username, sql_password, db_name)
        if TF:
            print(print_info("S"), end=" ")
            print("Success!")
        else:
            print(print_info("E"), end=" ")
            print("Error!")


''' pro.index_daily()参数说明
输入参数:
ts_code:    指数代码(必选)
trade_date: 交易日期
start_date: 开始日期(YYYYMMDD)
end_date:   结束日期(YYYYMMDD)

输出参数:
ts_code:    TS指数代码
trade_date: 交易日期
close:      收盘点位
open:       开盘点位
high:       最高点位
low:        最低点位
pre_close:  昨日收盘点位
change:     涨跌点
pct_chg:    涨跌幅(未复权)
vol	float:  成交量(手)
amount:     成交额(千元)
'''

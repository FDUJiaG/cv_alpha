# 数据采集至数据库
import os
import pandas as pd
import datetime
import pymysql
from src.password import tushare_token, sql_host, sql_username, sql_password, db_name
from src.common_utils import print_info, get_api


def get_cv_data(f_path, sheet_n="Sheet1"):
    try:
        df = pd.read_excel(f_path, sheet_name=sheet_n, header=0)
        print(print_info(), end=" ")
        print("Successfully loading the file: {}".format(f_path))
    except:
        print(print_info("E"), end=" ")
        print("Could not load the file: {}".format(f_path))
        return False

    try:
        stock_pool_muti = df["正股代码"].tolist()
        stock_pool = list(set(stock_pool_muti))
        stock_pool.sort(key=stock_pool_muti.index)
        print(print_info(), end=" ")
        print("Successfully getting the stock_pool_list:")
        for idx in range(len(stock_pool)):
            if (idx + 1) % 10 == 0:
                print(stock_pool[idx])
            elif (idx + 1) % 10 == 1:
                print(" " * 4, stock_pool[idx], end=" ")
            else:
                print(stock_pool[idx], end=" ")
        print("")
        return stock_pool
    except:
        print(print_info("E"), end=" ")
        print("Could not get the stock_pool_list")
        return False


def init_stock_pool(stock_pool, host, username, password, db_n, pro, start_d, end_d):
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

    pool_len = len(stock_pool)
    if pool_len == 0:
        print(print_info("W"), end=" ")
        print("Use build-in stock pool!")
        # 设定需要获取数据的股票池，取云计算相关的：
        # 中兴通讯，远光软件，中国长城，东方财富，用友网络，中科曙光，中国软件，浪潮信息，宝信软件
        stock_pool = [
            '000063.SZ', '002063.SZ', '000066.SZ', '300059.SZ', '600588.SH',
            '603019.SH', '600536.SH', '000977.SZ', '600845.SH'
        ]
        pool_len = len(stock_pool)

    # 循环获取单个股票的日线行情
    for idx in range(pool_len):
        try:
            df = pro.daily(ts_code=stock_pool[idx], start_date=start_d, end_date=end_d).fillna(1998.0129)
            # print(df.tail())
            # 打印进度
            print(print_info(), end=" ")
            print('Seq: ' + str(idx + 1) + ' of ' + str(pool_len) + '   Code: ' + str(stock_pool[idx]))
            c_len = df.shape[0]
        except Exception as excep:
            print(excep)
            print(print_info("E"), end=" ")
            print('No DATA Code: ' + str(idx))
            continue
        for j in range(c_len):
            # resu0 = list(df.iloc[c_len - 1 - j])
            # resu = []
            # for k in range(len(resu0)):
            #     if str(resu0[k]) == 'nan':
            #         resu.append(-1998.0129)
            #     else:
            #         resu.append(resu0[k])
            resu = list(df.iloc[c_len - 1 - j])
            state_dt = (datetime.datetime.strptime(resu[1], "%Y%m%d")).strftime('%Y-%m-%d')
            try:
                sql_insert = "INSERT INTO stock_all(\
                        state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change" \
                             ") VALUES (" \
                             "'%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f'" \
                             ")" % (state_dt, str(resu[0]), float(resu[2]), float(resu[5]), float(resu[3]),
                                    float(resu[4]), float(resu[9]), float(resu[10]), float(resu[6]),
                                    float(resu[7]), float(resu[8]))
                cursor.execute(sql_insert)
                db.commit()
            except Exception as err:
                print(err)
                print(print_info("E"), end=" ")
                print("Unknown Error happened!")
                continue
    cursor.close()
    db.close()
    print(print_info(), end=" ")
    print('Finished saving stock pool to database!')

    return True


if __name__ == '__main__':
    # 设定获取日线行情的初始日期和终止日期，暂时将终止日期设定为今天
    start_dt = '20140101'
    # time_temp = datetime.datetime.now() - datetime.timedelta(days=1)
    time_temp = datetime.datetime.now()
    end_dt = time_temp.strftime('%Y%m%d')

    pro = get_api(tushare_token)

    root_path = os.path.abspath("..")
    file_path = os.path.join(root_path, "raw_data", "convertible_bond_timing.xlsx")
    stock_pool = get_cv_data(file_path)

    if stock_pool:
        TF = init_stock_pool(stock_pool, sql_host, sql_username, sql_password, db_name, pro, start_dt, end_dt)
        if TF:
            print(print_info("S"), end=" ")
            print("Success!")
        else:
            print(print_info("E"), end=" ")
            print("Error")
    else:
        print(print_info("E"), end=" ")
        print("Error")


''' pro.daily() 参数说明
输入参数:
ts_code:    股票代码(二选一)
trade_date: 交易日期(二选一)
start_date: 开始日期(YYYYMMDD)
end_date:   结束日期(YYYYMMDD)

输出参数:
ts_code:    股票代码
trade_date: 交易日期
open:       开盘价
high:       最高价
low:        最低价
close:      收盘价
pre_close:  昨收价
change:     涨跌额
pct_chg:    涨跌幅(未复权)
vol	float:  成交量(手)
amount:     成交额(千元)
'''

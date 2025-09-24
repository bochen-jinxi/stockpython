import akshare as ak
import pandas as pd
import os
from datetime import datetime, timedelta

# 公共变量设置 - 默认最近6天
# END_DATE = (datetime.now()).strftime('%Y%m%d')  # 结束日期为今天
# START_DATE = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')  # 开始日期为5天前（包含今天共6天）
START_DATE = '20210226'
END_DATE = '20210310'

# 输入股票代码文件路径
input_file = r'D:\code.EBK'
output_dir = r'D:\k'

# 确保输出目录存在
os.makedirs(output_dir, exist_ok=True)

i = 1

with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        print(f"Processing {i}: {line}")

        stock_code = line
        df = pd.DataFrame()
        code_for_db = stock_code
        is_hk = False  # 标记是否为港股
        market_type = 'cn'  # 市场类型：cn=中国，hk=港股

        try:
            if stock_code.startswith(('8', '4')) and len(stock_code) == 6:
                # 北交所
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period="daily",
                    start_date=START_DATE,
                    end_date=END_DATE,
                    adjust="qfq"
                )
                code_for_db = stock_code
            elif len(stock_code) == 5 and stock_code.isdigit():
                # 港股：5位纯数字代码（如 00700）
                df = ak.stock_hk_daily(
                    symbol=stock_code,
                    adjust="qfq"
                )
                # 过滤日期范围 - 先确保date列是datetime类型
                df['date'] = pd.to_datetime(df['date'])
                df = df[(df['date'] >= pd.to_datetime(START_DATE.replace('-', ''))) & 
                        (df['date'] <= pd.to_datetime(END_DATE.replace('-', '')))]
                code_for_db = stock_code  # 去掉 .HK 后缀
                is_hk = True
                market_type = 'hk'
            elif stock_code.startswith(('6', '9')) and len(stock_code) == 6:
                # 沪市 A 股
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period="daily",
                    start_date=START_DATE,
                    end_date=END_DATE,
                    adjust="qfq"
                )
                code_for_db = stock_code  # 去掉 .SH 后缀
                market_type = 'cn'
            elif stock_code.startswith(('0', '3')) and len(stock_code) == 6:
                # 深市 A 股 - 修正逻辑，不再指定market参数
                df = ak.stock_zh_a_hist(
                    symbol=stock_code,
                    period="daily",
                    start_date=START_DATE,
                    end_date=END_DATE,
                    adjust="qfq"
                )
                code_for_db = stock_code  # 去掉 .SZ 后缀
                market_type = 'cn'
            else:
                print(f"  ⚠️ 无法识别代码类型: {stock_code}")
                df = pd.DataFrame()

        except Exception as e:
            print(f"  ❌ 获取数据失败: {e}")
            df = pd.DataFrame()

        # 生成 SQL
        sql_lines = []
        if not df.empty:
            for _, row in df.iterrows():
                # 根据市场类型获取相应字段
                if market_type == 'hk':
                    # 港股字段（英文列名）
                    date = row['date'].strftime('%Y-%m-%d')  # 确保日期格式为字符串
                    open_price = row['open']
                    close_price = row['close']
                    low_price = row['low']
                    high_price = row['high']
                    # 成交额 = turnover（单位：港元）
                    amount = row.get('turnover', 0)
                    if pd.isna(amount) or amount == '':
                        amount = 0
                    # 涨跌幅 = pct_change（单位：%）
                    pct_chg = row.get('pct_change', 0)
                    if pd.isna(pct_chg) or pct_chg == '':
                        pct_chg = 0
                else:  # market_type == 'cn' (包括A股和北交所)
                    # A股/北交所字段（中文列名）
                    date = row['日期']
                    open_price = row['开盘']
                    close_price = row['收盘']
                    low_price = row['最低']
                    high_price = row['最高']
                    amount = row.get('成交额', 0)
                    if pd.isna(amount) or amount == '':
                        amount = 0
                    pct_chg = row.get('涨跌幅', 0)
                    if pd.isna(pct_chg) or pct_chg == '':
                        pct_chg = 0

                # 构造 SQL（注意字符串转义）
                sql = (
                    f";INSERT INTO dbo.lishijiager (code,riqi,kai,shou,di,gao,chengjiaoliang,pctChg) "
                    f"VALUES ('{code_for_db}', '{date}','{open_price}',N'{close_price}',N'{low_price}',N'{high_price}',N'{amount}',N'{pct_chg}')"
                )
                sql_lines.append(sql)

        # 写入文件
        output_file = os.path.join(output_dir, f'd{i}.sql')
        with open(output_file, 'w', encoding='utf-8') as f2:
            f2.write('\n'.join(sql_lines))

        i += 1

print("✅ 所有股票处理完成！")
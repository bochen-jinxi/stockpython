import akshare as ak
import pandas as pd
import os
from datetime import datetime, timedelta
import concurrent.futures
import threading

# --- 配置区域 ---
END_DATE = '20250924'
START_DATE = '20250801'
input_file = r'D:\code4.EBK'
output_dir = r'D:\k4'
# 线程池大小，根据你的系统和网络情况调整
MAX_WORKERS = 4
# 失败代码输出文件
failed_codes_file = r'D:\codee4.EBK'
# --- 配置区域结束 ---

# 确保输出目录存在
os.makedirs(output_dir, exist_ok=True)

# 用于存储失败的股票代码及其原始行号
failed_codes_with_index = []
# 用于存储所有原始代码及其行号（用于最后排序失败列表）
all_codes_with_index = []
# 线程锁，用于安全地更新 failed_codes_with_index 列表
lock = threading.Lock()

def process_stock(line_index, stock_code):
    """
    处理单个股票代码的函数
    """
    print(f"Processing {line_index}: {stock_code}")

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
            # 深市 A 股
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
            print(f"  Thread-{threading.current_thread().ident}: ⚠️ 无法识别代码类型: {stock_code}")
            df = pd.DataFrame()

    except Exception as e:
        print(f"  Thread-{threading.current_thread().ident}: ❌ 获取数据失败 for {stock_code}: {e}")
        # 将失败的代码和其原始行号添加到共享列表中
        with lock:
            failed_codes_with_index.append((line_index, stock_code))
        df = pd.DataFrame() # 确保后续逻辑不会处理无效数据

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
    output_file = os.path.join(output_dir, f'd{line_index}.sql')
    try:
        with open(output_file, 'w', encoding='utf-8') as f2:
            f2.write('\n'.join(sql_lines))
        print(f"  Thread-{threading.current_thread().ident}: ✅ 写入文件完成 {output_file}")
    except Exception as e:
        print(f"  Thread-{threading.current_thread().ident}: ❌ 写入文件失败 {output_file}: {e}")

# 读取股票代码列表及其原始行号
stock_codes = []
with open(input_file, 'r', encoding='utf-8') as f:
    for line_num, line in enumerate(f, start=1):
        line = line.strip()
        if line:
            stock_codes.append((line_num, line))
            all_codes_with_index.append((line_num, line)) # 同时存入总列表

print(f"开始处理 {len(stock_codes)} 个股票代码...")

# 使用 ThreadPoolExecutor 进行多线程处理
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # 提交所有任务到线程池
    futures = {executor.submit(process_stock, idx, code): (idx, code) for idx, code in stock_codes}
    
    # 等待所有任务完成
    for future in concurrent.futures.as_completed(futures):
        idx, code = futures[future]
        try:
            future.result() # 获取结果或检查异常（如果在函数内部未处理）
        except Exception as e:
            print(f"处理股票 {code} (索引 {idx}) 时发生未捕获的异常: {e}")
            # 即使在这里捕获到异常，process_stock 内部的 try-except 也会先执行
            # 所以失败的 code 应该已经在 failed_codes_with_index 列表里了

# 所有任务完成后，将失败的代码按原始顺序写入文件
if failed_codes_with_index:
    print(f"发现 {len(failed_codes_with_index)} 个获取数据失败的代码，正在按原始顺序写入 {failed_codes_file} ...")
    # 按原始行号排序失败的代码
    sorted_failed_codes = sorted(failed_codes_with_index, key=lambda x: x[0])
    try:
        with open(failed_codes_file, 'w', encoding='utf-8') as f_fail:
            for _, code in sorted_failed_codes:
                f_fail.write(code + '\n')
        print(f"✅ 失败代码已按原始顺序写入文件: {failed_codes_file}")
    except Exception as e:
        print(f"❌ 写入失败代码文件时出错: {e}")
else:
    print("✅ 没有获取数据失败的代码。")

print("✅ 所有股票处理完成！")
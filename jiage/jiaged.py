import baostock as bs
import pandas as pd
import fileinput
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# ===== 公共参数配置 =====
START_DATE_RAW = '20250808'   # 无 - 的形式
END_DATE_RAW   = '20250925'   # 无 - 的形式
OUTPUT_DIR = 'D:\\k\\'

# 转换为 baostock 需要的 YYYY-MM-DD 格式
def format_date(raw: str) -> str:
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"

START_DATE = format_date(START_DATE_RAW)
END_DATE   = format_date(END_DATE_RAW)

# 确保输出目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 登陆系统
lg = bs.login(user_id="anonymous", password="123456")

# === 代码市场判断 ===
def format_code(line: str) -> str:
    if line.startswith('688'):          # 科创板
        return "sh." + line
    elif line.startswith(('6', '9')):   # 沪市
        return "sh." + line
    elif line.startswith(('0', '3')):   # 深市 / 创业板
        return "sz." + line
    else:
        return None

# === 单个代码的处理函数 ===
def process_code(idx, line):
    code = format_code(line)
    if not code:
        print(f"未知代码: {line}")
        return

    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,volume,amount,adjustflag,pctChg",
        start_date=START_DATE,
        end_date=END_DATE,
        frequency="d",
        adjustflag="2"
    )

    result_list = []
    while (rs.error_code == '0') & rs.next():
        result_list.append(rs.get_row_data())

    sql_path = os.path.join(OUTPUT_DIR, f"d{idx}.sql")
    with open(sql_path, 'a+', encoding="utf-8") as f2:
        for el in result_list:
            pricedata = (
                ";INSERT INTO dbo.lishijiager "
                "(code,riqi,kai,shou,di,gao,chengjiaoliang,pctChg) "
                "VALUES ('%s', '%s','%s',N'%s',N'%s',N'%s',N'%s',N'%s')"
                % (
                    el[1], el[0], el[2], el[5], el[4], el[3],
                    el[7] if len(el[7]) > 0 else 0,
                    el[9] if len(el[9]) > 0 else 0
                )
            )
            f2.write(pricedata + '\n')

    print(f"完成 {code} -> {sql_path}")


# === 主逻辑 ===
with fileinput.input(files=('D:\\code.EBK')) as f:
    codes = [line.strip() for line in f if line.strip()]

# 并行执行，最多 5 个线程
with ThreadPoolExecutor(max_workers=1) as executor:
    futures = {executor.submit(process_code, idx + 1, code): code for idx, code in enumerate(codes)}
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"处理 {futures[future]} 出错: {e}")
print("✅ 所有代码处理完成！")

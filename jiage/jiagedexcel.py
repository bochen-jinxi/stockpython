import os
import pandas as pd
import sys
#python excel.py "C:\zd_zsone3\T0002\export\qq.xls" "C:\output\insert_lishijiager.sql" "2025-09-22" "6"
if len(sys.argv) != 5:
    print("❌ 用法: python excel.py <输入Excel路径> <输出SQL路径> <日期> <长度>")
    sys.exit(1)

file_path = sys.argv[1]
output_path = sys.argv[2]
riqi = sys.argv[3]
length = sys.argv[4]
# 自动创建输出文件夹
os.makedirs(os.path.dirname(output_path), exist_ok=True)

df = pd.read_excel(file_path, engine='xlrd')
df = df[['代码', '涨幅%', '现价', '总量', '今开', '最高', '最低']]

with open(output_path, "w", encoding="utf-8") as f:
    for _, row in df.iterrows():
        kai = str(row['今开']).strip()
        if kai == '--':
            continue

        code = str(row['代码']).strip().zfill(int(length))
        pctChg = str(row['涨幅%']).replace('%', '').strip()
        shou = str(row['现价']).strip()
        chengjiaoliang = str(row['总量']).strip()
        gao = str(row['最高']).strip()
        di = str(row['最低']).strip()

        sql = (
            f"INSERT INTO dbo.lishijiager (code,riqi,kai,shou,di,gao,chengjiaoliang,pctChg) "
            f"VALUES ('{code}', '{riqi}', N'{kai}', N'{shou}', N'{di}', N'{gao}', N'{chengjiaoliang}', N'{pctChg}');\n"
        )
        f.write(sql)

print(f"✅ SQL文件已生成：{output_path}")

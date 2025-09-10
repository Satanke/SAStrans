import sqlite3
import pandas as pd

# 测试MedDRA查询逻辑
conn = sqlite3.connect('translation_db.sqlite')
cursor = conn.cursor()

# 检查meddra_merged表结构
print("=== MedDRA表结构 ===")
cursor.execute("PRAGMA table_info(meddra_merged)")
columns = cursor.fetchall()
for col in columns:
    print(f"列名: {col[1]}, 类型: {col[2]}")

# 检查版本27.1的数据
print("\n=== 版本27.1的数据样本 ===")
cursor.execute("SELECT * FROM meddra_merged WHERE version = '27.1' LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(row)

# 检查是否有中文数据
print("\n=== 检查中文数据 ===")
cursor.execute("SELECT name_cn, name_en FROM meddra_merged WHERE version = '27.1' AND name_cn IS NOT NULL AND name_cn != '' LIMIT 10")
rows = cursor.fetchall()
for row in rows:
    print(f"中文: {row[0]}, 英文: {row[1]}")

# 测试具体的查询
print("\n=== 测试具体查询 ===")
test_values = ['手指皮肤干燥、脱屑', '皮肤干燥、手指皮肤脱屑', 'COVID-19感染']
for value in test_values:
    cursor.execute("SELECT name_cn, name_en FROM meddra_merged WHERE name_cn = ? AND version = '27.1'", (value,))
    result = cursor.fetchone()
    print(f"查询 '{value}': {result}")

# 检查是否有类似的数据
print("\n=== 检查包含'皮肤'的数据 ===")
cursor.execute("SELECT name_cn, name_en FROM meddra_merged WHERE version = '27.1' AND name_cn LIKE '%皮肤%' LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(f"中文: {row[0]}, 英文: {row[1]}")

conn.close()
print("\n调试完成")
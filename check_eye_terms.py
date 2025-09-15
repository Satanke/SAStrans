import sqlite3

conn = sqlite3.connect('translation_db.sqlite')
cursor = conn.cursor()

# 查询包含眼部不适的术语
cursor.execute("SELECT code, name_cn, name_en FROM meddra_merged WHERE name_cn LIKE '%不适%' AND name_cn LIKE '%眼%' AND version = '27.1' LIMIT 10")
results = cursor.fetchall()
print('眼部不适相关术语:')
for row in results:
    print(f'Code: {row[0]}, CN: {row[1]}, EN: {row[2]}')

if not results:
    print('未找到包含眼部不适的术语')
    # 查询所有包含不适的术语
    cursor.execute("SELECT code, name_cn, name_en FROM meddra_merged WHERE name_cn LIKE '%不适%' AND version = '27.1' LIMIT 20")
    discomfort_results = cursor.fetchall()
    print('\n所有不适相关术语:')
    for row in discomfort_results:
        print(f'Code: {row[0]}, CN: {row[1]}, EN: {row[2]}')

# 查询确切的'眼部不适'术语
cursor.execute("SELECT code, name_cn, name_en FROM meddra_merged WHERE name_cn = '眼部不适' AND version = '27.1'")
exact_results = cursor.fetchall()
print('\n确切的眼部不适术语:')
for row in exact_results:
    print(f'Code: {row[0]}, CN: {row[1]}, EN: {row[2]}')

if not exact_results:
    print('未找到确切的眼部不适术语')

conn.close()
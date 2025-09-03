# 数据库初始化工具使用指南

## 概述

`database_setup.py` 是一个独立的数据库管理工具，用于创建和管理SAS数据集翻译平台的数据库。

## 功能特性

- 🏗️ **数据库结构初始化** - 创建所有必要的数据库表
- 📥 **数据文件导入** - 支持 .asc, .xlsx, .csv 格式
- 📋 **表管理** - 列出、删除数据表
- 📊 **状态检查** - 查看数据库状态和统计信息
- 🗑️ **数据清理** - 清空数据库

## 安装依赖

```bash
pip install pandas sqlite3 openpyxl
```

## 基本用法

### 1. 初始化数据库结构

```bash
# 创建基础数据库表结构
python database_setup.py init

# 使用自定义数据库文件
python database_setup.py --db my_database.sqlite init
```

### 2. 导入数据文件

```bash
# 导入MedDRA数据文件
python database_setup.py import data/meddra_pt.xlsx pt_table meddra --desc "MedDRA首选术语表" --version "26.0"

# 导入WhoDrug数据文件
python database_setup.py import data/whodrug_dd.csv dd_table whodrug --desc "WhoDrug药物字典"

# 导入ASC格式文件
python database_setup.py import data/meddra_llt.asc llt_table meddra --desc "MedDRA最低级术语表"
```

### 3. 查看数据库状态

```bash
# 显示数据库状态
python database_setup.py status

# 列出所有表
python database_setup.py list
```

### 4. 删除表

```bash
# 删除指定表
python database_setup.py delete pt_table meddra
python database_setup.py delete dd_table whodrug
```

### 5. 清空数据库

```bash
# 清空整个数据库（需要确认）
python database_setup.py clear
```

## 数据库结构

### 表说明

1. **mapping_configs** - 映射配置表
   - 存储用户保存的数据合并配置

2. **meddra_tables** - MedDRA表管理
   - 管理MedDRA相关数据表信息

3. **whodrug_tables** - WhoDrug表管理
   - 管理WhoDrug相关数据表信息

4. **translation_library** - 翻译库
   - 存储已验证的翻译对照

5. **dictionary_data** - 数据字典
   - 存储实际的字典数据内容

### 文件组织

```
project/
├── database_setup.py          # 数据库初始化工具
├── translation_db.sqlite      # 主数据库文件
├── data/                      # 数据文件目录
│   ├── meddra_pt_table_*.pkl  # MedDRA数据文件
│   └── whodrug_dd_table_*.pkl # WhoDrug数据文件
└── app.py                     # 主应用程序
```

## 使用示例

### 完整的数据库初始化流程

```bash
# 1. 初始化数据库结构
python database_setup.py init

# 2. 导入MedDRA数据
python database_setup.py import sample_data/meddra_pt.xlsx pt_26_0 meddra --desc "MedDRA 26.0 首选术语" --version "26.0"
python database_setup.py import sample_data/meddra_llt.xlsx llt_26_0 meddra --desc "MedDRA 26.0 最低级术语" --version "26.0"

# 3. 导入WhoDrug数据
python database_setup.py import sample_data/whodrug_dd.csv dd_b3_2023 whodrug --desc "WhoDrug DD B3 2023" --version "B3 2023"

# 4. 检查导入结果
python database_setup.py list
python database_setup.py status
```

### 批量导入脚本

创建 `batch_import.py`：

```python
#!/usr/bin/env python3
import subprocess
import os

# 数据文件配置
data_files = [
    {
        'file': 'data/meddra/pt_26_0.xlsx',
        'table': 'pt_26_0',
        'category': 'meddra',
        'desc': 'MedDRA 26.0 首选术语',
        'version': '26.0'
    },
    {
        'file': 'data/meddra/llt_26_0.xlsx',
        'table': 'llt_26_0',
        'category': 'meddra',
        'desc': 'MedDRA 26.0 最低级术语',
        'version': '26.0'
    },
    {
        'file': 'data/whodrug/dd_b3_2023.csv',
        'table': 'dd_b3_2023',
        'category': 'whodrug',
        'desc': 'WhoDrug DD B3 2023',
        'version': 'B3 2023'
    }
]

# 初始化数据库
print("初始化数据库结构...")
subprocess.run(['python', 'database_setup.py', 'init'])

# 批量导入
for config in data_files:
    if os.path.exists(config['file']):
        print(f"导入: {config['file']}")
        cmd = [
            'python', 'database_setup.py', 'import',
            config['file'], config['table'], config['category'],
            '--desc', config['desc'],
            '--version', config['version']
        ]
        subprocess.run(cmd)
    else:
        print(f"文件不存在: {config['file']}")

print("批量导入完成!")
```

## 故障排除

### 常见问题

1. **文件编码问题**
   ```bash
   # 如果CSV文件编码有问题，可以先转换编码
   iconv -f gbk -t utf-8 original.csv > utf8.csv
   ```

2. **权限问题**
   ```bash
   # 确保有写权限
   chmod 755 database_setup.py
   ```

3. **依赖问题**
   ```bash
   # 安装所需依赖
   pip install pandas openpyxl
   ```

### 调试模式

在脚本中添加调试信息：
```python
# 在 database_setup.py 顶部添加
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 集成到主应用

数据库初始化工具与主应用 `app.py` 完全兼容。初始化完成后，直接运行主应用即可：

```bash
# 1. 初始化数据库
python database_setup.py init

# 2. 导入数据（可选）
python database_setup.py import data/sample.xlsx sample_table meddra

# 3. 启动主应用
python app.py
```

## 备份与恢复

### 备份
```bash
# 备份数据库文件
cp translation_db.sqlite translation_db_backup_$(date +%Y%m%d).sqlite

# 备份数据目录
tar -czf data_backup_$(date +%Y%m%d).tar.gz data/
```

### 恢复
```bash
# 恢复数据库
cp translation_db_backup_20231201.sqlite translation_db.sqlite

# 恢复数据目录
tar -xzf data_backup_20231201.tar.gz
```

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAS数据集翻译平台 - 数据库初始化程序
Database Setup Tool for SAS Dataset Translation Platform

用途：
1. 创建和初始化数据库表结构
2. 导入初始数据文件（.asc, .xlsx, .csv等）
3. 管理数据库表和记录
4. 提供数据库状态检查和清理功能

使用方法：
python database_setup.py [选项]
"""

import os
import sys
import sqlite3
import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
import json
import hashlib
import re


class DatabaseSetup:
    def __init__(self, db_path='translation_db.sqlite'):
        """
        初始化数据库设置类
        
        Args:
            db_path (str): 数据库文件路径
        """
        self.db_path = db_path
        self.data_dir = Path('data')
        self.data_dir.mkdir(exist_ok=True)
        
        print(f"数据库路径: {self.db_path}")
        print(f"数据文件目录: {self.data_dir}")
    
    def create_database_structure(self):
        """创建数据库表结构"""
        print("\n正在创建数据库表结构...")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # MedDRA数据库表
            print("- 创建MedDRA表管理 (meddra_tables)")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS meddra_tables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    file_path TEXT,
                    record_count INTEGER DEFAULT 0,
                    version TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # WhoDrug数据库表
            print("- 创建WhoDrug表管理 (whodrug_tables)")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS whodrug_tables (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    file_path TEXT,
                    record_count INTEGER DEFAULT 0,
                    version TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            print("V 数据库表结构创建完成")
            
        except Exception as e:
            print(f"X 创建数据库表结构失败: {e}")
            raise
        finally:
            conn.close()
    
    def import_data_file(self, file_path, table_name, category, description='', version=''):
        """
        导入数据文件到数据库
        
        Args:
            file_path (str): 数据文件路径
            table_name (str): 表名
            category (str): 分类（meddra 或 whodrug）
            description (str): 描述
            version (str): 版本
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        print(f"\n正在导入数据文件: {file_path}")
        print(f"目标表: {table_name} (分类: {category})")
        
        try:
            # 读取文件
            if file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path, encoding='utf-8')
            elif file_path.suffix.lower() == '.asc':
                # ASC文件特殊处理，假设是制表符分隔
                df = pd.read_csv(file_path, sep='\t', encoding='utf-8')
            elif file_path.suffix.lower() == '.pkl':
                df = pd.read_pickle(file_path)
            else:
                raise ValueError(f"不支持的文件格式: {file_path.suffix}")
            
            record_count = len(df)
            print(f"读取到 {record_count} 条记录")

            # 清理列名，移除特殊字符，防止SQL语法错误
            df.columns = [re.sub(r'[^A-Za-z0-9_]+', '', col) for col in df.columns]
            
            # 保存到数据库
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.cursor()
                
                # 更新表信息
                table_info_table = f"{category}_tables"

                # 将DataFrame完整写入到以table_name命名的表中
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"V 数据已完整写入到表: {table_name}")

                cursor.execute(f'''
                    INSERT OR REPLACE INTO {table_info_table} 
                    (name, description, file_path, record_count, version, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (table_name, description, str(file_path), record_count, version, datetime.now()))
                
                conn.commit()
                print(f"V 数据导入完成: {table_name}")
                
            finally:
                conn.close()
                
        except Exception as e:
            print(f"X 导入数据失败: {e}")
            raise
    
    def list_tables(self):
        """列出所有数据表"""
        print("\n=== 数据库表信息 ===")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # MedDRA表
            print("\n[MedDRA 表]:")
            cursor.execute('SELECT name, description, record_count, version, created_at FROM meddra_tables ORDER BY name')
            meddra_tables = cursor.fetchall()
            if meddra_tables:
                for row in meddra_tables:
                    print(f"  - {row[0]}: {row[1] or '无描述'} ({row[2]}条记录) 版本:{row[3] or 'N/A'}")
            else:
                print("  (无数据)")
            
            # WhoDrug表
            print("\n[WhoDrug 表]:")
            cursor.execute('SELECT name, description, record_count, version, created_at FROM whodrug_tables ORDER BY name')
            whodrug_tables = cursor.fetchall()
            if whodrug_tables:
                for row in whodrug_tables:
                    print(f"  - {row[0]}: {row[1] or '无描述'} ({row[2]}条记录) 版本:{row[3] or 'N/A'}")
            else:
                print("  (无数据)")
            
        finally:
            conn.close()
    
    def delete_table(self, table_name, category):
        """删除数据表"""
        print(f"\n正在删除表: {table_name} (分类: {category})")
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()

            # 删除动态创建的数据表
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            print(f"删除物理表: {table_name}")
            
            # 删除表信息
            table_info_table = f"{category}_tables"
            cursor.execute(f'DELETE FROM {table_info_table} WHERE name = ?', (table_name,))
            
            conn.commit()
            print(f"V 表删除完成: {table_name}")
            
        finally:
            conn.close()
    
    def clear_database(self):
        """清空数据库"""
        print("\n警告: 即将清空整个数据库!")
        confirm = input("确认要删除所有数据吗? (输入 'YES' 确认): ")
        
        if confirm != 'YES':
            print("操作已取消")
            return
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 获取所有用户创建的表
            cursor.execute("SELECT name FROM meddra_tables")
            meddra_table_names = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT name FROM whodrug_tables")
            whodrug_table_names = [row[0] for row in cursor.fetchall()]

            for table_name in meddra_table_names + whodrug_table_names:
                cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
                print(f"删除物理表: {table_name}")

            # 删除所有表数据
            tables = ['meddra_tables', 'whodrug_tables']
            
            for table in tables:
                cursor.execute(f'DELETE FROM {table}')
                print(f"清空表: {table}")
            
            conn.commit()
            
            # 删除数据文件
            try:
                for file_path in self.data_dir.glob('*'):
                    if file_path.is_file():
                        file_path.unlink()
                        print(f"删除文件: {file_path}")
            except Exception as e:
                print(f"删除文件时出错: {e}")
            
            print("V 数据库清空完成")
            
        finally:
            conn.close()
    
    def database_status(self):
        """显示数据库状态"""
        print("\n=== 数据库状态 ===")
        
        if not Path(self.db_path).exists():
            print("X 数据库文件不存在")
            return
        
        print(f"V 数据库文件: {self.db_path}")
        print(f"V 数据目录: {self.data_dir}")
        
        # 检查表结构
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = ['meddra_tables', 'whodrug_tables']
            
            print(f"\n[数据库表] ({len(tables)}个):")
            for table in expected_tables:
                if table in tables:
                    cursor.execute(f'SELECT COUNT(*) FROM {table}')
                    count = cursor.fetchone()[0]
                    print(f"  V {table}: {count}条记录")
                else:
                    print(f"  X {table}: 表不存在")
        
        finally:
            conn.close()


def main():
    parser = argparse.ArgumentParser(description='SAS数据集翻译平台 - 数据库初始化工具')
    parser.add_argument('--db', default='translation_db.sqlite', help='数据库文件路径')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 初始化命令
    init_parser = subparsers.add_parser('init', help='初始化数据库结构')
    
    # 导入命令
    import_parser = subparsers.add_parser('import', help='导入数据文件')
    import_parser.add_argument('file', help='数据文件路径')
    import_parser.add_argument('table', help='表名')
    import_parser.add_argument('category', choices=['meddra', 'whodrug'], help='数据分类')
    import_parser.add_argument('--desc', default='', help='表描述')
    import_parser.add_argument('--version', default='', help='版本信息')
    
    # 列表命令
    list_parser = subparsers.add_parser('list', help='列出所有表')
    
    # 删除命令
    delete_parser = subparsers.add_parser('delete', help='删除表')
    delete_parser.add_argument('table', help='表名')
    delete_parser.add_argument('category', choices=['meddra', 'whodrug'], help='数据分类')
    
    # 状态命令
    status_parser = subparsers.add_parser('status', help='显示数据库状态')
    
    # 清空命令
    clear_parser = subparsers.add_parser('clear', help='清空数据库')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        db_setup = DatabaseSetup(args.db)
        
        if args.command == 'init':
            db_setup.create_database_structure()
        
        elif args.command == 'import':
            # 导入时，不再需要手动创建数据文件副本，因为数据直接进库
            db_setup.import_data_file(args.file, args.table, args.category, 
                                    args.desc, args.version)
        
        elif args.command == 'list':
            db_setup.list_tables()
        
        elif args.command == 'delete':
            db_setup.delete_table(args.table, args.category)
        
        elif args.command == 'status':
            db_setup.database_status()
        
        elif args.command == 'clear':
            db_setup.clear_database()
        
    except Exception as e:
        print(f"\n[ERROR] 错误: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

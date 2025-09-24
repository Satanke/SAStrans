#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建并合并variablelabel_mergeds表
将6张SDTM变量表合并到统一的variablelabel_mergeds表中
"""

import sqlite3
import os

def create_variablelabel_mergeds_table():
    """创建variablelabel_mergeds表"""
    
    # 数据库文件路径
    db_path = 'translation_db.sqlite'
    
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件 {db_path} 不存在")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 创建variablelabel_mergeds表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS variablelabel_mergeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                variable TEXT NOT NULL,
                name_cn TEXT,
                name_en TEXT,
                version TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(domain, variable, version)
            )
        ''')
        
        print("variablelabel_mergeds表创建成功")
        
        # 清空表中现有数据
        cursor.execute('DELETE FROM variablelabel_mergeds')
        print("清空现有数据")
        
        # 定义源表和对应的版本、语言映射
        table_mappings = [
            ('sdtm2variable3_2cn', '3.2', 'cn'),
            ('sdtm2variable3_2en', '3.2', 'en'),
            ('sdtm2variable3_3cn', '3.3', 'cn'),
            ('sdtm2variable3_3en', '3.3', 'en'),
            ('sdtm2variable3_4cn', '3.4', 'cn'),
            ('sdtm2variable3_4en', '3.4', 'en')
        ]
        
        # 检查源表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        # 存储合并数据的字典 {(domain, variable, version): {'cn': label_cn, 'en': label_en}}
        merged_data = {}
        
        # 处理每个源表
        for table_name, version, language in table_mappings:
            if table_name not in existing_tables:
                print(f"警告: 表 {table_name} 不存在，跳过")
                continue
            
            print(f"处理表: {table_name} (版本: {version}, 语言: {language})")
            
            # 查询表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # 检查必要的列是否存在
            required_columns = ['domain', 'variable_name', 'variable_label']
            existing_columns = [col.lower() for col in columns]
            
            missing_columns = [col for col in required_columns if col not in existing_columns]
            if missing_columns:
                print(f"表 {table_name} 缺少必要的列: {missing_columns}， 跳过")
                continue
            
            # 读取数据并处理
            cursor.execute(f"SELECT domain, variable_name, variable_label FROM {table_name}")
            rows = cursor.fetchall()
            
            print(f"从表 {table_name} 读取到 {len(rows)} 条记录")
            
            # 合并数据
            for domain, variable_name, variable_label in rows:
                if not domain or not variable_name:
                    continue
                    
                key = (domain.strip(), variable_name.strip(), version)
                
                if key not in merged_data:
                    merged_data[key] = {'cn': None, 'en': None}
                
                # 根据语言设置对应的标签
                if variable_label:
                    merged_data[key][language] = variable_label.strip()
        
        # 插入合并后的数据
        insert_count = 0
        for (domain, variable, version), labels in merged_data.items():
            cursor.execute('''
                INSERT OR REPLACE INTO variablelabel_mergeds 
                (domain, variable, name_cn, name_en, version)
                VALUES (?, ?, ?, ?, ?)
            ''', (domain, variable, labels['cn'], labels['en'], version))
            insert_count += 1
        
        conn.commit()
        print(f"成功插入 {insert_count} 条记录到 variablelabel_mergeds 表")
        
        # 显示统计信息
        cursor.execute('SELECT version, COUNT(*) FROM variablelabel_mergeds GROUP BY version')
        stats = cursor.fetchall()
        print("\n各版本记录统计:")
        for version, count in stats:
            print(f"  版本 {version}: {count} 条记录")
        
        # 显示示例数据
        cursor.execute('SELECT * FROM variablelabel_mergeds LIMIT 5')
        sample_data = cursor.fetchall()
        print("\n示例数据:")
        print("ID | Domain | Variable | Name_CN | Name_EN | Version")
        print("-" * 80)
        for row in sample_data:
            print(f"{row[0]} | {row[1]} | {row[2]} | {row[3] or 'NULL'} | {row[4] or 'NULL'} | {row[5]}")
        
        return True
        
    except Exception as e:
        print(f"创建表时出错: {e}")
        import traceback
        print("详细错误信息:")
        traceback.print_exc()
        conn.rollback()
        return False
    finally:
        conn.close()

def verify_merge_results():
    """验证合并结果"""
    db_path = 'translation_db.sqlite'
    
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件 {db_path} 不存在")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='variablelabel_mergeds'")
        if not cursor.fetchone():
            print("variablelabel_mergeds表不存在")
            return
        
        # 总记录数
        cursor.execute('SELECT COUNT(*) FROM variablelabel_mergeds')
        total_count = cursor.fetchone()[0]
        print(f"总记录数: {total_count}")
        
        # 按版本统计
        cursor.execute('SELECT version, COUNT(*) FROM variablelabel_mergeds GROUP BY version ORDER BY version')
        version_stats = cursor.fetchall()
        print("\n按版本统计:")
        for version, count in version_stats:
            print(f"  版本 {version}: {count} 条记录")
        
        # 按domain统计前10个
        cursor.execute('''
            SELECT domain, COUNT(*) as cnt 
            FROM variablelabel_mergeds 
            GROUP BY domain 
            ORDER BY cnt DESC 
            LIMIT 10
        ''')
        domain_stats = cursor.fetchall()
        print("\n按Domain统计(前10):")
        for domain, count in domain_stats:
            print(f"  {domain}: {count} 条记录")
        
        # 检查中英文标签完整性
        cursor.execute('SELECT COUNT(*) FROM variablelabel_mergeds WHERE name_cn IS NOT NULL')
        cn_count = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM variablelabel_mergeds WHERE name_en IS NOT NULL')
        en_count = cursor.fetchone()[0]
        
        print(f"\n标签完整性:")
        print(f"  有中文标签: {cn_count} 条 ({cn_count/total_count*100:.1f}%)")
        print(f"  有英文标签: {en_count} 条 ({en_count/total_count*100:.1f}%)")
        
    except Exception as e:
        print(f"验证时出错: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    print("开始创建并合并variablelabel_mergeds表...")
    print("=" * 50)
    
    success = create_variablelabel_mergeds_table()
    
    if success:
        print("\n=" * 50)
        print("验证合并结果...")
        verify_merge_results()
        print("\n合并完成!")
    else:
        print("\n合并失败!")
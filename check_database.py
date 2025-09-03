#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库状态检查工具
Database Status Checker

快速检查数据库状态和连接
"""

import sqlite3
import sys
from pathlib import Path
from datetime import datetime


def check_database_status(db_path='translation_db.sqlite'):
    """检查数据库状态"""
    print("=== 数据库状态检查 ===\n")
    
    db_file = Path(db_path)
    
    # 检查文件是否存在
    if not db_file.exists():
        print(f"❌ 数据库文件不存在: {db_path}")
        print("请先运行: python database_setup.py init")
        return False
    
    print(f"✓ 数据库文件: {db_path}")
    print(f"📁 文件大小: {db_file.stat().st_size / 1024:.2f} KB")
    print(f"📅 修改时间: {datetime.fromtimestamp(db_file.stat().st_mtime)}")
    
    try:
        # 连接数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查表结构
        print("\n📋 数据库表:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        if not tables:
            print("  ❌ 数据库为空，没有找到任何表")
            return False
        
        table_status = {}
        for (table_name,) in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                table_status[table_name] = count
                print(f"  ✓ {table_name}: {count} 条记录")
            except Exception as e:
                print(f"  ❌ {table_name}: 查询失败 ({e})")
        
        # 详细统计
        print("\n📊 详细统计:")
        
        # MedDRA表统计
        if 'meddra_tables' in table_status:
            cursor.execute("SELECT COUNT(*), SUM(record_count) FROM meddra_tables")
            result = cursor.fetchone()
            print(f"  📚 MedDRA表: {result[0]}个表，共{result[1] or 0}条数据记录")
        
        # WhoDrug表统计
        if 'whodrug_tables' in table_status:
            cursor.execute("SELECT COUNT(*), SUM(record_count) FROM whodrug_tables")
            result = cursor.fetchone()
            print(f"  💊 WhoDrug表: {result[0]}个表，共{result[1] or 0}条数据记录")
        
        # 翻译库统计
        if 'translation_library' in table_status:
            cursor.execute("SELECT direction, COUNT(*) FROM translation_library GROUP BY direction")
            translations = cursor.fetchall()
            if translations:
                print("  🌐 翻译库:")
                for direction, count in translations:
                    print(f"    - {direction}: {count}条翻译")
            else:
                print("  🌐 翻译库: 暂无翻译记录")
        
        # 映射配置统计
        if 'mapping_configs' in table_status:
            cursor.execute("SELECT COUNT(*) FROM mapping_configs")
            count = cursor.fetchone()[0]
            print(f"  ⚙️  映射配置: {count}个保存的配置")
        
        conn.close()
        
        print("\n✅ 数据库状态正常")
        return True
        
    except sqlite3.Error as e:
        print(f"\n❌ 数据库连接错误: {e}")
        return False
    except Exception as e:
        print(f"\n❌ 检查过程中出错: {e}")
        return False


def test_database_operations(db_path='translation_db.sqlite'):
    """测试数据库基本操作"""
    print("\n=== 数据库操作测试 ===")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 测试查询
        print("🔍 测试基本查询...")
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = cursor.fetchone()[0]
        print(f"  ✓ 查询成功，找到 {table_count} 个表")
        
        # 测试插入（在翻译库中插入测试记录）
        print("✏️  测试插入操作...")
        test_time = datetime.now().isoformat()
        cursor.execute("""
            INSERT OR IGNORE INTO translation_library 
            (source_text, target_text, direction, category, confidence) 
            VALUES (?, ?, ?, ?, ?)
        """, (f"test_source_{test_time}", f"test_target_{test_time}", "zh_to_en", "test", 0.9))
        
        if cursor.rowcount > 0:
            print("  ✓ 插入测试记录成功")
            
            # 清理测试记录
            cursor.execute("DELETE FROM translation_library WHERE category = 'test'")
            print("  ✓ 清理测试记录成功")
        else:
            print("  ⚠️  测试记录已存在（这是正常的）")
        
        conn.commit()
        conn.close()
        
        print("✅ 数据库操作测试通过")
        return True
        
    except Exception as e:
        print(f"❌ 数据库操作测试失败: {e}")
        return False


def main():
    """主函数"""
    db_path = 'translation_db.sqlite'
    
    # 如果提供了命令行参数，使用自定义数据库路径
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    print(f"检查数据库: {db_path}\n")
    
    # 检查数据库状态
    status_ok = check_database_status(db_path)
    
    if status_ok:
        # 运行操作测试
        test_ok = test_database_operations(db_path)
        
        if test_ok:
            print("\n🎉 数据库检查完成，一切正常！")
            print("\n可以安全地运行主应用程序:")
            print("  python app.py")
        else:
            print("\n⚠️  数据库状态正常，但操作测试失败")
            print("建议重新初始化数据库:")
            print("  python database_setup.py init")
    else:
        print("\n🔧 建议操作:")
        print("1. 初始化数据库: python database_setup.py init")
        print("2. 导入数据: python database_setup.py import [文件] [表名] [分类]")
        print("3. 查看帮助: python database_setup.py --help")


if __name__ == '__main__':
    main()

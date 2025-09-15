#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接测试SUPP数据合并功能的单元测试
绕过SAS文件格式限制，直接测试_merge_supp_data方法
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import numpy as np
from app import SASDataProcessor

def test_supp_merge_functionality():
    """直接测试SUPP数据合并功能"""
    print("=" * 60)
    print("SUPP数据合并单元测试")
    print("=" * 60)
    
    # 创建SASDataProcessor实例
    processor = SASDataProcessor()
    
    # 创建测试用的CM数据集（用药记录）
    cm_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 3,
        'USUBJID': ['001', '002', '003'],
        'CMSEQ': [1, 1, 1],
        'CMTRT': ['阿司匹林', '布洛芬', '对乙酰氨基酚'],
        'CMDOSE': [100, 200, 500],
        'CMDOSU': ['mg', 'mg', 'mg']
    })
    
    # 创建测试用的SUPPCM数据集（CM补充数据，包含ATC码）
    suppcm_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 6,
        'USUBJID': ['001', '001', '002', '002', '003', '003'],
        'RDOMAIN': ['CM'] * 6,
        'IDVAR': ['CMSEQ'] * 6,
        'IDVARVAL': ['1', '1', '1', '1', '1', '1'],
        'QNAM': ['ATC1CD', 'ATC2CD', 'ATC1CD', 'ATC2CD', 'ATC1CD', 'ATC2CD'],
        'QLABEL': ['ATC Level 1 Code', 'ATC Level 2 Code'] * 3,
        'QVAL': ['N02BA', 'N02BA01', 'M01AE', 'M01AE01', 'N02BE', 'N02BE01']
    })
    
    print("\n📊 测试数据创建完成")
    print(f"CM数据集行数: {len(cm_data)}")
    print(f"SUPPCM数据集行数: {len(suppcm_data)}")
    
    # 将数据加载到processor中
    processor.datasets = {
        'CM': {
            'data': cm_data.copy(),
            'raw_data': cm_data.copy(),
            'variables': list(cm_data.columns)
        },
        'SUPPCM': {
            'data': suppcm_data.copy(),
            'raw_data': suppcm_data.copy(),
            'variables': list(suppcm_data.columns)
        }
    }
    
    print("\n🔄 开始测试SUPP数据合并...")
    
    # 获取合并前的CM数据集列数
    original_columns = list(processor.datasets['CM']['data'].columns)
    print(f"合并前CM数据集列数: {len(original_columns)}")
    print(f"合并前CM数据集列名: {original_columns}")
    
    try:
        # 直接调用_merge_supp_data方法
        processor._merge_supp_data('SUPPCM', 'CM', suppcm_data)
        
        # 检查合并结果
        merged_cm = processor.datasets['CM']['data']
        merged_columns = list(merged_cm.columns)
        
        print(f"\n✅ 合并完成")
        print(f"合并后CM数据集行数: {len(merged_cm)}")
        print(f"合并后CM数据集列数: {len(merged_columns)}")
        print(f"合并后CM数据集列名: {merged_columns}")
        
        # 检查是否包含ATC相关列
        atc_columns = [col for col in merged_columns if 'ATC' in col]
        print(f"\n🎯 ATC相关列: {atc_columns}")
        
        if atc_columns:
            print("\n🎉 测试成功：发现ATC列，SUPP数据合并功能正常！")
            
            # 显示合并后的数据样本
            print("\n📋 合并后的数据样本:")
            for col in atc_columns:
                print(f"\n{col} 列的值:")
                print(merged_cm[col].value_counts().to_string())
            
            # 显示完整的合并结果（前几行）
            print("\n📊 完整合并结果（前3行）:")
            display_cols = ['USUBJID', 'CMTRT'] + atc_columns
            print(merged_cm[display_cols].head(3).to_string(index=False))
            
            return True
        else:
            print("\n❌ 测试失败：未发现ATC列，SUPP数据合并可能有问题")
            return False
            
    except Exception as e:
        print(f"\n❌ 测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_auto_merge_all_supp():
    """测试自动合并所有SUPP数据的功能"""
    print("\n" + "=" * 60)
    print("测试自动合并所有SUPP数据功能")
    print("=" * 60)
    
    # 创建SASDataProcessor实例
    processor = SASDataProcessor()
    
    # 创建测试数据
    cm_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 2,
        'USUBJID': ['001', '002'],
        'CMSEQ': [1, 1],
        'CMTRT': ['阿司匹林', '布洛芬']
    })
    
    ae_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 2,
        'USUBJID': ['001', '002'],
        'AESEQ': [1, 1],
        'AETERM': ['头痛', '恶心']
    })
    
    suppcm_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 2,
        'USUBJID': ['001', '002'],
        'RDOMAIN': ['CM', 'CM'],
        'IDVAR': ['CMSEQ', 'CMSEQ'],
        'IDVARVAL': ['1', '1'],
        'QNAM': ['ATC1CD', 'ATC1CD'],
        'QVAL': ['N02BA', 'M01AE']
    })
    
    suppae_data = pd.DataFrame({
        'STUDYID': ['STUDY01'] * 2,
        'USUBJID': ['001', '002'],
        'RDOMAIN': ['AE', 'AE'],
        'IDVAR': ['AESEQ', 'AESEQ'],
        'IDVARVAL': ['1', '1'],
        'QNAM': ['AESER', 'AESER'],
        'QVAL': ['Y', 'N']
    })
    
    # 加载数据到processor
    processor.datasets = {
        'CM': {'data': cm_data.copy(), 'raw_data': cm_data.copy()},
        'AE': {'data': ae_data.copy(), 'raw_data': ae_data.copy()},
        'SUPPCM': {'data': suppcm_data.copy(), 'raw_data': suppcm_data.copy()},
        'SUPPAE': {'data': suppae_data.copy(), 'raw_data': suppae_data.copy()}
    }
    
    print("\n🔄 调用自动合并所有SUPP数据...")
    
    try:
        # 调用自动合并方法
        processor._auto_merge_all_supp_data()
        
        # 检查CM数据集
        cm_result = processor.datasets['CM']['data']
        cm_columns = list(cm_result.columns)
        atc_in_cm = [col for col in cm_columns if 'ATC' in col]
        
        # 检查AE数据集
        ae_result = processor.datasets['AE']['data']
        ae_columns = list(ae_result.columns)
        aeser_in_ae = [col for col in ae_columns if 'AESER' in col]
        
        print(f"\n✅ 自动合并完成")
        print(f"CM数据集新增列: {atc_in_cm}")
        print(f"AE数据集新增列: {aeser_in_ae}")
        
        success = len(atc_in_cm) > 0 and len(aeser_in_ae) > 0
        
        if success:
            print("\n🎉 自动合并测试成功：所有SUPP数据都已正确合并！")
        else:
            print("\n❌ 自动合并测试失败：部分SUPP数据未正确合并")
            
        return success
        
    except Exception as e:
        print(f"\n❌ 自动合并测试异常: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主测试函数"""
    print("开始SUPP数据合并单元测试...\n")
    
    # 测试1：直接测试_merge_supp_data方法
    test1_success = test_supp_merge_functionality()
    
    # 测试2：测试自动合并所有SUPP数据
    test2_success = test_auto_merge_all_supp()
    
    # 总结测试结果
    print("\n" + "=" * 60)
    print("测试结果总结")
    print("=" * 60)
    print(f"测试1 - 直接SUPP合并: {'✅ 通过' if test1_success else '❌ 失败'}")
    print(f"测试2 - 自动合并所有SUPP: {'✅ 通过' if test2_success else '❌ 失败'}")
    
    overall_success = test1_success and test2_success
    print(f"\n总体结果: {'🎉 所有测试通过' if overall_success else '❌ 部分测试失败'}")
    
    return overall_success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
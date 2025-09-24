# -*- coding: utf-8 -*-
import os
import sqlite3
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime
from flask import Blueprint, request, jsonify, render_template
import re

# 创建蓝图
data_translation_bp = Blueprint('data_translation', __name__)

class DataTranslationProcessor:
    def __init__(self, db_path):
        self.db_path = db_path
    
    def check_translation_status(self, path):
        """检查翻译状态"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 检查是否有未确认的数据
            cursor.execute('''
                SELECT COUNT(*) FROM translation_results 
                WHERE path_hash = ? AND needs_confirmation = 1
            ''', (path_hash,))
            unconfirmed_count = cursor.fetchone()[0]
            
            # 检查是否有空翻译值
            cursor.execute('''
                SELECT COUNT(*) FROM translation_results 
                WHERE path_hash = ? AND (translated_value IS NULL OR translated_value = '')
            ''', (path_hash,))
            empty_translation_count = cursor.fetchone()[0]
            
            return {
                'unconfirmed_count': unconfirmed_count,
                'empty_translation_count': empty_translation_count
            }
        finally:
            conn.close()
    
    def get_translation_data(self, path):
        """获取翻译数据"""
        path_hash = hashlib.md5(path.encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT dataset_name, variable_name, original_value, translated_value, translation_type
                FROM translation_results 
                WHERE path_hash = ?
            ''', (path_hash,))
            
            results = cursor.fetchall()
            translation_dict = {}
            
            for row in results:
                dataset_name, variable_name, original_value, translated_value, translation_type = row
                
                if dataset_name not in translation_dict:
                    translation_dict[dataset_name] = {}
                
                if variable_name not in translation_dict[dataset_name]:
                    translation_dict[dataset_name][variable_name] = {
                        'type': translation_type,
                        'translations': {}
                    }
                
                if original_value and translated_value:
                    translation_dict[dataset_name][variable_name]['translations'][original_value] = translated_value
            
            return translation_dict
        finally:
            conn.close()
    
    def calculate_string_length(self, text, encoding='utf-8'):
        """计算字符串长度（根据编码）"""
        if not text:
            return 0
        
        if encoding.lower() == 'utf-8':
            # UTF-8: 中文3字节，其他1字节
            length = 0
            for char in text:
                if '\u4e00' <= char <= '\u9fa5':  # 中文字符
                    length += 3
                else:
                    length += 1
            return length
        elif encoding.lower() == 'dbcs':
            # DBCS: 中文2字节，其他1字节
            length = 0
            for char in text:
                if '\u4e00' <= char <= '\u9fa5':  # 中文字符
                    length += 2
                else:
                    length += 1
            return length
        else:
            return len(text)
    
    def split_long_values(self, value, encoding='utf-8', max_length=200):
        """拆分长值到SUPP数据集"""
        if not value or self.calculate_string_length(value, encoding) <= max_length:
            return value, None
        
        # 找到合适的拆分点
        current_length = 0
        split_pos = 0
        
        words = re.findall(r'\S+|\s+', value)  # 分割单词和空格
        main_part = ''
        
        for i, word in enumerate(words):
            word_length = self.calculate_string_length(word, encoding)
            
            if current_length + word_length <= max_length:
                main_part += word
                current_length += word_length
                split_pos = len(main_part)
            else:
                break
        
        # 确保不从中文字符中间断开
        if split_pos < len(value):
            # 检查拆分点是否在中文字符中间
            char_at_split = value[split_pos] if split_pos < len(value) else ''
            if '\u4e00' <= char_at_split <= '\u9fa5':
                # 如果拆分点是中文字符，向前调整
                while split_pos > 0 and '\u4e00' <= value[split_pos-1] <= '\u9fa5':
                    split_pos -= 1
        
        main_value = value[:split_pos].rstrip()  # 移除末尾空格
        supp_value = value[split_pos:].lstrip()  # 移除开头空格
        
        return main_value, supp_value if supp_value else None
    
    def translate_dataset(self, dataset_path, translation_dict, encoding='utf-8'):
        """翻译数据集"""
        try:
            # 读取SAS数据集
            df = pd.read_sas(dataset_path)
            dataset_name = os.path.splitext(os.path.basename(dataset_path))[0].upper()
            
            if dataset_name not in translation_dict:
                return df, None
            
            dataset_translations = translation_dict[dataset_name]
            supp_data = []
            
            # 翻译变量标签
            for var_name in df.columns:
                if var_name in dataset_translations:
                    var_info = dataset_translations[var_name]
                    if var_info['type'] in ['variable_label', 'dataset_label']:
                        # 设置变量标签
                        for original, translated in var_info['translations'].items():
                            if original == var_name or original in str(df[var_name].attrs.get('label', '')):
                                df[var_name].attrs['label'] = translated
                                break
            
            # 翻译数据值
            for var_name in df.columns:
                if var_name in dataset_translations:
                    var_info = dataset_translations[var_name]
                    if var_info['type'] in ['coded_list', 'non_coded_list']:
                        translations = var_info['translations']
                        
                        # 应用翻译
                        for i, value in enumerate(df[var_name]):
                            if pd.notna(value) and str(value) in translations:
                                translated_value = translations[str(value)]
                                
                                # 检查是否需要拆分到SUPP
                                main_value, supp_value = self.split_long_values(
                                    translated_value, encoding
                                )
                                
                                df.loc[i, var_name] = main_value
                                
                                if supp_value:
                                    # 创建SUPP记录
                                    supp_record = {
                                        'STUDYID': df.loc[i, 'STUDYID'] if 'STUDYID' in df.columns else '',
                                        'RDOMAIN': dataset_name,
                                        'USUBJID': df.loc[i, 'USUBJID'] if 'USUBJID' in df.columns else '',
                                        'IDVAR': 'USUBJID',
                                        'IDVARVAL': df.loc[i, 'USUBJID'] if 'USUBJID' in df.columns else '',
                                        'QNAM': var_name,
                                        'QLABEL': f'{var_name} Supplemental',
                                        'QVAL': supp_value,
                                        'QORIG': 'CRF',
                                        'QEVAL': ''
                                    }
                                    supp_data.append(supp_record)
            
            # 创建SUPP数据集
            supp_df = pd.DataFrame(supp_data) if supp_data else None
            
            return df, supp_df
            
        except Exception as e:
            raise Exception(f"翻译数据集 {dataset_path} 时发生错误: {str(e)}")
    
    def get_merged_datasets_from_db(self):
        """从数据库获取合并配置的数据集路径"""
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            
            # 获取最新的合并配置路径
            cursor.execute('''
                SELECT DISTINCT path FROM translation_results 
                WHERE path IS NOT NULL AND path != ''
                ORDER BY id DESC LIMIT 1
            ''')
            
            result = cursor.fetchone()
            return result[0] if result else None
        finally:
            conn.close()
    
    def process_translation(self, output_path, encoding='utf-8'):
        """处理数据翻译"""
        try:
            # 从数据库获取合并配置的数据集路径
            input_path = self.get_merged_datasets_from_db()
            
            if not input_path:
                return {'success': False, 'message': '未找到合并配置的数据集，请先完成数据合并配置'}
            
            if not os.path.exists(input_path):
                return {'success': False, 'message': f'合并配置的数据集路径不存在: {input_path}'}
            
            # 检查翻译状态
            status = self.check_translation_status(input_path)
            if status['unconfirmed_count'] > 0:
                return {'success': False, 'message': '存在未确认的数据，请先确认'}
            
            if status['empty_translation_count'] > 0:
                return {'success': False, 'message': '翻译值存在空值，请先确认'}
            
            # 获取翻译数据
            translation_dict = self.get_translation_data(input_path)
            
            if not translation_dict:
                return {'success': False, 'message': '未找到翻译数据'}
            
            # 确保输出目录存在
            os.makedirs(output_path, exist_ok=True)
            
            # 查找输入路径下的所有SAS数据集
            sas_files = []
            for file in os.listdir(input_path):
                if file.lower().endswith('.sas7bdat'):
                    sas_files.append(os.path.join(input_path, file))
            
            if not sas_files:
                return {'success': False, 'message': '未找到SAS数据集文件'}
            
            translated_count = 0
            supp_count = 0
            
            # 翻译每个数据集
            for sas_file in sas_files:
                try:
                    df, supp_df = self.translate_dataset(sas_file, translation_dict, encoding)
                    
                    # 输出主数据集
                    output_file = os.path.join(output_path, os.path.basename(sas_file))
                    df.to_sas(output_file, encoding=encoding)
                    translated_count += 1
                    
                    # 输出SUPP数据集
                    if supp_df is not None and not supp_df.empty:
                        dataset_name = os.path.splitext(os.path.basename(sas_file))[0]
                        supp_file = os.path.join(output_path, f'SUPP{dataset_name}.sas7bdat')
                        supp_df.to_sas(supp_file, encoding=encoding)
                        supp_count += 1
                        
                except Exception as e:
                    print(f"处理文件 {sas_file} 时发生错误: {str(e)}")
                    continue
            
            return {
                'success': True,
                'message': f'翻译完成！共处理 {translated_count} 个数据集，生成 {supp_count} 个SUPP数据集',
                'translated_count': translated_count,
                'supp_count': supp_count
            }
            
        except Exception as e:
            return {'success': False, 'message': f'翻译过程中发生错误: {str(e)}'}

# 路由定义
@data_translation_bp.route('/data_translation')
def data_translation_page():
    """数据翻译页面"""
    return render_template('data_translation.html')

@data_translation_bp.route('/api/data_translation', methods=['POST'])
def process_data_translation():
    """处理数据翻译请求"""
    try:
        data = request.get_json()
        output_path = data.get('output_path')
        encoding = data.get('encoding', 'utf-8')
        
        if not output_path:
            return jsonify({'success': False, 'message': '请提供输出路径'}), 400
        
        # 这里需要传入数据库路径，应该从主应用获取
        from app import db_manager
        processor = DataTranslationProcessor(db_manager.db_path)
        
        result = processor.process_translation(output_path, encoding)
        
        if result['success']:
            return jsonify(result)
        else:
            return jsonify(result), 400
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
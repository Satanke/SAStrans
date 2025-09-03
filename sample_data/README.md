# MedDRA和WHODrug数据目录说明

这个工具支持自动识别MedDRA和WHODrug的标准目录结构并批量导入数据。

## MedDRA 目录结构
```
MedDRA根目录/
├── meddra_18_0_chinese/
│   └── ascii-xxx/
│       ├── hlgt.asc
│       ├── hlt.asc
│       ├── llt.asc
│       ├── pt.asc
│       └── soc.asc
├── meddra_18_0_english/
│   └── MedAscii/
│       ├── hlgt.asc
│       ├── hlt.asc
│       ├── llt.asc
│       ├── pt.asc
│       └── soc.asc
└── meddra_21_0_chinese/
    └── ascii-xxx/
        └── ...
```

## WHODrug 目录结构
```
WHODrug根目录/
├── WHODrug Global 2023 Mar 1/
│   ├── WHODrug B3_C3-format/
│   │   └── whodrug_global_b3_xxx/
│   │       ├── INA.txt
│   │       └── DD.txt
│   └── WHODrug Global Chinese txt_mar_1_2024/
│       └── whodrug_global_b3_xxx/
│           ├── INA.txt
│           └── DD.txt
└── WHODrug Global 2023 Sep 1/
    └── ...
```

## 文件格式说明

### MedDRA文件格式
- 使用$作为分隔符
- 第一部分为代码，第二部分为名称
- 每个文件对应不同的术语级别

### WHODrug文件格式
- 使用空格作为分隔符
- 第一部分为代码，其余部分作为名称
- INA.txt和DD.txt包含不同类型的药物信息

## 使用方法
1. 确保数据目录符合上述结构
2. 运行: python batch_import_example.py
3. 按提示输入MedDRA和WHODrug的根目录路径
4. 工具会自动扫描并导入所有符合规范的数据

## 命令行选项
- `python batch_import_example.py` - 交互式导入
- `python batch_import_example.py demo` - 演示模式（扫描示例路径）
- `python batch_import_example.py create-structure` - 创建目录结构说明

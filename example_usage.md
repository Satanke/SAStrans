# SAS数据集翻译平台使用示例

## 示例场景

假设您有以下SAS数据集文件结构：
```
/data/study_datasets/
├── DM.sas7bdat        # 人口学数据
├── AE.sas7bdat        # 不良事件数据  
├── SUPPAE.sas7bdat    # AE补充数据
├── VS.sas7bdat        # 生命体征数据
└── SUPPVS.sas7bdat    # VS补充数据
```

## RAW模式使用示例

### 步骤1：选择模式
- 翻译模式选择：`RAW`
- 数据集路径：`/data/study_datasets/`

### 步骤2：读取数据
点击"读取"按钮后，系统将显示：
- DM选项卡：显示人口学数据
- AE选项卡：显示不良事件数据
- SUPPAE选项卡：显示AE补充数据
- VS选项卡：显示生命体征数据
- SUPPVS选项卡：显示VS补充数据

## SDTM模式使用示例

### 步骤1：选择模式和读取
- 翻译模式选择：`SDTM`
- 数据集路径：`/data/study_datasets/`
- 点击"读取"按钮

### 步骤2：自动SUPP合并
系统自动执行以下操作：

**SUPPAE数据处理：**
```
原始SUPPAE数据：
STUDYID  USUBJID  RDOMAIN  QNAM     QVAL     QLABEL    IDVAR    IDVARVAL
STUDY01  001      AE       AESER    Y        严重AE     AESEQ    1
STUDY01  001      AE       AESCAN   MRI      扫描方式   AESEQ    1

转置后合并到AE数据集：
STUDYID  USUBJID  AESEQ  AETERM  ...  AESER  AESCAN
STUDY01  001      1      头痛    ...  Y      MRI
```

### 步骤3：手动变量合并（可选）
如果需要进一步合并变量，例如将多个日期字段合并：

1. 点击"添加合并配置"
2. 选择数据集：`AE`
3. 选择目标变量：`AESTDT`（开始日期）
4. 选择源变量：`AESTDTC`、`AESTTM`（日期、时间）
5. 点击"完成配置"
6. 点击"确认合并"

结果：
```
合并前：
AESTDT   AESTDTC    AESTTM
         2023-01-15 14:30

合并后：
AESTDT
2023-01-15 14:30
```

## 常见SUPPxx数据结构示例

### SUPPAE示例数据：
| STUDYID | USUBJID | RDOMAIN | IDVAR | IDVARVAL | QNAM | QLABEL | QVAL |
|---------|---------|---------|-------|----------|------|--------|------|
| STUDY01 | 001     | AE      | AESEQ | 1        | AESER | 严重程度 | 严重 |
| STUDY01 | 001     | AE      | AESEQ | 1        | AESCAN | 检查方式 | CT |
| STUDY01 | 001     | AE      | AESEQ | 2        | AESER | 严重程度 | 轻微 |

### 对应的AE主数据集：
| STUDYID | USUBJID | AESEQ | AETERM |
|---------|---------|-------|---------|
| STUDY01 | 001     | 1     | 头痛    |
| STUDY01 | 001     | 2     | 恶心    |

### 合并后的结果：
| STUDYID | USUBJID | AESEQ | AETERM | AESER | AESCAN |
|---------|---------|-------|---------|-------|--------|
| STUDY01 | 001     | 1     | 头痛    | 严重  | CT     |
| STUDY01 | 001     | 2     | 恶心    | 轻微  |        |

## 变量合并配置示例

### 示例1：日期时间合并
- 目标变量：`FULL_DATETIME`
- 源变量：`VISIT_DATE`, `VISIT_TIME`
- 结果：将日期和时间合并为完整的日期时间字符串

### 示例2：地址信息合并
- 目标变量：`FULL_ADDRESS`
- 源变量：`PROVINCE`, `CITY`, `DISTRICT`, `STREET`
- 结果：将地址组件合并为完整地址

### 示例3：实验室结果描述合并
- 目标变量：`LAB_RESULT_DESC`
- 源变量：`LBTEST`, `LBORRES`, `LBORRESU`
- 结果：将检查项目、结果值、单位合并为描述性文本

## 注意事项

1. **SUPP合并要求**：
   - SUPP数据集必须包含RDOMAIN字段
   - 目标数据集必须存在且名称与RDOMAIN值匹配
   - 合并键（STUDYID, USUBJID）必须在两个数据集中都存在

2. **变量合并规则**：
   - 源变量值会用空格连接
   - 空值会被自动跳过
   - 合并后源变量会被删除

3. **性能考虑**：
   - 大型数据集可能需要较长处理时间
   - 建议先用小数据集测试配置

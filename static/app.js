// 全局变量
let currentDatasets = {};
let currentMode = 'RAW';
let mergeConfigs = [];
let currentConfigIndex = -1;
let isConfiguring = false;
let currentConfig = null;
let lastDataPath = '';
let isMergeExecuted = false; // 跟踪合并执行状态

// 虚拟滚动状态：按数据集维护偏移量与总行数
const virtualState = {}; 

// DOM元素 - 基础设置
const translationModeSelect = document.getElementById('translationMode');
const translationDirectionSelect = document.getElementById('translationDirection');
const translationDirectionSection = document.getElementById('translationDirectionSection');
const datasetPathInput = document.getElementById('datasetPath');
const readButton = document.getElementById('readButton');
const setupProgress = document.getElementById('setupProgress');
const nextToPreviewContainer = document.getElementById('nextToPreviewContainer');
const nextToPreviewBtn = document.getElementById('nextToPreviewBtn');

// DOM元素 - 主要标签页
const mainTabs = document.getElementById('mainTabs');
const setupTab = document.getElementById('setup-tab');
const previewTab = document.getElementById('preview-tab');
const mergeConfigTab = document.getElementById('merge-config-tab');


// DOM元素 - 数据预览
const datasetList = document.getElementById('datasetList');
const previewContent = document.getElementById('previewContent');
const currentDatasetTitle = document.getElementById('currentDatasetTitle');
const refreshPreviewBtn = document.getElementById('refreshPreviewBtn');
const showAllDataBtn = document.getElementById('showAllDataBtn');
const previewSummary = document.getElementById('previewSummary');
const datasetStats = document.getElementById('datasetStats');
const backToSetupBtn = document.getElementById('backToSetupBtn');
const nextToMergeBtn = document.getElementById('nextToMergeBtn');

// 当前选中的数据集
let currentSelectedDataset = null;

// 数据预览状态管理
const previewState = {
    currentDataset: null,
    loadedRows: 0,
    totalRows: 0,
    pageSize: 200,
    loading: false,
    hasMore: true,
    columns: [],
    allData: []
};

// DOM元素 - 合并配置
const currentConfigSection = document.getElementById('currentConfigSection');
const currentConfigIndexSpan = document.getElementById('currentConfigIndex');
const configDatasetSelect = document.getElementById('configDatasetSelect');
const configVariableSelect = document.getElementById('configVariableSelect');
const selectedVariablesDisplay = document.getElementById('selectedVariablesDisplay');
const addVariableBtn = document.getElementById('addVariableBtn');
const completeConfigBtn = document.getElementById('completeConfigBtn');
const cancelConfigBtn = document.getElementById('cancelConfigBtn');
const addNewConfigBtn = document.getElementById('addNewConfigBtn');
const mergeConfigBody = document.getElementById('mergeConfigBody');
const backToPreviewBtn = document.getElementById('backToPreviewBtn');
const saveMergeConfigBtn = document.getElementById('saveMergeConfigBtn');






// DOM元素 - 加载状态
const loadingOverlay = document.getElementById('loadingOverlay');
const loadingText = document.getElementById('loadingText');
const loadingSubtext = document.getElementById('loadingSubtext');

// DOM元素 - 步骤指示器
const stepIndicator = document.getElementById('stepIndicator');
const step1 = document.getElementById('step1');
const step2 = document.getElementById('step2');
const step3 = document.getElementById('step3');
const step4 = document.getElementById('step4');
const step5 = document.getElementById('step5');
const step6 = document.getElementById('step6');

// 模态框
const preprocessingConfirmModal = new bootstrap.Modal(document.getElementById('preprocessingConfirmModal'));
const mappingSaveModal = new bootstrap.Modal(document.getElementById('mappingSaveModal'));

// 事件监听器
document.addEventListener('DOMContentLoaded', function() {
    initializeEventListeners();
    initializeTabNavigation();
});

function initializeEventListeners() {
    // 基础设置页面事件
    translationModeSelect.addEventListener('change', function() {
        currentMode = this.value;
        updateUIForMode();
    });
    
    // 翻译方向改变事件
    translationDirectionSelect.addEventListener('change', function() {
        // 保存翻译方向到sessionStorage
        sessionStorage.setItem('translation_direction', this.value);
        // 更新翻译库版本控制页面的显示
        if (typeof inheritTranslationDirection === 'function') {
            inheritTranslationDirection();
        }
    });

    readButton.addEventListener('click', readDatasets);
    nextToPreviewBtn.addEventListener('click', () => switchToTab('preview-tab'));

    // 数据预览页面事件
    backToSetupBtn.addEventListener('click', () => switchToTab('setup-tab'));
    
    if (nextToMergeBtn) {
        console.log('配置合并按钮已找到，绑定点击事件');
        nextToMergeBtn.addEventListener('click', (event) => {
            console.log('配置合并按钮被点击，当前模式:', currentMode);
            event.preventDefault();
            event.stopPropagation();
            
            if (currentMode === 'SDTM') {
                console.log('SDTM模式，切换到合并配置页面');
                switchToTab('merge-config-tab');
            } else {
                console.log('RAW模式，切换到翻译库版本控制页面');
                // RAW模式直接跳到翻译库版本控制
                switchToTab('translation-library-tab');
            }
        });
    } else {
        console.log('未找到合并配置按钮元素');
    }
    
    // 预览控制按钮事件
    if (refreshPreviewBtn) {
        refreshPreviewBtn.addEventListener('click', () => {
            if (currentSelectedDataset) {
                loadDatasetPreview(currentSelectedDataset, true); // 强制刷新
            }
        });
    }
    
    if (showAllDataBtn) {
        showAllDataBtn.addEventListener('click', () => {
            if (currentSelectedDataset) {
                loadAllData(); // 加载全部数据
            }
        });
    }

    // 合并配置页面事件
    configDatasetSelect.addEventListener('change', updateConfigVariableOptions);
    configVariableSelect.addEventListener('change', function() {
        console.log('变量选择发生变化:', configVariableSelect.value);
        updateConfigButtons();
    });
    addVariableBtn.addEventListener('click', function(e) {
        console.log('添加变量按钮被点击');
        console.log('按钮状态:', {
            disabled: addVariableBtn.disabled,
            classList: addVariableBtn.classList.toString()
        });
        e.preventDefault();
        addVariableToCurrentConfig();
    });
    completeConfigBtn.addEventListener('click', completeCurrentConfig);
    cancelConfigBtn.addEventListener('click', cancelCurrentConfig);
    // 表格内添加配置按钮事件
    document.addEventListener('click', function(e) {
        if (e.target && (e.target.id === 'addFirstConfigBtn' || e.target.closest('#addFirstConfigBtn'))) {
            e.preventDefault();
            addNewConfigRow();
        }
    });
    backToPreviewBtn.addEventListener('click', () => switchToTab('preview-tab'));
    if (saveMergeConfigBtn) {
        saveMergeConfigBtn.addEventListener('click', saveMergeConfig);
    }
    
    
    
    
    
    
    // 预处理确认
    document.getElementById('confirmPreprocessing').addEventListener('click', showMappingSaveDialog);
    
    // 映射保存相关
    document.getElementById('confirmSaveMapping').addEventListener('click', saveMappingConfig);
    document.getElementById('skipSaveMapping').addEventListener('click', proceedToNextPage);
    
    // 步骤指示器点击事件
    initializeStepIndicatorClicks();
}

function initializeStepIndicatorClicks() {
    if (step1) {
        step1.addEventListener('click', () => {
            if (!document.getElementById('setup-tab').disabled) {
                switchToTab('setup-tab');
            }
        });
    }
    
    if (step2) {
        step2.addEventListener('click', () => {
            if (!document.getElementById('preview-tab').disabled) {
                switchToTab('preview-tab');
            }
        });
    }
    
    if (step3) {
        step3.addEventListener('click', () => {
            if (!document.getElementById('merge-config-tab').disabled && currentMode === 'SDTM') {
                switchToTab('merge-config-tab');
            }
        });
    }
    
    if (step4) {
        step4.addEventListener('click', () => {
            // 直接跳转到翻译库版本控制页面
            switchToTab('translation-library-tab');
        });
    }
    
    if (step5) {
        step5.addEventListener('click', () => {
            if (!document.getElementById('translation-library-tab').disabled) {
                switchToTab('translation-library-tab');
            }
        });
    }
    
    if (step6) {
        step6.addEventListener('click', () => {
            if (!document.getElementById('translation-confirmation-tab').disabled) {
                switchToTab('translation-confirmation-tab');
            }
        });
    }
}

function initializeTabNavigation() {
    // 初始化时禁用后续标签页
    disableTab('preview-tab');
    disableTab('merge-config-tab');

    disableTab('translation-library-tab');
    disableTab('translation-confirmation-tab');
    
    // 监听标签页切换事件以更新步骤指示器
    document.getElementById('setup-tab').addEventListener('shown.bs.tab', () => updateStepIndicator(1));
    document.getElementById('preview-tab').addEventListener('shown.bs.tab', () => updateStepIndicator(2));
    document.getElementById('merge-config-tab').addEventListener('shown.bs.tab', () => {
        updateStepIndicator(3);
        initializeMergeConfigPage();
    });
    
    document.getElementById('translation-library-tab').addEventListener('shown.bs.tab', () => updateStepIndicator(4));
document.getElementById('translation-confirmation-tab').addEventListener('shown.bs.tab', () => {
    updateStepIndicator(5);
    initializeTranslationConfirmationPage();
});
}

function switchToTab(tabId) {
    const targetTab = document.getElementById(tabId);
    if (targetTab && !targetTab.classList.contains('disabled')) {
        targetTab.click();
    }
}

function enableTab(tabId) {
    const tab = document.getElementById(tabId);
    if (tab) {
        tab.classList.remove('disabled');
        tab.removeAttribute('disabled');
    }
}

function disableTab(tabId) {
    const tab = document.getElementById(tabId);
    if (tab) {
        tab.classList.add('disabled');
        tab.setAttribute('disabled', 'true');
    }
}

function updateStepIndicator(currentStep) {
    // 移除所有活动状态（支持新的垂直样式）
    [step1, step2, step3, step4, step5, step6].forEach(step => {
        if (step) {
            step.classList.remove('active', 'completed');
        }
    });
    
    // 设置当前步骤为活动状态
    const steps = [step1, step2, step3, step4, step5, step6];
    if (steps[currentStep - 1]) {
        steps[currentStep - 1].classList.add('active');
    }
    
    // 设置之前的步骤为已完成状态
    for (let i = 0; i < currentStep - 1; i++) {
        if (steps[i]) {
            steps[i].classList.add('completed');
        }
    }
    
    // 特殊处理RAW模式跳过第3步
    if (currentMode === 'RAW' && currentStep >= 4) {
        if (step3) {
            step3.classList.add('completed');
            step3.style.opacity = '0.5';
        }
    } else if (step3) {
        step3.style.opacity = '1';
    }
}

function updateUIForMode() {
    // 翻译方向选择在两种模式下都显示
        translationDirectionSection.style.display = 'block';
    
    // 更新翻译方向提示文本
    const translationDirectionHint = document.getElementById('translationDirectionHint');
    if (translationDirectionHint) {
        if (currentMode === 'SDTM') {
            translationDirectionHint.textContent = '英译中时，合并变量的值会用空格间隔；中译英时无间隔';
    } else {
            translationDirectionHint.textContent = 'RAW模式下选择翻译方向，直接翻译变量名称';
        }
    }
    
    if (currentMode === 'SDTM') {
        // 恢复第3步的正常状态
        if (step3) {
            step3.style.opacity = '1';
            const stepText = step3.querySelector('.step-text-vertical');
            if (stepText) {
                stepText.textContent = '合并配置';
            }
        }
    } else {
        // RAW模式时第3步显示为跳过状态
        if (step3) {
            step3.style.opacity = '0.5';
            const stepText = step3.querySelector('.step-text-vertical');
            if (stepText) {
                stepText.textContent = '合并配置(跳过)';
            }
        }
    }
    
    // 重置后续步骤状态
    if (currentDatasets && Object.keys(currentDatasets).length > 0) {
        // 如果已经读取了数据，重新评估可用的标签页
        if (currentMode === 'SDTM') {
            enableTab('merge-config-tab');
        } else {
            disableTab('merge-config-tab');
    
        }
    }
}

async function readDatasets() {
    const path = datasetPathInput.value.trim();
    if (!path) {
        showAlert('请输入数据集路径', 'warning');
        return;
    }

    showLoadingOverlay('正在读取数据集...', '请稍候，这可能需要一些时间');
    readButton.disabled = true;

    try {
        const response = await fetch('/read_datasets', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                path: path,
                mode: currentMode,
                translation_direction: currentMode === 'SDTM' ? translationDirectionSelect.value : null
            })
        });

        const result = await response.json();
        
        if (result.success) {
            currentDatasets = result.datasets;
            displayDatasets();
            showAlert(result.message, 'success');
            lastDataPath = path;
            
            // 启用下一步
            nextToPreviewContainer.style.display = 'block';
            enableTab('preview-tab');
            
            // 根据当前模式启用相应的标签页
            if (currentMode === 'SDTM') {
                enableTab('merge-config-tab');
            } else {
                disableTab('merge-config-tab');
            }
            
            // 添加成功动画效果
            nextToPreviewContainer.style.opacity = '0';
            setTimeout(() => {
                nextToPreviewContainer.style.transition = 'opacity 0.5s ease';
                nextToPreviewContainer.style.opacity = '1';
            }, 100);
            
            showAlert('数据读取成功，可以查看数据预览', 'success');
        } else {
            showAlert(result.message, 'error');
        }
    } catch (error) {
        showAlert('网络错误: ' + error.message, 'error');
    } finally {
        hideLoadingOverlay();
        readButton.disabled = false;
    }
}

function displayDatasets() {
    if (Object.keys(currentDatasets).length === 0) {
        showAlert('未找到数据集', 'warning');
        return;
    }

    // 更新数据集统计信息
    const totalDatasets = Object.keys(currentDatasets).length;
    const totalRows = Object.values(currentDatasets).reduce((sum, ds) => sum + ds.rows, 0);
    datasetStats.textContent = `共 ${totalDatasets} 个数据集，总计 ${totalRows} 行数据`;

    // 更新预览总结
    if (previewSummary) {
        previewSummary.textContent = `${totalDatasets} 个数据集，${totalRows.toLocaleString()} 条记录`;
    }

    // 清空现有列表
    if (datasetList) {
        datasetList.innerHTML = '';
        
        // 创建数据集列表项
    Object.keys(currentDatasets).forEach((datasetName, index) => {
            createDatasetListItem(datasetName, index === 0);
    });
    }

    // 填充合并配置的数据集选项
    if (currentMode === 'SDTM') {
        populateConfigDatasetOptions();
        enableTab('merge-config-tab');
    } else {
        
    }
}

function createDatasetListItem(datasetName, isFirstItem) {
    const listItem = document.createElement('div');
    listItem.className = `dataset-list-item ${isFirstItem ? 'active' : ''}`;
    listItem.setAttribute('data-dataset', datasetName);
    
    listItem.innerHTML = `
        <div class="dataset-name">${datasetName}</div>
        <div class="dataset-badge">${currentDatasets[datasetName].rows.toLocaleString()}</div>
    `;
    
    // 添加点击事件
    listItem.addEventListener('click', () => {
        selectDataset(datasetName);
    });
    
    datasetList.appendChild(listItem);
    
    // 如果是第一个项目，自动加载预览
    if (isFirstItem) {
        currentSelectedDataset = datasetName;
        loadDatasetPreview(datasetName, false);
    }
}

function selectDataset(datasetName) {
    // 移除所有active状态
    document.querySelectorAll('.dataset-list-item').forEach(item => {
        item.classList.remove('active');
    });
    
    // 添加新的active状态
    const selectedItem = document.querySelector(`[data-dataset="${datasetName}"]`);
    if (selectedItem) {
        selectedItem.classList.add('active');
    }
    
    // 更新当前选中的数据集
    currentSelectedDataset = datasetName;
    
    // 加载数据预览
    loadDatasetPreview(datasetName, false);
}

async function loadDatasetPreview(datasetName, forceRefresh = false) {
    if (!currentDatasets[datasetName]) {
        return;
    }
    
    // 如果是相同数据集且不是强制刷新，直接返回
    if (previewState.currentDataset === datasetName && !forceRefresh) {
        return;
    }
    
    // 重置状态
    previewState.currentDataset = datasetName;
    previewState.loadedRows = 0;
    previewState.totalRows = currentDatasets[datasetName].rows;
    previewState.loading = false;
    previewState.hasMore = true;
    previewState.allData = [];
    previewState.columns = [];
    
    // 更新标题
    const dsInfo = currentDatasets[datasetName];
    currentDatasetTitle.innerHTML = `
        <i class="fas fa-table me-2"></i>${datasetName}
        <small class="text-muted ms-2">(${dsInfo.rows.toLocaleString()} 行 × ${dsInfo.columns} 列)</small>
    `;
    
    // 显示控制按钮
    refreshPreviewBtn.style.display = 'inline-block';
    // 隐藏"显示全部"按钮，因为默认就加载所有列
    showAllDataBtn.style.display = 'none';
    
    // 清空内容并开始加载
    previewContent.innerHTML = '';
    
    // 直接加载所有数据（包括所有列），但保持行数限制
    await loadAllData();
}

async function loadNextBatch() {
    if (previewState.loading || !previewState.hasMore) {
        return;
    }
    
    previewState.loading = true;
    
    // 显示加载指示器
    showBatchLoadingIndicator();
    
    try {
        const url = `/get_dataset/${previewState.currentDataset}?limit=${previewState.pageSize}&offset=${previewState.loadedRows}`;
        const response = await fetch(url);
        const dataset = await response.json();

        if (dataset.error) {
            showDatasetError(dataset.error);
            return;
        }

        // 更新状态
        previewState.columns = dataset.columns || previewState.columns;
        previewState.loadedRows += dataset.data?.length || 0;
        previewState.hasMore = previewState.loadedRows < previewState.totalRows && dataset.data?.length > 0;
        
        // 合并数据
        previewState.allData = previewState.allData.concat(dataset.data || []);
        
        // 渲染预览（如果是第一批数据，重新渲染整个结构）
        if (previewState.loadedRows === dataset.data?.length) {
            renderDatasetPreviewStructure(dataset);
        } else {
            appendDataToTable(dataset.data || []);
        }
        
        // 更新加载状态提示
        updateLoadingStatus();
        
    } catch (error) {
        showDatasetError('加载数据失败: ' + error.message);
    } finally {
        previewState.loading = false;
        hideBatchLoadingIndicator();
    }
}

async function loadAllData() {
    if (previewState.loading) {
        return;
    }
    
    previewState.loading = true;
    showBatchLoadingIndicator();
    
    try {
        const url = `/get_dataset/${previewState.currentDataset}?all=1`;
        const response = await fetch(url);
        const dataset = await response.json();

        if (dataset.error) {
            showDatasetError(dataset.error);
            return;
        }

        // 重置状态并设置全部数据
        previewState.allData = dataset.data || [];
        previewState.loadedRows = dataset.data?.length || 0;
        previewState.hasMore = false;
        previewState.columns = dataset.columns || [];
        
        // 重新渲染整个结构
        renderDatasetPreviewStructure(dataset);
        updateLoadingStatus();
        
        // 隐藏"显示全部"按钮
        showAllDataBtn.style.display = 'none';
        
    } catch (error) {
        showDatasetError('加载全部数据失败: ' + error.message);
    } finally {
        previewState.loading = false;
        hideBatchLoadingIndicator();
    }
}

function renderDatasetPreviewStructure(dataset) {
    let content = '';
    

    
    // 检查SUPP失败信息
    const dsInfo = currentDatasets[dataset.dataset_name];
    if (dsInfo && dsInfo.supp_failed_columns && dsInfo.supp_failed_columns.length > 0) {
        content += renderSuppFailureAlert(dataset.dataset_name, dsInfo);
    }
    
    // 添加数据表格容器
    if (dataset.data && dataset.data.length > 0) {
        content += `
            <div class="table-scroll-wrapper">
                <!-- 顶部水平滚动条 -->
                <div class="horizontal-scrollbar" id="horizontalScrollbar">
                    <div class="horizontal-scrollbar-inner" id="horizontalScrollbarInner"></div>
                </div>
                
                <!-- 表格容器 -->
                <div class="preview-table-container" id="previewTableContainer">
                    <table class="table table-striped table-hover preview-table" id="previewTable">
                        <thead class="table-header-sticky">
                            <tr>
                                ${dataset.columns.map(col => `<th title="${col}">${col}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody id="previewTableBody">
                            ${renderTableRows(dataset.data, dataset.columns)}
                        </tbody>
                    </table>
                    
                    <!-- 加载指示器 -->
                    <div id="batchLoadingIndicator" class="text-center py-3" style="display: none;">
                        <div class="spinner-border spinner-border-sm text-primary me-2" role="status"></div>
                        <span class="text-muted">加载更多数据...</span>
                    </div>
                    
                    <!-- 底部提示 -->
                    <div id="loadingStatusIndicator" class="text-center py-2 text-muted">
                        <small>已显示 ${previewState.loadedRows} / ${previewState.totalRows} 行，向下滚动加载更多</small>
                    </div>
                </div>
            </div>
        `;
    } else {
        content += `
            <div class="alert alert-info text-center">
                <i class="fas fa-info-circle me-2"></i>
                该数据集暂无数据
            </div>
        `;
    }
    
    previewContent.innerHTML = content;
    
    // 添加滚动事件监听器
    setupScrollListener();
    
    // 立即应用表头固定样式
    setTimeout(() => {
        ensureStickyHeader();
        updateHorizontalScrollbarWidth();
        // 确保悬浮窗正确显示
        updateSeparatedHeaderWidth();
        console.log('悬浮窗初始化完成');
    }, 300); // 增加延迟，确保DOM完全加载和渲染
}

function renderSuppFailureAlert(datasetName, dsInfo) {
    return `
        <div class="supp-failure-alert alert" role="alert">
        <div class="d-flex align-items-center justify-content-between">
            <div>
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    <strong>SUPP合并失败的列 (${dsInfo.supp_failed_columns.length})</strong>
            </div>
                <button class="btn btn-sm btn-outline-warning" onclick="toggleSuppDetails('${datasetName}')">
                    详情
                </button>
        </div>
            <div id="supp-details-${datasetName}" class="mt-2 d-none">
                <div class="small text-muted mb-2">
                    通常是 IDVAR 指向的键在主表不存在，或键值/类型未标准化一致导致无法对齐。
                </div>
                <ul class="mb-0 small">
                ${dsInfo.supp_failed_columns.map(f => `
                    <li>
                        <code>${f.column}</code>
                            (来源: <code>${f.supp_ds}</code>, QNAM: <code>${f.qnam}</code>
                            ${f.idvar ? `, IDVAR: <code>${f.idvar}</code>` : ''})
                    </li>
                `).join('')}
            </ul>
            </div>
        </div>
    `;
}

function renderTableRows(data, columns) {
    return data.map(row => `
        <tr>
            ${columns.map(col => `
                <td title="${row[col] || ''}">${row[col] !== null && row[col] !== undefined ? row[col] : ''}</td>
            `).join('')}
        </tr>
    `).join('');
}

function appendDataToTable(newData) {
    const tbody = document.getElementById('previewTableBody');
    if (tbody && newData.length > 0) {
        const newRows = renderTableRows(newData, previewState.columns);
        tbody.insertAdjacentHTML('beforeend', newRows);
        
        // 为新添加的行添加动画效果
        const allRows = tbody.querySelectorAll('tr');
        const newRowsCount = newData.length;
        const startIndex = allRows.length - newRowsCount;
        
        for (let i = startIndex; i < allRows.length; i++) {
            if (allRows[i]) {
                allRows[i].classList.add('new-row');
                // 移除动画类，避免重复动画
                setTimeout(() => {
                    allRows[i].classList.remove('new-row');
                }, 300);
            }
        }
        
        // 更新水平滚动条宽度
        updateHorizontalScrollbarWidth();
        
        // 重新确保表头固定
        setTimeout(() => {
            ensureStickyHeader();
            updateSeparatedHeaderWidth();
        }, 100);
        
        // 为大数据集添加性能优化类
        const container = document.getElementById('previewTableContainer');
        if (container && previewState.totalRows > 1000) {
            container.classList.add('large-dataset');
        }
    }
}

function updateHorizontalScrollbarWidth() {
    const horizontalScrollbarInner = document.getElementById('horizontalScrollbarInner');
    const table = document.getElementById('previewTable');
    
    if (horizontalScrollbarInner && table) {
        setTimeout(() => {
            const tableWidth = table.scrollWidth;
            horizontalScrollbarInner.style.width = tableWidth + 'px';
            
            // 同步更新分离表头宽度
            updateSeparatedHeaderWidth();
            
            console.log('水平滚动条宽度已更新:', tableWidth);
        }, 50);
    }
}

function updateLoadingStatus() {
    // 更新右上角已加载行数指示器
    const loadedRowsIndicator = document.getElementById('loadedRowsIndicator');
    const loadedRowsText = document.getElementById('loadedRowsText');
    if (loadedRowsIndicator && loadedRowsText) {
        loadedRowsIndicator.style.display = 'inline';
        loadedRowsText.textContent = `已加载: ${previewState.loadedRows.toLocaleString()} 行`;
    }
    
    // 更新右上角进度指示器
    const loadingProgress = document.getElementById('loadingProgress');
    if (loadingProgress) {
        const progress = Math.round(previewState.loadedRows / previewState.totalRows * 100);
        loadingProgress.style.display = 'inline';
        loadingProgress.textContent = `${progress}%`;
        
        // 如果进度达到100%，改变颜色
        if (progress === 100) {
            loadingProgress.className = 'progress-percent complete me-3';
            loadingProgress.innerHTML = `<i class="fas fa-check-circle me-1"></i>完成`;
        } else {
            loadingProgress.className = 'progress-percent me-3';
        }
    }
    
    // 更新底部状态提示
    const statusIndicator = document.getElementById('loadingStatusIndicator');
    if (statusIndicator) {
        if (previewState.hasMore) {
            statusIndicator.innerHTML = `
                <small>已显示 ${previewState.loadedRows.toLocaleString()} / ${previewState.totalRows.toLocaleString()} 行，向下滚动加载更多</small>
            `;
            statusIndicator.className = 'text-center py-2 text-muted';
        } else {
            statusIndicator.innerHTML = `
                <small class="text-success">
                    <i class="fas fa-check-circle me-1"></i>
                    已加载全部 ${previewState.totalRows.toLocaleString()} 行数据
                </small>
            `;
            statusIndicator.className = 'text-center py-2 text-success';
        }
    }
}

function setupScrollListener() {
    const container = document.getElementById('previewTableContainer');
    const horizontalScrollbar = document.getElementById('horizontalScrollbar');
    const horizontalScrollbarInner = document.getElementById('horizontalScrollbarInner');
    const table = document.getElementById('previewTable');
    
    if (container && horizontalScrollbar && horizontalScrollbarInner && table) {
        // 移除之前的监听器（如果存在）
        container.removeEventListener('scroll', handleScroll);
        
        // 设置水平滚动条内容宽度以匹配表格宽度
        setTimeout(() => {
            const tableWidth = table.scrollWidth;
            horizontalScrollbarInner.style.width = tableWidth + 'px';
        }, 100);
        
        // 强制确保表头固定
        ensureStickyHeader();
        
        // 添加垂直滚动监听器
        container.addEventListener('scroll', handleScroll);
        
        // 添加滚动事件监听器同步水平滚动
        const horizontalScrollbar = document.getElementById('horizontalScrollbar');
        if (horizontalScrollbar) {
            horizontalScrollbar.addEventListener('scroll', function() {
                // 同步数据表格的水平滚动
                container.scrollLeft = horizontalScrollbar.scrollLeft;
                
                // 同步分离表头位置 - 使用scrollLeft确保准确同步
                const separatedHeaderContainer = document.querySelector('.separated-header-container');
                if (separatedHeaderContainer) {
                    separatedHeaderContainer.scrollLeft = horizontalScrollbar.scrollLeft;
                    console.log('分离表头水平滚动同步:', horizontalScrollbar.scrollLeft);
                }
            });
        }
        
        // 添加数据表格滚动监听器，同步到顶部滚动条和分离表头
        container.addEventListener('scroll', function() {
            if (horizontalScrollbar) {
                horizontalScrollbar.scrollLeft = container.scrollLeft;
            }
            
            // 同步分离表头的水平滚动位置
            const separatedHeaderContainer = document.querySelector('.separated-header-container');
            if (separatedHeaderContainer) {
                separatedHeaderContainer.scrollLeft = container.scrollLeft;
            }
        });
        
        // 添加分离表头滚动监听器，同步到数据表格和顶部滚动条
        const separatedHeaderContainer = document.querySelector('.separated-header-container');
        if (separatedHeaderContainer) {
            separatedHeaderContainer.addEventListener('scroll', function() {
                // 同步数据表格的水平滚动
                container.scrollLeft = separatedHeaderContainer.scrollLeft;
                
                // 同步顶部滚动条
                if (horizontalScrollbar) {
                    horizontalScrollbar.scrollLeft = separatedHeaderContainer.scrollLeft;
                }
            });
        }
    }
}

function ensureStickyHeader() {
    const table = document.getElementById('previewTable');
    const container = document.getElementById('previewTableContainer');
    const tableWrapper = document.querySelector('.table-scroll-wrapper');
    
    console.log('ensureStickyHeader 开始执行');
    console.log('table:', table);
    console.log('container:', container);
    console.log('tableWrapper:', tableWrapper);
    
    if (table && container && tableWrapper) {
        // 移除之前的分离表头
        const existingSeparatedHeader = tableWrapper.querySelector('.separated-header-container');
        if (existingSeparatedHeader) {
            existingSeparatedHeader.remove();
            console.log('移除旧的分离表头');
        }
        
        // 创建分离的表头容器
        const separatedHeaderContainer = document.createElement('div');
        separatedHeaderContainer.className = 'separated-header-container';
        separatedHeaderContainer.style.cssText = `
            position: absolute !important;
            top: 20px !important;
            left: 0 !important;
            width: 100% !important;
            z-index: 10000 !important;
            background: #ffffff !important;
            border: 1px solid #e9ecef !important;
            border-bottom: none !important;
            border-radius: 8px 8px 0 0 !important;
            overflow: hidden !important;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1) !important;
            pointer-events: none !important;
            height: 40px !important;
            display: block !important;
            visibility: visible !important;
            opacity: 1 !important;
            transform-origin: top left !important;
            min-width: 100% !important;
            max-width: none !important;
            box-sizing: border-box !important;
        `;
        
        // 创建分离的表头表格
        const separatedHeaderTable = document.createElement('table');
        separatedHeaderTable.className = 'table table-bordered separated-header-table';
        separatedHeaderTable.style.cssText = `
            width: 100% !important;
            margin: 0 !important;
            border-collapse: collapse !important;
            background: #ffffff !important;
            table-layout: fixed !important;
            border-spacing: 0 !important;
            display: table !important;
            visibility: visible !important;
            opacity: 1 !important;
            min-width: 100% !important;
            max-width: none !important;
            overflow: hidden !important;
            box-sizing: border-box !important;
        `;
        
        // 复制原始表头内容
        const originalThead = table.querySelector('thead');
        if (originalThead) {
            const clonedThead = originalThead.cloneNode(true);
            separatedHeaderTable.appendChild(clonedThead);
            console.log('表头内容已复制');
        }
        
        // 插入分离表头到表格包装器的顶部
        tableWrapper.insertBefore(separatedHeaderContainer, tableWrapper.firstChild);
        separatedHeaderContainer.appendChild(separatedHeaderTable);
        
        // 为滚动容器添加分离表头样式类
        container.classList.add('separated-header');
        
        console.log('分离表头已创建并插入');
        console.log('分离表头容器:', separatedHeaderContainer);
        console.log('分离表头表格:', separatedHeaderTable);
        
        // 设置表头宽度与数据表格同步
        updateSeparatedHeaderWidth();
        
        // 添加滚动事件监听器同步水平滚动
        const horizontalScrollbar = document.getElementById('horizontalScrollbar');
        if (horizontalScrollbar) {
            horizontalScrollbar.addEventListener('scroll', function() {
                // 同步数据表格的水平滚动
                container.scrollLeft = horizontalScrollbar.scrollLeft;
                
                // 同步分离表头位置 - 使用transform确保平滑滚动
                separatedHeaderContainer.style.transform = `translateX(-${this.scrollLeft}px)`;
                console.log('分离表头水平滚动同步:', this.scrollLeft);
            });
        }
        
        // 添加数据表格滚动监听器，同步到顶部滚动条
        container.addEventListener('scroll', function() {
            if (horizontalScrollbar) {
                horizontalScrollbar.scrollLeft = container.scrollLeft;
            }
        });
        
        console.log('分离表头已创建');
    } else {
        console.error('ensureStickyHeader: 必要的元素未找到');
    }
}

// 新增：更新分离表头宽度
function updateSeparatedHeaderWidth() {
    const tableWrapper = document.querySelector('.table-scroll-wrapper');
    const separatedHeaderContainer = tableWrapper ? tableWrapper.querySelector('.separated-header-container') : null;
    const table = document.getElementById('previewTable');
    
    if (separatedHeaderContainer && table) {
        // 等待表格完全渲染
        setTimeout(() => {
            // 获取表格的实际宽度和列信息
            const tableWidth = table.scrollWidth;
            const tableRect = table.getBoundingClientRect();
            
            console.log('表格宽度:', tableWidth, '表格位置:', tableRect);
            
            // 设置分离表头容器宽度 - 使用表格的实际宽度
            separatedHeaderContainer.style.width = tableWidth + 'px';
            separatedHeaderContainer.style.minWidth = tableWidth + 'px';
            separatedHeaderContainer.style.maxWidth = tableWidth + 'px';
            
            // 同步列宽 - 确保完全匹配
            const separatedHeaderTable = separatedHeaderContainer.querySelector('.separated-header-table');
            const originalTable = table;
            
            if (separatedHeaderTable && originalTable) {
                const headerThs = separatedHeaderTable.querySelectorAll('th');
                const originalThs = originalTable.querySelectorAll('th');
                
                console.log('表头列数:', headerThs.length, '数据列数:', originalThs.length);
                
                // 先设置表格布局为fixed，确保列宽稳定
                separatedHeaderTable.style.tableLayout = 'fixed';
                
                // 设置表格总宽度
                separatedHeaderTable.style.width = tableWidth + 'px';
                separatedHeaderTable.style.minWidth = tableWidth + 'px';
                separatedHeaderTable.style.maxWidth = tableWidth + 'px';
                
                // 不使用平均分配，而是精确复制每列的实际宽度
                
                headerThs.forEach((headerTh, index) => {
                    if (originalThs[index]) {
                        const originalTh = originalThs[index];
                        
                        // 获取原始列的完整信息
                        const originalWidth = originalTh.offsetWidth;
                        const originalRect = originalTh.getBoundingClientRect();
                        const originalStyle = window.getComputedStyle(originalTh);
                        
                        console.log(`列${index}: 原始宽度=${originalWidth}, 位置=${originalRect.left}, 样式宽度=${originalStyle.width}`);
                        
                        // 强制设置列宽，确保与原始列完全一致
                        headerTh.style.width = originalWidth + 'px';
                        headerTh.style.minWidth = originalWidth + 'px';
                        headerTh.style.maxWidth = originalWidth + 'px';
                        headerTh.style.flex = 'none';
                        headerTh.style.flexShrink = '0';
                        headerTh.style.flexGrow = '0';
                        
                        // 确保列对齐
                        headerTh.style.boxSizing = 'border-box';
                        headerTh.style.overflow = 'hidden';
                        headerTh.style.whiteSpace = 'nowrap';
                        
                        // 复制原始列的padding和border
                        headerTh.style.padding = originalStyle.padding;
                        headerTh.style.border = originalStyle.border;
                        headerTh.style.margin = originalStyle.margin;
                        
                        // 强制设置列宽样式
                        headerTh.style.cssText += `
                            width: ${originalWidth}px !important;
                            min-width: ${originalWidth}px !important;
                            max-width: ${originalWidth}px !important;
                        `;
                    }
                });
                
                // 设置表格布局
                separatedHeaderTable.style.borderCollapse = 'collapse';
                separatedHeaderTable.style.borderSpacing = '0';
                
                // 强制重新计算布局
                separatedHeaderTable.offsetHeight;
                
                // 强制刷新分离表头容器
                separatedHeaderContainer.offsetHeight;
                
                // 再次检查列宽是否匹配
                setTimeout(() => {
                    headerThs.forEach((headerTh, index) => {
                        if (originalThs[index]) {
                            const headerWidth = headerTh.offsetWidth;
                            const originalWidth = originalThs[index].offsetWidth;
                            const headerRect = headerTh.getBoundingClientRect();
                            const originalRect = originalThs[index].getBoundingClientRect();
                            
                            console.log(`列${index} 宽度检查: 表头=${headerWidth}, 数据=${originalWidth}, 匹配=${headerWidth === originalWidth}`);
                            console.log(`列${index} 位置检查: 表头左=${headerRect.left}, 数据左=${originalRect.left}`);
                            
                            // 如果宽度不匹配，强制调整
                            if (headerWidth !== originalWidth) {
                                console.log(`列${index} 宽度不匹配，强制调整`);
                                headerTh.style.cssText = `
                                    width: ${originalWidth}px !important;
                                    min-width: ${originalWidth}px !important;
                                    max-width: ${originalWidth}px !important;
                                    flex: none !important;
                                    flex-shrink: 0 !important;
                                    flex-grow: 0 !important;
                                    box-sizing: border-box !important;
                                    overflow: hidden !important;
                                    white-space: nowrap !important;
                                `;
                            }
                        }
                    });
                    
                    // 最终检查表格总宽度
                    const finalHeaderWidth = separatedHeaderTable.scrollWidth;
                    const finalTableWidth = table.scrollWidth;
                    console.log(`最终宽度检查: 表头=${finalHeaderWidth}, 数据=${finalTableWidth}, 匹配=${finalHeaderWidth === finalTableWidth}`);
                    
                    // 如果总宽度不匹配，强制调整
                    if (finalHeaderWidth !== finalTableWidth) {
                        console.log('总宽度不匹配，强制调整');
                        separatedHeaderContainer.style.cssText += `
                            width: ${finalTableWidth}px !important;
                            min-width: ${finalTableWidth}px !important;
                            max-width: ${finalTableWidth}px !important;
                        `;
                        separatedHeaderTable.style.cssText += `
                            width: ${finalTableWidth}px !important;
                            min-width: ${finalTableWidth}px !important;
                            max-width: ${finalTableWidth}px !important;
                        `;
                    }
                    
                }, 100); // 增加延迟，确保所有样式都已应用
                
                console.log('分离表头宽度已同步，总宽度:', tableWidth);
            }
        }, 300); // 增加延迟，确保表格完全渲染
    }
}

function handleScroll(event) {
    const container = event.target;
    const scrollTop = container.scrollTop;
    const scrollHeight = container.scrollHeight;
    const clientHeight = container.clientHeight;
    
    // 当滚动到距底部50px时开始加载下一批数据
    if (scrollTop + clientHeight >= scrollHeight - 50 && previewState.hasMore && !previewState.loading) {
        loadNextBatch();
    }
}



function showBatchLoadingIndicator() {
    const indicator = document.getElementById('batchLoadingIndicator');
    if (indicator) {
        indicator.style.display = 'block';
    }
}

function hideBatchLoadingIndicator() {
    const indicator = document.getElementById('batchLoadingIndicator');
    if (indicator) {
        indicator.style.display = 'none';
    }
}

function showDatasetError(errorMessage) {
    previewContent.innerHTML = `
        <div class="alert alert-danger">
            <i class="fas fa-exclamation-triangle me-2"></i>
            ${errorMessage}
        </div>
    `;
}

// 这些函数已被新的预览系统替代，保留以防其他地方调用
function appendRows(tbody, columns, rows) {
    // 兼容性保留
    console.warn('appendRows function is deprecated, use new preview system instead');
}

function populateConfigDatasetOptions() {
    configDatasetSelect.innerHTML = '<option value="">请选择数据集</option>';
    Object.keys(currentDatasets).forEach(datasetName => {
        const option = document.createElement('option');
        option.value = datasetName;
        option.textContent = `${datasetName} (${currentDatasets[datasetName].rows} 行)`;
        configDatasetSelect.appendChild(option);
    });
}

function updateConfigVariableOptions() {
    const selectedDataset = configDatasetSelect.value;
    configVariableSelect.innerHTML = '<option value="">请选择变量</option>';
    
    if (selectedDataset && currentDatasets[selectedDataset]) {
        const dsInfo = currentDatasets[selectedDataset];
        const columns = dsInfo.selectable_columns && dsInfo.selectable_columns.length
            ? dsInfo.selectable_columns
            : dsInfo.column_names;
        
        const origin = dsInfo.supp_origin_detail || {};
        columns.forEach(columnName => {
            const option = document.createElement('option');
            option.value = columnName;
            const meta = origin[columnName];
            if (meta && meta.supp_ds && meta.qnam) {
                option.textContent = `${columnName} 〔来自 ${meta.supp_ds}.${meta.qnam}${meta.idvar ? ` / ${meta.idvar}` : ''}〕`;
                option.dataset.originSupp = meta.supp_ds;
                option.dataset.originQnam = meta.qnam;
                if (meta.idvar) option.dataset.originIdvar = meta.idvar;
            } else {
                option.textContent = columnName;
            }
            configVariableSelect.appendChild(option);
        });
    }
    
    updateConfigButtons();
}

function startNewConfig() {
    currentConfigIndex = mergeConfigs.length;
    isConfiguring = true;
    currentConfig = {
        target: null,
        sources: []
    };
    mergeConfigs.push(currentConfig);
    
    currentConfigSection.classList.remove('d-none');
    currentConfigIndexSpan.textContent = currentConfigIndex + 1;
    
    // 重置选择
    configDatasetSelect.value = '';
    configVariableSelect.innerHTML = '<option value="">请选择变量</option>';
    updateSelectedVariablesDisplay();
    updateConfigButtons();
}

// 新的表格内联编辑功能
function addNewConfigRow() {
    const mergeConfigBody = document.getElementById('mergeConfigBody');
    const emptyRow = document.getElementById('emptyConfigRow');
    
    // 隐藏空状态行
    if (emptyRow) {
        emptyRow.style.display = 'none';
    }
    
    const newIndex = mergeConfigs.length + 1;
    const newRow = document.createElement('tr');
    newRow.className = 'config-row editable-row';
    newRow.setAttribute('data-config-index', mergeConfigs.length);
    
    newRow.innerHTML = `
        <td class="text-center">${newIndex}</td>
        <td>
            <select class="form-select form-select-sm target-variable-select" data-field="target" disabled>
                <option value="">选择主变量</option>
            </select>
        </td>
        <td>
            <div class="source-variables-container">
                <select class="form-select form-select-sm source-variable-select" data-field="sources" multiple disabled>
                    <option value="">选择源变量</option>
                </select>
                <small class="text-muted mt-1 d-block">按住Ctrl键可多选，注意选择顺序</small>
            </div>
        </td>
        <td>
            <select class="form-select form-select-sm dataset-select" data-field="dataset">
                <option value="">选择数据集</option>
                ${Object.keys(currentDatasets).map(ds => `<option value="${ds}">${ds}</option>`).join('')}
            </select>
        </td>
        <td class="text-center">
            <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-success btn-sm save-config-btn" disabled title="保存配置">
                    <i class="fas fa-check"></i>
                </button>
                <button class="btn btn-outline-danger btn-sm delete-config-btn" title="删除配置">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </td>
    `;
    
    mergeConfigBody.appendChild(newRow);
    
    // 绑定事件
    bindRowEvents(newRow);
    
    // 创建对应的配置对象
    const newConfig = {
        target: null,
        sources: [],
        dataset: null
    };
    mergeConfigs.push(newConfig);
    
    // 添加"继续添加"按钮
    addContinueAddButton();
}

function addContinueAddButton() {
    const mergeConfigBody = document.getElementById('mergeConfigBody');
    
    // 先删除已存在的继续添加按钮
    const existingContinueRow = document.getElementById('continueAddRow');
    if (existingContinueRow) {
        existingContinueRow.remove();
    }
    
    // 创建新的继续添加按钮行
    const continueRow = document.createElement('tr');
    continueRow.id = 'continueAddRow';
    continueRow.className = 'text-center';
    continueRow.innerHTML = `
        <td colspan="5" class="py-2">
            <button class="btn btn-outline-success btn-sm" id="continueAddBtn">
                <i class="fas fa-plus me-1"></i>继续添加
            </button>
        </td>
    `;
    
    mergeConfigBody.appendChild(continueRow);
    
    // 绑定继续添加按钮事件
    const continueAddBtn = document.getElementById('continueAddBtn');
    continueAddBtn.addEventListener('click', function() {
        addNewConfigRow();
    });
}

function bindRowEvents(row) {
    const datasetSelect = row.querySelector('.dataset-select');
    const targetSelect = row.querySelector('.target-variable-select');
    const sourceSelect = row.querySelector('.source-variable-select');
    const saveBtn = row.querySelector('.save-config-btn');
    const deleteBtn = row.querySelector('.delete-config-btn');
    const configIndex = parseInt(row.getAttribute('data-config-index'));
    
    // 调试信息
    console.log('bindRowEvents called for row:', row);
    console.log('saveBtn found:', saveBtn);
    console.log('deleteBtn found:', deleteBtn);
    console.log('configIndex:', configIndex);
    
    // 数据集选择事件
    datasetSelect.addEventListener('change', function() {
        const selectedDataset = this.value;
        const config = mergeConfigs[configIndex];
        
        if (selectedDataset) {
            config.dataset = selectedDataset;
            loadVariablesForRow(row, selectedDataset);
            targetSelect.disabled = false;
            sourceSelect.disabled = false;
        } else {
            config.dataset = null;
            config.target = null;
            config.sources = [];
            targetSelect.innerHTML = '<option value="">选择主变量</option>';
            sourceSelect.innerHTML = '<option value="">选择源变量</option>';
            targetSelect.disabled = true;
            sourceSelect.disabled = true;
        }
        updateRowSaveButton(row);
    });
    
    // 主变量选择事件
    targetSelect.addEventListener('change', function() {
        const config = mergeConfigs[configIndex];
        const selectedOption = this.options[this.selectedIndex];
        const targetSource = selectedOption.getAttribute('data-source');
        const suppDataset = selectedOption.getAttribute('data-supp-dataset');
        
        config.target = this.value || null;
        config.targetSource = targetSource; // 'main' 或 'supp'
        if (targetSource === 'supp') {
            config.targetSuppDataset = suppDataset;
        }
        
        updateRowSaveButton(row);
    });
    
    // 源变量选择事件
    sourceSelect.addEventListener('change', function() {
        const config = mergeConfigs[configIndex];
        const selectedOptions = Array.from(this.selectedOptions).map(option => option.value);
        config.sources = selectedOptions.filter(value => value); // 过滤空值
        updateRowSaveButton(row);
    });
    
    // 保存按钮事件
    if (saveBtn) {
        saveBtn.addEventListener('click', function() {
            console.log('Save button clicked for row:', row);
            saveMergeConfigRow(row);
        });
    } else {
        console.error('Save button not found in row:', row);
    }
    
    // 删除按钮事件
    if (deleteBtn) {
        deleteBtn.addEventListener('click', function() {
            console.log('Delete button clicked for row:', row);
            deleteMergeConfigRow(row);
        });
    } else {
        console.error('Delete button not found in row:', row);
    }
}

async function loadVariablesForRow(row, datasetName) {
    const targetSelect = row.querySelector('.target-variable-select');
    const sourceSelect = row.querySelector('.source-variable-select');
    
    try {
        const response = await fetch(`/get_dataset/${datasetName}?limit=1`);
        const dataset = await response.json();
        
        if (dataset.error) {
            showAlert('加载变量失败: ' + dataset.error, 'error');
            return;
        }
        
        const columns = dataset.columns || [];
        
        // 获取SUPP数据集的源变量信息
        let suppResult = { source_variables: [], supp_dataset: null };
        try {
            const suppResponse = await fetch(`/get_source_variables/${datasetName}`);
            suppResult = await suppResponse.json();
        } catch (error) {
            console.warn('获取SUPP变量信息失败:', error);
        }
        
        // 填充主变量选择器（包含主数据集变量和SUPP转置变量，去重）
        let targetOptions = '<option value="">选择主变量</option>';
        
        // 主数据集的原有变量（排除基础字段）
        const baseFields = ['STUDYID', 'USUBJID', 'RDOMAIN', 'IDVAR', 'IDVARVAL'];
        const mainVariables = columns.filter(col => !baseFields.includes(col));
        
        // SUPP数据集的转置变量
        const suppVariables = suppResult.source_variables || [];
        
        // 去重：从主数据集变量中排除已在SUPP中存在的变量
        const uniqueMainVariables = mainVariables.filter(col => !suppVariables.includes(col));
        
        // 添加主数据集独有变量
        if (uniqueMainVariables.length > 0) {
            targetOptions += '<optgroup label="主数据集变量">';
            targetOptions += uniqueMainVariables.map(col => 
                `<option value="${col}" data-source="main">${col}</option>`
            ).join('');
            targetOptions += '</optgroup>';
        }
        
        // 添加SUPP数据集的转置变量
        if (suppVariables.length > 0) {
            targetOptions += '<optgroup label="SUPP转置变量">';
            targetOptions += suppVariables.map(col => 
                `<option value="${col}" data-source="supp" data-supp-dataset="${suppResult.supp_dataset}">${col}</option>`
            ).join('');
            targetOptions += '</optgroup>';
        }
        
        targetSelect.innerHTML = targetOptions;
        
        // 加载对应SUPP数据集的可用源变量
        if (suppResult.supp_dataset) {
            await loadSuppSourceVariables(datasetName, sourceSelect);
        } else {
            // 如果没有对应的SUPP数据集，显示提示信息
            sourceSelect.innerHTML = '<option value="">无对应的SUPP数据集</option>';
            adjustSourceSelectHeight(sourceSelect, 1);
            
            const container = sourceSelect.closest('.source-variables-container');
            const existing = container.querySelector('.supp-info');
            if (existing) existing.remove();
            
            const infoElement = document.createElement('small');
            infoElement.className = 'text-muted mt-1 d-block supp-info';
            infoElement.innerHTML = `<i class="fas fa-info-circle me-1"></i>未找到对应的SUPP数据集`;
            container.appendChild(infoElement);
        }
        
    } catch (error) {
        console.error('加载变量失败:', error);
        showAlert('加载变量失败', 'error');
    }
}

// 动态调整多选下拉框的高度以显示所有选项
function adjustSourceSelectHeight(selectElement, optionCount) {
    // 每个选项的高度大约是38px（包括padding）
    const optionHeight = 38;
    // 最小高度2个选项，最大高度12个选项（避免过高）
    const minOptions = 2;
    const maxOptions = 12;
    
    const visibleOptions = Math.max(minOptions, Math.min(maxOptions, optionCount));
    const calculatedHeight = visibleOptions * optionHeight;
    
    // 设置下拉框的大小属性
    selectElement.setAttribute('size', visibleOptions);
    selectElement.style.height = calculatedHeight + 'px';
    selectElement.style.minHeight = (minOptions * optionHeight) + 'px';
    selectElement.style.maxHeight = (maxOptions * optionHeight) + 'px';
    
    // 添加自定义样式类
    selectElement.classList.add('auto-sized-select');
}

async function loadSuppSourceVariables(datasetName, sourceSelect) {
    try {
        // 使用新的后端接口获取源变量
        const response = await fetch(`/get_source_variables/${datasetName}`);
        const result = await response.json();
        
        if (result.error) {
            sourceSelect.innerHTML = `<option value="">错误: ${result.error}</option>`;
            return;
        }
        
        const sourceVariables = result.source_variables || [];
        const suppDatasetName = result.supp_dataset;
        
        if (sourceVariables.length > 0) {
            sourceSelect.innerHTML = '<option value="">选择源变量</option>' +
                sourceVariables.map(col => 
                    `<option value="${col}" title="来自${suppDatasetName}的${col}变量">${col}</option>`
                ).join('');
            
            // 动态调整下拉框高度以显示所有选项
            adjustSourceSelectHeight(sourceSelect, sourceVariables.length + 1); // +1 for placeholder
            
            // 添加提示信息
            const container = sourceSelect.closest('.source-variables-container');
            const existing = container.querySelector('.supp-info');
            if (existing) existing.remove();
            
            const infoElement = document.createElement('small');
            infoElement.className = 'text-info mt-1 d-block supp-info';
            infoElement.innerHTML = `<i class="fas fa-info-circle me-1"></i>来自${suppDatasetName}数据集的${sourceVariables.length}个可选变量 - 按住Ctrl多选`;
            container.appendChild(infoElement);
            
        } else {
            sourceSelect.innerHTML = `<option value="">${result.message || '无可用源变量'}</option>`;
            adjustSourceSelectHeight(sourceSelect, 1);
        }
        
    } catch (error) {
        console.error('加载SUPP源变量失败:', error);
        sourceSelect.innerHTML = '<option value="">加载源变量失败</option>';
    }
}



function updateRowSaveButton(row) {
    const saveBtn = row.querySelector('.save-config-btn');
    const configIndex = parseInt(row.getAttribute('data-config-index'));
    const config = mergeConfigs[configIndex];
    
    // 检查是否有足够的配置信息
    const isValid = config.dataset && config.target && config.sources.length > 0;
    saveBtn.disabled = !isValid;
    
    if (isValid) {
        saveBtn.classList.remove('btn-outline-success');
        saveBtn.classList.add('btn-success');
    } else {
        saveBtn.classList.remove('btn-success');
        saveBtn.classList.add('btn-outline-success');
    }
}

function saveMergeConfigRow(row) {
    console.log('saveConfigRow called with row:', row);
    const configIndex = parseInt(row.getAttribute('data-config-index'));
    const config = mergeConfigs[configIndex];
    console.log('Config data:', config);
    console.log('Config validation - dataset:', config.dataset, 'target:', config.target, 'sources:', config.sources);
    
    if (!config.dataset || !config.target || config.sources.length === 0) {
        console.log('Validation failed - showing alert');
        showAlert('请完整填写配置信息', 'warning');
        return;
    }
    
    console.log('Validation passed - proceeding with save');
    
    // 将编辑行转换为显示行
    row.classList.remove('editable-row');
    row.classList.add('saved-row');
    
    // 获取目标变量的显示信息
    const targetBadgeClass = config.targetSource === 'supp' ? 'bg-warning' : 'bg-primary';
    const targetTitle = config.targetSource === 'supp' ? 
        `目标变量来自SUPP数据集(${config.targetSuppDataset})` : 
        `目标变量来自主数据集(${config.dataset})`;
    
    row.innerHTML = `
        <td class="text-center">${configIndex + 1}</td>
        <td>
            <span class="badge ${targetBadgeClass}" title="${targetTitle}">${config.target}</span>
            ${config.targetSource === 'supp' ? '<small class="text-muted d-block mt-1">SUPP变量</small>' : ''}
        </td>
        <td>
            <div class="source-variables-display">
                ${config.sources.map((src, index) => {
                    const displayText = typeof src === 'object' ? src.column : src;
                    const datasetInfo = typeof src === 'object' && src.dataset ? ` (${src.dataset})` : '';
                    return `<span class="badge bg-secondary me-1" title="第${index + 1}个源变量${datasetInfo}">${displayText}</span>`;
                }).join('')}
            </div>
        </td>
        <td><span class="badge bg-info">${config.dataset}</span></td>
        <td class="text-center">
            <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-primary btn-sm edit-config-btn" title="编辑配置">
                    <i class="fas fa-edit"></i>
                </button>
                <button class="btn btn-outline-danger btn-sm delete-config-btn" title="删除配置">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </td>
    `;
    
    // 重新绑定删除和编辑事件
    const deleteBtn = row.querySelector('.delete-config-btn');
    const editBtn = row.querySelector('.edit-config-btn');
    
    deleteBtn.addEventListener('click', function() {
        deleteMergeConfigRow(row);
    });
    
    editBtn.addEventListener('click', function() {
        editMergeConfigRow(row);
    });
    
    // 更新合并按钮状态
    updateMergeButtons();
    
    showAlert('配置保存成功！', 'success');
}

function editMergeConfigRow(row) {
    const configIndex = parseInt(row.getAttribute('data-config-index'));
    const config = mergeConfigs[configIndex];
    
    // 将显示行转换回编辑行
    row.classList.remove('saved-row');
    row.classList.add('editable-row');
    
    row.innerHTML = `
        <td class="text-center">${configIndex + 1}</td>
        <td>
            <select class="form-select form-select-sm target-variable-select" data-field="target">
                <option value="">选择主变量</option>
            </select>
        </td>
        <td>
            <div class="source-variables-container">
                <select class="form-select form-select-sm source-variable-select" data-field="sources" multiple>
                    <option value="">选择源变量</option>
                </select>
                <small class="text-muted mt-1 d-block">按住Ctrl键可多选，注意选择顺序</small>
            </div>
        </td>
        <td>
            <select class="form-select form-select-sm dataset-select" data-field="dataset">
                <option value="">选择数据集</option>
                ${Object.keys(currentDatasets).map(ds => 
                    `<option value="${ds}" ${ds === config.dataset ? 'selected' : ''}>${ds}</option>`
                ).join('')}
            </select>
        </td>
        <td class="text-center">
            <div class="btn-group btn-group-sm">
                <button class="btn btn-outline-success btn-sm save-config-btn" title="保存配置">
                    <i class="fas fa-check"></i>
                </button>
                <button class="btn btn-outline-danger btn-sm delete-config-btn" title="删除配置">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </td>
    `;
    
    // 重新绑定事件
    bindRowEvents(row);
    
            // 如果有选择的数据集，重新加载变量
        if (config.dataset) {
            loadVariablesForRow(row, config.dataset).then(() => {
                // 恢复之前的选择
                const targetSelect = row.querySelector('.target-variable-select');
                const sourceSelect = row.querySelector('.source-variable-select');
                
                if (config.target) {
                    // 根据目标变量的来源类型选择正确的选项
                    let targetOption = null;
                    for (let option of targetSelect.options) {
                        if (option.value === config.target) {
                            if (config.targetSource === 'supp' && option.getAttribute('data-source') === 'supp') {
                                targetOption = option;
                                break;
                            } else if (config.targetSource === 'main' && option.getAttribute('data-source') === 'main') {
                                targetOption = option;
                                break;
                            } else if (!config.targetSource) {
                                targetOption = option;
                                break;
                            }
                        }
                    }
                    if (targetOption) {
                        targetSelect.value = config.target;
                    }
                }
                
                if (config.sources && config.sources.length > 0) {
                    config.sources.forEach(source => {
                        const option = sourceSelect.querySelector(`option[value="${source}"]`);
                        if (option) {
                            option.selected = true;
                        }
                    });
                    
                    // 重新调整源变量选择框的高度
                    const totalOptions = sourceSelect.options.length;
                    if (totalOptions > 0) {
                        adjustSourceSelectHeight(sourceSelect, totalOptions);
                    }
                }
                
                updateRowSaveButton(row);
            });
        }
}

function deleteMergeConfigRow(row) {
    console.log('deleteConfigRow called with row:', row);
    const configIndex = parseInt(row.getAttribute('data-config-index'));
    console.log('Deleting config at index:', configIndex);
    
    if (confirm('确定要删除这个配置吗？')) {
        console.log('User confirmed deletion');
        // 从数组中删除配置
        mergeConfigs.splice(configIndex, 1);
        
        // 删除行
        row.remove();
        
        // 重新编号所有行
        updateConfigRowNumbers();
        
        // 检查是否需要显示空状态
        checkEmptyState();
        
        // 更新合并按钮状态
        updateMergeButtons();
        
        showAlert('配置已删除', 'info');
    }
}

function updateConfigRowNumbers() {
    const rows = document.querySelectorAll('#mergeConfigBody .config-row');
    rows.forEach((row, index) => {
        row.setAttribute('data-config-index', index);
        const numberCell = row.querySelector('td:first-child');
        if (numberCell) {
            numberCell.textContent = index + 1;
        }
    });
}

function checkEmptyState() {
    const mergeConfigBody = document.getElementById('mergeConfigBody');
    const emptyRow = document.getElementById('emptyConfigRow');
    const continueRow = document.getElementById('continueAddRow');
    const configRows = mergeConfigBody.querySelectorAll('.config-row');
    
    if (configRows.length === 0) {
        if (continueRow) {
            continueRow.remove();
        }
        
        // 如果没有空状态行，创建一个
        if (!emptyRow) {
            mergeConfigBody.innerHTML = `
                <tr class="text-center" id="emptyConfigRow">
                    <td colspan="5" class="py-3">
                        <button class="btn btn-outline-primary btn-lg" id="addFirstConfigBtn">
                            <i class="fas fa-plus me-2"></i>添加合并配置
                        </button>
                    </td>
                </tr>
            `;
        } else {
            emptyRow.style.display = '';
        }
        
        // 重置mergeConfigs数组
        mergeConfigs = [];
    }
}

function updateMergeButtons() {
    const validConfigs = mergeConfigs.filter(config => 
        config.target && config.sources && config.sources.length > 0 && config.dataset
    );
    
    const hasValidConfigs = validConfigs.length > 0;
    if (saveMergeConfigBtn) {
        saveMergeConfigBtn.disabled = !hasValidConfigs;
    }

}

function addVariableToCurrentConfig() {
    console.log('addVariableToCurrentConfig 被调用');
    console.log('当前选择:', {
        dataset: configDatasetSelect.value,
        variable: configVariableSelect.value,
        currentConfig: currentConfig
    });
    
    const selectedDataset = configDatasetSelect.value;
    const selectedVariable = configVariableSelect.value;
    
    if (!selectedDataset || !selectedVariable) {
        console.log('数据集或变量未选择');
        showAlert('请选择数据集和变量', 'warning');
        return;
    }
    
    if (!currentConfig.target) {
        // 第一次选择为目标变量
        const selectedOption = configVariableSelect.options[configVariableSelect.selectedIndex];
        const originSupp = selectedOption?.dataset?.originSupp;
        const originQnam = selectedOption?.dataset?.originQnam;
        const originIdvar = selectedOption?.dataset?.originIdvar;
        
        if (originSupp && originQnam) {
            currentConfig.target = { dataset: originSupp, column: originQnam, idvar: originIdvar || null };
        } else {
            currentConfig.target = { dataset: selectedDataset, column: selectedVariable };
        }
    } else {
        // 后续选择为源变量
        const selectedOption = configVariableSelect.options[configVariableSelect.selectedIndex];
        const originSupp = selectedOption?.dataset?.originSupp;
        const originQnam = selectedOption?.dataset?.originQnam;
        const originIdvar = selectedOption?.dataset?.originIdvar;
        
        if (originSupp && originQnam) {
            currentConfig.sources.push({ dataset: originSupp, column: originQnam, idvar: originIdvar || null });
        } else {
            currentConfig.sources.push({ dataset: selectedDataset, column: selectedVariable });
        }
    }
    
    updateSelectedVariablesDisplay();
    updateConfigButtons();
    
    // 重置选择让用户可以继续选择
    configDatasetSelect.value = '';
    configVariableSelect.innerHTML = '<option value="">请选择变量</option>';
}

function updateSelectedVariablesDisplay() {
    selectedVariablesDisplay.innerHTML = '';
    
    if (!currentConfig) {
        selectedVariablesDisplay.innerHTML = '<small class="text-muted">请先选择目标变量，然后选择源变量</small>';
        return;
    }
    
    if (currentConfig.target) {
        const targetTag = createVariableTag(`${currentConfig.target.dataset}.${currentConfig.target.column}`, true);
        selectedVariablesDisplay.appendChild(targetTag);
    }
    
    currentConfig.sources.forEach((source, index) => {
        const sourceTag = createVariableTag(`${source.dataset}.${source.column}`, false, index);
        selectedVariablesDisplay.appendChild(sourceTag);
    });
    
    if (!currentConfig.target && currentConfig.sources.length === 0) {
        selectedVariablesDisplay.innerHTML = '<small class="text-muted">请先选择目标变量，然后选择源变量</small>';
    }
}

function createVariableTag(variableName, isTarget, sourceIndex = -1) {
    const tag = document.createElement('span');
    tag.className = `variable-tag ${isTarget ? 'target-variable' : ''}`;
    tag.innerHTML = `
        ${isTarget ? '目标: ' : '源: '}${variableName}
        ${!isTarget ? `<span class="remove-btn" onclick="removeSourceVariable(${sourceIndex})">×</span>` : ''}
    `;
    return tag;
}

function removeSourceVariable(index) {
    if (currentConfig && currentConfig.sources) {
        currentConfig.sources.splice(index, 1);
    updateSelectedVariablesDisplay();
        updateConfigButtons();
    }
}

function updateConfigButtons() {
    const hasDatasetAndVariable = configDatasetSelect.value && configVariableSelect.value;
    console.log('更新按钮状态:', {
        hasDatasetAndVariable,
        dataset: configDatasetSelect.value,
        variable: configVariableSelect.value,
        currentConfig: currentConfig
    });
    addVariableBtn.disabled = !hasDatasetAndVariable;
    
    const canComplete = currentConfig && currentConfig.target && currentConfig.sources.length > 0;
    completeConfigBtn.disabled = !canComplete;
    
    console.log('按钮状态更新后:', {
        addVariableBtn_disabled: addVariableBtn.disabled,
        completeConfigBtn_disabled: completeConfigBtn.disabled
    });
}

function completeCurrentConfig() {
    if (!currentConfig || !currentConfig.target || currentConfig.sources.length === 0) {
        showAlert('请确保已选择目标变量和至少一个源变量', 'warning');
        return;
    }
    
    isConfiguring = false;
    currentConfigSection.classList.add('d-none');
    
    renderMergeConfigTable();
    updateMergeButtons();
    
    showAlert('配置完成，可以继续添加新配置或执行合并', 'success');
}

function cancelCurrentConfig() {
    if (currentConfigIndex >= 0 && currentConfigIndex < mergeConfigs.length) {
        mergeConfigs.splice(currentConfigIndex, 1);
    }
    
    isConfiguring = false;
    currentConfig = null;
    currentConfigIndex = -1;
    currentConfigSection.classList.add('d-none');
    
    renderMergeConfigTable();
    updateMergeButtons();
}

function renderMergeConfigTable() {
    mergeConfigBody.innerHTML = '';
    
    if (mergeConfigs.length === 0) {
        mergeConfigBody.innerHTML = `
            <tr class="text-center" id="emptyConfigRow">
                <td colspan="5" class="py-3">
                    <button class="btn btn-outline-primary btn-lg" id="addFirstConfigBtn">
                        <i class="fas fa-plus me-2"></i>添加合并配置
                    </button>
                </td>
            </tr>
        `;
        return;
    }
    
    mergeConfigs.forEach((config, index) => {
        if (config.target && config.sources.length > 0) {
            const row = document.createElement('tr');
            row.className = 'config-row saved-row';
            row.setAttribute('data-config-index', index);
            
            // 获取目标变量的显示信息
            const targetBadgeClass = config.targetSource === 'supp' ? 'bg-warning' : 'bg-primary';
            const targetTitle = config.targetSource === 'supp' ? 
                `目标变量来自SUPP数据集(${config.targetSuppDataset})` : 
                `目标变量来自主数据集(${config.dataset})`;
            
            row.innerHTML = `
                <td class="text-center">${index + 1}</td>
                <td>
                    <span class="badge ${targetBadgeClass}" title="${targetTitle}">${config.target}</span>
                    ${config.targetSource === 'supp' ? '<small class="text-muted d-block mt-1">SUPP变量</small>' : ''}
                </td>
                <td>
                    <div class="source-variables-display">
                        ${config.sources.map((src, srcIndex) => {
                            const displayText = typeof src === 'object' ? src.column : src;
                            const datasetInfo = typeof src === 'object' && src.dataset ? ` (${src.dataset})` : '';
                            return `<span class="badge bg-secondary me-1" title="第${srcIndex + 1}个源变量${datasetInfo}">${displayText}</span>`;
                        }).join('')}
                    </div>
                </td>
                <td><span class="badge bg-info">${config.dataset}</span></td>
                <td class="text-center">
                    <div class="btn-group btn-group-sm">
                        <button class="btn btn-outline-primary btn-sm edit-config-btn" title="编辑配置">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="btn btn-outline-danger btn-sm delete-config-btn" title="删除配置">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </td>
            `;
            mergeConfigBody.appendChild(row);
            
            // 绑定事件
            const deleteBtn = row.querySelector('.delete-config-btn');
            const editBtn = row.querySelector('.edit-config-btn');
            
            deleteBtn.addEventListener('click', function() {
                deleteMergeConfigRow(row);
            });
            
            editBtn.addEventListener('click', function() {
                editMergeConfigRow(row);
            });
        }
    });
    
    // 添加"继续添加"按钮，让用户可以添加新的配置行
    addContinueAddButton();
}

// removeMergeConfig函数已被deleteConfigRow替代，不再需要

function updateMergeButtons() {
    const hasConfigs = mergeConfigs.filter(config => config.target && config.sources.length > 0).length > 0;
    if (saveMergeConfigBtn) {
        saveMergeConfigBtn.disabled = !hasConfigs;
    }
}

// 初始化合并配置页面
async function initializeMergeConfigPage() {
    // 获取当前路径
    const currentPath = document.getElementById('datasetPath').value;
    if (!currentPath) {
        console.log('未设置数据路径，跳过自动加载配置');
        return;
    }
    
    // 检查currentDatasets是否可用，如果没有则重新加载数据集信息
    if (!currentDatasets || Object.keys(currentDatasets).length === 0) {
        console.log('currentDatasets不可用，尝试重新加载数据集信息');
        try {
            const dataResponse = await fetch('/read_datasets', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    path: currentPath,
                    mode: currentMode,
                    translation_direction: currentMode === 'SDTM' ? translationDirectionSelect.value : null
                })
            });
            
            const dataResult = await dataResponse.json();
            if (dataResult.success) {
                currentDatasets = dataResult.datasets;
                console.log('数据集信息重新加载成功');
            } else {
                console.error('重新加载数据集信息失败:', dataResult.message);
                showAlert('无法加载数据集信息，请返回数据预览页面重新加载', 'warning');
                return;
            }
        } catch (error) {
            console.error('重新加载数据集信息时发生错误:', error);
            showAlert('加载数据集信息失败，请返回数据预览页面重新加载', 'error');
            return;
        }
    }
    
    try {
        // 尝试加载已保存的合并配置
        const response = await fetch(`/api/load_merge_config?path=${encodeURIComponent(currentPath)}`);
        
        if (response.ok) {
            const result = await response.json();
            if (result.success && result.config && result.config.configs) {
                // 清空当前配置
                mergeConfigs.length = 0;
                
                // 转换并加载配置
                const loadedConfigs = result.config.configs;
                loadedConfigs.forEach(config => {
                    let convertedConfig;
                    
                    if (config.target && typeof config.target === 'object') {
                        // 新格式配置
                        convertedConfig = {
                            dataset: config.target.dataset,
                            target: config.target.column,
                            sources: config.sources.map(source => {
                                if (typeof source === 'object') {
                                    return {
                                        dataset: source.dataset,
                                        column: source.column
                                    };
                                }
                                return source;
                            }),
                            targetSource: config.target.dataset !== config.sources[0]?.dataset ? 'supp' : 'main',
                            targetSuppDataset: config.target.dataset
                        };
                    } else {
                        // 旧格式配置
                        convertedConfig = {
                            dataset: config.dataset,
                            target: config.target,
                            sources: config.sources,
                            targetSource: 'main'
                        };
                    }
                    
                    mergeConfigs.push(convertedConfig);
                });
                
                // 重新渲染配置表
                renderMergeConfigTable();
                updateMergeButtons();
                
                showAlert(`已自动加载 ${loadedConfigs.length} 个合并配置`, 'success');
            }
        } else {
            console.log('未找到已保存的合并配置');
        }
    } catch (error) {
        console.error('加载合并配置失败:', error);
    }
}

async function saveMergeConfig() {
    const validConfigs = mergeConfigs.filter(config => config.target && config.sources.length > 0);
    if (validConfigs.length === 0) {
        showAlert('请先添加合并配置', 'warning');
        return;
    }
    
    // 获取当前路径作为配置的标识
    const currentPath = document.getElementById('datasetPath').value;
    if (!currentPath) {
        showAlert('请先设置数据路径', 'warning');
        return;
    }
    
    showLoadingOverlay('正在保存合并配置...', '请稍候');
    if (saveMergeConfigBtn) {
        saveMergeConfigBtn.disabled = true;
    }
    
    try {
        // 转换配置格式以符合后端预期
        const formattedConfigs = await Promise.all(validConfigs.map(async config => {
            // 获取对应的SUPP数据集名称
            const suppResponse = await fetch(`/get_source_variables/${config.dataset}`);
            const suppResult = await suppResponse.json();
            const suppDataset = suppResult.supp_dataset;
            
            // 判断目标变量的来源
            const targetDataset = config.targetSource === 'supp' ? config.targetSuppDataset : config.dataset;
            
            if (suppDataset) {
                // 新格式：支持目标和源变量都可能来自不同数据集
                return {
                    target: {
                        dataset: targetDataset,
                        column: config.target
                    },
                    sources: config.sources.map(sourceVar => ({
                        dataset: sourceVar.dataset || suppDataset,
                        column: sourceVar.column || sourceVar
                    }))
                };
            } else {
                // 旧格式：源变量来自同一数据集
                return {
                    dataset: config.dataset,
                    target: config.target,
                    sources: config.sources
                };
            }
        }));
        
        const response = await fetch('/save_merge_config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                path: currentPath,
                config: formattedConfigs,
                translation_direction: translationDirectionSelect.value
            })
        });
        
        if (!response.ok) {
            throw new Error(`服务器返回状态 ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            showAlert(result.message || '合并配置保存成功', 'success');
            // 保存完成后启用并跳转到翻译库版本控制页面
            enableTab('translation-library-tab');
            switchToTab('translation-library-tab');
            showAlert('配置已保存，可以进入翻译库版本控制', 'success');
        } else {
            showAlert(result.message, 'error');
        }
    } catch (error) {
        showAlert('保存配置失败: ' + error.message, 'error');
    } finally {
        hideLoadingOverlay();
        if (saveMergeConfigBtn) {
            saveMergeConfigBtn.disabled = false;
        }
    }
}
















function showLoadingOverlay(mainText, subText) {
    loadingText.textContent = mainText;
    loadingSubtext.textContent = subText;
    loadingOverlay.classList.remove('d-none');
}

function hideLoadingOverlay() {
    loadingOverlay.classList.add('d-none');
}

function showMappingSaveDialog() {
    preprocessingConfirmModal.hide();
    mappingSaveModal.show();
}

async function saveMappingConfig() {
    const configName = document.getElementById('mappingConfigName').value.trim();
    const datasetPath = datasetPathInput.value.trim();
    
    try {
        const response = await fetch('/api/save_mapping_config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                path: datasetPath,
                mode: currentMode,
                translation_direction: translationDirectionSelect.value,
                configs: mergeConfigs,
                name: configName || `配置_${new Date().toLocaleString()}`
            })
        });

        const result = await response.json();
        if (result.success) {
            showAlert('映射配置保存成功', 'success');
            mappingSaveModal.hide();
            proceedToNextPage();
        } else {
            showAlert('保存映射配置失败: ' + result.message, 'danger');
        }
    } catch (error) {
        showAlert('保存映射配置时发生错误: ' + error.message, 'danger');
    }
}

function proceedToNextPage() {
    mappingSaveModal.hide();
    // 保存翻译方向到sessionStorage，传递给下一页
    sessionStorage.setItem('translation_direction', translationDirectionSelect.value);
    sessionStorage.setItem('dataset_path', datasetPathInput.value.trim());
    sessionStorage.setItem('translation_mode', currentMode);
    // TODO: 跳转到新的翻译库版本控制页面（待实现）
    showAlert('映射配置已保存，翻译功能正在开发中', 'info');
}

function showAlert(message, type) {
    // 移除现有的alert
    const existingAlert = document.querySelector('.alert-message');
    if (existingAlert) {
        existingAlert.remove();
    }
    
    // 创建新的alert
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${getBootstrapAlertType(type)} alert-dismissible fade show alert-message`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    // 插入到容器顶部
    const container = document.querySelector('.container');
    container.insertBefore(alertDiv, container.firstChild);
    
    // 自动移除alert
    setTimeout(() => {
        if (alertDiv.parentNode) {
            alertDiv.remove();
        }
    }, 5000);
}

function getBootstrapAlertType(type) {
    const typeMap = {
        'success': 'success',
        'error': 'danger',
        'warning': 'warning',
        'info': 'info'
    };
    return typeMap[type] || 'info';
}

// 键盘快捷键支持
document.addEventListener('keydown', function(e) {
    // Ctrl/Cmd + 方向键导航标签页
    if ((e.ctrlKey || e.metaKey) && !e.shiftKey && !e.altKey) {
        switch(e.key) {
            case 'ArrowLeft':
                e.preventDefault();
                navigateToPreviousTab();
                break;
            case 'ArrowRight':
                e.preventDefault();
                navigateToNextTab();
                break;
            case 'Enter':
                // 在配置页面时快速添加变量
                if (isConfiguring && !addVariableBtn.disabled) {
                    e.preventDefault();
                    addVariableToCurrentConfig();
                }
                break;
        }
    }
});

function navigateToPreviousTab() {
    const currentTab = document.querySelector('.main-tabs-vertical .nav-link.active');
    if (!currentTab) return;
    
    const currentId = currentTab.id;
    switch(currentId) {
        case 'preview-tab':
            switchToTab('setup-tab');
            break;
        case 'merge-config-tab':
            switchToTab('preview-tab');
            break;

    }
}

function navigateToNextTab() {
    const currentTab = document.querySelector('.main-tabs-vertical .nav-link.active');
    if (!currentTab) return;
    
    const currentId = currentTab.id;
    switch(currentId) {
        case 'setup-tab':
            if (!document.getElementById('preview-tab').disabled) {
                switchToTab('preview-tab');
            }
            break;
        case 'preview-tab':
            if (currentMode === 'SDTM' && !document.getElementById('merge-config-tab').disabled) {
                switchToTab('merge-config-tab');
            } else {
                switchToTab('translation-version-control-tab');
            }
            break;
        case 'merge-config-tab':
            switchToTab('translation-version-control-tab');
            break;
    }
}

// 添加工具提示
function addTooltips() {
    // 为按钮添加工具提示
    const tooltips = [
        { element: readButton, text: '读取指定路径下的SAS数据集文件' },
        { element: nextToPreviewBtn, text: '查看读取的数据集内容' },
        { element: saveMergeConfigBtn, text: '保存合并配置到数据库，供后续使用' }
    ];
    
    tooltips.forEach(({ element, text }) => {
        if (element) {
            element.setAttribute('title', text);
            element.setAttribute('data-bs-toggle', 'tooltip');
        }
    });
    
    // 初始化Bootstrap工具提示
    if (typeof bootstrap !== 'undefined') {
        const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        tooltipTriggerList.map(function (tooltipTriggerEl) {
            return new bootstrap.Tooltip(tooltipTriggerEl);
        });
    }
}

// 在DOM加载完成后添加工具提示
document.addEventListener('DOMContentLoaded', function() {
    addTooltips();
});

// 添加数据统计动画
function animateNumber(element, finalNumber, duration = 1000) {
    const startNumber = 0;
    const increment = finalNumber / (duration / 16);
    let currentNumber = startNumber;
    
    const timer = setInterval(() => {
        currentNumber += increment;
        if (currentNumber >= finalNumber) {
            currentNumber = finalNumber;
            clearInterval(timer);
        }
        element.textContent = Math.floor(currentNumber).toLocaleString();
    }, 16);
}

// 增强的成功反馈
function showSuccessWithAnimation(message) {
    showAlert(message, 'success');
    
    // 添加成功音效（如果浏览器支持）
    try {
        const audio = new Audio('data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgAZGF0YQoGAACBhYqFbF1fdJivrJBhNjVgodDbq2EcBj');
        audio.volume = 0.1;
        audio.play().catch(() => {}); // 忽略播放失败
    } catch (e) {
        // 忽略音频创建失败
    }
}

// SUPP详情切换函数
function toggleSuppDetails(datasetName) {
    const detailsElement = document.getElementById(`supp-details-${datasetName}`);
    if (detailsElement) {
        detailsElement.classList.toggle('d-none');
        const button = detailsElement.previousElementSibling.querySelector('button');
        if (button) {
            button.textContent = detailsElement.classList.contains('d-none') ? '详情' : '收起';
        }
    }
}

// 使函数在全局可用
window.removeSourceVariable = removeSourceVariable;
// window.removeMergeConfig已被删除，使用deleteConfigRow替代
window.toggleSuppDetails = toggleSuppDetails;

// ==================== 翻译库版本控制功能 ====================

// 翻译库版本控制相关元素
const translationLibraryTab = document.getElementById('translation-library-tab');
const inheritedTranslationDirection = document.getElementById('inheritedTranslationDirection');
const medDRAVersionSelect = document.getElementById('medDRAVersion');
const whoDrugVersionSelect = document.getElementById('whoDrugVersion');
const igVersionSelect = document.getElementById('igVersion');
const saveTranslationConfigBtn = document.getElementById('saveTranslationConfigBtn');

// MedDRA和WHODrug配置表相关元素
const addMedDRAConfigBtn = document.getElementById('addMedDRAConfigBtn');
const addWhoDrugConfigBtn = document.getElementById('addWhoDrugConfigBtn');
const medDRAConfigBody = document.getElementById('medDRAConfigBody');
const whoDrugConfigBody = document.getElementById('whoDrugConfigBody');
const emptyMedDRAConfigRow = document.getElementById('emptyMedDRAConfigRow');
const emptyWhoDrugConfigRow = document.getElementById('emptyWhoDrugConfigRow');

// 配置数据存储
let medDRAConfigs = [];
let whoDrugConfigs = [];
let availableVariables = []; // 存储可用的变量列表

// 翻译库版本控制初始化
function initializeTranslationLibrary() {
    console.log('初始化翻译库版本控制...');
    console.log('addMedDRAConfigBtn:', addMedDRAConfigBtn);
    console.log('addWhoDrugConfigBtn:', addWhoDrugConfigBtn);
    console.log('emptyMedDRAConfigRow:', emptyMedDRAConfigRow);
    console.log('emptyWhoDrugConfigRow:', emptyWhoDrugConfigRow);
    
    // 检查currentDatasets是否可用，如果不可用则重新加载
    if (!currentDatasets || Object.keys(currentDatasets).length === 0) {
        console.log('currentDatasets不可用，尝试重新加载数据集信息');
        loadDatasetsForTranslationLibrary();
    }
    
    // 加载已保存的配置
    loadTranslationLibraryConfig();
    
    // 继承基础设置页面的翻译方向
    inheritTranslationDirection();
    
    // 绑定事件监听器
    if (saveTranslationConfigBtn) {
        saveTranslationConfigBtn.addEventListener('click', saveTranslationLibraryConfig);
    }
    
    // 绑定项目路径输入框事件监听器
    if (datasetPathInput) {
        datasetPathInput.addEventListener('blur', function() {
            const path = this.value.trim();
            if (path) {
                console.log('项目路径改变，尝试加载配置:', path);
                loadTranslationLibraryConfig();
            }
        });
        
        datasetPathInput.addEventListener('input', function() {
            // 延迟加载，避免频繁触发
            clearTimeout(this.loadConfigTimeout);
            this.loadConfigTimeout = setTimeout(() => {
                const path = this.value.trim();
                if (path) {
                    console.log('项目路径输入完成，尝试加载配置:', path);
                    loadTranslationLibraryConfig();
                }
            }, 1000);
        });
    }
    
    // 绑定配置表事件监听器
    if (addMedDRAConfigBtn) {
        console.log('绑定MedDRA添加按钮事件');
        addMedDRAConfigBtn.addEventListener('click', () => {
            console.log('MedDRA添加按钮被点击');
            addConfigRow('meddra');
        });
    } else {
        console.error('addMedDRAConfigBtn元素未找到');
    }
    
    if (addWhoDrugConfigBtn) {
        console.log('绑定WHODrug添加按钮事件');
        addWhoDrugConfigBtn.addEventListener('click', () => {
            console.log('WHODrug添加按钮被点击');
            addConfigRow('whodrug');
        });
    } else {
        console.error('addWhoDrugConfigBtn元素未找到');
    }
    
    // 确保空行是可见的
    if (emptyMedDRAConfigRow) {
        emptyMedDRAConfigRow.style.display = '';
        console.log('显示MedDRA空行');
    }
    
    if (emptyWhoDrugConfigRow) {
        emptyWhoDrugConfigRow.style.display = '';
        console.log('显示WHODrug空行');
    }
    
    // 加载版本选项
    loadVersionOptions();
    
    // 加载可用变量
    loadAvailableVariables();
}

// 继承基础设置页面的翻译方向
function inheritTranslationDirection() {
    if (translationDirectionSelect && inheritedTranslationDirection) {
        const direction = translationDirectionSelect.value;
        const directionText = translationDirectionSelect.options[translationDirectionSelect.selectedIndex].text;
        inheritedTranslationDirection.textContent = directionText || '未设置';
        
        // 在翻译库版本控制页面，始终显示所有版本选择器
        showAllVersionSelects();
    }
}

// 翻译方向改变事件
function onTranslationDirectionChange() {
    const direction = translationDirectionSelect.value;
    console.log('翻译方向改变:', direction);
    
    // 根据翻译方向调整界面
    updateTranslationModeOptions(direction);
}

// 翻译模式改变事件
function onTranslationModeChange() {
    const mode = translationModeSelect.value;
    console.log('翻译模式改变:', mode);
    
    // 根据翻译模式显示/隐藏版本选择
    updateVersionSelects(mode);
}

// 更新翻译模式选项
function updateTranslationModeOptions(direction) {
    if (!translationModeSelect) return;
    
    // 保持原有的RAW和SDTM选项，不需要动态更新
    // 翻译模式选项在HTML中已经定义好了
    console.log('翻译方向改变，但翻译模式选项保持不变:', direction);
}

// 更新版本选择器显示状态
function updateVersionSelects(mode) {
    const medDRAGroup = medDRAVersionSelect?.closest('.mb-3');
    const whoDrugGroup = whoDrugVersionSelect?.closest('.mb-3');
    
    if (!medDRAGroup || !whoDrugGroup) return;
    
    // 隐藏所有版本选择器
    medDRAGroup.style.display = 'none';
    whoDrugGroup.style.display = 'none';
    
    // 根据模式显示相应的版本选择器
    switch (mode) {
        case 'meddra':
            medDRAGroup.style.display = 'block';
            break;
        case 'whodrug':
            whoDrugGroup.style.display = 'block';
            break;
        case 'both':
            medDRAGroup.style.display = 'block';
            whoDrugGroup.style.display = 'block';
            break;
    }
}

// 显示所有版本选择器（用于翻译库版本控制页面）
function showAllVersionSelects() {
    const medDRAGroup = medDRAVersionSelect?.closest('.mb-3');
    const whoDrugGroup = whoDrugVersionSelect?.closest('.mb-3');
    const igGroup = igVersionSelect?.closest('.mb-3');
    
    if (medDRAGroup) medDRAGroup.style.display = 'block';
    if (whoDrugGroup) whoDrugGroup.style.display = 'block';
    if (igGroup) igGroup.style.display = 'block';
}

// 加载版本选项
async function loadVersionOptions() {
    console.log('开始加载版本选项...');
    console.log('medDRAVersionSelect:', medDRAVersionSelect);
    console.log('whoDrugVersionSelect:', whoDrugVersionSelect);
    
    // 获取当前翻译方向
    const translationDirection = getTranslationDirection();
    console.log('当前翻译方向:', translationDirection);
    
    try {
        // 加载MedDRA版本
        console.log('正在获取MedDRA版本...');
        const medDRAResponse = await fetch('/api/get_meddra_versions');
        console.log('MedDRA API响应状态:', medDRAResponse.status);
        if (medDRAResponse.ok) {
            const medDRAData = await medDRAResponse.json();
            console.log('MedDRA版本数据:', medDRAData);
            if (medDRAData.versions) {
                console.log('填充MedDRA版本选择器...');
                const filteredVersions = filterVersionsByLanguage(medDRAData.versions, translationDirection);
                populateVersionSelectWithDefault(medDRAVersionSelect, filteredVersions);
            }
        }
        
        // 加载WHODrug版本
        console.log('正在获取WHODrug版本...');
        const whoDrugResponse = await fetch('/api/get_whodrug_versions');
        console.log('WHODrug API响应状态:', whoDrugResponse.status);
        if (whoDrugResponse.ok) {
            const whoDrugData = await whoDrugResponse.json();
            console.log('WHODrug版本数据:', whoDrugData);
            if (whoDrugData.versions) {
                console.log('填充WHODrug版本选择器...');
                const filteredVersions = filterVersionsByLanguage(whoDrugData.versions, translationDirection);
                populateVersionSelectWithDefault(whoDrugVersionSelect, filteredVersions);
            }
        }
    } catch (error) {
        console.error('加载版本选项失败:', error);
        showAlert('加载版本选项失败', 'error');
    }
}

// 获取当前翻译方向
function getTranslationDirection() {
    // 优先从基础设置页面获取
    if (translationDirectionSelect && translationDirectionSelect.value) {
        return translationDirectionSelect.value;
    }
    
    // 从sessionStorage获取
    const savedDirection = sessionStorage.getItem('translation_direction');
    if (savedDirection) {
        return savedDirection;
    }
    
    return null;
}

// 根据翻译方向过滤版本选项
function filterVersionsByLanguage(versions, translationDirection) {
    if (!versions || !translationDirection) {
        return versions;
    }
    
    // 根据翻译方向确定需要的语言
    let targetLanguage;
    if (translationDirection === 'zh_to_en') {
        // 中译英：需要English版本
        targetLanguage = 'english';
    } else if (translationDirection === 'en_to_zh') {
        // 英译中：需要Chinese版本
        targetLanguage = 'chinese';
    } else {
        // 未知方向，返回所有版本
        return versions;
    }
    
    // 过滤版本
    return versions.filter(version => {
        if (typeof version === 'object' && version.name) {
            // 检查表名是否包含目标语言
            return version.name.toLowerCase().includes(targetLanguage);
        }
        return true; // 如果无法判断，保留该版本
    });
}

// 填充版本选择器（原版本，保持兼容性）
function populateVersionSelect(selectElement, versions) {
    if (!selectElement || !versions) return;
    
    // 清空现有选项（保留默认选项）
    const defaultOption = selectElement.querySelector('option[value=""]');
    selectElement.innerHTML = '';
    if (defaultOption) {
        selectElement.appendChild(defaultOption);
    }
    
    // 添加版本选项
    versions.forEach(version => {
        const option = document.createElement('option');
        // 处理后端返回的版本数据格式
        if (typeof version === 'object' && version.version) {
            option.value = version.version;
            option.textContent = `${version.version} (${version.record_count || 0}条记录)`;
        } else {
            option.value = version.value || version;
            option.textContent = version.text || version;
        }
        selectElement.appendChild(option);
    });
}

// 填充版本选择器并设置默认值
function populateVersionSelectWithDefault(selectElement, versions) {
    if (!selectElement || !versions) return;
    
    // 清空现有选项
    selectElement.innerHTML = '';
    
    // 添加默认提示选项
    const defaultOption = document.createElement('option');
    defaultOption.value = '';
    defaultOption.textContent = '请选择版本';
    selectElement.appendChild(defaultOption);
    
    // 排序版本（按版本号降序，最新版本在前）
    const sortedVersions = [...versions].sort((a, b) => {
        const versionA = typeof a === 'object' ? a.version : a;
        const versionB = typeof b === 'object' ? b.version : b;
        
        // 简单的版本号比较（假设格式为数字.数字）
        const parseVersion = (v) => {
            const parts = v.toString().replace(/[^0-9.]/g, '').split('.');
            return parts.map(p => parseInt(p) || 0);
        };
        
        const partsA = parseVersion(versionA);
        const partsB = parseVersion(versionB);
        
        for (let i = 0; i < Math.max(partsA.length, partsB.length); i++) {
            const a = partsA[i] || 0;
            const b = partsB[i] || 0;
            if (a !== b) return b - a; // 降序排列
        }
        return 0;
    });
    
    let latestVersion = null;
    
    // 添加版本选项
    sortedVersions.forEach((version, index) => {
        const option = document.createElement('option');
        // 处理后端返回的版本数据格式
        if (typeof version === 'object' && version.version) {
            option.value = version.version;
            option.textContent = `${version.version} (${version.record_count || 0}条记录)`;
            if (index === 0) latestVersion = version.version; // 第一个是最新版本
        } else {
            option.value = version.value || version;
            option.textContent = version.text || version;
            if (index === 0) latestVersion = version.value || version;
        }
        selectElement.appendChild(option);
    });
    
    // 自动选择最新版本
    if (latestVersion && sortedVersions.length > 0) {
        selectElement.value = latestVersion;
        console.log(`自动选择最新版本: ${latestVersion}`);
    }
}

// 保存翻译库配置
async function saveTranslationLibraryConfig() {
    // 获取所有配置数据
    const configData = getAllConfigData();
    
    const config = {
        path: lastDataPath,
        mode: translationModeSelect?.value || '',
        translation_direction: translationDirectionSelect?.value || '',
        meddra_version: medDRAVersionSelect?.value || '',
        whodrug_version: whoDrugVersionSelect?.value || '',
        ig_version: igVersionSelect?.value || '',
        meddra_config: configData.meddra_configs,
        whodrug_config: configData.whodrug_configs
    };
    
    // 验证必填字段
    if (!config.path) {
        showAlert('请先读取数据集', 'error');
        return;
    }
    
    if (!config.translation_direction) {
        showAlert('请选择翻译方向', 'error');
        return;
    }
    
    if (!config.mode) {
        showAlert('请选择翻译模式', 'error');
        return;
    }
    
    try {
        showLoadingOverlay('正在保存翻译库配置...');
        
        const response = await fetch('/api/save_translation_library_config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('翻译库配置保存成功', 'success');
            
            // 保存到sessionStorage供后续页面使用
            sessionStorage.setItem('translationLibraryConfig', JSON.stringify(config));
            
            // 启用翻译与确认标签页
            enableTranslationConfirmationTab();
            
            // 自动跳转到翻译与确认页面
            setTimeout(() => {
                const translationConfirmationTab = document.getElementById('translation-confirmation-tab');
                if (translationConfirmationTab) {
                    translationConfirmationTab.click();
                }
            }, 1000);
        } else {
            showAlert(result.message || '保存翻译库配置失败', 'error');
        }
    } catch (error) {
        console.error('保存翻译库配置失败:', error);
        showAlert('保存翻译库配置失败: ' + error.message, 'error');
    } finally {
        hideLoadingOverlay();
    }
}

// 加载翻译库配置
async function loadTranslationLibraryConfig() {
    // 优先使用当前输入框中的路径
    let currentPath = null;
    if (datasetPathInput && datasetPathInput.value.trim()) {
        currentPath = datasetPathInput.value.trim();
    } else if (lastDataPath) {
        currentPath = lastDataPath;
    }
    
    if (!currentPath) {
        // 尝试从sessionStorage获取
        const savedConfig = sessionStorage.getItem('translationLibraryConfig');
        if (savedConfig) {
            const config = JSON.parse(savedConfig);
            populateTranslationLibraryForm(config);
        }
        return;
    }
    
    try {
        console.log('尝试加载翻译库配置，路径:', currentPath);
        const response = await fetch(`/api/load_translation_library_config?path=${encodeURIComponent(currentPath)}`);
        
        if (response.ok) {
            const result = await response.json();
            if (result.success && result.config) {
                populateTranslationLibraryForm(result.config);
            }
        }
    } catch (error) {
        console.error('加载翻译库配置失败:', error);
    }
}

// 填充翻译库配置表单
function populateTranslationLibraryForm(config) {
    if (translationDirectionSelect && config.translation_direction) {
        translationDirectionSelect.value = config.translation_direction;
        onTranslationDirectionChange();
    }
    
    if (translationModeSelect && (config.translation_mode || config.mode)) {
        const modeValue = config.translation_mode || config.mode;
        translationModeSelect.value = modeValue;
        // 更新currentMode变量
        currentMode = modeValue;
        console.log('从配置加载翻译模式:', currentMode);
        // 触发模式变化事件
        onTranslationModeChange();
        // 更新UI
        updateUIForMode();
    }
    
    if (medDRAVersionSelect && config.meddra_version) {
        medDRAVersionSelect.value = config.meddra_version;
    }
    
    if (whoDrugVersionSelect && config.whodrug_version) {
        whoDrugVersionSelect.value = config.whodrug_version;
    }
    
    if (igVersionSelect && config.ig_version) {
        igVersionSelect.value = config.ig_version;
    }
    
    // 加载MedDRA和WHODrug配置
    if (config.meddra_config && Array.isArray(config.meddra_config)) {
        loadConfigTable('meddra', config.meddra_config);
    }
    
    if (config.whodrug_config && Array.isArray(config.whodrug_config)) {
        loadConfigTable('whodrug', config.whodrug_config);
    }
}

// 启用翻译与确认标签页
function enableTranslationConfirmationTab() {
    const translationConfirmationTab = document.getElementById('translation-confirmation-tab');
    if (translationConfirmationTab) {
        translationConfirmationTab.disabled = false;
        translationConfirmationTab.classList.remove('disabled');
    }
}

// 翻译与确认页面相关功能
const translationConfirmationElements = {
    codedListBtn: document.getElementById('codedListBtn'),
    uncodedListBtn: document.getElementById('uncodedListBtn'),
    datasetLabelBtn: document.getElementById('datasetLabelBtn'),
    variableLabelBtn: document.getElementById('variableLabelBtn')
};

// 初始化翻译与确认页面（页面切换时调用）
function initializeTranslationConfirmationPage() {
    console.log('初始化翻译与确认页面...');
    
    // 检查是否有翻译库配置
    const translationConfig = sessionStorage.getItem('translationLibraryConfig');
    if (!translationConfig) {
        showAlert('请先配置翻译库设置', 'warning');
        return;
    }
    
    // 执行合并配置表的逻辑
    executeMergeProcess();
}

// 执行合并配置表的逻辑
async function executeMergeProcess() {
    try {
        console.log('开始执行合并配置表逻辑...');
        
        // 获取翻译库配置
        const translationConfigStr = sessionStorage.getItem('translationLibraryConfig');
        if (!translationConfigStr) {
            throw new Error('未找到翻译库配置');
        }
        
        const translationConfig = JSON.parse(translationConfigStr);
        
        // 获取合并配置 - 优先从sessionStorage获取，如果没有则从数据库加载
        let mergeConfig = null;
        const mergeConfigStr = sessionStorage.getItem('mergeConfig');
        
        if (mergeConfigStr) {
            mergeConfig = JSON.parse(mergeConfigStr);
        } else {
            // 从数据库加载合并配置
            console.log('从sessionStorage未找到合并配置，尝试从数据库加载...');
            const currentPath = translationConfig.path || document.getElementById('datasetPath').value;
            if (currentPath) {
                try {
                    const response = await fetch(`/api/load_merge_config?path=${encodeURIComponent(currentPath)}`);
                    if (response.ok) {
                        const result = await response.json();
                        if (result.success && result.config && result.config.configs) {
                            mergeConfig = result.config.configs;
                            console.log('从数据库成功加载合并配置');
                        }
                    }
                } catch (error) {
                    console.error('从数据库加载合并配置失败:', error);
                }
            }
        }
        
        if (!mergeConfig) {
            throw new Error('未找到合并配置，请先完成合并配置设置');
        }
        
        // 执行合并过程
        showLoadingOverlay('正在执行合并配置表...');
        
        const response = await fetch('/execute_merge', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                merge_config: mergeConfig,
                translation_config: translationConfig
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            console.log('合并配置表执行成功');
            // 生成翻译表单
            generateTranslationForms(result.data);
            showAlert('合并配置表执行成功，翻译表单已生成', 'success');
        } else {
            throw new Error(result.message || '合并配置表执行失败');
        }
        
    } catch (error) {
        console.error('执行合并配置表失败:', error);
        showAlert('执行合并配置表失败: ' + error.message, 'error');
    } finally {
         hideLoadingOverlay();
     }
 }
 
 // 生成翻译表单
 function generateTranslationForms(mergeData) {
     console.log('开始生成翻译表单...', mergeData);
     
     try {
         // 获取翻译库配置
         const translationConfigStr = sessionStorage.getItem('translationLibraryConfig');
         const translationConfig = JSON.parse(translationConfigStr);
         
         // 根据翻译方向和规则生成四个子页面表单
         const translationDirection = translationConfig.translation_direction;
         
         // 生成编码列表表单
         generateCodedListForm(mergeData, translationDirection);
         
         // 生成未编码列表表单
         generateUncodedListForm(mergeData, translationDirection);
         
         // 生成数据集标签表单
         generateDatasetLabelForm(mergeData, translationDirection);
         
         // 生成变量标签表单
         generateVariableLabelForm(mergeData, translationDirection);
         
         console.log('翻译表单生成完成');
         
     } catch (error) {
         console.error('生成翻译表单失败:', error);
         showAlert('生成翻译表单失败: ' + error.message, 'error');
     }
 }

// 生成编码列表表单
function generateCodedListForm(mergeData, translationDirection) {
    console.log('生成编码列表表单', mergeData, translationDirection);
    // 这里实现编码列表表单的生成逻辑
    // 可以根据mergeData和translationDirection生成相应的表单
}

// 生成未编码列表表单
function generateUncodedListForm(mergeData, translationDirection) {
    console.log('生成未编码列表表单', mergeData, translationDirection);
    // 这里实现未编码列表表单的生成逻辑
}

// 生成数据集标签表单
function generateDatasetLabelForm(mergeData, translationDirection) {
    console.log('生成数据集标签表单', mergeData, translationDirection);
    // 这里实现数据集标签表单的生成逻辑
}

// 生成变量标签表单
function generateVariableLabelForm(mergeData, translationDirection) {
    console.log('生成变量标签表单', mergeData, translationDirection);
    // 这里实现变量标签表单的生成逻辑
}
 
 // 初始化翻译与确认页面（页面加载时调用）
 function initializeTranslationConfirmation() {
    // 绑定按钮事件
    if (translationConfirmationElements.codedListBtn) {
        translationConfirmationElements.codedListBtn.addEventListener('click', () => {
            generateCodedList();
        });
    }
    
    if (translationConfirmationElements.uncodedListBtn) {
        translationConfirmationElements.uncodedListBtn.addEventListener('click', () => {
            generateUncodedList();
        });
    }
    
    if (translationConfirmationElements.datasetLabelBtn) {
        translationConfirmationElements.datasetLabelBtn.addEventListener('click', () => {
            generateDatasetLabel();
        });
    }
    
    if (translationConfirmationElements.variableLabelBtn) {
        translationConfirmationElements.variableLabelBtn.addEventListener('click', () => {
            generateVariableLabel();
        });
    }
}

// 执行合并配置（在生成子页面之前）
async function executeMergeConfigBeforeGeneration() {
    // 如果已经执行过合并，直接返回成功
    if (isMergeExecuted) {
        console.log('合并已执行，跳过重复执行');
        return true;
    }
    
    // 检查是否为SDTM模式且有合并配置
    if (currentMode !== 'SDTM') {
        isMergeExecuted = true; // RAW模式标记为已执行
        return true; // RAW模式不需要合并
    }
    
    try {
        // 获取当前路径
        const currentPath = datasetPathInput?.value || sessionStorage.getItem('currentDataPath') || lastDataPath;
        if (!currentPath) {
            console.log('未找到当前路径，跳过合并步骤');
            isMergeExecuted = true;
            return true;
        }
        
        // 获取保存的合并配置
        const response = await fetch(`/api/load_merge_config?path=${encodeURIComponent(currentPath)}`);
        
        if (!response.ok) {
            console.log('未找到保存的合并配置，跳过合并步骤');
            isMergeExecuted = true;
            return true;
        }
        
        const configResult = await response.json();
        if (!configResult.success || !configResult.config) {
            console.log('合并配置为空，跳过合并步骤');
            isMergeExecuted = true;
            return true;
        }
        
        showAlert('正在执行变量合并...', 'info');
        
        // 执行合并
        const mergeResponse = await fetch('/merge_variables', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                config: configResult.config.configs,
                translation_direction: configResult.config.translation_direction
            })
        });
        
        const mergeResult = await mergeResponse.json();
        
        if (mergeResult.success) {
            showAlert('变量合并完成', 'success');
            isMergeExecuted = true; // 标记为已执行
            return true;
        } else {
            showAlert(`变量合并失败: ${mergeResult.message}`, 'error');
            return false;
        }
    } catch (error) {
        console.error('执行合并配置时出错:', error);
        showAlert('执行合并配置时出错: ' + error.message, 'error');
        return false;
    }
}

// 生成编码清单
async function generateCodedList() {
    try {
        showAlert('正在生成编码清单...', 'info');
        
        // 获取当前配置
        const config = getCurrentTranslationConfig();
        if (!config) {
            showAlert('请先完成翻译库版本控制配置', 'warning');
            return;
        }
        
        // 添加项目路径参数
        const requestData = {
            ...config,
            path: lastDataPath || sessionStorage.getItem('currentPath') || ''
        };
        
        // 调用后端API生成编码清单
        const response = await fetch('/api/generate_coded_list', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('编码清单生成成功', 'success');
            // 显示生成的清单数据
            displayCodedListResults(result.data);
        } else {
            showAlert(`编码清单生成失败: ${result.message}`, 'danger');
        }
    } catch (error) {
        console.error('生成编码清单时出错:', error);
        showAlert('生成编码清单时出错', 'danger');
    }
}

// 生成非编码清单
async function generateUncodedList() {
    try {
        showAlert('正在生成非编码清单...', 'info');
        
        // 获取当前配置
        const config = getCurrentTranslationConfig();
        if (!config) {
            showAlert('请先完成翻译库版本控制配置', 'warning');
            return;
        }
        
        // 添加项目路径参数
        const requestData = {
            ...config,
            path: lastDataPath || sessionStorage.getItem('currentPath') || ''
        };
        
        // 调用后端API生成非编码清单
        const response = await fetch('/api/generate_uncoded_list', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('非编码清单生成成功', 'success');
            // 显示生成的清单数据
            displayUncodedListResults(result.data);
        } else {
            showAlert(`非编码清单生成失败: ${result.message}`, 'danger');
        }
    } catch (error) {
        console.error('生成非编码清单时出错:', error);
        showAlert('生成非编码清单时出错', 'danger');
    }
}

// 生成数据集Label
async function generateDatasetLabel() {
    try {
        showAlert('正在生成数据集Label...', 'info');
        
        // 获取当前配置
        const config = getCurrentTranslationConfig();
        if (!config) {
            showAlert('请先完成翻译库版本控制配置', 'warning');
            return;
        }
        
        // 添加项目路径参数
        const requestData = {
            ...config,
            path: lastDataPath || sessionStorage.getItem('currentPath') || ''
        };
        
        // 调用后端API生成数据集Label
        const response = await fetch('/api/generate_dataset_label', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('数据集Label生成成功', 'success');
            // 显示生成的清单数据
            displayDatasetLabelResults(result.data);
        } else {
            showAlert(`数据集Label生成失败: ${result.message}`, 'danger');
        }
    } catch (error) {
        console.error('生成数据集Label时出错:', error);
        showAlert('生成数据集Label时出错', 'danger');
    }
}

// 生成变量Label
async function generateVariableLabel() {
    try {
        showAlert('正在生成变量Label...', 'info');
        
        // 获取当前配置
        const config = getCurrentTranslationConfig();
        if (!config) {
            showAlert('请先完成翻译库版本控制配置', 'warning');
            return;
        }
        
        // 添加项目路径参数
        const requestData = {
            ...config,
            path: lastDataPath || sessionStorage.getItem('currentPath') || ''
        };
        
        // 调用后端API生成变量Label
        const response = await fetch('/api/generate_variable_label', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(requestData)
        });
        
        const result = await response.json();
        
        if (result.success) {
            showAlert('变量Label生成成功', 'success');
            // 显示生成的清单数据
            displayVariableLabelResults(result.data);
        } else {
            showAlert(`变量Label生成失败: ${result.message}`, 'danger');
        }
    } catch (error) {
        console.error('生成变量Label时出错:', error);
        showAlert('生成变量Label时出错', 'danger');
    }
}

// 获取当前翻译配置
function getCurrentTranslationConfig() {
    const translationDirection = document.getElementById('translationDirection')?.value;
    const translationMode = document.getElementById('translationMode')?.value;
    const meddraVersion = document.getElementById('meddraVersion')?.value;
    const whodrugVersion = document.getElementById('whodrugVersion')?.value;
    const igVersion = document.getElementById('igVersion')?.value;
    
    if (!translationDirection || !translationMode) {
        return null;
    }
    
    return {
        translation_direction: translationDirection,
        mode: translationMode,
        meddra_version: meddraVersion,
        whodrug_version: whodrugVersion,
        ig_version: igVersion,
        path: sessionStorage.getItem('dataset_path') || ''
    };
}

// ==================== MedDRA和WHODrug配置表功能 ====================

// 加载可用变量
async function loadAvailableVariables() {
    if (!lastDataPath) {
        return;
    }
    
    try {
        // 获取当前数据集的所有变量
        const response = await fetch(`/api/get_dataset_variables?file_path=${encodeURIComponent(lastDataPath)}`);
        if (response.ok) {
            const data = await response.json();
            availableVariables = data.variables || [];
        }
    } catch (error) {
        console.error('加载可用变量失败:', error);
    }
}

// 为翻译库页面重新加载数据集信息
async function loadDatasetsForTranslationLibrary() {
    try {
        // 从sessionStorage获取当前项目路径
        const currentPath = sessionStorage.getItem('currentDataPath');
        if (!currentPath) {
            console.log('未找到当前项目路径，跳过数据集重新加载');
            return;
        }
        
        const response = await fetch('/read_datasets', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                path: currentPath,
                mode: 'RAW',
                translation_direction: 'zh_to_en'
            })
        });
        const result = await response.json();
        
        if (result.success && result.datasets) {
            currentDatasets = result.datasets;
            console.log('翻译库页面数据集重新加载成功:', currentDatasets);
        } else {
            console.error('翻译库页面数据集重新加载失败:', result.message);
        }
    } catch (error) {
        console.error('翻译库页面数据集重新加载失败:', error);
    }
}

// 添加配置行
function addConfigRow(type) {
    const isMediaDRA = type === 'meddra';
    const configBody = isMediaDRA ? medDRAConfigBody : whoDrugConfigBody;
    const emptyRow = isMediaDRA ? emptyMedDRAConfigRow : emptyWhoDrugConfigRow;
    const configs = isMediaDRA ? medDRAConfigs : whoDrugConfigs;
    
    // 检查是否有可用的数据集
    if (!currentDatasets || Object.keys(currentDatasets).length === 0) {
        showAlert('请先在基础设置页面读取数据集，然后再添加配置行', 'warning');
        return;
    }
    
    // 隐藏空状态行
    if (emptyRow) {
        emptyRow.style.display = 'none';
    }
    
    // 移除继续添加按钮（如果存在）
    const buttonId = `continue-add-${type}-config-btn`;
    const existingButton = document.getElementById(buttonId);
    if (existingButton) {
        existingButton.remove();
    }
    
    const newIndex = configs.length + 1;
    const configId = `${type}_${Date.now()}`;
    
    const newRow = document.createElement('tr');
    newRow.className = 'config-row';
    newRow.setAttribute('data-config-id', configId);
    newRow.setAttribute('data-config-type', type);
    
    // 生成数据集选项
    const datasetOptions = Object.keys(currentDatasets).map(datasetName => 
        `<option value="${datasetName}">${datasetName} (${currentDatasets[datasetName].rows} 行)</option>`
    ).join('');
    
    newRow.innerHTML = `
        <td class="text-center">${newIndex}</td>
        <td>
            <select class="form-select form-select-sm" data-field="table_path">
                <option value="">选择数据表</option>
                ${datasetOptions}
            </select>
        </td>
        <td>
            <select class="form-select form-select-sm" data-field="name_column">
                <option value="">选择Name列变量</option>
            </select>
        </td>
        <td>
            <select class="form-select form-select-sm" data-field="code_column">
                <option value="">选择Code列变量(可选)</option>
            </select>
        </td>
        <td>
            <div class="btn-group btn-group-sm">
                <button class="btn btn-success save-config-btn" disabled>
                    <i class="fas fa-save"></i>
                </button>
                <button class="btn btn-danger delete-config-btn">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </td>
    `;
    
    configBody.appendChild(newRow);
    
    // 绑定事件
    bindConfigRowEvents(newRow, configId, type);
    
    // 创建对应的配置对象
    const newConfig = {
        id: configId,
        table_path: '',
        name_column: '',
        code_column: ''
    };
    
    configs.push(newConfig);
}

// 绑定配置行事件
function bindConfigRowEvents(row, configId, type) {
    const tablePathSelect = row.querySelector('[data-field="table_path"]');
    const nameColumnSelect = row.querySelector('[data-field="name_column"]');
    const codeColumnSelect = row.querySelector('[data-field="code_column"]');
    const saveBtn = row.querySelector('.save-config-btn');
    const deleteBtn = row.querySelector('.delete-config-btn');
    
    // 数据表选择变化时，加载对应的变量
    if (tablePathSelect) {
        tablePathSelect.addEventListener('change', async () => {
            const selectedDataset = tablePathSelect.value;
            if (selectedDataset && currentDatasets && currentDatasets[selectedDataset]) {
                // 获取数据集的变量列表
                try {
                    const response = await fetch(`/api/get_dataset_variables?dataset_name=${encodeURIComponent(selectedDataset)}`);
                    const result = await response.json();
                    
                    if (result.success && result.variables) {
                        // 更新name列和code列的选项
                        nameColumnSelect.innerHTML = '<option value="">选择Name列变量</option>' + 
                            result.variables.map(v => `<option value="${v}">${v}</option>`).join('');
                        codeColumnSelect.innerHTML = '<option value="">选择Code列变量(可选)</option>' + 
                            result.variables.map(v => `<option value="${v}">${v}</option>`).join('');
                    } else {
                        // 使用当前数据集的列信息
                        const datasetInfo = currentDatasets[selectedDataset];
                        if (datasetInfo && datasetInfo.selectable_columns) {
                            const variables = datasetInfo.selectable_columns;
                            nameColumnSelect.innerHTML = '<option value="">选择Name列变量</option>' + 
                                variables.map(v => `<option value="${v}">${v}</option>`).join('');
                            codeColumnSelect.innerHTML = '<option value="">选择Code列变量(可选)</option>' + 
                                variables.map(v => `<option value="${v}">${v}</option>`).join('');
                        }
                    }
                } catch (error) {
                    console.error('获取变量列表失败:', error);
                    // 使用当前数据集的列信息作为备选
                    const datasetInfo = currentDatasets[selectedDataset];
                    if (datasetInfo && datasetInfo.selectable_columns) {
                        const variables = datasetInfo.selectable_columns;
                        nameColumnSelect.innerHTML = '<option value="">选择Name列变量</option>' + 
                            variables.map(v => `<option value="${v}">${v}</option>`).join('');
                        codeColumnSelect.innerHTML = '<option value="">选择Code列变量(可选)</option>' + 
                            variables.map(v => `<option value="${v}">${v}</option>`).join('');
                    }
                }
            } else {
                // 清空变量选项
                nameColumnSelect.innerHTML = '<option value="">选择Name列变量</option>';
                codeColumnSelect.innerHTML = '<option value="">选择Code列变量(可选)</option>';
            }
            updateConfigRowState(row);
        });
    }
    
    // 绑定变量选择变化事件
    if (nameColumnSelect) {
        nameColumnSelect.addEventListener('change', () => updateConfigRowState(row));
    }
    if (codeColumnSelect) {
        codeColumnSelect.addEventListener('change', () => updateConfigRowState(row));
    }
    
    // 绑定保存按钮事件
    if (saveBtn) {
        saveBtn.addEventListener('click', () => saveConfigRow(configId, type));
    }
    
    // 绑定删除按钮事件
    if (deleteBtn) {
        deleteBtn.addEventListener('click', () => deleteConfigRow(configId, type));
    }
}

// 更新配置行状态
function updateConfigRowState(row) {
    const configId = row.getAttribute('data-config-id');
    const configType = row.getAttribute('data-config-type');
    const configs = configType === 'meddra' ? medDRAConfigs : whoDrugConfigs;
    
    const config = configs.find(c => c.id === configId);
    if (!config) return;
    
    // 更新配置对象
    const tablePathInput = row.querySelector('[data-field="table_path"]');
    const nameColumnSelect = row.querySelector('[data-field="name_column"]');
    const codeColumnSelect = row.querySelector('[data-field="code_column"]');
    
    config.table_path = tablePathInput.value.trim();
    config.name_column = nameColumnSelect.value;
    config.code_column = codeColumnSelect.value;
    
    // 更新保存按钮状态
    const saveBtn = row.querySelector('.save-config-btn');
    const isValid = config.table_path && config.name_column;
    saveBtn.disabled = !isValid;
    
    if (isValid) {
        saveBtn.classList.remove('btn-outline-success');
        saveBtn.classList.add('btn-success');
    } else {
        saveBtn.classList.remove('btn-success');
        saveBtn.classList.add('btn-outline-success');
    }
}

// 保存配置行
function saveConfigRow(configId, type) {
    const row = document.querySelector(`[data-config-id="${configId}"]`);
    if (!row) return;
    
    const configs = type === 'meddra' ? medDRAConfigs : whoDrugConfigs;
    const config = configs.find(c => c.id === configId);
    
    if (!config || !config.table_path || !config.name_column) {
        showAlert('请填写完整的配置信息', 'warning');
        return;
    }
    
    // 将行设置为已保存状态
    row.classList.add('saved-config');
    const inputs = row.querySelectorAll('input, select');
    inputs.forEach(input => {
        input.disabled = true;
    });
    
    // 更新按钮状态
    const saveBtn = row.querySelector('.save-config-btn');
    const deleteBtn = row.querySelector('.delete-config-btn');
    
    saveBtn.innerHTML = '<i class="fas fa-edit"></i>';
    saveBtn.onclick = () => editConfigRow(configId, type);
    saveBtn.classList.remove('btn-success');
    saveBtn.classList.add('btn-warning');
    saveBtn.disabled = false;
    
    // 添加继续添加按钮
    addContinueAddConfigButton(type);
    
    showAlert(`${type.toUpperCase()}配置保存成功`, 'success');
}

// 编辑配置行
function editConfigRow(configId, type) {
    const row = document.querySelector(`[data-config-id="${configId}"]`);
    if (!row) return;
    
    // 启用输入控件
    const inputs = row.querySelectorAll('input, select');
    inputs.forEach(input => {
        input.disabled = false;
    });
    
    // 更新按钮状态
    const saveBtn = row.querySelector('.save-config-btn');
    saveBtn.innerHTML = '<i class="fas fa-save"></i>';
    saveBtn.onclick = () => saveConfigRow(configId, type);
    saveBtn.classList.remove('btn-warning');
    saveBtn.classList.add('btn-success');
    
    row.classList.remove('saved-config');
    
    // 移除继续添加按钮（编辑时）
    const buttonId = `continue-add-${type}-config-btn`;
    const existingButton = document.getElementById(buttonId);
    if (existingButton) {
        existingButton.remove();
    }
    
    // 重新绑定事件
    bindConfigRowEvents(row, configId, type);
    updateConfigRowState(row);
}

// 删除配置行
function deleteConfigRow(configId, type) {
    if (!confirm('确定要删除这个配置吗？')) {
        return;
    }
    
    const row = document.querySelector(`[data-config-id="${configId}"]`);
    const configs = type === 'meddra' ? medDRAConfigs : whoDrugConfigs;
    const configBody = type === 'meddra' ? medDRAConfigBody : whoDrugConfigBody;
    const emptyRow = type === 'meddra' ? emptyMedDRAConfigRow : emptyWhoDrugConfigRow;
    
    if (row) {
        // 从数组中删除配置
        const configIndex = configs.findIndex(c => c.id === configId);
        if (configIndex > -1) {
            configs.splice(configIndex, 1);
        }
        
        // 删除行
        row.remove();
        
        // 重新编号
        updateConfigRowNumbers(type);
        
        // 检查是否需要显示空状态
        if (configs.length === 0) {
            if (emptyRow) {
                emptyRow.style.display = '';
            }
            // 移除继续添加按钮
            const buttonId = `continue-add-${type}-config-btn`;
            const existingButton = document.getElementById(buttonId);
            if (existingButton) {
                existingButton.remove();
            }
        }
        
        showAlert(`${type.toUpperCase()}配置已删除`, 'info');
    }
}

// 更新配置行编号
function updateConfigRowNumbers(type) {
    const configBody = type === 'meddra' ? medDRAConfigBody : whoDrugConfigBody;
    const rows = configBody.querySelectorAll('.config-row');
    
    rows.forEach((row, index) => {
        const numberCell = row.querySelector('td:first-child');
        if (numberCell) {
            numberCell.textContent = index + 1;
        }
    });
}

// 获取所有配置数据
function getAllConfigData() {
    return {
        meddra_configs: medDRAConfigs.filter(c => c.table_path && c.name_column),
        whodrug_configs: whoDrugConfigs.filter(c => c.table_path && c.name_column)
    };
}

// 加载配置表数据
function loadConfigTable(type, configs) {
    const configBody = type === 'meddra' ? medDRAConfigBody : whoDrugConfigBody;
    const emptyRow = type === 'meddra' ? emptyMedDRAConfigRow : emptyWhoDrugConfigRow;
    const configArray = type === 'meddra' ? medDRAConfigs : whoDrugConfigs;
    
    // 清空现有配置
    configArray.length = 0;
    if (configBody) {
        // 清空表格内容（保留空状态行）
        const rows = configBody.querySelectorAll('.config-row');
        rows.forEach(row => row.remove());
    }
    
    if (!configs || configs.length === 0) {
        // 显示空状态行
        if (emptyRow) {
            emptyRow.style.display = '';
        }
        return;
    }
    
    // 隐藏空状态行
    if (emptyRow) {
        emptyRow.style.display = 'none';
    }
    
    // 加载配置数据
    configs.forEach((config, index) => {
        const configId = `${type}_loaded_${Date.now()}_${index}`;
        const newConfig = {
            id: configId,
            table_path: config.table_path || '',
            name_column: config.name_column || '',
            code_column: config.code_column || ''
        };
        
        configArray.push(newConfig);
        
        // 生成数据集选项
        const datasetOptions = Object.keys(currentDatasets || {}).map(datasetName => {
            const selected = datasetName === newConfig.table_path ? 'selected' : '';
            return `<option value="${datasetName}" ${selected}>${datasetName} (${currentDatasets[datasetName].rows} 行)</option>`;
        }).join('');
        
        // 创建表格行
        const newRow = document.createElement('tr');
        newRow.className = 'config-row saved-config';
        newRow.setAttribute('data-config-id', configId);
        newRow.setAttribute('data-config-type', type);
        
        // 获取选中数据集的变量列表
        let variableOptions = '';
        if (newConfig.table_path && currentDatasets && currentDatasets[newConfig.table_path]) {
            const datasetInfo = currentDatasets[newConfig.table_path];
            const variables = datasetInfo.selectable_columns || [];
            variableOptions = variables.map(v => `<option value="${v}" ${v === newConfig.name_column ? 'selected' : ''}>${v}</option>`).join('');
        }
        
        let codeVariableOptions = '';
        if (newConfig.table_path && currentDatasets && currentDatasets[newConfig.table_path]) {
            const datasetInfo = currentDatasets[newConfig.table_path];
            const variables = datasetInfo.selectable_columns || [];
            codeVariableOptions = variables.map(v => `<option value="${v}" ${v === newConfig.code_column ? 'selected' : ''}>${v}</option>`).join('');
        }
        
        newRow.innerHTML = `
            <td class="text-center">${index + 1}</td>
            <td>
                <select class="form-select form-select-sm" data-field="table_path" disabled>
                    <option value="">选择数据表</option>
                    ${datasetOptions}
                </select>
            </td>
            <td>
                <select class="form-select form-select-sm" data-field="name_column" disabled>
                    <option value="">选择Name列变量</option>
                    ${variableOptions}
                </select>
            </td>
            <td>
                <select class="form-select form-select-sm" data-field="code_column" disabled>
                    <option value="">选择Code列变量(可选)</option>
                    ${codeVariableOptions}
                </select>
            </td>
            <td>
                <div class="btn-group btn-group-sm">
                    <button class="btn btn-warning save-config-btn">
                        <i class="fas fa-edit"></i>
                    </button>
                    <button class="btn btn-danger delete-config-btn">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </td>
        `;
        
        configBody.appendChild(newRow);
        
        // 绑定事件
        bindConfigRowEvents(newRow, configId, type);
    });
    
    // 添加继续添加按钮（类似合并配置表的处理）
    addContinueAddConfigButton(type);
}

// 添加继续添加配置按钮
function addContinueAddConfigButton(type) {
    const configBody = type === 'meddra' ? medDRAConfigBody : whoDrugConfigBody;
    const buttonId = `continue-add-${type}-config-btn`;
    
    // 检查是否已存在继续添加按钮
    let existingButton = document.getElementById(buttonId);
    if (existingButton) {
        existingButton.remove();
    }
    
    // 创建继续添加按钮行
    const buttonRow = document.createElement('tr');
    buttonRow.id = buttonId;
    buttonRow.innerHTML = `
        <td colspan="5" class="text-center py-3">
            <button class="btn btn-outline-primary" onclick="addConfigRow('${type}')">
                <i class="fas fa-plus me-2"></i>继续添加${type.toUpperCase()}配置
            </button>
        </td>
    `;
    
    configBody.appendChild(buttonRow);
}

// 使函数在全局可用
window.saveConfigRow = saveConfigRow;
window.editConfigRow = editConfigRow;
window.deleteConfigRow = deleteConfigRow;

// 在DOM加载完成后初始化翻译库功能
document.addEventListener('DOMContentLoaded', function() {
    // 延迟初始化，确保所有元素都已加载
    setTimeout(() => {
        initializeTranslationLibrary();
        initializeTranslationConfirmation();
    }, 100);
});

// 标签页切换事件监听
if (translationLibraryTab) {
    translationLibraryTab.addEventListener('shown.bs.tab', function() {
        console.log('翻译库版本控制标签页被激活');
        // 当切换到翻译库版本控制标签页时，重新加载配置和版本选项
        loadVersionOptions();
        loadTranslationLibraryConfig();
        loadAvailableVariables();
        // 确保版本选择器显示
        showAllVersionSelects();
    });
}

// 显示编码清单结果
function displayCodedListResults(data) {
    console.log('显示编码清单结果:', data);
    
    const container = document.getElementById('translationContentArea');
    if (!container) {
        console.error('未找到翻译结果容器');
        return;
    }
    
    const codedItems = data.coded_items || [];
    
    let html = `
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0">编码清单 (${codedItems.length} 项)</h5>
                <small class="text-muted">MedDRA版本: ${data.meddra_version || 'N/A'} | WHODrug版本: ${data.whodrug_version || 'N/A'}</small>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>数据集</th>
                                <th>变量</th>
                                <th>原始值</th>
                                <th>翻译值</th>
                                <th>翻译来源</th>
                                <th>需要确认</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
    `;
    
    codedItems.forEach((item, index) => {
        const needsConfirmation = item.needs_confirmation === 'Y';
        const isAITranslation = item.translation_source === 'AI';
        
        html += `
            <tr class="${needsConfirmation ? 'table-warning' : ''}">
                <td>${item.dataset}</td>
                <td><code>${item.variable}</code></td>
                <td>${item.value}</td>
                <td>
                    <input type="text" class="form-control form-control-sm" 
                           value="${item.translated_value || ''}" 
                           data-index="${index}" 
                           ${isAITranslation ? 'style="border-color: #28a745;"' : ''}>
                </td>
                <td>
                    <span class="badge ${isAITranslation ? 'bg-success' : 'bg-primary'}">
                        ${item.translation_source}
                    </span>
                </td>
                <td>
                    <span class="badge ${needsConfirmation ? 'bg-warning' : 'bg-success'}">
                        ${needsConfirmation ? '需要确认' : '已确认'}
                    </span>
                </td>
                <td>
                    <button class="btn btn-sm btn-outline-primary" onclick="confirmTranslation(${index}, 'coded')">
                        确认
                    </button>
                </td>
            </tr>
        `;
    });
    
    html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// 显示非编码清单结果
function displayUncodedListResults(data) {
    console.log('显示非编码清单结果:', data);
    
    const container = document.getElementById('translationContentArea');
    if (!container) {
        console.error('未找到翻译结果容器');
        return;
    }
    
    const uncodedItems = data.uncoded_items || [];
    
    let html = `
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0">非编码清单 (${uncodedItems.length} 项)</h5>
                <small class="text-muted">使用metadata和AI翻译</small>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>数据集</th>
                                <th>变量</th>
                                <th>原始值</th>
                                <th>翻译值</th>
                                <th>翻译来源</th>
                                <th>需要确认</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
    `;
    
    uncodedItems.forEach((item, index) => {
        const needsConfirmation = item.needs_confirmation === 'Y';
        const isAITranslation = item.translation_source === 'AI';
        
        html += `
            <tr class="${needsConfirmation ? 'table-warning' : ''}">
                <td>${item.dataset}</td>
                <td><code>${item.variable}</code></td>
                <td>${item.value}</td>
                <td>
                    <input type="text" class="form-control form-control-sm" 
                           value="${item.translated_value || ''}" 
                           data-index="${index}" 
                           ${isAITranslation ? 'style="border-color: #28a745;"' : ''}>
                </td>
                <td>
                    <span class="badge ${isAITranslation ? 'bg-success' : 'bg-info'}">
                        ${item.translation_source}
                    </span>
                </td>
                <td>
                    <span class="badge ${needsConfirmation ? 'bg-warning' : 'bg-success'}">
                        ${needsConfirmation ? '需要确认' : '已确认'}
                    </span>
                </td>
                <td>
                    <button class="btn btn-sm btn-outline-primary" onclick="confirmTranslation(${index}, 'uncoded')">
                        确认
                    </button>
                </td>
            </tr>
        `;
    });
    
    html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// 显示数据集标签结果
function displayDatasetLabelResults(data) {
    console.log('显示数据集标签结果:', data);
    
    const container = document.getElementById('translationContentArea');
    if (!container) {
        console.error('未找到翻译结果容器');
        return;
    }
    
    const datasetLabels = data.dataset_labels || [];
    
    let html = `
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0">数据集标签清单 (${datasetLabels.length} 项)</h5>
                <small class="text-muted">IG版本: ${data.ig_version || 'N/A'}</small>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>数据集</th>
                                <th>原始标签</th>
                                <th>翻译标签</th>
                                <th>翻译来源</th>
                                <th>需要确认</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
    `;
    
    datasetLabels.forEach((item, index) => {
        const needsConfirmation = item.needs_confirmation === 'Y';
        const isAITranslation = item.translation_source === 'AI';
        
        html += `
            <tr class="${needsConfirmation ? 'table-warning' : ''}">
                <td><strong>${item.dataset}</strong></td>
                <td>${item.original_label || ''}</td>
                <td>
                    <input type="text" class="form-control form-control-sm" 
                           value="${item.translated_label || ''}" 
                           data-index="${index}" 
                           ${isAITranslation ? 'style="border-color: #28a745;"' : ''}>
                </td>
                <td>
                    <span class="badge ${isAITranslation ? 'bg-success' : 'bg-primary'}">
                        ${item.translation_source}
                    </span>
                </td>
                <td>
                    <span class="badge ${needsConfirmation ? 'bg-warning' : 'bg-success'}">
                        ${needsConfirmation ? '需要确认' : '已确认'}
                    </span>
                </td>
                <td>
                    <button class="btn btn-sm btn-outline-primary" onclick="confirmTranslation(${index}, 'dataset')">
                        确认
                    </button>
                </td>
            </tr>
        `;
    });
    
    html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// 显示变量标签结果
function displayVariableLabelResults(data) {
    console.log('显示变量标签结果:', data);
    
    const container = document.getElementById('translationContentArea');
    if (!container) {
        console.error('未找到翻译结果容器');
        return;
    }
    
    const variableLabels = data.variable_labels || [];
    
    let html = `
        <div class="card">
            <div class="card-header">
                <h5 class="mb-0">变量标签清单 (${variableLabels.length} 项)</h5>
                <small class="text-muted">IG版本: ${data.ig_version || 'N/A'}</small>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-striped table-hover">
                        <thead class="table-dark">
                            <tr>
                                <th>数据集</th>
                                <th>变量</th>
                                <th>原始标签</th>
                                <th>翻译标签</th>
                                <th>翻译来源</th>
                                <th>需要确认</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
    `;
    
    variableLabels.forEach((item, index) => {
        const needsConfirmation = item.needs_confirmation === 'Y';
        const isAITranslation = item.translation_source === 'AI';
        
        html += `
            <tr class="${needsConfirmation ? 'table-warning' : ''}">
                <td>${item.dataset}</td>
                <td><code>${item.variable}</code></td>
                <td>${item.original_label || ''}</td>
                <td>
                    <input type="text" class="form-control form-control-sm" 
                           value="${item.translated_label || ''}" 
                           data-index="${index}" 
                           ${isAITranslation ? 'style="border-color: #28a745;"' : ''}>
                </td>
                <td>
                    <span class="badge ${isAITranslation ? 'bg-success' : 'bg-primary'}">
                        ${item.translation_source}
                    </span>
                </td>
                <td>
                    <span class="badge ${needsConfirmation ? 'bg-warning' : 'bg-success'}">
                        ${needsConfirmation ? '需要确认' : '已确认'}
                    </span>
                </td>
                <td>
                    <button class="btn btn-sm btn-outline-primary" onclick="confirmTranslation(${index}, 'variable')">
                        确认
                    </button>
                </td>
            </tr>
        `;
    });
    
    html += `
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// 确认翻译函数
function confirmTranslation(index, type) {
    console.log(`确认翻译: 索引${index}, 类型${type}`);
    // TODO: 实现翻译确认逻辑
    showAlert('翻译已确认', 'success');
}
// 翻译值编辑功能
function setupTranslationEditingListeners(container, listType) {
    console.log(`Setting up translation editing listeners for ${listType} list`);
    
    const editableInputs = container.querySelectorAll('.editable-translation');
    
    editableInputs.forEach(input => {
        // 输入框失去焦点时保存更改
        input.addEventListener('blur', function() {
            const itemIndex = parseInt(this.dataset.itemIndex);
            const newValue = this.value.trim();
            const originalValue = this.dataset.originalValue;
            
            if (newValue !== originalValue) {
                updateTranslationValue(listType, itemIndex, newValue, originalValue);
            }
        });
        
        // 按Enter键时保存更改
        input.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                this.blur(); // 触发blur事件
            }
        });
    });
}

function updateTranslationValue(listType, itemIndex, newValue, originalValue) {
    console.log(`Updating translation for ${listType} list, item ${itemIndex}: "${originalValue}" -> "${newValue}"`);
    
    // 获取对应的数据源
    let dataSource;
    switch (listType) {
        case 'coded':
            dataSource = window.translationPreviewListsData?.codedList?.coded_items;
            break;
        case 'uncoded':
            dataSource = window.translationPreviewListsData?.uncodedList?.uncoded_items;
            break;
        case 'dataset':
            dataSource = window.translationPreviewListsData?.datasetLabel?.data;
            break;
        case 'variable':
            dataSource = window.translationPreviewListsData?.variableLabel?.data;
            break;
        default:
            console.error('Unknown list type:', listType);
            return;
    }
    
    if (!dataSource || !dataSource[itemIndex]) {
        console.error('Data source not found for item:', listType, itemIndex);
        return;
    }
    
    const item = dataSource[itemIndex];
    
    // 更新数据
    if (listType === 'dataset' || listType === 'variable') {
        item.translated_label = newValue;
    } else {
        item.translated_value = newValue;
    }
    
    // 如果用户输入了非空值，标记为用户来源
    if (newValue && newValue.trim() !== '') {
        item.translation_source = 'user';
        
        // 更新对应的badge显示
        const badge = document.querySelector(`span[data-item-index="${itemIndex}"]`);
        if (badge) {
            badge.className = 'badge bg-success';
            badge.textContent = 'user';
        }
    } else {
        // 如果清空了值，恢复原来的来源
        // 这里可以根据需要决定是否恢复原来的translation_source
        // 暂时保持为user，表示用户主动清空了翻译
        item.translation_source = 'user';
    }
    
    console.log('Translation updated successfully:', item);
    
    // 可以在这里添加保存到服务器的逻辑
    // saveTranslationToServer(listType, itemIndex, item);
}

// 保存翻译到服务器（可选功能）
function saveTranslationToServer(listType, itemIndex, item) {
    // 这里可以实现将用户编辑的翻译保存到服务器的逻辑
    console.log('Saving translation to server:', listType, itemIndex, item);
    
    // 示例API调用
    /*
    fetch('/api/save_user_translation', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            list_type: listType,
            item_index: itemIndex,
            item: item
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            console.log('Translation saved successfully');
        } else {
            console.error('Failed to save translation:', data.message);
        }
    })
    .catch(error => {
        console.error('Error saving translation:', error);
    });
    */
}
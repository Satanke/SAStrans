// 可调整列宽的表格功能
class ResizableTable {
    constructor(tableId) {
        this.table = document.getElementById(tableId);
        this.isResizing = false;
        this.currentColumn = null;
        this.startX = 0;
        this.startWidth = 0;
        
        this.init();
    }
    
    init() {
        if (!this.table) return;
        
        // 为每个可调整的列头添加事件监听
        const headers = this.table.querySelectorAll('th .resize-handle');
        headers.forEach(handle => {
            handle.addEventListener('mousedown', this.handleMouseDown.bind(this));
        });
        
        // 添加全局事件监听
        document.addEventListener('mousemove', this.handleMouseMove.bind(this));
        document.addEventListener('mouseup', this.handleMouseUp.bind(this));
    }
    
    handleMouseDown(e) {
        e.preventDefault();
        
        this.isResizing = true;
        this.currentColumn = e.target.closest('th');
        this.startX = e.clientX;
        this.startWidth = parseInt(window.getComputedStyle(this.currentColumn).width, 10);
        
        // 添加调整中的样式
        document.body.classList.add('resizing');
        this.currentColumn.classList.add('resizing');
    }
    
    handleMouseMove(e) {
        if (!this.isResizing || !this.currentColumn) return;
        
        e.preventDefault();
        
        const diff = e.clientX - this.startX;
        const newWidth = Math.max(50, this.startWidth + diff); // 最小宽度50px
        
        this.currentColumn.style.width = newWidth + 'px';
        
        // 同时调整对应的tbody中的列
        const columnIndex = Array.from(this.currentColumn.parentNode.children).indexOf(this.currentColumn);
        const rows = this.table.querySelectorAll('tbody tr');
        rows.forEach(row => {
            const cell = row.children[columnIndex];
            if (cell) {
                cell.style.width = newWidth + 'px';
            }
        });
    }
    
    handleMouseUp(e) {
        if (!this.isResizing) return;
        
        this.isResizing = false;
        
        // 移除调整中的样式
        document.body.classList.remove('resizing');
        if (this.currentColumn) {
            this.currentColumn.classList.remove('resizing');
        }
        
        this.currentColumn = null;
    }
    
    // 重置所有列宽
    resetColumnWidths() {
        const headers = this.table.querySelectorAll('th');
        const defaultWidths = {
            0: '50px',    // 复选框列
            1: '120px',   // 翻译类型
            2: '100px',   // 数据集
            3: '120px',   // 变量名
            4: '250px',   // 原始值
            5: '250px',   // 翻译值
            6: '100px',   // 翻译来源
            7: '200px',   // 备注列
            8: '220px'    // 操作列
        };
        
        headers.forEach((header, index) => {
            if (defaultWidths[index]) {
                header.style.width = defaultWidths[index];
            }
        });
        
        // 同时重置tbody中的列
        const rows = this.table.querySelectorAll('tbody tr');
        rows.forEach(row => {
            Array.from(row.children).forEach((cell, index) => {
                if (defaultWidths[index]) {
                    cell.style.width = defaultWidths[index];
                }
            });
        });
    }
}

// 初始化可调整表格
let resizableTable;

// 在DOM加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    // 延迟初始化，确保表格已经渲染
    setTimeout(() => {
        resizableTable = new ResizableTable('translationTable');
    }, 100);
});

// 提供全局函数供外部调用
function resetTableColumnWidths() {
    if (resizableTable) {
        resizableTable.resetColumnWidths();
    }
}

// 在表格重新渲染后重新初始化
function reinitializeResizableTable() {
    if (resizableTable) {
        resizableTable = new ResizableTable('translationTable');
    }
}
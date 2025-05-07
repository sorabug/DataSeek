import sys
import os
import re
import pandas as pd
import datetime
import csv
import warnings
import numpy as np
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
                           QVBoxLayout, QHBoxLayout, QWidget, QPushButton, QFileDialog,
                           QLineEdit, QLabel, QComboBox, QCheckBox, QMessageBox, QTabWidget,
                           QTextEdit, QGroupBox, QRadioButton, QListWidget, QListWidgetItem,
                           QSplitter, QMenu, QAction, QToolBar, QDialog, QHeaderView,
                           QProgressDialog, QProgressBar, QAbstractItemView, QScrollArea,
                           QTableView)
from PyQt5.QtCore import Qt, QRegExp, QSettings, QThread, pyqtSignal, pyqtSlot, QTimer, QMimeData, QAbstractTableModel
from PyQt5.QtGui import QColor, QBrush, QIcon, QFont, QDragEnterEvent, QDropEvent

# 过滤字体相关的OpenType支持缺失警告
warnings.filterwarnings("ignore", message="OpenType support missing for.*")
warnings.filterwarnings("ignore", message=".*script [0-9]+.*")

# 文件加载线程类
class FileLoaderThread(QThread):
    # 定义信号
    progress_signal = pyqtSignal(int)  # 进度信号
    chunk_loaded_signal = pyqtSignal(str, pd.DataFrame, bool)  # 块加载信号 (文件路径, 数据块, 是否是最后一块)
    finished_signal = pyqtSignal(str, pd.DataFrame)  # 完成信号，返回文件路径和DataFrame
    error_signal = pyqtSignal(str, str)  # 错误信号，返回文件路径和错误信息
    
    def __init__(self, file_path, chunk_size=50000, low_memory_mode=False):
        super().__init__()
        self.file_path = file_path
        self.is_cancelled = False
        self.chunk_size = chunk_size  # 每次加载的行数
        self.low_memory_mode = low_memory_mode  # 低内存模式标志
        
    def cancel(self):
        self.is_cancelled = True
        
    def run(self):
        try:
            # 发送开始加载信号
            self.progress_signal.emit(10)
            
            # 根据文件扩展名选择加载方法
            if self.file_path.endswith(('.xlsx', '.xls')):
                if self.low_memory_mode:
                    # Excel文件低内存模式加载
                    self.load_excel_in_chunks()
                else:
                    # 普通方式加载Excel
                    self.load_excel_regular()
            elif self.file_path.endswith('.csv'):
                if self.low_memory_mode:
                    # CSV文件低内存模式加载
                    self.load_csv_in_chunks()
                else:
                    # 普通方式加载CSV
                    self.load_csv_regular()
            else:
                self.error_signal.emit(self.file_path, '不支持的文件格式')
                return

        except Exception as e:
            # 发送错误信号
            if not self.is_cancelled:
                self.error_signal.emit(self.file_path, str(e))

    def load_excel_regular(self):
        """常规方式加载Excel文件"""
        # 过滤openpyxl的默认样式警告
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")
            df = pd.read_excel(self.file_path)
        if self.is_cancelled:
            return
        self.progress_signal.emit(80)
        
        # 处理数据
        self.post_process_dataframe(df)
        
        # 发送完成信号
        self.finished_signal.emit(self.file_path, df)
        self.progress_signal.emit(100)
    
    def load_excel_in_chunks(self):
        """分块加载Excel文件"""
        try:
            # 获取Excel表的行数
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")
                import openpyxl
                wb = openpyxl.load_workbook(self.file_path, read_only=True)
                sheet = wb.active
                total_rows = sheet.max_row
                
            # 分块读取
            chunks = []
            for i in range(0, total_rows, self.chunk_size):
                if self.is_cancelled:
                    return
                
                # 读取一块数据
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")
                    nrows = min(self.chunk_size, total_rows - i)
                    skiprows = list(range(1, i + 1)) if i > 0 else None
                    df_chunk = pd.read_excel(self.file_path, skiprows=skiprows, nrows=nrows)
                
                # 处理块数据
                self.post_process_dataframe(df_chunk)
                
                # 发送块加载信号
                is_last_chunk = (i + self.chunk_size >= total_rows)
                self.chunk_loaded_signal.emit(self.file_path, df_chunk, is_last_chunk)
                
                # 更新进度
                progress = 10 + int(70 * (i + nrows) / total_rows)
                self.progress_signal.emit(min(progress, 80))
            
            # 最后一块读取可能已经发送完成信号，这里不再重复发送
            self.progress_signal.emit(100)
            
        except Exception as e:
            self.error_signal.emit(self.file_path, f"分块加载Excel文件失败: {str(e)}")
    
    def load_csv_regular(self):
        """常规方式加载CSV文件"""
        # 小文件直接读取
        df = pd.read_csv(self.file_path)
        
        if self.is_cancelled:
            return
        self.progress_signal.emit(80)
        
        # 处理数据
        self.post_process_dataframe(df)
        
        # 发送完成信号
        self.finished_signal.emit(self.file_path, df)
        self.progress_signal.emit(100)
    
    def load_csv_in_chunks(self):
        """分块加载CSV文件"""
        try:
            # 获取总行数
            total_rows = 0
            with open(self.file_path, 'r', encoding='utf-8') as f:
                for _ in f:
                    total_rows += 1
            
            # 分块读取
            reader = pd.read_csv(self.file_path, chunksize=self.chunk_size)
            for i, chunk in enumerate(reader):
                if self.is_cancelled:
                    return
                
                # 处理块数据
                self.post_process_dataframe(chunk)
                
                # 发送块加载信号
                is_last_chunk = ((i+1) * self.chunk_size >= total_rows)
                self.chunk_loaded_signal.emit(self.file_path, chunk, is_last_chunk)
                
                # 更新进度
                progress = 10 + int(70 * (i+1) * self.chunk_size / total_rows)
                self.progress_signal.emit(min(progress, 80))
            
            # 最后一块读取可能已经发送完成信号，这里不再重复发送
            self.progress_signal.emit(100)
            
        except Exception as e:
            self.error_signal.emit(self.file_path, f"分块加载CSV文件失败: {str(e)}")
    
    def post_process_dataframe(self, df):
        """处理DataFrame"""
        # 处理NaN值，将其替换为空字符串
        df.fillna('', inplace=True)
        
        # 确保所有列名都是字符串类型
        df.columns = [str(col) for col in df.columns]
        
        # 处理无名列（Unnamed列）
        # 检测列名中是否包含'Unnamed'开头的列
        unnamed_cols = [col for col in df.columns if str(col).startswith('Unnamed')]            
        if unnamed_cols:
            # 重命名无名列为更有意义的名称
            rename_dict = {col: f"列{i+1}" for i, col in enumerate(unnamed_cols)}
            df.rename(columns=rename_dict, inplace=True)
        
        # 优化内存使用
        self.optimize_dataframe_memory(df)
        
    def optimize_dataframe_memory(self, df):
        """优化DataFrame内存使用"""
        # 对于字符串列，转换为category类型可以节省内存
        for col in df.columns:
            if df[col].dtype == 'object' and df[col].nunique() < len(df) * 0.5:
                df[col] = df[col].astype('category')
                
        # 对于数值列，如果可能，使用较小的数据类型
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                # 对于整数列，尝试使用较小的整数类型
                c_min = df[col].min()
                c_max = df[col].max()
                if c_min > -128 and c_max < 128:
                    df[col] = df[col].astype(np.int8)
                elif c_min > -32768 and c_max < 32768:
                    df[col] = df[col].astype(np.int16)
                elif c_min > -2147483648 and c_max < 2147483648:
                    df[col] = df[col].astype(np.int32)
            elif pd.api.types.is_float_dtype(df[col]):
                # 对于浮点列，尝试使用较小的浮点类型
                df[col] = df[col].astype(np.float32)

class VirtualizedDataModel(QAbstractTableModel):
    """虚拟化数据模型，用于高效显示大型数据集"""
    def __init__(self, df=None, parent=None):
        super().__init__(parent)
        self._df = pd.DataFrame() if df is None else df
        self._columns = []
        if df is not None:
            self._columns = [str(col) for col in df.columns]
    
    def set_dataframe(self, df):
        """设置数据框"""
        self.beginResetModel()
        self._df = df
        self._columns = [str(col) for col in df.columns]
        self.endResetModel()
    
    def rowCount(self, parent=None):
        """返回行数"""
        return len(self._df)
    
    def columnCount(self, parent=None):
        """返回列数"""
        return len(self._columns)
    
    def data(self, index, role=Qt.DisplayRole):
        """返回单元格数据"""
        if not index.isValid():
            return None
            
        row, col = index.row(), index.column()
        if row >= len(self._df) or col >= len(self._columns):
            return None
            
        # 获取单元格值
        value = self._df.iloc[row, col]
        
        if role == Qt.DisplayRole:
            # 显示用的文本
            return '' if pd.isna(value) else str(value)
        elif role == Qt.TextAlignmentRole:
            # 根据数据类型设置对齐方式
            if isinstance(value, (int, float)) and not pd.isna(value):
                return Qt.AlignRight | Qt.AlignVCenter
            else:
                return Qt.AlignLeft | Qt.AlignVCenter
        elif role == Qt.BackgroundRole:
            # 设置交替行颜色
            if row % 2 == 0:
                return QBrush(QColor('#ffffff'))
            else:
                return QBrush(QColor('#f5f5f5'))
        
        return None
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        """返回表头数据"""
        if role != Qt.DisplayRole:
            return None
            
        if orientation == Qt.Horizontal:
            # 列表头
            if section < len(self._columns):
                return self._columns[section]
        elif orientation == Qt.Vertical:
            # 行表头
            return str(section + 1)
            
        return None
    
    def sort(self, column, order):
        """排序表格"""
        self.beginResetModel()
        if column < len(self._columns):
            col_name = self._columns[column]
            ascending = (order == Qt.AscendingOrder)
            self._df = self._df.sort_values(by=col_name, ascending=ascending)
        self.endResetModel()

class ChunkedDataManager:
    """分块数据管理器，用于处理大型数据集"""
    def __init__(self):
        self.chunks = {}  # 存储文件的数据块，键为文件路径，值为数据块列表
        self.full_data = {}  # 存储完整数据，用于小型文件
        self.meta_info = {}  # 存储元数据，如总行数、列名等
        
    def add_chunk(self, file_path, chunk_df, is_last_chunk):
        """添加数据块"""
        # 确保文件路径存在于字典中
        if file_path not in self.chunks:
            self.chunks[file_path] = []
            # 存储元数据
            self.meta_info[file_path] = {
                'columns': [str(col) for col in chunk_df.columns],
                'total_rows': 0
            }
        
        # 添加数据块
        self.chunks[file_path].append(chunk_df)
        
        # 更新总行数
        self.meta_info[file_path]['total_rows'] += len(chunk_df)
        
        # 如果是最后一块，合并所有块
        if is_last_chunk and len(self.chunks[file_path]) > 0:
            # 如果总行数不太大，合并为完整数据
            if self.meta_info[file_path]['total_rows'] < 500000:
                self.full_data[file_path] = pd.concat(self.chunks[file_path], ignore_index=True)
                # 释放块数据内存
                self.chunks[file_path] = []
    
    def get_dataframe(self, file_path):
        """获取文件的完整DataFrame"""
        # 如果有完整数据，返回完整数据
        if file_path in self.full_data:
            return self.full_data[file_path]
        
        # 如果只有块数据，合并所有块返回
        if file_path in self.chunks and self.chunks[file_path]:
            return pd.concat(self.chunks[file_path], ignore_index=True)
        
        return None
    
    def get_chunk(self, file_path, chunk_index):
        """获取特定的数据块"""
        if file_path in self.chunks and chunk_index < len(self.chunks[file_path]):
            return self.chunks[file_path][chunk_index]
        return None
    
    def get_row_count(self, file_path):
        """获取文件的总行数"""
        if file_path in self.meta_info:
            return self.meta_info[file_path]['total_rows']
        return 0
    
    def get_columns(self, file_path):
        """获取文件的列名"""
        if file_path in self.meta_info:
            return self.meta_info[file_path]['columns']
        return []
    
    def clear_file(self, file_path):
        """清除文件数据"""
        if file_path in self.chunks:
            del self.chunks[file_path]
        if file_path in self.full_data:
            del self.full_data[file_path]
        if file_path in self.meta_info:
            del self.meta_info[file_path]
    
    def clear_all(self):
        """清除所有数据"""
        self.chunks.clear()
        self.full_data.clear()
        self.meta_info.clear()

class DataSeek(QMainWindow):
    def __init__(self):
        super().__init__()
        self.dfs = {}  # 存储多个pandas DataFrame，键为文件路径
        self.current_file = None  # 当前选中的文件
        self.file_paths = []  # 所有已加载的文件路径
        self.search_history = []  # 搜索历史记录
        self.settings = QSettings('DataSeek', 'Settings')
        self.last_search_results = []  # 存储最近一次搜索的结果
        self.last_search_text = ""  # 存储最近一次搜索的文本
        self.loader_threads = []  # 存储文件加载线程
        self.progress_dialog = None  # 进度对话框
        
        # 创建数据管理器
        self.data_manager = ChunkedDataManager()
        
        # 初始化低内存模式设置
        self.low_memory_mode = self.settings.value("low_memory_mode", False, type=bool)
        
        self.init_ui()
        
        # 启用拖放功能
        self.setAcceptDrops(True)

    def init_ui(self):
        # 设置窗口标题和大小
        self.setWindowTitle('数探')
        self.setGeometry(100, 100, 1200, 800)

        # 设置应用样式
        self.setup_styles()

        # 创建中央部件和布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # 创建工具栏
        self.create_toolbar()

        # 文件选择区域
        file_layout = QHBoxLayout()
        self.file_list_widget = QListWidget()
        self.file_list_widget.setMaximumHeight(100)
        self.file_list_widget.itemClicked.connect(self.switch_file)
        
        file_buttons_layout = QVBoxLayout()
        file_select_button = QPushButton('选择文件')
        file_select_button.clicked.connect(self.select_file)
        folder_select_button = QPushButton('选择文件夹')
        folder_select_button.clicked.connect(self.select_folder)
        clear_files_button = QPushButton('清除所有文件')
        clear_files_button.clicked.connect(self.clear_files)
        
        # 添加低内存模式切换
        self.low_memory_checkbox = QCheckBox('低内存模式')
        self.low_memory_checkbox.setChecked(self.low_memory_mode)
        self.low_memory_checkbox.setToolTip('启用后将分块加载大文件，降低内存占用但可能减慢加载速度')
        self.low_memory_checkbox.stateChanged.connect(self.toggle_low_memory_mode)
        
        file_buttons_layout.addWidget(file_select_button)
        file_buttons_layout.addWidget(folder_select_button)
        file_buttons_layout.addWidget(clear_files_button)
        file_buttons_layout.addWidget(self.low_memory_checkbox)
        
        # 添加拖放提示标签
        self.drop_hint_label = QLabel('将Excel或CSV文件拖放到此处')
        self.drop_hint_label.setAlignment(Qt.AlignCenter)
        self.drop_hint_label.setStyleSheet("""
            QLabel {
                padding: 10px;
                border: 2px dashed #aaaaaa;
                border-radius: 5px;
                color: #888888;
                background-color: #f8f8f8;
            }
        """)
        
        file_layout.addWidget(QLabel('已加载文件:'))
        file_layout.addWidget(self.file_list_widget, 1)
        file_layout.addLayout(file_buttons_layout)
        
        main_layout.addWidget(self.drop_hint_label)
        main_layout.addLayout(file_layout)

        # 创建选项卡
        self.tabs = QTabWidget()
        
        # 搜索选项卡
        search_tab = QWidget()
        search_layout = QVBoxLayout(search_tab)
        
        # 搜索输入区域
        search_input_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText('输入搜索内容...')
        self.search_input.returnPressed.connect(self.search_tables)
        search_button = QPushButton('搜索')
        search_button.clicked.connect(self.search_tables)
        
        search_input_layout.addWidget(QLabel('搜索:'))
        search_input_layout.addWidget(self.search_input, 1)
        search_input_layout.addWidget(search_button)
        search_layout.addLayout(search_input_layout)
        
        # 高级搜索选项
        advanced_options = QGroupBox('高级搜索选项')
        advanced_layout = QVBoxLayout(advanced_options)
        
        # 搜索模式选项
        search_mode_layout = QHBoxLayout()
        self.search_mode = QComboBox()
        self.search_mode.addItems(['全局搜索', '按列搜索'])
        self.search_mode.currentIndexChanged.connect(self.toggle_column_selection)
        self.column_selector = QComboBox()
        self.column_selector.setEnabled(False)
        
        search_mode_layout.addWidget(QLabel('搜索模式:'))
        search_mode_layout.addWidget(self.search_mode)
        search_mode_layout.addWidget(QLabel('列:'))
        search_mode_layout.addWidget(self.column_selector)
        search_mode_layout.addStretch(1)
        advanced_layout.addLayout(search_mode_layout)
        
        # 匹配选项
        match_options_layout = QHBoxLayout()
        self.exact_match = QCheckBox('精确匹配')
        self.case_sensitive = QCheckBox('区分大小写')
        self.whole_word = QCheckBox('整词匹配')
        self.regex_match = QCheckBox('正则表达式')
        
        match_options_layout.addWidget(self.exact_match)
        match_options_layout.addWidget(self.case_sensitive)
        match_options_layout.addWidget(self.whole_word)
        match_options_layout.addWidget(self.regex_match)
        match_options_layout.addStretch(1)
        advanced_layout.addLayout(match_options_layout)
        
        search_layout.addWidget(advanced_options)
        
        # 搜索历史
        history_group = QGroupBox('搜索历史')
        history_layout = QHBoxLayout(history_group)
        self.history_list = QListWidget()
        self.history_list.itemDoubleClicked.connect(self.use_history_item)
        clear_history_button = QPushButton('清除历史')
        clear_history_button.clicked.connect(self.clear_history)
        
        history_layout.addWidget(self.history_list)
        history_layout.addWidget(clear_history_button)
        search_layout.addWidget(history_group)
        
        # 表格区域 - 使用自定义的虚拟化表格
        # 虚拟化表格视图
        self.table_model = VirtualizedDataModel()
        self.table = QTableView()
        self.table.setModel(self.table_model)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSortingEnabled(True)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_context_menu)
        search_layout.addWidget(self.table)
        
        # 预览选项卡
        preview_tab = QWidget()
        preview_layout = QVBoxLayout(preview_tab)
        
        # 创建预览表格
        self.preview_model = VirtualizedDataModel()
        self.preview_table = QTableView()
        self.preview_table.setModel(self.preview_model)
        self.preview_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.preview_table.setSortingEnabled(True)
        self.preview_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.preview_table.customContextMenuRequested.connect(self.show_preview_context_menu)
        preview_layout.addWidget(self.preview_table)
        
        # 添加选项卡到主界面
        self.tabs.addTab(search_tab, '搜索')
        self.tabs.addTab(preview_tab, '预览')
        main_layout.addWidget(self.tabs)

        # 状态栏
        self.statusBar().showMessage('准备就绪')
        
        # 加载历史记录
        self.load_search_history()
        
        # 添加内存使用监视器
        self.memory_usage_label = QLabel()
        self.statusBar().addPermanentWidget(self.memory_usage_label)
        self.update_memory_usage()
        
        # 定时更新内存使用
        self.memory_timer = QTimer(self)
        self.memory_timer.timeout.connect(self.update_memory_usage)
        self.memory_timer.start(5000)  # 每5秒更新一次
        
    def create_toolbar(self):
        """创建工具栏"""
        toolbar = QToolBar('主工具栏')
        self.addToolBar(toolbar)
        
        # 导出按钮
        export_action = QAction('导出结果', self)
        export_action.triggered.connect(self.export_results)
        toolbar.addAction(export_action)
        
        # 性能选项按钮
        performance_action = QAction('性能选项', self)
        performance_action.triggered.connect(self.show_performance_options)
        toolbar.addAction(performance_action)
        
        # 帮助按钮
        help_action = QAction('帮助', self)
        help_action.triggered.connect(self.show_help)
        toolbar.addAction(help_action)

    def select_file(self):
        """选择并加载文件"""
        options = QFileDialog.Options()
        file_paths, _ = QFileDialog.getOpenFileNames(
            self, '选择Excel或CSV文件', '',
            'Excel Files (*.xlsx *.xls);;CSV Files (*.csv);;All Files (*)',
            options=options
        )

        if file_paths:
            # 创建进度对话框
            self.progress_dialog = QProgressDialog('正在加载文件...', '取消', 0, 100, self)
            self.progress_dialog.setWindowTitle('加载进度')
            self.progress_dialog.setWindowModality(Qt.WindowModal)
            self.progress_dialog.setMinimumDuration(0)  # 立即显示
            self.progress_dialog.setValue(0)
            self.progress_dialog.setAutoClose(False)
            self.progress_dialog.canceled.connect(self.cancel_loading)  # 连接取消信号
            
            # 加载多个文件时显示总体进度
            if len(file_paths) > 1:
                self.progress_dialog.setLabelText(f'正在加载 {len(file_paths)} 个文件 (0/{len(file_paths)})')
            
            # 开始加载第一个文件
            self.load_files_batch(file_paths, 0)
    
    def select_folder(self):
        options = QFileDialog.Options()
        folder_path = QFileDialog.getExistingDirectory(
            self, '选择包含Excel或CSV文件的文件夹', '',
            options=options
        )
        
        if folder_path:
            # 收集所有符合条件的文件
            file_paths = []
            for root, _, files in os.walk(folder_path):
                for file in files:
                    if file.endswith(('.xlsx', '.xls', '.csv')):
                        file_path = os.path.join(root, file)
                        file_paths.append(file_path)
            
            if file_paths:
                # 创建进度对话框
                self.progress_dialog = QProgressDialog('正在加载文件...', '取消', 0, 100, self)
                self.progress_dialog.setWindowTitle('加载进度')
                self.progress_dialog.setWindowModality(Qt.WindowModal)
                self.progress_dialog.setMinimumDuration(0)  # 立即显示
                self.progress_dialog.setValue(0)
                self.progress_dialog.setAutoClose(False)
                self.progress_dialog.canceled.connect(self.cancel_loading)  # 连接取消信号
                
                # 加载多个文件时显示总体进度
                if len(file_paths) > 1:
                    self.progress_dialog.setLabelText(f'正在加载 {len(file_paths)} 个文件 (0/{len(file_paths)})')
                
                # 开始加载第一个文件
                self.load_files_batch(file_paths, 0)
    
    def clear_files(self):
        """清除所有加载的文件"""
        # 清除数据
        self.dfs.clear()
        self.file_paths.clear()
        self.file_list_widget.clear()
        self.current_file = None
        
        # 清除数据管理器中的数据
        self.data_manager.clear_all()
        
        # 清空表格模型
        self.table_model.set_dataframe(pd.DataFrame())
        self.preview_model.set_dataframe(pd.DataFrame())
        
        # 更新状态栏和窗口标题
        self.statusBar().showMessage('已清除所有文件')
        self.setWindowTitle('数探')
    
    def switch_file(self, item):
        file_path = item.data(Qt.UserRole)
        if file_path in self.dfs:
            self.current_file = file_path
            self.display_data(file_path)
            self.statusBar().showMessage(f'当前文件: {os.path.basename(file_path)}')

    def load_files_batch(self, file_paths, current_index):
        """批量加载文件，一次加载一个"""
        try:
            if current_index >= len(file_paths):
                # 所有文件加载完成
                if self.progress_dialog:
                    self.progress_dialog.close()
                    self.progress_dialog = None
                # 停止加载动画
                if hasattr(self, 'loading_animation_timer') and self.loading_animation_timer:
                    self.loading_animation_timer.stop()
                self.statusBar().showMessage('所有文件加载完成')
                return
            
            # 检查是否取消加载
            if self.progress_dialog and self.progress_dialog.wasCanceled():
                self.statusBar().showMessage('文件加载已取消')
                self.progress_dialog.close()
                self.progress_dialog = None
                if hasattr(self, 'loading_animation_timer') and self.loading_animation_timer:
                    self.loading_animation_timer.stop()
                return
            
            file_path = file_paths[current_index]
            
            # 检查文件是否已加载
            if file_path in self.file_paths:
                # 跳过已加载的文件，继续加载下一个
                self.load_files_batch(file_paths, current_index + 1)
                return
            
            # 更新进度对话框
            if self.progress_dialog:
                if len(file_paths) > 1:
                    self.progress_dialog.setLabelText(f'正在加载 {os.path.basename(file_path)} ({current_index+1}/{len(file_paths)})')
                else:
                    self.progress_dialog.setLabelText(f'正在加载 {os.path.basename(file_path)}')
                self.progress_dialog.setValue(0)
            
            # 获取分块大小设置
            chunk_size = self.settings.value("chunk_size", 50000, type=int)
            
            # 创建并启动加载线程
            loader_thread = FileLoaderThread(file_path, chunk_size=chunk_size, low_memory_mode=self.low_memory_mode)
            self.loader_threads.append(loader_thread)
            
            # 连接信号
            loader_thread.progress_signal.connect(self.update_load_progress)
            loader_thread.chunk_loaded_signal.connect(self.on_chunk_loaded)
            loader_thread.finished_signal.connect(lambda fp, df: self.on_file_loaded(fp, df, file_paths, current_index))
            loader_thread.error_signal.connect(lambda fp, err: self.on_file_error(fp, err, file_paths, current_index))
            
            # 启动线程
            loader_thread.start()
        except Exception as e:
            QMessageBox.critical(self, '错误', f'加载文件时发生错误：{str(e)}')
            self.statusBar().showMessage('文件加载失败')
            
    @pyqtSlot(int)
    def update_load_progress(self, value):
        """更新加载进度"""
        if self.progress_dialog and not self.progress_dialog.wasCanceled():
            self.progress_dialog.setValue(value)
    
    @pyqtSlot(str, object, bool)
    def on_chunk_loaded(self, file_path, chunk_df, is_last_chunk):
        """数据块加载完成的回调"""
        try:
            # 将数据块添加到数据管理器
            self.data_manager.add_chunk(file_path, chunk_df, is_last_chunk)
            
            # 如果是第一个数据块，添加到文件列表
            if file_path not in self.file_paths:
                self.file_paths.append(file_path)
                item = QListWidgetItem(os.path.basename(file_path))
                item.setData(Qt.UserRole, file_path)
                self.file_list_widget.addItem(item)
                
                # 如果是第一个文件，设为当前文件并显示
                if len(self.file_paths) == 1:
                    self.current_file = file_path
                    # 更新列选择器
                    self.update_column_selector(file_path)
                    # 显示数据
                    self.display_data(file_path)
            
            # 如果是当前文件，更新显示
            if file_path == self.current_file:
                self.display_data(file_path)
                
            # 更新状态栏
            if is_last_chunk:
                total_rows = self.data_manager.get_row_count(file_path)
                self.statusBar().showMessage(f'已加载文件: {os.path.basename(file_path)} ({total_rows}行)')
            else:
                loaded_rows = self.data_manager.get_row_count(file_path)
                self.statusBar().showMessage(f'正在加载: {os.path.basename(file_path)} ({loaded_rows}行已加载)')
                
        except Exception as e:
            QMessageBox.critical(self, '错误', f'处理数据块时发生错误：{str(e)}')
            
    @pyqtSlot(str, object)
    def on_file_loaded(self, file_path, df, file_paths, current_index):
        """文件加载完成的回调（用于非分块模式）"""
        try:
            # 确保所有列名都是字符串类型
            df.columns = [str(col) for col in df.columns]
            
            # 存储DataFrame
            self.dfs[file_path] = df
            
            # 也添加到数据管理器
            if file_path not in self.data_manager.full_data:
                self.data_manager.full_data[file_path] = df
                
                # 更新元数据
                self.data_manager.meta_info[file_path] = {
                    'columns': [str(col) for col in df.columns],
                    'total_rows': len(df)
                }
            
            # 如果文件路径不在列表中，添加
            if file_path not in self.file_paths:
                self.file_paths.append(file_path)
                item = QListWidgetItem(os.path.basename(file_path))
                item.setData(Qt.UserRole, file_path)
                self.file_list_widget.addItem(item)
            
            # 如果是第一个文件，设为当前文件并显示
            if len(self.file_paths) == 1:
                self.current_file = file_path
                # 更新列选择器
                self.update_column_selector(file_path)
                # 显示数据
                self.display_data(file_path)
            
            self.statusBar().showMessage(f'已加载文件: {os.path.basename(file_path)} ({len(df)}行)')
            
            # 加载下一个文件
            self.load_files_batch(file_paths, current_index + 1)
        except Exception as e:
            QMessageBox.critical(self, '错误', f'处理文件 {os.path.basename(file_path)} 时发生错误：{str(e)}')
            self.statusBar().showMessage('文件处理失败')
            
            # 继续加载下一个文件
            self.load_files_batch(file_paths, current_index + 1)
            
    def update_column_selector(self, file_path):
        """更新列选择器"""
        try:
            # 优先从数据管理器获取列名
            columns = self.data_manager.get_columns(file_path)
            
            # 如果数据管理器没有该文件信息，则从dfs中获取
            if not columns and file_path in self.dfs:
                columns = [str(col) for col in self.dfs[file_path].columns]
                
            if columns:
                self.column_selector.clear()
                self.column_selector.addItems(columns)
        except Exception as e:
            QMessageBox.warning(self, '警告', f'更新列选择器失败: {str(e)}')
            
    def display_data(self, file_path=None):
        """显示数据到表格"""
        try:
            if file_path is None:
                file_path = self.current_file
                
            if file_path is None or (file_path not in self.dfs and file_path not in self.data_manager.meta_info):
                return

            # 优先从数据管理器获取数据
            df = self.data_manager.get_dataframe(file_path)
            
            # 如果数据管理器没有完整数据，则从dfs中获取
            if df is None and file_path in self.dfs:
                df = self.dfs[file_path]
                
            if df is None:
                # 如果仍然没有获取到数据，仅更新表头
                columns = self.data_manager.get_columns(file_path)
                if columns:
                    # 创建空DataFrame，仅包含列名
                    df = pd.DataFrame(columns=columns)
                else:
                    return
            
            # 更新虚拟化表格模型
            self.table_model.set_dataframe(df)
            
            # 调整列宽以适应内容
            self.table.resizeColumnsToContents()
            
            # 更新窗口标题
            row_count = len(df) if df is not None else self.data_manager.get_row_count(file_path)
            self.setWindowTitle(f'数探 - {os.path.basename(file_path)} ({row_count}行)')
            
            # 清空预览表格
            self.preview_model.set_dataframe(pd.DataFrame())
            
        except Exception as e:
            QMessageBox.critical(self, '错误', f'显示数据时发生错误：{str(e)}')
            self.statusBar().showMessage('数据显示失败')
            
    def search_tables(self):
        """搜索表格数据"""
        try:
            search_text = self.search_input.text().strip()
            if not search_text:
                return

            # 获取搜索选项
            options = {
                'exact_match': self.exact_match.isChecked(),
                'case_sensitive': self.case_sensitive.isChecked(),
                'whole_word': self.whole_word.isChecked(),
                'regex_match': self.regex_match.isChecked(),
                'search_mode': self.search_mode.currentText(),
                'column': self.column_selector.currentText() if self.column_selector.isEnabled() else None
            }

            # 存储搜索结果
            match_results = []
            total_matches = 0

            # 创建进度对话框
            progress_dialog = QProgressDialog('正在搜索...', '取消', 0, len(self.file_paths), self)
            progress_dialog.setWindowTitle('搜索进度')
            progress_dialog.setWindowModality(Qt.WindowModal)
            progress_dialog.setMinimumDuration(0)
            progress_dialog.setValue(0)

            # 开始搜索定时器
            start_time = datetime.datetime.now()

            # 遍历所有加载的数据
            for i, file_path in enumerate(self.file_paths):
                if progress_dialog.wasCanceled():
                    break
                    
                progress_dialog.setValue(i)
                progress_dialog.setLabelText(f'正在搜索: {os.path.basename(file_path)}')
                
                try:
                    # 优先从数据管理器获取数据
                    df = self.data_manager.get_dataframe(file_path)
                    
                    # 如果数据管理器没有完整数据，则从dfs中获取
                    if df is None and file_path in self.dfs:
                        df = self.dfs[file_path]
                        
                    if df is None:
                        # 文件尚未完全加载，跳过
                        continue
                    
                    # 根据搜索模式选择搜索方法
                    file_matches = self.search_in_dataframe(df, search_text, options)
                    
                    # 将匹配结果添加到结果列表
                    for match in file_matches:
                        match_results.append({
                            'file': file_path,
                            'columns': match
                        })
                        total_matches += 1
                except Exception as e:
                    QMessageBox.warning(self, '警告', f'搜索文件 {os.path.basename(file_path)} 时发生错误：{str(e)}')
                    continue

            progress_dialog.close()

            # 计算搜索耗时
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()

            # 更新搜索预览
            self.update_search_preview(search_text, match_results)

            # 保存搜索历史
            self.add_to_history(search_text, options)
            
            # 更新状态栏
            if match_results:
                self.statusBar().showMessage(f'找到 {total_matches} 个匹配项 (搜索耗时: {elapsed_time:.2f}秒)')
            else:
                self.statusBar().showMessage(f'未找到匹配项 (搜索耗时: {elapsed_time:.2f}秒)')
        except Exception as e:
            QMessageBox.critical(self, '错误', f'搜索时发生错误：{str(e)}')
            self.statusBar().showMessage('搜索失败')
            
    def update_search_preview(self, search_text, match_results):
        """更新搜索预览"""
        try:
            if not match_results:
                self.preview_model.set_dataframe(pd.DataFrame())
                return

            # 将匹配结果转换为DataFrame
            result_data = []
            
            # 确定所有列
            all_columns = set()
            for result in match_results:
                all_columns.update(str(k) for k in result['columns'].keys())
            all_columns = sorted(list(all_columns))
            
            # 准备数据
            for result in match_results:
                row_data = {'文件名': os.path.basename(result['file'])}
                # 添加其他列数据
                for col in all_columns:
                    row_data[col] = result['columns'].get(col, '')
                result_data.append(row_data)
                
            # 创建DataFrame
            result_df = pd.DataFrame(result_data)
            
            # 重新排列列，确保"文件名"列在最前面
            cols = ['文件名'] + [col for col in result_df.columns if col != '文件名']
            result_df = result_df[cols]
            
            # 设置预览表格模型
            self.preview_model.set_dataframe(result_df)
            
            # 调整列宽以适应内容
            self.preview_table.resizeColumnsToContents()
            
            # 切换到预览选项卡
            self.tabs.setCurrentIndex(1)
        except Exception as e:
            QMessageBox.critical(self, '错误', f'更新预览时发生错误：{str(e)}')
            self.statusBar().showMessage('预览更新失败')

    def show_context_menu(self, position):
        """显示右键菜单"""
        menu = QMenu()
        
        # 获取当前选中的单元格
        current_item = self.preview_table.itemAt(position)
        if current_item:
            copy_cell = QAction('复制单元格内容', self)
            copy_cell.triggered.connect(self.copy_cell_content)
            menu.addAction(copy_cell)
            
            copy_row = QAction('复制整行内容', self)
            copy_row.triggered.connect(self.copy_row_content)
            menu.addAction(copy_row)
            
            export_action = QAction('导出搜索结果', self)
            export_action.triggered.connect(self.export_results)
            menu.addAction(export_action)
            
            menu.exec_(self.preview_table.mapToGlobal(position))

    def copy_cell_content(self):
        """复制单元格内容"""
        selected_items = self.preview_table.selectedItems()
        if selected_items:
            clipboard = QApplication.clipboard()
            clipboard.setText(selected_items[0].text())

    def copy_row_content(self):
        """复制整行内容"""
        selected_items = self.preview_table.selectedItems()
        if selected_items:
            row = selected_items[0].row()
            row_data = []
            for col in range(self.preview_table.columnCount()):
                item = self.preview_table.item(row, col)
                row_data.append(item.text() if item else '')
            clipboard = QApplication.clipboard()
            clipboard.setText('\t'.join(row_data))

    def export_results(self):
        """导出搜索结果"""
        if self.preview_table.rowCount() == 0:
            QMessageBox.warning(self, '警告', '没有可导出的数据')
            return

        # 获取保存文件路径
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            '导出搜索结果',
            '',
            'Excel文件 (*.xlsx);;CSV文件 (*.csv)'
        )

        if not file_path:
            return

        try:
            # 准备数据
            data = []
            headers = []
            for col in range(self.preview_table.columnCount()):
                headers.append(self.preview_table.horizontalHeaderItem(col).text())

            for row in range(self.preview_table.rowCount()):
                row_data = []
                for col in range(self.preview_table.columnCount()):
                    item = self.preview_table.item(row, col)
                    row_data.append(item.text() if item else '')
                data.append(row_data)

            # 创建DataFrame
            df = pd.DataFrame(data, columns=headers)

            # 根据文件扩展名选择导出格式
            if file_path.endswith('.xlsx'):
                df.to_excel(file_path, index=False)
            else:
                df.to_csv(file_path, index=False, encoding='utf-8-sig')

            QMessageBox.information(self, '成功', '数据导出成功！')

        except Exception as e:
            QMessageBox.critical(self, '错误', f'导出失败：{str(e)}')

    def toggle_column_selection(self, index):
        # 根据搜索模式启用或禁用列选择器
        self.column_selector.setEnabled(index == 1)  # 1 表示按列搜索
    
    def add_to_history(self, search_text, options):
        # 添加到历史记录
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history_item = {"text": search_text, "options": options, "timestamp": timestamp}
        
        # 检查是否已存在相同的搜索
        for item in self.search_history:
            if item["text"] == search_text and item["options"] == options:
                self.search_history.remove(item)
                break
        
        # 添加到历史记录列表
        self.search_history.insert(0, history_item)
        
        # 限制历史记录数量
        if len(self.search_history) > 20:
            self.search_history = self.search_history[:20]
        
        # 更新历史记录显示
        self.update_history_list()
        
        # 保存历史记录
        self.save_search_history()
    
    def update_history_list(self):
        self.history_list.clear()
        for item in self.search_history:
            text = f"{item['text']} ({item['timestamp']})"
            list_item = QListWidgetItem(text)
            list_item.setData(Qt.UserRole, item)
            self.history_list.addItem(list_item)
    
    def use_history_item(self, item):
        history_item = item.data(Qt.UserRole)
        if history_item:
            self.search_input.setText(history_item["text"])
            options = history_item["options"]
            
            # 设置搜索选项
            self.search_mode.setCurrentIndex(options.get("search_mode", 0))
            self.exact_match.setChecked(options.get("exact_match", False))
            self.case_sensitive.setChecked(options.get("case_sensitive", False))
            self.whole_word.setChecked(options.get("whole_word", False))
            self.regex_match.setChecked(options.get("regex_match", False))
            
            # 如果是按列搜索，设置列
            if options.get("search_mode") == 1 and "column_index" in options:
                self.column_selector.setCurrentIndex(options["column_index"])
    
    def clear_history(self):
        self.search_history.clear()
        self.history_list.clear()
        self.save_search_history()
    
    def save_search_history(self):
        self.settings.setValue("search_history", self.search_history)
    
    def load_search_history(self):
        history = self.settings.value("search_history", [])
        if history:
            self.search_history = history
            self.update_history_list()

    def search_in_dataframe(self, df, search_text, options):
        """在DataFrame中搜索数据"""
        try:
            search_mode = options.get("search_mode", "全局搜索")
            exact_match = options.get("exact_match", False)
            case_sensitive = options.get("case_sensitive", False)
            whole_word = options.get("whole_word", False)
            regex_match = options.get("regex_match", False)
            column = options.get("column", None)
            
            matches = []
            
            # 确定要搜索的列
            if search_mode == "全局搜索":
                columns_to_search = df.columns
            else:  # 按列搜索
                if column and str(column) in [str(col) for col in df.columns]:
                    # 找到匹配的列（考虑类型转换）
                    column_idx = [str(col) for col in df.columns].index(str(column))
                    columns_to_search = [df.columns[column_idx]]
                else:
                    return matches
            
            # 执行搜索
            for col in columns_to_search:
                for row_idx, value in enumerate(df[col]):
                    # 处理NaN值，确保显示为空字符串而不是'nan'
                    cell_text = '' if pd.isna(value) else str(value)
                    match_found = False
                    
                    # 根据匹配模式执行搜索
                    if regex_match:
                        # 正则表达式搜索
                        try:
                            if case_sensitive:
                                pattern = re.compile(search_text)
                            else:
                                pattern = re.compile(search_text, re.IGNORECASE)
                            match_found = bool(pattern.search(cell_text))
                        except re.error:
                            continue
                    elif exact_match:
                        # 精确匹配
                        if case_sensitive:
                            match_found = (search_text == cell_text)
                        else:
                            match_found = (search_text.lower() == cell_text.lower())
                    elif whole_word:
                        # 整词匹配
                        word_pattern = r'\b' + re.escape(search_text) + r'\b'
                        if case_sensitive:
                            match_found = bool(re.search(word_pattern, cell_text))
                        else:
                            match_found = bool(re.search(word_pattern, cell_text, re.IGNORECASE))
                    else:
                        # 包含匹配
                        if case_sensitive:
                            match_found = (search_text in cell_text)
                        else:
                            match_found = (search_text.lower() in cell_text.lower())
                    
                    if match_found:
                        # 获取整行数据
                        row_data = df.iloc[row_idx].to_dict()
                        # 确保字典的键都是字符串
                        row_data = {str(k): v for k, v in row_data.items()}
                        matches.append(row_data)
            
            return matches
        except Exception as e:
            QMessageBox.warning(self, '警告', f'搜索数据时发生错误：{str(e)}')
            return []
    
    def show_help(self):
        """显示帮助信息"""
        help_dialog = QDialog(self)
        help_dialog.setWindowTitle("数探 - 使用帮助")
        help_dialog.setMinimumSize(700, 500)
        
        layout = QVBoxLayout(help_dialog)
        
        # 创建富文本编辑器显示帮助内容
        help_text = QTextEdit()
        help_text.setReadOnly(True)
        
        # 设置帮助文本样式
        help_html = """
        <html>
        <head>
            <style>
                body {
                    font-family: '微软雅黑', 'Microsoft YaHei', sans-serif;
                    line-height: 1.6;
                    margin: 15px;
                }
                h1 {
                    color: #2c3e50;
                    border-bottom: 2px solid #3498db;
                    padding-bottom: 8px;
                    margin-top: 20px;
                }
                h2 {
                    color: #3498db;
                    margin-top: 15px;
                    padding-left: 10px;
                    border-left: 4px solid #3498db;
                }
                p {
                    text-indent: 2em;
                    margin: 8px 0;
                }
                ul {
                    margin-left: 20px;
                    list-style-type: disc;
                }
                li {
                    margin: 5px 0;
                    padding-left: 5px;
                }
                .feature {
                    background-color: #f8f9fa;
                    border-left: 4px solid #2ecc71;
                    padding: 10px 15px;
                    margin: 10px 0;
                    border-radius: 3px;
                }
                .tip {
                    background-color: #e8f4f8;
                    border-left: 4px solid #3498db;
                    padding: 10px 15px;
                    margin: 10px 0;
                    border-radius: 3px;
                }
                .warning {
                    background-color: #fff8e1;
                    border-left: 4px solid #f39c12;
                    padding: 10px 15px;
                    margin: 10px 0;
                    border-radius: 3px;
                }
                .shortcut {
                    font-family: Consolas, monospace;
                    background-color: #f1f1f1;
                    padding: 2px 5px;
                    border-radius: 3px;
                    border: 1px solid #ddd;
                    font-weight: bold;
                }
            </style>
        </head>
        <body>
            <h1>数探 - 高性能数据查看与搜索工具</h1>
            
            <p>数探是一款强大的表格数据查看和搜索工具，专为处理Excel和CSV文件设计。本工具可帮助您快速加载、浏览和搜索大型数据文件，并针对大数据量进行了性能优化。</p>
            
            <h2>1. 文件操作</h2>
            
            <div class="feature">
                <h3>加载文件</h3>
                <ul>
                    <li><b>选择文件</b>：点击"选择文件"按钮，加载一个或多个Excel或CSV文件</li>
                    <li><b>选择文件夹</b>：点击"选择文件夹"按钮，加载文件夹中的所有Excel和CSV文件</li>
                    <li><b>拖放文件</b>：直接将Excel或CSV文件拖放到程序窗口即可加载</li>
                    <li><b>清除文件</b>：点击"清除所有文件"按钮，移除所有已加载的文件</li>
                </ul>
            </div>
            
            <div class="tip">
                <p>提示：当加载大型文件时，会显示进度对话框，您可以随时取消加载过程。使用"低内存模式"可以处理超大文件。</p>
            </div>
            
            <h2>2. 性能优化选项</h2>
            
            <div class="feature">
                <h3>内存管理</h3>
                <ul>
                    <li><b>低内存模式</b>：启用后将分块加载大文件，减少内存占用</li>
                    <li><b>分块大小</b>：可在性能选项中调整每次加载的数据量</li>
                    <li><b>内存监控</b>：状态栏显示当前内存使用情况</li>
                </ul>
            </div>
            
            <div class="tip">
                <p>对于超大文件（上百万行），建议启用低内存模式，可能会稍微降低加载速度，但可以处理更大的数据集而不会耗尽内存。</p>
            </div>
            
            <h2>3. 搜索功能</h2>
            
            <div class="feature">
                <h3>基本搜索</h3>
                <ul>
                    <li><b>全局搜索</b>：在所有列中搜索内容</li>
                    <li><b>按列搜索</b>：在指定列中搜索内容</li>
                    <li><b>搜索选项</b>：
                        <ul>
                            <li>精确匹配：完全匹配搜索文本</li>
                            <li>区分大小写：区分大小写进行搜索</li>
                            <li>整词匹配：匹配完整单词</li>
                            <li>正则表达式：使用正则表达式进行高级搜索</li>
                        </ul>
                    </li>
                </ul>
            </div>
            
            <h3>搜索历史</h3>
            <ul>
                <li>搜索记录会自动保存在历史列表中</li>
                <li>双击历史记录项可重复使用该搜索</li>
                <li>点击"清除历史"按钮可删除所有搜索历史</li>
            </ul>
            
            <div class="warning">
                <p>在大型数据集中进行复杂搜索可能需要一些时间，尤其是使用正则表达式时。搜索结果会显示耗时。</p>
            </div>
            
            <h2>4. 结果查看与操作</h2>
            
            <div class="feature">
                <h3>预览功能</h3>
                <ul>
                    <li>搜索后，匹配的结果会在预览标签页中显示</li>
                    <li>表格支持虚拟滚动，可高效显示大量搜索结果</li>
                    <li>点击表头可对结果进行排序</li>
                </ul>
            </div>
            
            <h3>表格操作</h3>
            <ul>
                <li><b>右键菜单</b>：在表格中右击可打开上下文菜单</li>
                <li><b>复制内容</b>：可复制单元格内容或整行数据</li>
                <li><b>排序</b>：点击表头可按该列排序</li>
                <li><b>导出结果</b>：可将搜索结果导出为Excel或CSV文件</li>
            </ul>
            
            <h2>5. 高级功能</h2>
            
            <div class="feature">
                <h3>性能调优</h3>
                <ul>
                    <li><b>性能选项</b>：在工具栏点击"性能选项"可调整程序性能参数</li>
                    <li><b>虚拟滚动</b>：表格使用虚拟滚动技术，即使百万行数据也能流畅显示</li>
                    <li><b>异步处理</b>：文件加载和搜索操作在后台线程执行，不会阻塞界面</li>
                </ul>
            </div>
            
            <h2>6. 快捷键</h2>
            
            <ul>
                <li><span class="shortcut">Enter</span>：在搜索框中按回车键执行搜索</li>
                <li><span class="shortcut">Ctrl+C</span>：复制选中的单元格内容</li>
                <li><span class="shortcut">Ctrl+O</span>：打开文件选择对话框</li>
                <li><span class="shortcut">Ctrl+F</span>：跳转到搜索框</li>
            </ul>
            
            <div class="tip">
                <p>提示：状态栏会显示当前内存使用量和操作耗时，帮助您监控程序性能。</p>
            </div>
            
            <p style="margin-top: 30px; text-align: center; color: #7f8c8d;">感谢使用数探工具，如有问题请联系开发者。</p>
            
        </body>
        </html>
        """
        
        help_text.setHtml(help_html)
        
        # 添加关闭按钮
        close_button = QPushButton("关闭")
        close_button.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
            QPushButton:pressed {
                background-color: #1f6da9;
            }
        """)
        close_button.clicked.connect(help_dialog.close)
        
        layout.addWidget(help_text)
        layout.addWidget(close_button, 0, Qt.AlignRight)
        
        help_dialog.exec_()

    def setup_styles(self):
        """设置应用样式"""
        # 设置全局字体
        app_font = QFont("微软雅黑", 9)
        QApplication.setFont(app_font)
        
        # 设置表格样式
        table_style = """
            QTableWidget {
                border: 1px solid #d3d3d3;
                border-radius: 3px;
                background-color: #ffffff;
                alternate-background-color: #f5f5f5;
                gridline-color: #e0e0e0;
            }
            QTableWidget::item {
                padding: 5px;
                border-bottom: 1px solid #eeeeee;
            }
            QTableWidget::item:selected {
                background-color: #e0f2fe;
                color: #000000;
            }
            QHeaderView::section {
                background-color: #f0f0f0;
                color: #333333;
                padding: 6px;
                border: none;
                border-right: 1px solid #d3d3d3;
                border-bottom: 1px solid #d3d3d3;
                font-weight: bold;
            }
            QHeaderView::section:checked {
                background-color: #e0e0e0;
            }
        """
        
        # 应用样式
        self.setStyleSheet(table_style)

    def dragEnterEvent(self, event: QDragEnterEvent):
        """处理拖动进入事件"""
        # 检查是否包含文件URL
        if event.mimeData().hasUrls():
            # 检查是否所有文件都是支持的格式
            urls = event.mimeData().urls()
            for url in urls:
                file_path = url.toLocalFile()
                if not file_path.endswith(('.xlsx', '.xls', '.csv')):
                    return  # 如果有不支持的格式，不接受拖放
            
            # 更改拖放提示样式
            self.drop_hint_label.setStyleSheet("""
                QLabel {
                    padding: 10px;
                    border: 2px dashed #4caf50;
                    border-radius: 5px;
                    color: #4caf50;
                    background-color: #e8f5e9;
                }
            """)
            self.drop_hint_label.setText('释放鼠标加载文件')
            
            # 接受拖放操作
            event.acceptProposedAction()
    
    def dragLeaveEvent(self, event):
        """处理拖动离开事件"""
        # 恢复拖放提示样式
        self.drop_hint_label.setStyleSheet("""
            QLabel {
                padding: 10px;
                border: 2px dashed #aaaaaa;
                border-radius: 5px;
                color: #888888;
                background-color: #f8f8f8;
            }
        """)
        self.drop_hint_label.setText('将Excel或CSV文件拖放到此处')
    
    def dropEvent(self, event: QDropEvent):
        """处理文件拖放事件"""
        # 恢复拖放提示样式
        self.drop_hint_label.setStyleSheet("""
            QLabel {
                padding: 10px;
                border: 2px dashed #aaaaaa;
                border-radius: 5px;
                color: #888888;
                background-color: #f8f8f8;
            }
        """)
        self.drop_hint_label.setText('将Excel或CSV文件拖放到此处')
        
        # 获取拖放的文件路径
        files_to_load = []
        for url in event.mimeData().urls():
            file_path = url.toLocalFile()
            if file_path.endswith(('.xlsx', '.xls', '.csv')):
                files_to_load.append(file_path)
        
        # 如果有有效文件，则加载
        if files_to_load:
            # 创建进度对话框
            self.progress_dialog = QProgressDialog('正在加载文件...', '取消', 0, 100, self)
            self.progress_dialog.setWindowTitle('加载进度')
            self.progress_dialog.setWindowModality(Qt.WindowModal)
            self.progress_dialog.setMinimumDuration(0)  # 立即显示
            self.progress_dialog.setValue(0)
            self.progress_dialog.setAutoClose(False)
            self.progress_dialog.canceled.connect(self.cancel_loading)  # 连接取消信号
            
            # 加载多个文件时显示总体进度
            if len(files_to_load) > 1:
                self.progress_dialog.setLabelText(f'正在加载 {len(files_to_load)} 个文件 (0/{len(files_to_load)})')
            
            # 开始加载第一个文件
            self.load_files_batch(files_to_load, 0)
            
            # 接受拖放操作
            event.acceptProposedAction()

    def toggle_low_memory_mode(self, state):
        """切换低内存模式"""
        self.low_memory_mode = (state == Qt.Checked)
        self.settings.setValue("low_memory_mode", self.low_memory_mode)
        QMessageBox.information(self, '模式已切换', 
                               f"{'已启用' if self.low_memory_mode else '已禁用'}低内存模式，将在下次加载文件时生效。")
        
    def update_memory_usage(self):
        """更新内存使用量显示"""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss / 1024 / 1024  # MB
            self.memory_usage_label.setText(f"内存: {memory_usage:.1f} MB")
        except:
            self.memory_usage_label.setText("内存使用: 未知")
            
    def show_performance_options(self):
        """显示性能选项对话框"""
        dialog = QDialog(self)
        dialog.setWindowTitle("性能选项")
        dialog.setMinimumWidth(400)
        
        layout = QVBoxLayout(dialog)
        
        # 低内存模式选项
        memory_group = QGroupBox("内存管理")
        memory_layout = QVBoxLayout()
        
        low_memory_checkbox = QCheckBox("启用低内存模式")
        low_memory_checkbox.setChecked(self.low_memory_mode)
        low_memory_checkbox.setToolTip("分块加载大文件，减少内存占用")
        
        chunk_size_label = QLabel("分块大小:")
        chunk_size_combo = QComboBox()
        chunk_size_combo.addItems(["10,000行", "50,000行", "100,000行", "200,000行"])
        chunk_size_combo.setCurrentIndex(1)  # 默认50,000行
        
        memory_layout.addWidget(low_memory_checkbox)
        memory_layout.addWidget(chunk_size_label)
        memory_layout.addWidget(chunk_size_combo)
        memory_group.setLayout(memory_layout)
        
        # 表格性能选项
        table_group = QGroupBox("表格显示")
        table_layout = QVBoxLayout()
        
        lazy_loading = QCheckBox("懒加载模式")
        lazy_loading.setChecked(True)
        lazy_loading.setToolTip("仅在滚动到可见区域时加载数据")
        
        preload_size = QLabel("预加载行数:")
        preload_combo = QComboBox()
        preload_combo.addItems(["100行", "500行", "1,000行", "5,000行"])
        preload_combo.setCurrentIndex(1)  # 默认500行
        
        table_layout.addWidget(lazy_loading)
        table_layout.addWidget(preload_size)
        table_layout.addWidget(preload_combo)
        table_group.setLayout(table_layout)
        
        layout.addWidget(memory_group)
        layout.addWidget(table_group)
        
        # 当前内存使用情况
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss / 1024 / 1024  # MB
            memory_info = QLabel(f"当前内存使用: {memory_usage:.1f} MB")
        except:
            memory_info = QLabel("当前内存使用: 未知")
        layout.addWidget(memory_info)
        
        # 按钮
        button_layout = QHBoxLayout()
        save_button = QPushButton("保存")
        save_button.clicked.connect(dialog.accept)
        cancel_button = QPushButton("取消")
        cancel_button.clicked.connect(dialog.reject)
        
        button_layout.addWidget(save_button)
        button_layout.addWidget(cancel_button)
        layout.addLayout(button_layout)
        
        # 执行对话框
        if dialog.exec_() == QDialog.Accepted:
            # 保存设置
            self.low_memory_mode = low_memory_checkbox.isChecked()
            self.settings.setValue("low_memory_mode", self.low_memory_mode)
            
            # 获取分块大小设置
            chunk_size_text = chunk_size_combo.currentText()
            chunk_size = int(chunk_size_text.split(',')[0].replace(',', ''))
            self.settings.setValue("chunk_size", chunk_size)
            
            # 获取预加载设置
            preload_text = preload_combo.currentText()
            preload_rows = int(preload_text.split('行')[0].replace(',', ''))
            self.settings.setValue("preload_rows", preload_rows)
            
            QMessageBox.information(self, "设置已保存", "新的性能设置将在下次加载文件时生效。")
            
    def show_preview_context_menu(self, position):
        """显示预览表格的上下文菜单"""
        self.show_context_menu(position, is_preview=True)
            
    def show_context_menu(self, position, is_preview=False):
        """显示表格上下文菜单"""
        menu = QMenu()
        
        # 确定使用哪个表格
        table_view = self.preview_table if is_preview else self.table
        
        # 获取当前选中的行
        selected_indexes = table_view.selectedIndexes()
        if selected_indexes:
            copy_cell = QAction('复制单元格内容', self)
            copy_cell.triggered.connect(lambda: self.copy_cell_content(is_preview))
            menu.addAction(copy_cell)
            
            copy_row = QAction('复制整行内容', self)
            copy_row.triggered.connect(lambda: self.copy_row_content(is_preview))
            menu.addAction(copy_row)
            
            if is_preview:
                export_action = QAction('导出搜索结果', self)
                export_action.triggered.connect(self.export_results)
                menu.addAction(export_action)
            
            menu.exec_(table_view.mapToGlobal(position))
            
    def copy_cell_content(self, is_preview=False):
        """复制单元格内容"""
        table_view = self.preview_table if is_preview else self.table
        indexes = table_view.selectedIndexes()
        if not indexes:
            return
            
        # 获取单元格数据
        model = table_view.model()
        cell_text = model.data(indexes[0], Qt.DisplayRole)
        
        # 复制到剪贴板
        if cell_text:
            clipboard = QApplication.clipboard()
            clipboard.setText(cell_text)
            
    def copy_row_content(self, is_preview=False):
        """复制整行内容"""
        table_view = self.preview_table if is_preview else self.table
        selection_model = table_view.selectionModel()
        if not selection_model.hasSelection():
            return
            
        # 获取选中的行
        row = selection_model.selectedRows()[0].row()
        model = table_view.model()
        
        # 收集行数据
        row_data = []
        for col in range(model.columnCount()):
            index = model.index(row, col)
            cell_text = model.data(index, Qt.DisplayRole)
            row_data.append(cell_text if cell_text else '')
            
        # 复制到剪贴板
        clipboard = QApplication.clipboard()
        clipboard.setText('\t'.join(row_data))

def main():
    app = QApplication(sys.argv)
    window = DataSeek()
    window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
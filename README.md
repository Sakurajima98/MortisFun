# walnutMortis / Mortisfun

这是一个“ Python WebSocket/服务端”的项目，

- Python 服务端（WebSocket 长连接 + 多服务编排）
  - `app.py`：WebSocket 服务主入口，初始化配置、日志、DataManager，并按配置装配多种服务；同时启动定时/后台任务
  - `data_manager.py`：文件型数据访问层
  - `service/`：业务服务集合
  - `utils/`：通用工具

## 快速开始

 - 不建议部署

### 1) 准备运行环境

- Python：用于启动 WebSocket 与定时/后台服务（若只使用 PHP Web 前端展示，Python 也可能仍需要用于数据产出）

### 2) 配置数据根目录

> 实际所需的目录结构以你启用的功能模块为准。

### 3) 启动 Python WebSocket 服务

在项目根目录执行：

```bash
python app.py
```

依赖安装（按项目 import 需求，至少建议安装）：

```bash
pip install websockets pillow requests aiohttp
```

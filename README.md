# 新闻联播·金融速览 AI

自动采集央视《新闻联播》，AI 多智能体流水线深度分析，生成竖屏金融解读视频。

## 效果

一个日期输入 → 一条竖屏解读视频。

```
xwlb_tool 采集          LangGraph 7节点流程                     输出
─────────────       ────────────────────────────            ─────────
下载完整视频   ─┬─→ curator   (筛选4-6条)                       final_video.mp4
切分16个片段   ─┤       │                                    curator_result.json
提取口播文字   ─┘  verify ←─ fix (最多2轮)                    analyses.json
                        │         │                          script.json
                   script → outro → compose              verifications.json
```

每个节点有对应的 prompt 文件。compose 节点遵循 `composer.txt` 中的布局常量、字幕规则、水印规范和 7 项渲染后校验。

---

## 快速开始

### 环境准备

```bash
brew install ffmpeg
pip install requests beautifulsoup4 edge-tts pillow langgraph langgraph-checkpoint
```

### 运行

**方式一：Claude Code（推荐，一句话）**

在 Claude Code 中直接说：

```
生成2026年6月25日新闻联播AI解读
```

Claude Code 会自动完成：采集数据 → AI 分析 → 视频合成。约 8-10 分钟出片。

**方式二：命令行（逐步骤执行）**

```bash
# 1. 采集数据（必须加 --prefer-chapters，HLS 只有 480p）
python xwlb_tool.py 2026-06-25 -q 2000 --prefer-chapters

# 2. 运行流水线
export DEEPSEEK_API_KEY="sk-xxx"
export DEEPSEEK_BASE_URL="https://api.deepseek.com"
export DEEPSEEK_MODEL="deepseek-chat"
python flow/langgraph_pipeline.py 20260625

# 3. 查看结果
open flow/outputs/20260625_finance/final_video.mp4

# 中断恢复
python flow/langgraph_pipeline.py 20260625 --resume
```

## 项目结构

```
xwlb_tool.py                    # 数据采集：下载 + 切片 + 文字稿
│
flow/
├── langgraph_pipeline.py       # 主流水线 (LangGraph StateGraph, 7节点)
│
├── prompts/                    # Agent 提示词（大脑）
│   ├── curator.txt             #   筛选师：初筛12条 → 精选4-6条
│   ├── analyst.txt             #   分析师：证据链 + 合规红线
│   ├── verifier.txt            #   校验官：三维审计 (事实 | 合规 | 逻辑)
│   └── composer.txt            #   合成师：布局 + 字幕规范 + 7项校验
│
├── utils/                      # 工具模块（手脚）
│   ├── compose_video.py        #   竖屏合成引擎
│   ├── composer_config.py      #   布局常量
│   ├── video_blur.py           #   CCTV 水印覆盖
│   ├── tts.py                  #   语音合成 + 字幕时间轴
│   └── evidence.py             #   证据链记录
│
└── outputs/                    # 生成产物
    └── 20260625_finance/
        ├── final_video.mp4
        ├── script.json
        ├── analyses.json
        ├── curator_result.json
        └── verifications.json
```

---


## 配置

```bash

# 流水线参数（在 langgraph_pipeline.py 中修改）
CURATOR_MAX = 12            # 初筛上限
CURATOR_FINAL_MAX = 6       # 精选上限（最少4条）
ANALYST_MAX_RETRIES = 5     # 分析师自修复次数
MAX_VERIFY_ROUNDS = 2       # 校验-修正轮次上限
```

---


## 规则文档

- [composer.txt](flow/prompts/composer.txt) — 完整视频合成规则（布局/字幕/片尾/校验）
- [curator.txt](flow/prompts/curator.txt) — 两阶段新闻筛选 + 放宽重试
- [analyst.txt](flow/prompts/analyst.txt) — 分析师提示词 + 自修复指令
- [verifier.txt](flow/prompts/verifier.txt) — 三维独立校验规范

## License

MIT

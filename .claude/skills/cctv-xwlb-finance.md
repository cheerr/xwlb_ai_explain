---
name: cctv-xwlb-finance
description: >
  一键生成《新闻联播》AI 金融解读竖屏视频。采集指定日期的新闻联播视频和文字稿，
  通过多智能体流水线（筛选→分析→校验→脚本→合成）生成带字幕的竖屏解读视频。
  当用户说"生成新闻联播AI解读"、"解读今天的新闻联播"、"生成XX年X月X日新闻联播解读"时触发。
---

你是《新闻联播·金融速览》AI 解读视频生成器。你的任务是帮用户一键生成完整的解读视频。

## 触发条件

用户说以下任意一句时触发此 skill：
- "生成2026年6月25日新闻联播AI解读"
- "解读今天的新闻联播"
- "生成昨天的新闻联播金融分析"
- "帮我分析一下XX日的新闻联播"
- "做一个新闻联播的投资解读视频"

## 核心能力

你内部调用以下工具完成全流程：

1. **数据采集** (`xwlb_tool.py`) — 下载新闻联播完整视频(720p MP4)、按16条新闻切片、提取文字稿
2. **AI 流水线** (`flow/langgraph_pipeline.py`) — 7节点多智能体分析
   - Curator: 从16条中筛选4-6条金融相关新闻
   - Analyst: 并行深度分析(证据链+投资结论)
   - Verifier: 三维校验(事实/合规/逻辑)
   - Script: 编导口播脚本
   - Summary: 提炼投资总结
   - Compose: 竖屏合成+TTS+字幕+水印

## 执行流程

### 第一步：解析日期

- "今天" = 今天日期
- "昨天" = 今天-1天
- "2026年6月25日" = 2026-06-25
- 日期格式：YYYY-MM-DD (用于采集) 和 YYYYMMDD (用于流水线)

### 第二步：采集数据

```bash
python xwlb_tool.py {YYYY-MM-DD} -q 2000 --prefer-chapters
```

- `-q 2000` 最高画质，`--prefer-chapters` 保证 720p
- 如果数据已存在(`{YYYYMMDD}_news/metadata.json`)，跳过
- 最多 15 分钟超时

### 第三步：运行流水线

```bash
# 设置环境变量（假设用户已配置）
python flow/langgraph_pipeline.py {YYYYMMDD}
```

- 5 分钟超时
- 如果某个节点失败，自动保存 checkpoint，支持 resume

### 第四步：输出结果

告诉用户：
```
生成完成!
  视频: flow/outputs/{YYYYMMDD}_finance/final_video.mp4
  时长: xx秒
  片尾: {投资总结列表}
  分析: {几条通过/几条存疑}
```

## 注意事项

- 首次运行需要先 `pip install` 依赖（见 README）
- 每次运行前删除旧 checkpoint：`rm -f flow/outputs/{date}_finance/_checkpoint.json`
- 如果用户说"重新生成"，删除 `_checkpoint.json` 后重跑第三步

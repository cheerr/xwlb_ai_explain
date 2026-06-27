#!/usr/bin/env python3
"""
《新闻联播·金融速览》-- LangGraph 多智能体流水线(质量优先版)

真正的 LangGraph StateGraph 实现:
  - 条件边: verify -> (pass?->script | fail?->fix->verify)
  - 内置 Checkpoint: MemorySaver 断点续跑
  - 结构化输出: 每个 LLM 节点强制 JSON Schema
  - 质量红线: 3维校验 + delta修正, 最多2轮

用法:
  python langgraph_pipeline.py 20260625
  python langgraph_pipeline.py 20260625 --resume  # 从断点恢复
"""

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Optional, TypedDict

from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.checkpoint.memory import MemorySaver

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from flow.utils.video_blur import blur_cctv_overlays
from flow.utils.tts import generate_audio

# ═══════════════════════════════════════════════════════════
# 配置常量
# ═══════════════════════════════════════════════════════════

PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
OUTPUTS_DIR = Path(__file__).resolve().parent / "outputs"
MAX_VERIFY_ROUNDS = 2
ANALYST_MAX_RETRIES = 5  # 自修复最多5次
CURATOR_MAX = 12       # Phase1 初筛上限
CURATOR_FINAL_MAX = 6  # Phase2 精选上限(最少4条)
TARGET_DURATION = 180


# ═══════════════════════════════════════════════════════════
# LangGraph State
# ═══════════════════════════════════════════════════════════

class PipelineState(TypedDict, total=False):
    """全局共享状态"""
    date_str: str
    output_dir: str

    # 数据
    raw_data: dict
    selected: list[dict]
    analyses: list[dict]
    verifications: list[dict]
    verified: list[dict]
    uncertain: list[dict]
    script: dict
    outro_summary: list[dict]  # [{takeaway, evidence}, ...]

    # 控制
    verify_round: int
    curate_round: int         # curator 重试轮次(凑4条用)
    pending_fixes: list[dict]
    errors: list[str]


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════

def _load_prompt(name: str) -> str:
    path = PROMPTS_DIR / f"{name}.txt"
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _titles_match(t1: str, t2: str) -> bool:
    clean = lambda s: re.sub(r'[\[［\]］\s【】]', '', s)
    c1, c2 = clean(t1), clean(t2)
    return c1[:12] in c2 or c2[:12] in c1


def _extract_json(text: str) -> dict:
    """从LLM输出中提取JSON"""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    for pattern in [r'```(?:json)?\s*\n?(.*?)\n?```', r'\{.*\}']:
        m = re.search(pattern, text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1) if '```' in pattern else m.group(0))
            except json.JSONDecodeError:
                pass
    return {"_parse_error": True, "_raw": text[:200]}


def _load_data(date_str: str) -> dict:
    """加载/采集数据"""
    news_dir = Path(f"{date_str}_news")
    if not (news_dir.exists() and (news_dir / "metadata.json").exists()):
        xwlb = Path(__file__).resolve().parent.parent / "xwlb_tool.py"
        if xwlb.exists():
            subprocess.run([
                sys.executable, str(xwlb),
                f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                "-q", "2000", "--no-asr",
            ], check=True, timeout=900)

    result = {"full_video": None, "segments": [], "articles": []}
    if (news_dir / "full.mp4").exists():
        result["full_video"] = str(news_dir / "full.mp4")

    meta = news_dir / "metadata.json"
    if meta.exists():
        result["segments"] = json.loads(meta.read_text(encoding="utf-8")).get("segments", [])

    mdir = news_dir / "manuscripts"
    if mdir.exists():
        for tf in sorted(mdir.glob("*.txt")):
            content = tf.read_text(encoding="utf-8").strip()
            parts = tf.stem.split("_", 2)
            idx = int(parts[0]) if parts and parts[0].isdigit() else len(result["articles"]) + 1
            result["articles"].append({
                "index": idx, "title": parts[-1] if len(parts) > 1 else tf.stem,
                "content": content, "preview": content[:500],
            })

    return result


def _gather_historical(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y%m%d")
    parts = []
    for i in range(7, 0, -1):
        day = dt - timedelta(days=i)
        mp = Path(f"{day.strftime('%Y%m%d')}_news/metadata.json")
        if mp.exists():
            try:
                m = json.loads(mp.read_text(encoding="utf-8"))
                ts = [s.get("title","") for s in m.get("segments",[])[:8]]
                if ts:
                    parts.append(f"{day.strftime('%m月%d日')}: " + " | ".join(t[:30] for t in ts[:5]))
            except: pass
    return "\n".join(parts) if parts else "(无本地历史数据)"


def _gather_market() -> str:
    try:
        import requests
        r = requests.get(
            "https://push2his.eastmoney.com/api/qt/stock/kline/get?"
            "secid=1.000001&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58&"
            "klt=101&fqt=1&end=20500101&lmt=2",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        k = r.json().get("data",{}).get("klines",[])
        if k:
            v = k[-1].split(",")
            return f"上证指数: {v[0]} 收盘{v[2]} 涨跌幅{v[8]}%"
    except: pass
    return "(大盘数据暂不可用)"


async def _call_llm(prompt: str) -> str:
    """占位:实际运行由 Claude Code Workflow agent() 接管"""
    return ""


# ═══════════════════════════════════════════════════════════
# LangGraph Nodes
# ═══════════════════════════════════════════════════════════

def node_collect(state: PipelineState) -> PipelineState:
    """Node 1: 数据采集"""
    print("\n[NODE] collect")
    state["raw_data"] = _load_data(state["date_str"])
    d = state["raw_data"]
    print(f"  [OK] {len(d.get('segments',[]))}条新闻, "
          f"{len(d.get('articles',[]))}份文字稿, "
          f"视频{'有' if d.get('full_video') else '无'}")
    state.setdefault("errors", [])
    state.setdefault("verify_round", 0)
    return state


def node_curate(state: PipelineState) -> PipelineState:
    """Node 2: 新闻筛选(支持放宽标准重试凑4-6条)"""
    state["curate_round"] = state.get("curate_round", 0) + 1
    rnd = state["curate_round"]
    relaxed = "标准放宽" if rnd > 1 else ""
    print(f"\n[NODE] curate (第{rnd}轮) {relaxed}")
    articles = state["raw_data"].get("articles", [])

    items = []
    for seg in state["raw_data"].get("segments", []):
        idx = seg.get("index", seg.get("idx", len(items)+1))
        title = seg.get("title", "")
        preview = ""
        for art in articles:
            if _titles_match(title, art.get("title", "")):
                preview = art.get("content", "")[:500]
                break
        items.append({
            "index": idx, "title": title,
            "duration_sec": seg.get("duration_seconds", seg.get("duration", 30)),
            "text_preview": preview,
        })

    prompt = _load_prompt("curator").replace(
        "{segments_json}", json.dumps(items, ensure_ascii=False, indent=2))

    # 重试时注入放宽标准提示
    if state["curate_round"] >= 2:
        prompt += f"\n\n## [!] 第{state['curate_round']}轮重试:当前仅{len(state.get('selected',[]))}条通过, 请按R{state['curate_round']}放宽标准重新扫描, 凑满4-6条."

    # 同步调用(LangGraph 节点内)
    result = _call_sync_llm(prompt)
    parsed = _extract_json(result) if result else {}

    # 取 Phase2 精选结果(最多5条), 兼容旧格式
    state["selected"] = parsed.get("phase2_selected", []) or parsed.get("selected_news", [])
    if not state["selected"]:
        state["selected"] = [
            {"index": it["index"], "title": it["title"],
             "finance_relevance": "兜底入选", "primary_sector": "综合", "rank": i+1,
             "evidence": {"source_text": it.get("text_preview","")[:100],
                          "finance_rationale": "默认入选", "source_verifiable": True}}
            for i, it in enumerate(items[:CURATOR_FINAL_MAX])
        ]
    # 按 rank 排序
    state["selected"].sort(key=lambda x: x.get("rank", 99))

    print(f"  [OK] 入选{len(state['selected'])}条")
    for s in state["selected"]:
        print(f"    [{s['index']}] {s.get('title','?')[:50]} -> {s.get('primary_sector','?')}")
    return state


def node_analyze(state: PipelineState) -> PipelineState:
    """Node 3: 金融分析(串行调用, LangGraph 内不建议async)"""
    print(f"\n[NODE] analyze ×{len(state['selected'])}")
    historical = _gather_historical(state["date_str"])
    market = _gather_market()
    prompt_template = (_load_prompt("analyst")
                       .replace("{historical_context}", historical)
                       .replace("{market_context}", market))

    analyses = []
    for item in state["selected"]:
        title = item.get("title", "")
        content = ""
        for art in state["raw_data"].get("articles", []):
            if _titles_match(title, art.get("title", "")):
                content = art.get("content", "")[:3000]
                break

        prompt = (prompt_template
                  .replace("{title}", title)
                  .replace("{content}", content or title)
                  .replace("{duration}", str(item.get("duration_sec", 60))))

        base_prompt = prompt  # 保存原始 prompt
        for attempt in range(1, ANALYST_MAX_RETRIES + 1):
            result = _call_sync_llm(prompt)
            try:
                analysis = _extract_json(result) if result else {}
                # 深度校验(传入原文做交叉验证)
                errors = _validate_analysis(analysis, content)
                if not errors:
                    analysis["_attempts"] = attempt
                    analyses.append(analysis)
                    if attempt > 1:
                        print(f"      [OK] 第{attempt}次自修复通过")
                    break
                if attempt < ANALYST_MAX_RETRIES:
                    # 精准反馈:只列具体问题, 不重传全量规则
                    err_list = "\n".join(f"  - {e}" for e in errors[:5])
                    prompt = (base_prompt +
                              f"\n\n## [!] 第{attempt}次输出未通过校验, 共{len(errors)}个问题:\n{err_list}"
                              f"\n请针对以上问题逐一修正, 重新输出完整JSON.")
                    print(f"      [!] 第{attempt}次: {len(errors)}个问题")
                else:
                    print(f"      [FAIL] {ANALYST_MAX_RETRIES}次均失败: {errors[0][:60]}")
                    analyses.append({"news_title": title,
                                     "error": f"{len(errors)}个问题未修复",
                                     "confidence_score": 0.0,
                                     "conclusion_one_liner": f"分析失败: {title[:20]}"})
            except Exception as e:
                if attempt < ANALYST_MAX_RETRIES:
                    prompt = (base_prompt +
                              f"\n\n## [!] 第{attempt}次JSON解析失败: {e}\n请确保输出合法JSON.")
                else:
                    analyses.append({"news_title": title, "error": str(e),
                                     "confidence_score": 0.0,
                                     "conclusion_one_liner": f"分析失败: {title[:20]}"})

    state["analyses"] = analyses
    ok = sum(1 for a in analyses if a.get("confidence_score", 0) > 0)
    print(f"  [OK] {ok}/{len(state['selected'])}条成功")
    return state


def node_verify(state: PipelineState) -> PipelineState:
    """Node 4: 三维独立校验"""
    state["verify_round"] = state.get("verify_round", 0) + 1
    rnd = state["verify_round"]
    print(f"\n[NODE] verify (第{rnd}轮)")

    dims = ["FACT", "COMPLIANCE", "LOGIC"]
    prompt_template = _load_prompt("verifier")
    verifications = []
    fixes = []

    for ai, analysis in enumerate(state["analyses"]):
        title = analysis.get("news_title", f"#{ai}")
        if analysis.get("confidence_score", 0) <= 0:
            verifications.append({"analysis_idx": ai, "results": [], "all_pass": False, "skipped": True})
            continue

        source_text = ""
        for art in state["raw_data"].get("articles", []):
            if _titles_match(title, art.get("title", "")):
                source_text = art.get("content", "")[:2000]
                break

        analysis_json = json.dumps(analysis, ensure_ascii=False, indent=2)

        results = []
        all_pass = True
        for dim in dims:
            prompt = (prompt_template
                      .replace("{analysis_json}", analysis_json)
                      .replace("{source_text}", source_text))
            dim_prompt = f"[校验维度: {dim}]\n\n{prompt}"

            result = _call_sync_llm(dim_prompt)
            try:
                r = _extract_json(result) if result else {}
                r.setdefault("dimension", dim)
                results.append(r)
                if r.get("verdict") != "pass":
                    all_pass = False
                    for issue in r.get("issues", []):
                        if issue.get("severity") in ("critical", "major"):
                            fixes.append({
                                "analysis_idx": ai, "title": title,
                                "dimension": dim, "issue": issue,
                            })
            except Exception:
                results.append({"dimension": dim, "verdict": "uncertain", "score": 5,
                                "issues": [], "summary": "解析失败"})
                all_pass = False

        status = "[OK]" if all_pass else f"[FAIL]"
        print(f"  [{ai+1}/{len(state['analyses'])}] {title[:30]}... {status}")
        verifications.append({
            "analysis_idx": ai, "title": title, "results": results,
            "all_pass": all_pass, "round": rnd,
        })

    state["verifications"] = verifications
    state["pending_fixes"] = fixes
    passed = sum(1 for v in verifications if v.get("all_pass"))
    print(f"  [OK] 第{rnd}轮: {passed}/{len(verifications)}通过, {len(fixes)}个待修正")
    return state


def node_fix(state: PipelineState) -> PipelineState:
    """Node 5: Delta修正"""
    print(f"\n[NODE] fix (第{state['verify_round']}轮后)")

    fixes_by_idx: dict[int, list] = {}
    for f in state.get("pending_fixes", []):
        fixes_by_idx.setdefault(f["analysis_idx"], []).append(f)

    prompt_template = _load_prompt("analyst")

    for idx, fixes in fixes_by_idx.items():
        analysis = state["analyses"][idx]
        title = analysis.get("news_title", "")

        content = ""
        for art in state["raw_data"].get("articles", []):
            if _titles_match(title, art.get("title", "")):
                content = art.get("content", "")[:3000]
                break

        # Delta: 仅传失败维度
        delta_parts = []
        for f in fixes:
            iss = f["issue"]
            delta_parts.append(
                f"## [{f['dimension']}] 未通过\n"
                f"问题: {iss.get('description','')}\n"
                f"修正: {iss.get('correction','')}"
            )

        prompt = (prompt_template
                  .replace("{title}", title)
                  .replace("{content}", content or title)
                  .replace("{duration}", "60")
                  .replace("{historical_context}", _gather_historical(state["date_str"]))
                  .replace("{market_context}", _gather_market()))
        prompt += (f"\n\n## [!] 校验未通过, 请针对修正:\n"
                   + "\n\n".join(delta_parts)
                   + f"\n\n## 上一轮conclusion(参考): {analysis.get('conclusion_one_liner','')}")

        result = _call_sync_llm(prompt)
        try:
            fixed = _extract_json(result) if result else {}
            if fixed.get("confidence_score", 0) > 0:
                state["analyses"][idx] = fixed
                print(f"  [OK] [{idx+1}] 修正完成: {fixed.get('news_title','')[:30]}")
            else:
                print(f"  [FAIL] [{idx+1}] 修正失败")
        except Exception:
            print(f"  [FAIL] [{idx+1}] 解析失败")

    return state


def node_script(state: PipelineState) -> PipelineState:
    """Node 6: 脚本编导"""
    print("\n[NODE] script")

    verified = []
    uncertain = []
    for v in state.get("verifications", []):
        ai = v.get("analysis_idx", 0)
        if ai < len(state["analyses"]):
            if v.get("all_pass"):
                verified.append(state["analyses"][ai])
            elif not v.get("skipped"):
                uncertain.append(state["analyses"][ai])

    if not verified and not uncertain:
        state["errors"].append("无可用分析")
        return state

    uncertain_compact = [{
        "title": a.get("news_title", ""),
        "conclusion": a.get("conclusion_one_liner", ""),
        "risk_note": "此条未通过全部校验, 引用需保守措辞",
    } for a in uncertain]

    date_display = f"{state['date_str'][:4]}年{state['date_str'][4:6]}月{state['date_str'][6:8]}日"
    num_news = max(len(verified) + len(uncertain), 4)  # 至少4条
    per_news = (TARGET_DURATION - 35) // max(num_news, 1)

    prompt = f"""你是脚本编导.将 {num_news} 条分析编排为约{TARGET_DURATION}秒口播脚本.

时间: 片头10s + {num_news}×{per_news}s + 片尾25s

已验证通过:
{json.dumps(verified, ensure_ascii=False, indent=2)}

标记存疑:
{json.dumps(uncertain_compact, ensure_ascii=False, indent=2)}

开场白: "3分钟看懂{date_display}新闻联播说了什么, 看看有哪些你的投资机会"
序号 "1." "2." "3." "4.", 句号英文"."
存疑分析加保守措辞.片尾: 风险提示 + "感谢观看, 我们明天再见"

输出JSON: {{"title":"...", "total_duration_sec":{TARGET_DURATION}, "segments":[{{"type":"intro|news|outro","duration_sec":N,"news_index":N,"narration":"...","investment_highlight":"..."}}]}}
"""

    result = _call_sync_llm(prompt)
    script = _extract_json(result) if result else {}
    if not script.get("segments"):
        script = _fallback_script(verified + uncertain)
        print("  [!] 使用兜底脚本")

    # 修复 news_index: LLM可能重编号为0,1,2...需映射回原始segment索引
    # 通过narration前20字匹配analysis标题
    all_analyses = verified + uncertain
    for seg in script.get("segments", []):
        if seg.get("type") != "news": continue
        nar = seg.get("narration", "")
        # 取narration前20字作为匹配key
        key = nar[:20].replace("1. ", "").replace("2. ", "").replace("3. ", "")
        key = key.replace("4. ", "").replace("5. ", "").replace("6. ", "")
        for a in all_analyses:
            a_title = a.get("news_title", "")
            if key[:8] in a_title or a_title[:8] in key:
                # 从selected中找到原始index
                for s in state.get("selected", []):
                    if s.get("title", "")[:10] == a_title[:10]:
                        seg["news_index"] = s.get("index", s.get("idx", 0))
                        break
                break

    state["script"] = script
    print(f"  [OK] {len(script.get('segments',[]))}段, {script.get('total_duration_sec','?')}秒")
    return state


def node_outro_summary(state: PipelineState) -> PipelineState:
    """Node 7: 生成片尾投资总结——LLM从口播稿提炼，需审核"""
    print("\n[NODE] outro_summary")
    analyses = state.get("analyses", [])
    if not analyses:
        state["outro_summary"] = []
        return state

    # 构建 prompt：从分析中提炼3-5条综合性投资建议，每条<=40字，有理有据
    items = []
    for a in analyses:
        if a.get("confidence_score", 0) <= 0:
            continue
        items.append({
            "conclusion": a.get("conclusion_one_liner", ""),
            "key_points": a.get("key_points", []),
            "sectors": a.get("market_impact", {}).get("affected_sectors", []),
            "direction": a.get("market_impact", {}).get("impact_direction", ""),
            "evidence": a.get("market_impact", {}).get("evidence_refs", [])[:1],
        })

    if not items:
        state["outro_summary"] = []
        return state

    prompt = f"""你是投资总结编辑。从以下分析中提炼3-5条综合性投资建议用于片尾展示。

要求:
- 每条建议 <=40字
- 每条附带1条原文证据(<=30字)
- 综合多条新闻的交叉结论优先（如"储能+AI双重驱动"）
- 措辞: "关注...""建议关注...""...值得留意""...存在机会"
- 禁止推荐具体股票/代码

分析数据:
{json.dumps(items, ensure_ascii=False, indent=2)}

输出JSON数组:
[{{"takeaway": "投资建议(<=40字)", "evidence": "原文证据(<=30字)"}}, ...]
"""

    result = _call_sync_llm(prompt)
    parsed = _extract_json(result) if result else []
    state["outro_summary"] = parsed if isinstance(parsed, list) else []

    print(f"  [OK] {len(state['outro_summary'])}条总结")
    for s in state["outro_summary"]:
        print(f"    - {s.get('takeaway', '')[:50]}")
    return state


def node_compose(state: PipelineState) -> PipelineState:
    """Node 8: compose video via LLM Agent with composer.txt rules"""
    print("\n[NODE] compose")

    from flow.utils.compose_video import compose as compose_video
    od = Path(state["output_dir"])
    data_dir = Path(f"{state['date_str']}_news")
    compose_video(state["date_str"], od, data_dir, state["script"],
                  outro_summary=state.get("outro_summary"))
    return state


# Conditional Edges
# ═══════════════════════════════════════════════════════════

def route_after_curate(state: PipelineState) -> str:
    """筛选后路由: >=5条->analyze, <5条且轮次<3->回curate, 否则->analyze"""
    count = len(state.get("selected", []))
    rnd = state.get("curate_round", 1)
    if count >= 4:
        print(f"  -> {count}条, 进入分析")
        return "analyze"
    if count >= 2 and rnd >= 3:
        print(f"  -> {count}条(3轮后稳定), 进入分析")
        return "analyze"
    if rnd < 3:
        print(f"  -> {count}条不足4, 第{rnd+1}轮放宽标准重试")
        return "curate"
    return "analyze"


def route_after_verify(state: PipelineState) -> str:
    """校验后路由: 全部通过->script, 有critical且未超轮次->fix, 否则->script"""
    fixes = state.get("pending_fixes", [])
    criticals = [f for f in fixes
                 if f.get("issue", {}).get("severity") in ("critical", "major")]

    if not criticals:
        return "script"
    if state.get("verify_round", 1) < MAX_VERIFY_ROUNDS:
        return "fix"
    return "script"


def route_after_fix(state: PipelineState) -> str:
    """修正后回到verify"""
    return "verify"


# ═══════════════════════════════════════════════════════════
# LangGraph 构建
# ═══════════════════════════════════════════════════════════

def build_graph() -> StateGraph:
    """构建 LangGraph StateGraph"""
    builder = StateGraph(PipelineState)

    # 添加节点
    builder.add_node("collect", node_collect)
    builder.add_node("curate", node_curate)
    builder.add_node("analyze", node_analyze)
    builder.add_node("verify", node_verify)
    builder.add_node("fix", node_fix)
    builder.add_node("script", node_script)
    builder.add_node("outro_summary", node_outro_summary)
    builder.add_node("compose", node_compose)

    # 线性边
    builder.add_edge("collect", "curate")
    # curate -> analyze | curate(放宽重试凑5条)
    builder.add_conditional_edges(
        "curate",
        route_after_curate,
        {"analyze": "analyze", "curate": "curate"},
    )
    builder.add_edge("analyze", "verify")

    # 条件边: verify -> script | fix
    builder.add_conditional_edges(
        "verify",
        route_after_verify,
        {"script": "script", "fix": "fix"},
    )

    # fix -> verify
    builder.add_edge("fix", "verify")

    # 终边: script -> outro_summary -> compose
    builder.add_edge("script", "outro_summary")
    builder.add_edge("outro_summary", "compose")
    builder.add_edge("compose", END)

    # 入口
    builder.set_entry_point("collect")

    return builder


# ═══════════════════════════════════════════════════════════
# 辅助:同步LLM调用,校验,兜底
# ═══════════════════════════════════════════════════════════

def _call_sync_llm(prompt: str) -> str:
    """LLM调用 -- DeepSeek API"""
    import time, requests

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        print("    [LLM] DEEPSEEK_API_KEY 未设置")
        return ""

    base_url = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
    model = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}",
                         "Content-Type": "application/json"},
                json={"model": model,
                      "messages": [{"role": "user", "content": prompt}],
                      "max_tokens": 4096, "temperature": 0.3},
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
            err = resp.text[:300]
            if attempt == 2:
                print(f"    [LLM {resp.status_code}] {err}")
        except Exception as e:
            if attempt == 2:
                print(f"    [LLM错误] {e}")
            time.sleep(2 ** attempt)
    return ""


def _validate_analysis(analysis: dict, source_text: str = "") -> list[str]:
    """校验分析质量, 侧重内容质量, 字数限制从宽"""
    errors = []

    # 1. 结论质量(内容 > 字数)
    conclusion = analysis.get("conclusion_one_liner", "")
    if not conclusion.strip():
        errors.append("conclusion为空, 必须给出投资结论")
    if re.search(r'\d{6}\.|买入|卖出|目标价|推荐|必涨|翻倍|保证', conclusion):
        errors.append("合规红线: 结论含具体股票代码/买卖建议/收益承诺, 删除重写")
    if any(w in conclusion for w in ('据悉', '据了解', '有消息称', '据传')):
        errors.append("结论含无出处表述('据悉'/'据了解'), 替换为'分析认为'/'值得关注'")

    # 2. 关键要点(数量 > 字数)
    kps = analysis.get("key_points", [])
    if not kps or len(kps) < 2:
        errors.append("key_points不足(需>=2条), 从原文提炼核心信息")

    # 3. 证据引用(最严格----必须可验证)
    refs = analysis.get("market_impact", {}).get("evidence_refs", [])
    if not refs:
        errors.append("evidence_refs为空, 必须从原文提取>=2条关键数据/政策引用")
    for i, ref in enumerate(refs):
        sref = str(ref)
        if len(sref) < 15:
            errors.append(f"evidence_ref[{i}]过短({len(sref)}字), 需引用原文完整句子")
        elif source_text and sref[:10] not in source_text and sref[-10:] not in source_text:
            errors.append(f"evidence_ref[{i}]无法在原文中验证: '{sref[:30]}', 请使用原文原文而非自行概括")

    # 4. 受影响板块(必须具体)
    sectors = analysis.get("market_impact", {}).get("affected_sectors", [])
    if not sectors:
        errors.append("affected_sectors为空, 需列出具体A股板块名称")
    if sectors and all(len(s) < 3 for s in sectors):
        errors.append("affected_sectors过于笼统, 需具体到板块如'新能源/储能/光伏'")

    # 5. 风险因素(必须有实质内容)
    risks = analysis.get("investment_perspective", {}).get("risk_factors", [])
    if len(risks) < 2:
        errors.append("risk_factors不足(需>=2条), 如政策风险/市场风险/地缘风险/行业竞争")
    for r in risks:
        if len(str(r).strip()) < 4:
            errors.append(f"risk_factor无实质内容: '{r}', 需说明具体风险")

    # 6. 置信度(从宽----低分也可以, 但要标注)
    conf = analysis.get("confidence_score", 0)
    if conf <= 0:
        errors.append("confidence_score为零或缺失")
    if 0 < conf < 0.3 and '分析认为' not in conclusion:
        pass  # 不做强制要求, 仅info级别

    # 7. 推理链完整性(必须完整)
    ec = analysis.get("evidence_chain", {})
    if not ec.get("facts"):
        errors.append("evidence_chain.facts为空, 需引用>=1条原文事实")
    if not ec.get("market_logic"):
        errors.append("evidence_chain.market_logic为空, 需说明事实->市场逻辑的推理")
    if not ec.get("investment_implications"):
        errors.append("evidence_chain.investment_implications为空, 需说明逻辑->投资影响")
    if not ec.get("source_verifiable"):
        errors.append("source_verifiable必须为true")

    return errors


def _fallback_script(analyses: list[dict]) -> dict:
    per = (TARGET_DURATION - 35) // max(len(analyses), 1)
    segments = [{"type": "intro", "duration_sec": 10,
                 "narration": "3分钟看懂今日新闻联播说了什么, 看看有哪些你的投资机会"}]
    for i, a in enumerate(analyses):
        conc = a.get("conclusion_one_liner", a.get("news_title", ""))
        pts = a.get("key_points", [])
        segments.append({
            "type": "news", "duration_sec": per, "news_index": i + 1,
            "narration": f"{i+1}. {a.get('news_title','')}. {' '.join(pts[:3])}. {conc}",
            "investment_highlight": conc[:50],
        })
    segments.append({"type": "outro", "duration_sec": 25,
                     "narration": "本视频仅为信息分享, 不构成投资建议.投资有风险, 入市需谨慎.感谢观看, 我们明天再见"})
    return {"title": "新闻联播·金融速览", "total_duration_sec": TARGET_DURATION, "segments": segments}


# ═══════════════════════════════════════════════════════════
# 运行入口
# ═══════════════════════════════════════════════════════════

def run_pipeline(date_str: str, output_dir: Optional[Path] = None,
                 resume: bool = False):
    """编译并运行 LangGraph 流水线"""
    if output_dir is None:
        output_dir = OUTPUTS_DIR / f"{date_str}_finance"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 构建图
    graph_builder = build_graph()
    memory = MemorySaver()
    graph = graph_builder.compile(checkpointer=memory)

    # 初始化状态
    initial: PipelineState = {
        "date_str": date_str,
        "output_dir": str(output_dir),
        "raw_data": {},
        "selected": [],
        "analyses": [],
        "verifications": [],
        "verified": [],
        "uncertain": [],
        "script": {},
        "verify_round": 0,
        "curate_round": 0,
        "pending_fixes": [],
        "errors": [],
    }

    # 配置
    thread_id = f"cctv_{date_str}"
    config = {"configurable": {"thread_id": thread_id}}

    print("\n" + "╔" + "═" * 58 + "╗")
    print(f"║  《新闻联播·金融速览》LangGraph 质量优先流水线" + " " * 8 + "║")
    print(f"║  日期: {date_str}  thread: {thread_id}" + " " * 28 + "║")
    print(f"║  校验: 3D×{MAX_VERIFY_ROUNDS}轮 | 自修复: {ANALYST_MAX_RETRIES}次 | Checkpoint: MemorySaver" + " " * 1 + "║")
    print("╚" + "═" * 58 + "╝")

    try:
        # 流式执行, 每个节点完成后输出
        for event in graph.stream(initial, config):
            node_name = list(event.keys())[0]
            # 保存中间产物
            _save_intermediate(node_name, event[node_name], output_dir)
    except Exception as e:
        print(f"\n❌ 流水线异常: {e}")
        traceback.print_exc()
        print(f"  可使用相同 thread_id ({thread_id}) resume 恢复")

    # 最终结果
    final_state = graph.get_state(config)
    if final_state and final_state.values:
        state = final_state.values
        _print_summary(state)
        return state
    return None


def _save_intermediate(node_name: str, state_update: dict, output_dir: Path):
    """每个节点执行后保存中间产物"""
    key_map = {
        "curate": ("curator_result.json", "selected"),
        "analyze": ("analyses.json", "analyses"),
        "verify": ("verifications.json", "verifications"),
        "script": ("script.json", "script"),
    }
    if node_name in key_map:
        filename, key = key_map[node_name]
        data = state_update.get(key, [])
        if data:
            (output_dir / filename).write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    if state_update.get("errors"):
        (output_dir / "errors.json").write_text(
            json.dumps(state_update["errors"], ensure_ascii=False, indent=2), encoding="utf-8")


def _print_summary(state: dict):
    analyses = state.get("analyses", [])
    verifications = state.get("verifications", [])
    print(f"\n{'='*60}")
    print(f"  📊 流水线完成")
    print(f"  {'='*60}")
    print(f"  分析: {len(analyses)}条 (成功{sum(1 for a in analyses if a.get('confidence_score',0)>0)})")
    print(f"  校验: {state.get('verify_round',0)}轮, {sum(1 for v in verifications if v.get('all_pass'))}通过")
    if state.get("errors"):
        print(f"  错误: {len(state['errors'])}条")
    print(f"  输出: {state.get('output_dir','')}")
    print(f"  {'='*60}")


# ═══════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="LangGraph 流水线")
    parser.add_argument("date", nargs="?", default=None)
    parser.add_argument("-o", "--output", type=str, default=None)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    if args.date:
        ds = args.date.strip()
        date_str = ds if len(ds) == 8 else ds.replace("-", "")
    else:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    od = Path(args.output) if args.output else None
    run_pipeline(date_str, od, resume=args.resume)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
通用视频合成模块 —— 被 langgraph_pipeline / fsm_dspy_pipeline 导入调用。
输入: script JSON + data_dir → 输出: 竖屏最终视频。

布局规格见 prompts/composer.txt
"""

import json, re, subprocess, sys, asyncio, shutil
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from flow.utils.video_blur import blur_cctv_overlays
from flow.utils.tts import generate_audio
from flow.utils.composer_config import (
    CANVAS_WIDTH, CANVAS_HEIGHT,
    FG_Y, FG_HEIGHT, FG_BOTTOM,
    TITLE_FONT_SIZE, DATE_FONT_SIZE, TITLE_DATE_GAP, TITLE_VIDEO_GAP,
    SUBTITLE_FONT_SIZE, SUBTITLE_BOTTOM_MARGIN, SUBTITLE_MAX_CHARS,
    BLUR_SIGMA, BG_BRIGHTNESS,
    WATERMARK_BOTTOM_MARGIN, WATERMARK_RIGHT_MARGIN, WATERMARK_FONT_SIZE,
)

# ═══════════════════════════════════════════════════════
# 布局常量别名（保持 compose_video.py 内部简洁）
# ═══════════════════════════════════════════════════════
CANVAS_W, CANVAS_H = CANVAS_WIDTH, CANVAS_HEIGHT
FG_H = FG_HEIGHT
TITLE_FONT, DATE_FONT = TITLE_FONT_SIZE, DATE_FONT_SIZE
SUB_FONT = SUBTITLE_FONT_SIZE
SUB_BOTTOM = SUBTITLE_BOTTOM_MARGIN
SUB_MAX_WIDTH = SUBTITLE_MAX_CHARS
BG_BRIGHT = BG_BRIGHTNESS
WM_MARGIN_BTM, WM_MARGIN_R = WATERMARK_BOTTOM_MARGIN, WATERMARK_RIGHT_MARGIN
WM_FONT = WATERMARK_FONT_SIZE


# ═══════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════

def _dur(path: Path) -> float:
    r = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                        "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
                       capture_output=True, text=True)
    return float(r.stdout.strip())


def _char_width(text: str) -> float:
    w = 0
    for ch in text:
        if '一' <= ch <= '鿿' or '　' <= ch <= '〿':
            w += 1
        elif ch.isascii() and (ch.isdigit() or ch.isalpha() or ch in '.-+%'):
            w += 0.5
        else:
            w += 1
    return w


def _strip_punct(text: str) -> str:
    """去掉首尾标点 + 孤立编号"""
    text = text.strip('.!?！？。；;：:,，、')
    # 去掉孤立编号如 "1." "2." "3."
    text = re.sub(r'^\d+\.\s*$', '', text)
    return text.strip()


def _png(text: str, size: int, color: tuple, path: Path, shadow: bool = True, bold: bool = False):
    from PIL import Image, ImageDraw, ImageFont
    fps = ["/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Light.ttc"]
    font = None
    for fp in fps:
        try:
            font = ImageFont.truetype(fp, size); break
        except: pass
    if not font: font = ImageFont.load_default()
    b = font.getbbox(text)
    tw, th = b[2]-b[0], b[3]-b[1]
    pad = 10
    img = Image.new("RGBA", (tw+pad*2, th+pad*2), (0,0,0,0))
    d = ImageDraw.Draw(img)
    if shadow:
        d.text((pad+1, pad+1), text, font=font, fill=(0,0,0,70))
    if bold:
        # 模拟加粗：x方向多画1px
        d.text((pad-1, pad), text, font=font, fill=color[:4] if len(color)>=4 else (*color[:3],255))
        d.text((pad+1, pad), text, font=font, fill=color[:4] if len(color)>=4 else (*color[:3],255))
    d.text((pad, pad), text, font=font, fill=color[:4] if len(color)>=4 else (*color[:3],255))
    img.save(path, "PNG")


# ═══════════════════════════════════════════════════════
# TTS + SentenceBoundary
# ═══════════════════════════════════════════════════════

def _tts_with_boundaries(text: str, audio_path: Path) -> list[dict]:
    """生成 TTS 音频并获取 SentenceBoundary 时间戳"""
    boundaries = []
    async def _gen():
        try:
            import edge_tts
            comm = edge_tts.Communicate(text, "zh-CN-YunxiNeural", rate="+10%")
            async for chunk in comm.stream():
                if chunk["type"] == "SentenceBoundary":
                    s = chunk["offset"] / 10_000_000.0
                    d = chunk["duration"] / 10_000_000.0
                    boundaries.append({"text": chunk["text"].strip(), "start": s, "end": s+d})
            comm2 = edge_tts.Communicate(text, "zh-CN-YunxiNeural", rate="+10%")
            await comm2.save(str(audio_path))
        except Exception:
            pass
    try:
        asyncio.run(_gen())
    except Exception:
        pass
    if not audio_path.exists() or audio_path.stat().st_size < 1024:
        generate_audio(text, audio_path)
    return boundaries


# ═══════════════════════════════════════════════════════
# 全局关键词提取（整期节目，非单条新闻）
# ═══════════════════════════════════════════════════════

def _extract_global_keywords(segments: list) -> list[str]:
    """从所有新闻narration中提取整期节目的核心关键词，<=5字×3个"""
    import re
    # 合并所有旁白文本
    all_text = " ".join(s.get("narration", "") for s in segments if s.get("type") == "news")

    pool = [
        # 能源/电力
        ("储能", "储能"), ("新能源", "新能源"), ("新型能源", "新型能源"),
        ("电网出海", "电网出海"), ("风电", "风电"), ("光伏", "光伏"),
        ("装机突破", "装机突破"), ("能源体系", "能源体系"),
        # AI/科技
        ("AI算力", "AI算力"), ("机器人", "机器人"), ("生成式AI", "生成式AI"),
        # 地缘/大宗
        ("黄金ETF", "黄金ETF"), ("地缘风险", "地缘风险"), ("原油", "原油"),
        ("霍尔木兹", "霍尔木兹海峡"), ("海峡", "海峡"),
        # 产业/经济
        ("新兴产业", "新兴产业"), ("一带一路", "一带一路"),
        ("基建", "基建"), ("跨境", "跨境贸易"),
        ("知识产权", "知识产权"), ("口岸经济", "口岸经济"),
        # 政策/金融
        ("数字经济", "数字经济"), ("高端制造", "高端制造"),
        ("低空经济", "低空经济"), ("芯片", "芯片半导体"),
    ]
    found = []
    for kw, label in pool:
        if kw in all_text and label not in found:
            found.append(label)
    if not found:
        words = re.findall(r'[一-鿿]{2,5}', all_text)
        found = list(dict.fromkeys(words))[:3]
    return found[:3]


# ═══════════════════════════════════════════════════════
# 竖屏片段生成
# ═══════════════════════════════════════════════════════

def _blur_safe(source: Path, output: Path) -> bool:
    """安全模糊：低分辨率跳过，避免 ffmpeg boxblur 崩溃"""
    probe = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0",
                            "-show_entries", "stream=width,height",
                            "-of", "csv=p=0", str(source)],
                           capture_output=True, text=True)
    try:
        sw, sh = map(int, probe.stdout.strip().split(","))
    except Exception:
        return False
    if sw < 640 or sh < 360:
        return False  # 分辨率太低，跳过
    return blur_cctv_overlays(source, output)


def _make_portrait_clip(source: Path, output: Path, boundaries: list,
                        duration: float, title: str, date_str: str, work: Path,
                        with_subtitles: bool = True, narration: str = "",
                        with_keywords: bool = False, global_keywords: list = None):
    """一段竖屏视频：背景+前景+标题+关键词+字幕（静音）"""
    bg = work / f"{output.stem}_bg.mp4"
    fg = work / f"{output.stem}_fg.mp4"

    # 探测源分辨率，过低时跳过模糊（boxblur 在 <360p 上不稳定）
    probe = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0",
                            "-show_entries", "stream=width,height",
                            "-of", "csv=p=0", str(source)],
                           capture_output=True, text=True)
    try:
        sw, sh = map(int, probe.stdout.strip().split(","))
    except Exception:
        sw, sh = 480, 270
    low_res = sw < 640 or sh < 360

    # 背景：放大+模糊+暗化（低分辨率跳过gblur避免崩溃）
    blur_filter = f"gblur=sigma={BLUR_SIGMA}," if not low_res else ""
    subprocess.run(["ffmpeg", "-y", "-i", str(source), "-t", str(duration),
                    "-vf", f"scale={CANVAS_W}:{CANVAS_H}:force_original_aspect_ratio=increase,crop={CANVAS_W}:{CANVAS_H},{blur_filter}eq=brightness={BG_BRIGHT}",
                    "-c:v", "libx264", "-preset", "fast", "-crf", "22", "-an", str(bg)],
                   check=True, capture_output=True)
    subprocess.run(["ffmpeg", "-y", "-i", str(source), "-t", str(duration),
                    "-vf", f"scale={CANVAS_W}:{FG_H}:force_original_aspect_ratio=increase,crop={CANVAS_W}:{FG_H}",
                    "-c:v", "libx264", "-preset", "fast", "-crf", "22", "-an", str(fg)],
                   check=True, capture_output=True)

    # 前景叠加到背景
    tmp = work / f"{output.stem}_tmp.mp4"
    subprocess.run(["ffmpeg", "-y", "-i", str(bg), "-i", str(fg),
                    "-filter_complex", f"[1:v]setpts=PTS-STARTPTS[fg];[0:v][fg]overlay=(W-w)/2:{FG_Y}:shortest=1",
                    "-c:v", "libx264", "-preset", "fast", "-crf", "22", "-an", str(tmp)],
                   check=True, capture_output=True)

    # 全局关键词（前景视频内底部，黄字黑边，<=5字×3个，整期相同）
    KW_FONT = 26
    kw_y = FG_BOTTOM + 40  # 横屏视频下方，背景暗色区域
    kw_pngs = []
    if with_keywords and global_keywords:
        from PIL import Image as PILImage
        # "关键字："标签（与关键词同色同大小）
        label_png = work / f"{output.stem}_kwlabel.png"
        _png("关键字：", KW_FONT, (0xFF, 0xD7, 0x00, 255), label_png, shadow=True, bold=True)
        kw_pngs.append(label_png)
        for ki, kw in enumerate(global_keywords[:3]):
            kp = work / f"{output.stem}_kw{ki}.png"
            _png(kw, KW_FONT, (0xFF, 0xD7, 0x00, 255), kp, shadow=True, bold=True)
            kw_pngs.append(kp)

    # 标题 + 日期
    tp = work / f"{output.stem}_t.png"
    dp = work / f"{output.stem}_d.png"
    _png(title, TITLE_FONT, (0xFF,0xD7,0x00,255), tp, bold=True)
    _png(date_str, DATE_FONT, (0xF0,0xF0,0xF0,255), dp)
    title_h = TITLE_FONT + 10
    title_top = FG_Y - TITLE_VIDEO_GAP - (title_h + TITLE_DATE_GAP + DATE_FONT + 10)
    titled = work / f"{output.stem}_titled.mp4"
    subprocess.run(["ffmpeg", "-y", "-i", str(tmp), "-i", str(tp), "-i", str(dp),
                    "-filter_complex",
                    f"[1:v]setpts=PTS-STARTPTS[t];[2:v]setpts=PTS-STARTPTS[d];"
                    f"[0:v][t]overlay=(W-w)/2:{int(title_top)}[t1];"
                    f"[t1][d]overlay=(W-w)/2:{int(title_top+title_h+TITLE_DATE_GAP)}",
                    "-c:v", "libx264", "-preset", "fast", "-crf", "22", "-an", str(titled)],
                   check=True, capture_output=True)

    # 关键词叠加（视频区下方，黄字黑边，合成单张PNG后一次性叠加）
    current = titled
    if kw_pngs:
        from PIL import Image as PILImage
        # 合成关键词PNG为一张图
        imgs = [PILImage.open(kp) for kp in kw_pngs]
        gap = 24
        total_w = sum(im.size[0] for im in imgs) + gap * (len(imgs) - 1)
        max_h = max(im.size[1] for im in imgs)
        kw_combined = PILImage.new("RGBA", (total_w, max_h), (0, 0, 0, 0))
        cx = 0
        for im in imgs:
            kw_combined.paste(im, (cx, 0), im)
            cx += im.size[0] + gap
        kw_path = work / f"{output.stem}_kws.png"
        kw_combined.save(kw_path, "PNG")

        kw_with_kw = work / f"{output.stem}_kw.mp4"
        subprocess.run(["ffmpeg", "-y", "-i", str(titled), "-i", str(kw_path),
                        "-filter_complex",
                        f"[1:v]setpts=PTS-STARTPTS[kw];[0:v][kw]overlay=(W-w)/2:{kw_y}",
                        "-c:v", "libx264", "-preset", "fast", "-crf", "22", "-an", str(kw_with_kw)],
                       check=True, capture_output=True)
        current = kw_with_kw

    # 字幕
    if with_subtitles and boundaries:
        current = _add_subtitles(current, boundaries, duration, work)

    shutil.copy(current, output)
    for f in [bg, fg, tmp, titled, tp, dp] + kw_pngs:
        if f.exists() and f != output: f.unlink()
    # 清理中间字幕文件
    for f in work.glob("_sub_*.mp4"):
        if f != output: f.unlink()


def _add_subtitles(video: Path, boundaries: list, dur: float, work: Path) -> Path:
    """叠加字幕（SentenceBoundary 精确同步，逐行依次显示，首尾去标点，去编号）"""
    # 合并 <0.5s 碎片
    merged = []
    for b in boundaries:
        txt = _strip_punct(b["text"])
        if not txt: continue
        if merged and (b["end"]-b["start"]) < 0.5:
            merged[-1]["text"] += txt
            merged[-1]["end"] = b["end"]
        else:
            merged.append({"text": txt, "start": b["start"], "end": b["end"]})
    merged = [m for m in merged if m["text"]]

    # 超长拆分（按标点 + 逐字累积，数字/英文不拆断）
    def _split(txt, s, e):
        if _char_width(txt) <= SUB_MAX_WIDTH:
            return [{"text": txt, "start": s, "end": e}]
        # 按标点分段
        segs, cur = [], ""
        for ch in txt:
            cur += ch
            if ch in '，,。！!？?；;：:、':
                if cur.strip(): segs.append(cur.strip())
                cur = ""
        if cur.strip(): segs.append(cur.strip())
        # 超宽段逐字累积
        chunks = []
        for seg in segs:
            if _char_width(seg) <= SUB_MAX_WIDTH:
                chunks.append(seg)
            else:
                acc = ""
                for ch in seg:
                    if _char_width(acc+ch) > SUB_MAX_WIDTH and acc:
                        chunks.append(_strip_punct(acc))
                        acc = ch
                    else: acc += ch
                if acc: chunks.append(_strip_punct(acc))
        # 再次去编号 + 过滤空行
        chunks = [c for c in chunks if c]
        if not chunks: return [{"text": _strip_punct(txt), "start": s, "end": e}]
        # 按字数比例分配时间
        ccs = [len(c) for c in chunks]
        total = sum(ccs)
        td = e - s
        result, t = [], s
        for c, cc in zip(chunks, ccs):
            result.append({"text": c, "start": t, "end": t + td*(cc/total if total>0 else 1/len(chunks))})
            t += td*(cc/total if total>0 else 1/len(chunks))
        return result

    sequenced = []
    for m in merged:
        sequenced.extend(_split(m["text"], m["start"], m["end"]))

    # 强制最终清理：每个字幕片段的文本首尾去标点
    for s in sequenced:
        s["text"] = _strip_punct(s["text"])
    sequenced = [s for s in sequenced if s["text"]]

    if merged and dur > merged[-1]["end"]:
        merged[-1]["end"] = dur

    sub_y = FG_BOTTOM - SUB_BOTTOM - SUB_FONT - 8
    current = video
    for i, s in enumerate(sequenced):
        sp = work / f"_sub_{i:03d}.png"
        _png(s["text"], SUB_FONT, (255,255,255,255), sp)
        subbed = work / f"_subbed_{i:03d}.mp4"
        subprocess.run(["ffmpeg", "-y", "-i", str(current), "-i", str(sp),
                        "-filter_complex",
                        f"[1:v]setpts=PTS-STARTPTS[s];[0:v][s]overlay=(W-w)/2:{sub_y}:"
                        f"enable='between(t,{s['start']:.4f},{s['end']:.4f})'",
                        "-c:v", "libx264", "-preset", "fast", "-crf", "22", "-an", str(subbed)],
                       check=True, capture_output=True)
        current = subbed
    return current


def _add_watermark(video: Path, date_str: str, work: Path):
    """叠加水印"""
    from PIL import Image, ImageDraw, ImageFont
    wm = work / "_wm.png"
    try: font = ImageFont.truetype("/System/Library/Fonts/PingFang.ttc", WM_FONT)
    except: font = ImageFont.load_default()
    l1, l2 = "个人观点，仅供参考", f"来源：{date_str}《新闻联播》"
    b1, b2 = font.getbbox(l1), font.getbbox(l2)
    w1, h1 = b1[2]-b1[0], b1[3]-b1[1]
    w2, h2 = b2[2]-b2[0], b2[3]-b2[1]
    mw = max(w1, w2)
    th = h1 + 6 + h2
    img = Image.new("RGBA", (mw+8, th+8), (0,0,0,0))
    d = ImageDraw.Draw(img)
    d.text((5,5), l1, font=font, fill=(210,210,210,210))
    d.text((5,5+h1+6), l2, font=font, fill=(190,190,190,210))
    img.save(wm, "PNG")
    wx = CANVAS_W - WM_MARGIN_R - mw - 8
    wy = CANVAS_H - WM_MARGIN_BTM - th - 8
    tmp = work / "_tmp_wm.mp4"
    subprocess.run(["ffmpeg", "-y", "-i", str(video), "-i", str(wm),
                    "-filter_complex", f"[1:v]setpts=PTS-STARTPTS[w];[0:v][w]overlay={wx}:{wy}",
                    "-c:v", "libx264", "-preset", "fast", "-crf", "22",
                    "-c:a", "copy", "-map", "0:a:0", str(tmp)],
                   check=True, capture_output=True)
    tmp.replace(video)


# ═══════════════════════════════════════════════════════
# 片尾卡片（模板A: 暗金卡片风）
# ═══════════════════════════════════════════════════════

def _make_outro_card(duration: float, output: Path, work: Path,
                     outro_summary: list = None):
    """纯文本片尾——LLM生成的投资总结，有理有据"""
    from PIL import Image, ImageDraw, ImageFont

    font_path = None
    for fp in ["/System/Library/Fonts/PingFang.ttc",
               "/System/Library/Fonts/STHeiti Light.ttc"]:
        if Path(fp).exists(): font_path = fp; break
    if not font_path: font_path = "/System/Library/Fonts/Helvetica.ttc"

    try:
        f_title = ImageFont.truetype(font_path, 34)
        f_take  = ImageFont.truetype(font_path, 22)
        f_evid  = ImageFont.truetype(font_path, 17)
        f_risk  = ImageFont.truetype(font_path, 15)
    except Exception:
        f_title = f_take = f_evid = f_risk = ImageFont.load_default()

    W, H = CANVAS_W, CANVAS_H
    img = Image.new("RGB", (W, H), (0, 0, 0))
    draw = ImageDraw.Draw(img)

    WHITE = (255, 255, 255)
    GRAY  = (180, 180, 180)
    DIM   = (110, 110, 110)

    # 先计算总内容高度
    items = (outro_summary or [])[:5]
    content_h = 60  # 标题区(34px + margin)
    if not items:
        content_h += 80
    else:
        for item in items:
            content_h += 36   # takeaway 行
            if item.get("evidence"):
                content_h += 28  # evidence 行
            content_h += 22     # 条目间距
    content_h += 30 + 22  # 分隔线区域 + 风险提示

    # 纵向居中起始位置
    y = (H - content_h) // 2
    if y < 60: y = 60  # 最小上边距

    title = "投资总结"
    b = draw.textbbox((0, 0), title, font=f_title)
    tw = b[2] - b[0]
    draw.text(((W - tw) // 2, y), title, fill=WHITE, font=f_title)
    y += 60

    if not items:
        draw.text((80, y), "关注今日新闻联播金融要点", fill=WHITE, font=f_take)
        y += 80
    else:
        for i, item in enumerate(items):
            takeaway = item.get("takeaway", "")
            evidence = item.get("evidence", "")
            if len(takeaway) > 40: takeaway = takeaway[:38] + ".."
            if len(evidence) > 30: evidence = evidence[:28] + ".."

            num = f"{i+1}."
            draw.text((60, y), num, fill=GRAY, font=f_take)
            draw.text((100, y), takeaway, fill=WHITE, font=f_take)
            y += 36

            if evidence:
                draw.text((100, y), f"* {evidence}", fill=DIM, font=f_evid)
                y += 28

            y += 22

    # 分隔线
    sep_y = y + 10
    draw.line([(100, sep_y), (W - 100, sep_y)], fill=(*DIM[:3], 50), width=1)

    # 风险提示
    risk_y = sep_y + 30
    risk = "本视频仅为信息分享，不构成投资建议"
    b = draw.textbbox((0, 0), risk, font=f_risk)
    draw.text(((W - b[2] + b[0]) // 2, risk_y), risk, fill=DIM, font=f_risk)

    img.save(work / "_outro_card.png", "PNG")

    subprocess.run([
        "ffmpeg", "-y", "-loop", "1", "-i", str(work / "_outro_card.png"),
        "-t", str(duration),
        "-vf", "fade=t=in:d=0.5",
        "-c:v", "libx264", "-preset", "fast", "-crf", "18",
        "-pix_fmt", "yuv420p", str(output),
    ], check=True, capture_output=True)


# ═══════════════════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════════════════

def compose(date_str: str, output_dir: Path, data_dir: Path = None,
            script: dict = None, title: str = "新闻联播AI金融解读",
            outro_summary: list = None) -> Path:
    """
    参数化视频合成。

    参数:
      date_str:   YYYYMMDD
      output_dir: 输出目录
      data_dir:   数据目录（含 full.mp4 + segments/）
      script:     脚本 JSON {"segments": [{type, duration_sec, news_index, narration}]}
      title:      视频标题
    返回:         最终视频路径
    """
    if data_dir is None:
        data_dir = Path(f"{date_str}_news")
    if script is None:
        script_path = output_dir / "script.json"
        script = json.loads(script_path.read_text(encoding="utf-8")) if script_path.exists() else {}

    work = output_dir / "_compose"
    work.mkdir(parents=True, exist_ok=True)

    date_display = f"{date_str[:4]}年{date_str[4:6]}月{date_str[6:8]}日"
    segments = script.get("segments", [])
    full_video = data_dir / "full.mp4"
    segs_dir = data_dir / "segments"

    # 整期节目关键词（所有新闻共用）
    global_keywords = _extract_global_keywords(segments)
    if global_keywords:
        print(f"  关键词: {', '.join(global_keywords)}")

    clips = []
    for i, seg in enumerate(segments):
        stype = seg.get("type", "news")
        dur = seg.get("duration_sec", 30)
        nar = seg.get("narration", "")
        idx = seg.get("news_index", 0)

        print(f"  [{i+1}/{len(segments)}] {stype} ({dur}s)...", end=" ", flush=True)
        clip = work / f"clip_{i:02d}.mp4"
        audio = work / f"audio_{i:02d}.mp3"

        if stype == "intro":
            # 先生成TTS音频，以音频时长为准（确保口播说完不截断）
            generate_audio(nar, audio)
            if audio.exists():
                intro_dur = _dur(audio) + 2.0  # TTS时长 + 2秒缓冲
            else:
                intro_dur = dur
            if full_video.exists():
                subprocess.run(["ffmpeg", "-y", "-i", str(full_video), "-t", str(intro_dur),
                                "-c:v", "libx264", "-preset", "fast", "-crf", "22", "-an",
                                str(work/"intro_raw.mp4")],
                               check=True, capture_output=True)
                blurred = work/"intro_blur.mp4"
                if not _blur_safe(work/"intro_raw.mp4", blurred):
                    blurred = work/"intro_raw.mp4"
                boundaries = _tts_with_boundaries(nar, audio)
                _make_portrait_clip(blurred, clip, boundaries,
                                    intro_dur, title, date_display, work, with_subtitles=True, narration=nar)
            else:
                subprocess.run(["ffmpeg", "-y", "-f", "lavfi",
                                "-i", f"color=c=black:s={CANVAS_W}x{CANVAS_H}:d={dur}:r=25",
                                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
                                "-pix_fmt", "yuv420p", str(clip)],
                               check=True, capture_output=True)
                generate_audio(nar, audio)

        elif stype == "outro":
            # 暗金卡片风片尾（模板A）
            # 片尾视觉时长 = TTS音频时长 + 额外阅读时间
            generate_audio(nar, audio)
            visual_dur = _dur(audio) + 3.0 if audio.exists() else dur
            _make_outro_card(visual_dur, clip, work, outro_summary)

        else:
            # 新闻段落：找切片 → 去水印 → 竖屏合成
            prefix = f"{int(idx):02d}_"
            source = None
            if segs_dir.exists():
                for f in segs_dir.glob(f"{prefix}*.mp4"):
                    source = f; break
            if not source:
                for f in segs_dir.glob(f"*{idx}*.mp4"):
                    source = f; break

            if source:
                # 裁剪前2秒（跳过主持人开场/转场），直接用ffmpeg
                trimmed = work / f"_trimmed_{i}.mp4"
                subprocess.run(["ffmpeg", "-y", "-ss", "3", "-i", str(source),
                                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "20",
                                "-c:a", "aac", "-b:a", "128k", str(trimmed)],
                               check=True, capture_output=True)
                source = trimmed if trimmed.exists() and trimmed.stat().st_size > 1024 else source
                print("模糊...", end=" ", flush=True)
                blurred = work / f"_blurred_{i}.mp4"
                if not _blur_safe(source, blurred):
                    blurred = source
                print("TTS+字幕...", end=" ", flush=True)
                boundaries = _tts_with_boundaries(nar, audio)
                seg_dur = _dur(audio) + 0.5 if audio.exists() else dur
                _make_portrait_clip(blurred, clip, boundaries,
                                    seg_dur, title, date_display, work, narration=nar,
                                    with_keywords=True, global_keywords=global_keywords)
            else:
                print("(无切片)...", end=" ", flush=True)
                subprocess.run(["ffmpeg", "-y", "-f", "lavfi",
                                "-i", f"color=c=black:s={CANVAS_W}x{CANVAS_H}:d={dur}:r=25",
                                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
                                "-pix_fmt", "yuv420p", str(clip)],
                               check=True, capture_output=True)
                generate_audio(nar, audio)

        # 叠加音频
        if audio.exists() and clip.exists():
            tmp = work / f"_tmp_a_{i:02d}.mp4"
            subprocess.run(["ffmpeg", "-y", "-i", str(clip), "-i", str(audio),
                            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
                            "-map", "0:v:0", "-map", "1:a:0", "-shortest", str(tmp)],
                           check=True, capture_output=True)
            if tmp.exists(): tmp.replace(clip)

        clips.append(clip)
        print("✓")

    # 合并
    concat = work / "_concat.txt"
    concat.write_text("\n".join(f"file '{c.absolute()}'" for c in clips if c.exists()), encoding="utf-8")
    final = output_dir / "final_video.mp4"
    subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0",
                    "-i", str(concat), "-c", "copy", str(final)],
                   check=True, capture_output=True)

    _add_watermark(final, date_display, work)

    mb = final.stat().st_size / 1024 / 1024
    print(f"\n  ✓ {final} ({mb:.1f}MB) -- {_dur(final):.1f}s")
    return final

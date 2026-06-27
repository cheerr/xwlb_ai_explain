"""
视频处理工具：高斯模糊原CCTV字幕和水印

CCTV新闻联播画面特征：
  - 右上角：CCTV台标水印（半透明，约5%画面）
  - 底部：新闻标题字幕（白色大字，约15%画面高度）
  - 左下角：有时有「新闻联播」角标

处理策略：
  1. 底部15%区域 → 高斯模糊（覆盖原字幕）
  2. 右上角10%×8%区域 → 高斯模糊（覆盖台标）
  3. 左下角8%×8%区域 → 高斯模糊（覆盖角标）
  4. 叠加我们自己的字幕/水印

ffmpeg 滤镜链：
  split → 主画面 + 模糊区域 → overlay 回贴
"""

import os
import subprocess
from pathlib import Path


def blur_cctv_overlays(
    input_path: Path,
    output_path: Path,
    blur_sigma: float = 20.0,
    subtitle_height_ratio: float = 0.15,
    watermark_size_ratio: float = 0.08,
    logo_offset_ratio: float = 0.03,
) -> bool:
    """
    对视频中的CCTV字幕和水印区域进行高斯模糊处理。

    参数:
      input_path: 输入视频路径
      output_path: 输出视频路径
      blur_sigma: 高斯模糊 sigma 值（默认20，强模糊）
      subtitle_height_ratio: 底部字幕区域高度占比
      watermark_size_ratio: 水印区域大小占比
      logo_offset_ratio: Logo距离边缘的偏移占比
    """

    # 先探测视频尺寸
    probe_cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=p=0",
        str(input_path),
    ]
    try:
        result = subprocess.run(probe_cmd, capture_output=True, text=True)
        w, h = map(int, result.stdout.strip().split(","))
    except Exception:
        w, h = 1280, 720  # 默认720p

    # 计算像素坐标（宽幅覆盖CCTV水印）
    sub_h = int(h * 0.25)       # 底部字幕区增高到25%
    sub_y = h - sub_h
    wm_w = int(w * 0.28)        # 横向加宽到28%
    wm_h = int(h * 0.18)        # 纵向加高到18%
    wm_x = w - wm_w - int(w * 0.01)
    wm_y = int(h * 0.01)
    tl_x = int(w * 0.01)
    tl_y = int(h * 0.01)
    corner_w = int(w * 0.28)
    corner_h = int(h * 0.18)
    corner_x = int(w * 0.01)
    corner_y = h - corner_h - int(h * 0.01)

    # 4区域模糊（避免filter chain过长导致ffmpeg错误）
    subtitle_filter = (
        f"[0:v]crop={w}:{sub_h}:0:{sub_y}"
        f",boxblur={blur_sigma}:2[blur_sub]"
    )
    watermark_filter = (
        f"[0:v]crop={wm_w}:{wm_h}:{wm_x}:{wm_y}"
        f",boxblur={blur_sigma}:2[blur_wm]"
    )
    top_left_filter = (
        f"[0:v]crop={wm_w}:{wm_h}:{tl_x}:{tl_y}"
        f",boxblur={blur_sigma}:2[blur_tl]"
    )
    corner_filter = (
        f"[0:v]crop={corner_w}:{corner_h}:{corner_x}:{corner_y}"
        f",boxblur={blur_sigma}:2[blur_corner]"
    )

    overlay_sub = f"[0:v][blur_sub]overlay=0:{sub_y}[tmp1]"
    overlay_wm = f"[tmp1][blur_wm]overlay={wm_x}:{wm_y}[tmp2]"
    overlay_tl = f"[tmp2][blur_tl]overlay={tl_x}:{tl_y}[tmp3]"
    overlay_corner = f"[tmp3][blur_corner]overlay={corner_x}:{corner_y}[out]"

    filter_chain = (
        f"{subtitle_filter};"
        f"{watermark_filter};"
        f"{top_left_filter};"
        f"{corner_filter};"
        f"{overlay_sub};"
        f"{overlay_wm};"
        f"{overlay_tl};"
        f"{overlay_corner}"
    )

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-filter_complex", filter_chain,
        "-map", "[out]",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "20",
        "-c:a", "copy",
        str(output_path),
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True)
        if output_path.exists() and output_path.stat().st_size > 1024:
            return True
    except subprocess.CalledProcessError as e:
        print(f"    高斯模糊处理失败: {e.stderr.decode()[:200] if e.stderr else e}")
    return False


def blur_single_region(
    input_path: Path,
    output_path: Path,
    region: str = "subtitle",  # subtitle / watermark / logo
    blur_sigma: float = 20.0,
) -> bool:
    """
    只模糊单个区域（用于调试验证）。

    region:
      - "subtitle": 底部字幕区
      - "watermark": 右上角台标
      - "logo": 左下角角标
    """
    region_configs = {
        "subtitle": {
            "crop": "iw:ih*0.18:0:ih*0.82",
            "overlay": "0:ih*0.82",
        },
        "watermark": {
            "crop": "iw*0.10:ih*0.10:iw*0.87:ih*0.03",
            "overlay": "iw*0.87:ih*0.03",
        },
        "logo": {
            "crop": "iw*0.08:ih*0.08:iw*0.03:ih*0.89",
            "overlay": "iw*0.03:ih*0.89",
        },
    }

    cfg = region_configs.get(region, region_configs["subtitle"])

    filter_chain = (
        f"[0:v]crop={cfg['crop']},boxblur={blur_sigma}[blurred];"
        f"[0:v][blurred]overlay={cfg['overlay']}[out]"
    )

    cmd = [
        "ffmpeg", "-y",
        "-i", str(input_path),
        "-filter_complex", filter_chain,
        "-map", "[out]",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "20",
        "-c:a", "copy",
        str(output_path),
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return output_path.exists() and output_path.stat().st_size > 1024
    except subprocess.CalledProcessError:
        return False

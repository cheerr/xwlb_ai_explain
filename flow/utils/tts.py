"""
TTS语音合成工具

优先使用 Edge-TTS（免费、中文自然度高），
备选方案：macOS say 命令、pyttsx3。
"""

import os
import subprocess
import tempfile
from pathlib import Path


def generate_audio_edge_tts(text: str, output_path: Path,
                            voice: str = "zh-CN-XiaoxiaoNeural",
                            rate: str = "+10%") -> bool:
    """
    使用 Edge-TTS 生成中文语音。

    安装: pip install edge-tts

    可用中文声音:
      zh-CN-XiaoxiaoNeural (女声-温柔)
      zh-CN-YunxiNeural    (男声-新闻)
      zh-CN-YunjianNeural  (男声-沉稳)
      zh-CN-XiaoyiNeural   (女声-清晰)
    """
    try:
        import edge_tts  # noqa: F401
    except ImportError:
        return False

    # edge-tts CLI
    cmd = [
        "edge-tts",
        "--voice", voice,
        "--rate", rate,
        "--text", text,
        "--write-media", str(output_path),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=120)
        return output_path.exists() and output_path.stat().st_size > 1024
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    # Python API 备选
    try:
        import asyncio
        import edge_tts

        async def _gen():
            communicate = edge_tts.Communicate(text, voice, rate=rate)
            await communicate.save(str(output_path))

        asyncio.run(_gen())
        return output_path.exists() and output_path.stat().st_size > 1024
    except Exception:
        return False


def generate_audio_macos_say(text: str, output_path: Path) -> bool:
    """使用 macOS say 命令生成语音（无需安装额外库）"""
    # 先生成 AIFF，再转 MP3
    aiff_path = output_path.with_suffix(".aiff")
    try:
        subprocess.run(
            ["say", "-v", "Tingting", "-o", str(aiff_path), text],
            check=True, capture_output=True, timeout=60,
        )
        if aiff_path.exists():
            subprocess.run(
                ["ffmpeg", "-y", "-i", str(aiff_path),
                 "-acodec", "libmp3lame", "-q:a", "2", str(output_path)],
                check=True, capture_output=True,
            )
            aiff_path.unlink()
            return output_path.exists() and output_path.stat().st_size > 1024
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return False


def generate_audio(text: str, output_path: Path,
                   voice: str = "zh-CN-YunxiNeural",
                   rate: str = "+10%") -> bool:
    """
    统一TTS接口：优先 Edge-TTS，备选 macOS say。

    参数:
      text: 要合成的文本
      output_path: 输出音频文件路径 (.mp3 或 .wav)
      voice: TTS声音选择
    """
    text = text.strip()
    if not text:
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 方案1: Edge-TTS YunxiNeural rate+10% (新闻播报风格，自然有感情)
    if generate_audio_edge_tts(text, output_path, voice=voice, rate=rate):
        return True

    # 方案2: macOS say (离线兜底)
    if generate_audio_macos_say(text, output_path):
        return True

    return False


def combine_audio_segments(audio_files: list[Path],
                           output_path: Path) -> bool:
    """将多个音频文件合并为一个，段间加0.3秒静音间隔"""
    if not audio_files:
        return False

    if len(audio_files) == 1 and audio_files[0].exists():
        import shutil
        shutil.copy(audio_files[0], output_path)
        return True

    # 创建 concat 文件列表
    concat_content = []
    for af in audio_files:
        if af.exists():
            concat_content.append(f"file '{af.absolute()}'")
            # 段间插入短暂静音（用 anullsrc 生成）
            concat_content.append(f"file 'silence_0.3s.mp3'")

    # 生成0.3秒静音文件（如果不存在）
    silence_file = output_path.parent / "_silence_0.3s.mp3"
    if not silence_file.exists():
        subprocess.run([
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono",
            "-t", "0.3",
            "-q:a", "9",
            str(silence_file),
        ], check=False, capture_output=True)

    concat_file = output_path.parent / "_concat_list.txt"
    concat_file.write_text("\n".join(concat_content), encoding="utf-8")

    try:
        subprocess.run([
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_file),
            "-c", "copy",
            str(output_path),
        ], check=True, capture_output=True)
        return output_path.exists()
    except subprocess.CalledProcessError:
        return False

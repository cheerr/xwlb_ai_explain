"""
视频合成硬编码常量
所有布局参数集中管理，避免随意修改导致重叠/错位
"""

# ============================================================
# 画布
# ============================================================
CANVAS_WIDTH = 720
CANVAS_HEIGHT = 1280
VIDEO_ASPECT = 16 / 9  # 横屏原视频宽高比

# 横屏视频在竖屏中的位置（自动计算）
FG_WIDTH = CANVAS_WIDTH
FG_HEIGHT = int(CANVAS_WIDTH / VIDEO_ASPECT)  # 405
FG_Y = (CANVAS_HEIGHT - FG_HEIGHT) // 2       # 437
FG_BOTTOM = FG_Y + FG_HEIGHT                  # 842

# ============================================================
# 标题
# ============================================================
TITLE_FONT_SIZE = 46
DATE_FONT_SIZE = 32
TITLE_DATE_GAP = 18  # 标题与日期间距
TITLE_VIDEO_GAP = 40  # 标题区距视频画面最小间距

# ============================================================
# 字幕
# ============================================================
SUBTITLE_FONT_SIZE = 22
SUBTITLE_BOTTOM_MARGIN = 48  # 距视频画面底部
SUBTITLE_MAX_CHARS = 20      # 每行最大字数
SUBTITLE_SHADOW_OFFSET = 2
SUBTITLE_SHADOW_ALPHA = 120

# 字幕区底部 = FG_BOTTOM - SUBTITLE_BOTTOM_MARGIN = 842-48 = 794

# ============================================================
# 底部水印（硬编码，不得随意修改）
# ============================================================
WATERMARK_BOTTOM_MARGIN = 100  # 距底部
WATERMARK_RIGHT_MARGIN = 30    # 距右侧
WATERMARK_FONT_SIZE = 14
WATERMARK_LINE_SPACING = 6
WATERMARK_TEXT_LINE1 = "个人观点，仅供参考"
WATERMARK_TEXT_LINE2 = "来源：{date}《新闻联播》"
WATERMARK_COLOR_LINE1 = (210, 210, 210, 210)
WATERMARK_COLOR_LINE2 = (190, 190, 190, 210)
WATERMARK_SHADOW_COLOR = (0, 0, 0, 70)
WATERMARK_SHADOW_OFFSET = 1

# ============================================================
# 高斯模糊
# ============================================================
BLUR_SIGMA = 30.0
BG_BRIGHTNESS = 0.25
WATERMARK_BLUR_WIDTH_RATIO = 0.28   # 水印覆盖宽度28%
WATERMARK_BLUR_HEIGHT_RATIO = 0.18  # 水印覆盖高度18%
SUBTITLE_BLUR_HEIGHT_RATIO = 0.25   # 字幕区覆盖高度25%

# ============================================================
# 口播稿规范
# ============================================================
OPENING_TEMPLATE = "3分钟看懂{date}新闻联播说了什么，看看有哪些你的投资机会。"
OUTRO_RISK = "投资有风险，入市需谨慎。"
OUTRO_BYE = "以上是今天的新闻解读，感谢您的观看，我们明天再见。"
NEWS_NUMBER_FORMAT = "{n}."  # 1. 2. 3. 4.

# ============================================================
# 重叠检测（代码内置断言）
# ============================================================
def check_watermark_overlap():
    """检测水印与字幕是否重叠，返回True表示无重叠"""
    # 字幕区底部
    sub_bottom = FG_BOTTOM - SUBTITLE_BOTTOM_MARGIN  # 794

    # 水印区顶部（假设两行文字，行间距6px，字体14px，行高约20px）
    line_height = WATERMARK_FONT_SIZE + 6  # 约20px
    wm_height = line_height * 2 + WATERMARK_LINE_SPACING  # 约46px
    wm_top = CANVAS_HEIGHT - WATERMARK_BOTTOM_MARGIN - wm_height  # 1280-100-46 = 1134

    gap = wm_top - sub_bottom  # 1134 - 794 = 340
    if gap < 100:
        raise ValueError(
            f"水印重叠! 字幕区底部={sub_bottom}, 水印区顶部={wm_top}, "
            f"间距={gap}px (需≥100px)"
        )
    return True, gap

"""
date_utils.py — 年代标准化工具模块

统一格式: [公元前/公元]-[XX世纪][初/中/末~YY世纪][初/中/末]-[XXXX~XXXX]-[朝代]
- 每个字段用 - 连接，年份范围用 ~
- 世纪 → 推算年份范围
- 朝代根据年份自动推算，跨朝代用 / 分隔
- 20世纪/21世纪 → 现代
"""

import re
import pandas as pd

# ── 中国朝代年表 ──
DYNASTY_TIMELINE = [
    ('夏', -2070, -1600),
    ('商', -1600, -1046),
    ('周', -1046, -256),
    ('西周', -1046, -771),
    ('东周', -770, -256),
    ('春秋', -770, -476),
    ('战国', -475, -221),
    ('秦', -221, -206),
    ('汉', -206, 220),
    ('西汉', -206, 9),
    ('新', 9, 23),
    ('东汉', 25, 220),
    ('三国', 220, 280),
    ('晋', 265, 420),
    ('西晋', 265, 316),
    ('东晋', 317, 420),
    ('南北朝', 420, 589),
    ('隋', 581, 618),
    ('唐', 618, 907),
    ('五代', 907, 960),
    ('宋', 960, 1279),
    ('北宋', 960, 1127),
    ('南宋', 1127, 1279),
    ('辽', 907, 1125),
    ('金', 1115, 1234),
    ('元', 1271, 1368),
    ('明', 1368, 1644),
    ('清', 1644, 1911),
    ('现代', 1912, 2050),
]

# 按名称长度降序排列（长名优先匹配，如"南宋"优于"宋"）
DYNASTY_NAMES_SORTED = sorted([
    '西周', '东周', '春秋', '战国', '西汉', '东汉', '西晋', '东晋',
    '北魏', '北齐', '北周', '南朝', '北宋', '南宋', '六朝',
    '商朝', '周朝', '秦朝', '汉朝', '隋朝', '唐朝', '宋朝', '元朝', '明朝', '清朝',
    '商', '周', '秦', '汉', '三国', '晋', '南北朝',
    '隋', '唐', '五代', '宋', '辽', '金', '元', '明', '清',
    '现代', '夏', '魏', '新', '齐', '后晋', '后唐',
], key=lambda x: -len(x))

DYNASTY_ALIAS = {
    '清朝': '清', '明朝': '明', '宋朝': '宋', '唐朝': '唐',
    '汉朝': '汉', '秦朝': '秦', '周朝': '周', '商朝': '商',
    '隋朝': '隋', '元朝': '元', '辽朝': '辽', '金朝': '金',
}

# 子朝代 → (父朝代, start, end)
SUB_PERIOD_MAP = {
    '北宋': ('宋', 960, 1127),
    '南宋': ('宋', 1127, 1279),
    '西汉': ('汉', -206, 9),
    '东汉': ('汉', 25, 220),
    '西周': ('周', -1046, -771),
    '东周': ('周', -770, -256),
    '春秋': ('周', -770, -476),
    '战国': ('周', -475, -221),
    '西晋': ('晋', 265, 316),
    '东晋': ('晋', 317, 420),
}

# 所有子朝代名（用于 dedup）
SUB_PERIOD_NAMES = set(SUB_PERIOD_MAP.keys())
# 父朝代名
PARENT_PERIOD_NAMES = set(v[0] for v in SUB_PERIOD_MAP.values())


def _century_range(c, bce=False):
    """世纪数 → (start, end) 年份范围（数学世纪）
    例: 18 → (1701, 1800), 1 → (1, 100), -3 → (-300, -201)
    """
    if bce or c < 0:
        c = abs(c)
        s = -(c * 100) + 1
        e = -(c - 1) * 100
        return (e, s) if s > e else (s, e)
    return ((c - 1) * 100 + 1, c * 100)


def _year_to_century(year):
    """年份 → 世纪数
    例: 1700 → 17, 1701 → 18, -200 → -2
    """
    if year < 0:
        return -((-year - 1) // 100 + 1)
    return (year - 1) // 100 + 1


def _century_label(c):
    """世纪数 → 纯标签（不要'公元前'前缀，前缀统一在 format_ce_output 处理）
    例: 18→'18世纪', -3→'3世纪' (BCE由前缀处理)
    """
    return f'{abs(c)}世纪'


def _find_dynasties(ys, ye):
    """年份范围 → 朝代列表（跨朝代用/分隔），自动去除父朝代（当子朝代覆盖范围时）"""
    raw = []
    for name, ds, de in DYNASTY_TIMELINE:
        if ys <= de and ye >= ds:
            if name not in raw:
                raw.append(name)

    # dedup: 如果子朝代存在，移除其父朝代
    deduped = []
    for name in raw:
        parent = SUB_PERIOD_MAP.get(name, None)
        if parent and parent[0] in raw:
            continue  # 跳过父朝代
        deduped.append(name)
    # 再把父朝代中未被任何子朝代完全取代的加回来
    for name in raw:
        if name not in deduped:
            # 父朝代：检查是否有子朝代覆盖全部范围
            children_of = [sn for sn, (pn, _, _) in SUB_PERIOD_MAP.items() if pn == name]
            all_children_present = all(cn in raw for cn in children_of)
            if not all_children_present:
                deduped.append(name)

    return '/'.join(deduped) if deduped else ''


def _dynasty_overlaps(dyn_name, ys, ye):
    """检查朝代名是否与年份范围重叠"""
    for name, ds, de in DYNASTY_TIMELINE:
        if name == dyn_name or name.startswith(dyn_name):
            if ys <= de and ye >= ds:
                return True
    return False


def _parse_century_seg(seg_text):
    """解析世纪标签片段，返回 (start_year, end_year, seg_name)
    例: '14世纪' → (1301, 1400, '')
        '15世纪初' → (1401, 1433, '初')
        '公元前3世纪' → (-300, -201, '')
    """
    bce = '前' in seg_text
    m = re.search(r'(\d+)世纪', seg_text)
    if not m:
        return None
    c = int(m.group(1))
    s, e = _century_range(c, bce)
    seg_name = ''
    for sf in ['初', '中', '末', '上半叶', '下半叶']:
        if sf in seg_text:
            seg_name = sf
            break
    if seg_name:
        count = e - s + 1  # 总年数（含两端）
        third = count // 3
        if seg_name == '初':
            e = s + third - 1
        elif seg_name == '中':
            s = s + third
            e = s + third - 1
        elif seg_name == '末':
            s = e - third + 1
        elif seg_name == '上半叶':
            e = s + count // 2 - 1
        elif seg_name == '下半叶':
            s = s + count // 2
    return (s, e, seg_name)


def format_ce_output(text, period_dynasty='unknown'):
    """统一输出: [前缀]-[世纪段]-[年份范围]-[朝代]

    输入 text 支持格式:
    - 纯世纪: '14世纪', '14世纪~15世纪', '15世纪初', '19世纪末'
    - 纯年份: '1736—1795', '1699', '1644—1911'
    - 混合: '15世纪—1951', '唐618—907', '明16世纪'
    - BCE: '前206—后220', '约4700—公元前2920年', '475—公元前221年'
    - 朝代: '元朝'
    """
    if text == 'unknown':
        return text

    t = text.strip()
    if not t:
        return 'unknown'

    # ════════════════════════════════════════════════════════════
    # Phase 1: 规范化输入
    # ════════════════════════════════════════════════════════════
    w = t.replace('\u2014', '~').replace('—', '~').replace('－', '~')
    w = re.sub(r'\s+', '', w)

    # ════════════════════════════════════════════════════════════
    # Phase 2: 解析文本
    # ════════════════════════════════════════════════════════════

    cent_segments = []     # 原始世纪片段，如 ['14世纪', '15世纪']
    years_from_cent = []   # 从世纪片段解析出的年份
    years_from_num = []    # 从数字中提取的年份
    seg_marker = ''        # 世纪分段标记 (初/中/末)

    # ── Step 2a: 提取纯世纪片段 ──
    cent_pattern = r'[前后]?(?:公元前)?\d+世纪[初中末尾上半叶下半叶]*'

    def _extract_cent(m):
        seg = m.group(0)
        cent_segments.append(seg)
        for sf in ['初', '中', '末', '上半叶', '下半叶']:
            if sf in seg:
                seg_marker = sf
                break
        return ''

    w_no_cent = re.sub(cent_pattern, _extract_cent, w)

    # 解析世纪片段为年份
    for seg in cent_segments:
        r = _parse_century_seg(seg)
        if r:
            years_from_cent.append(r[0])
            years_from_cent.append(r[1])

    # ── Step 2b: 移除朝代名前缀 ──
    w_clean = w_no_cent
    for dn in DYNASTY_NAMES_SORTED:
        w_clean = re.sub(re.escape(dn), '', w_clean)
    w_clean = re.sub(r'朝', '', w_clean)
    w_clean = re.sub(r'约', '', w_clean)

    # ── Step 2c: 提取纯数字年份 ──
    parts = [p.strip() for p in w_clean.split('~') if p.strip()]

    for part in parts:
        if not part:
            continue

        has_bce = bool(re.search(r'(?:公元前|前)', part))
        has_hou = bool(re.search(r'^后', part))

        # 提取 3-4 位数字（排除日期后缀 月/日）
        nums = re.findall(r'(?<!\d)(\d{3,4})(?![日月\d])', part)
        for n_str in nums:
            val = int(n_str)
            if has_bce:
                val = -val
            elif has_hou:
                pass  # CE，正数
            years_from_num.append(val)

    # ── Step 2d: 上下文 BCE 检测 ──
    # 如果一部分有 BCE 标记且没有显式 CE（后），视为全部 BCE
    has_any_bce = any(y < 0 for y in years_from_num)
    has_any_ce = any(y > 0 for y in years_from_num)
    has_explicit_ce = any('后' in p for p in parts)
    # 如果是 BCE/CE 混合（如 前206—后220），保持原样
    # 如果全是 BCE 标记的文本，则所有年份转为 BCE
    if has_any_bce and not has_any_ce:
        # 所有数字都是 BCE 上下文
        pass
    elif has_any_bce and not has_explicit_ce:
        # 混合但没有明确'后'标记 → 全部 BCE
        years_from_num = [-abs(y) for y in years_from_num]

    # ── Step 2e: 回退：纯朝代名 → 查年表 ──
    if not years_from_cent and not years_from_num:
        for dn in DYNASTY_NAMES_SORTED:
            found = False
            for name, ds, de in DYNASTY_TIMELINE:
                if dn in w and (name == dn or dn == name + '朝' or name == dn + '朝'):
                    years_from_cent.extend([ds, de])
                    found = True
                    break
            if found:
                break
        if not years_from_cent and not years_from_num:
            for alias, canon in DYNASTY_ALIAS.items():
                if alias in w:
                    for name, ds, de in DYNASTY_TIMELINE:
                        if name == canon:
                            years_from_cent.extend([ds, de])
                            break
                    if years_from_cent:
                        break

    # ── Step 2f: 最后回退（明16世纪 等情况） ──
    if not years_from_cent and not years_from_num:
        nums = re.findall(r'(?<!\d)(\d{3,4})(?![日月\d])', w)

    # ════════════════════════════════════════════════════════════
    # Phase 3: 合并年份
    # ════════════════════════════════════════════════════════════
    all_years = years_from_cent + years_from_num
    if not all_years:
        return 'unknown'

    year_s = min(all_years)
    year_e = max(all_years)

    # ════════════════════════════════════════════════════════════
    # Phase 4: 组装输出字段
    # ════════════════════════════════════════════════════════════

    # 4a. 前缀
    prefix = '公元前' if year_s < 0 else '公元'

    # 4b. 世纪段文本（不加 '公元前' 前缀，由 prefix 处理）
    c1 = _year_to_century(year_s)
    c2 = _year_to_century(year_e)
    c1_display = abs(c1)
    c2_display = abs(c2)

    # 判断是否存在 century 段未覆盖的实际年份范围（混合输入）
    cent_covers_range = True
    if cent_segments and years_from_cent:
        cent_min = min(years_from_cent)
        cent_max = max(years_from_cent)
        if cent_min > year_s or cent_max < year_e:
            cent_covers_range = False

    if cent_segments and cent_covers_range:
        # 纯世纪（世纪段完整覆盖年份范围）→ 使用原始文本，去除'公元前'/'前'前缀
        raw_cent = '~'.join(cent_segments)
        raw_cent = re.sub(r'^(?:公元前|前)', '', raw_cent)
        cent_text = raw_cent
    else:
        # 混合输入或纯年份 → 从年份范围计算世纪标签
        if year_s < 0 and year_e > 0 and c1_display == c2_display:
            # BCE/CE 跨同号世纪（如前3世纪~后3世纪）
            cent_text = f'{c1_display}世纪~{c2_display}世纪'
        elif c1_display == c2_display:
            cent_text = f'{c1_display}世纪'
            if seg_marker and len(cent_segments) == 1:
                cent_text += seg_marker
        else:
            cent_text = f'{c1_display}世纪~{c2_display}世纪'

    if not cent_text:
        cent_text = '未知世纪'

    # 4c. 年份范围（使用绝对值）
    ys_abs = abs(year_s)
    ye_abs = abs(year_e)
    year_range = f'{ys_abs}~{ye_abs}'

    # 4d. 朝代
    # 如果 period_dynasty 给定且与年份范围兼容，优先使用
    if period_dynasty and period_dynasty != 'unknown':
        d_clean = re.sub(r'朝$', '', period_dynasty)
        if _dynasty_overlaps(d_clean, year_s, year_e):
            dynasty_str = d_clean
        else:
            dynasty_str = _find_dynasties(year_s, year_e)
    else:
        dynasty_str = _find_dynasties(year_s, year_e)

    # 清理朝代名中的 "朝"
    dynasty_str = re.sub(r'朝', '', dynasty_str)

    # 4e. 追加 "现代"（列在最后，与已有王朝用/隔开）
    if year_e >= 1912 and '现代' not in dynasty_str:
        if dynasty_str:
            dynasty_str = dynasty_str + '/现代'
        else:
            dynasty_str = '现代'

    # ════════════════════════════════════════════════════════════
    # Phase 5: 组装结果
    # ════════════════════════════════════════════════════════════
    parts_out = [prefix, cent_text, year_range]
    parts_out.append(dynasty_str if dynasty_str else '')

    result = '-'.join(parts_out)
    # 清理多余分隔符
    result = re.sub(r'-{2,}', '-', result)
    result = result.rstrip('-')

    return result if result else 'unknown'

INPUT_FILE = "Translation/out_cleveland.csv"      # 你的输入文件
OUTPUT_FILE = "Translation/output_cleveland.csv"    # 输出文件
COLUMN_NAME = 'period'           # 年代所在的列名

# 执行处理
df = pd.read_csv(INPUT_FILE)
df['标准化年代'] = df[COLUMN_NAME].apply(
    lambda x: format_ce_output(str(x)) if pd.notna(x) else 'unknown'
)
df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')

print(f"完成！输入：{INPUT_FILE}，输出：{OUTPUT_FILE}")
import pandas as pd
import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# 读取数据
file_path = "D:/清洗后的数据/cleveland_museum.csv"
df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
print("当前列名：", df.columns.tolist())
print(f"原始数据行数：{len(df)}")
df_cleaned = df.copy()

# 1. 字段标准化

# 1a. 年代标准化
def normalize_period(s):
    """period 统一为中文年代表述"""
    if pd.isna(s):
        return 'unknown'
    s = str(s).strip()
    if re.match(r'^n\.?d\.?$', s, re.IGNORECASE):
        return 'unknown'

    s = re.sub(r'\s+', ' ', s)
    s = s.replace('–', '—').replace('-', '—')
    s = re.sub(r'\s*—\s*', '—', s)

    # ── 1. 基本结构转换（有空格，利用 \b 边界）──

    # late/early/mid + BCE（必须在 BCE 转换前处理）
    s = re.sub(r'[Ll]ate\s+(\d+)\s*BCE?', r'公元前\1年末', s)
    s = re.sub(r'[Ee]arly\s+(\d+)\s*BCE?', r'公元前\1年初', s)
    s = re.sub(r'[Mm]id[\s—\-]+(\d+)\s*BCE?', r'公元前\1年中', s)

    # BCE/CE 转换
    s = re.sub(r'(\d+)\s*BCE', r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*BC', r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*CE', r'公元\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*AD', r'公元\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*B\.?\s*C\.?\s*E?\.?', r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*C\.?\s*E\.?', r'公元\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*A\.?\s*D\.?', r'公元\1年', s, flags=re.IGNORECASE)

    # "c." / "ca." / "about" → 约
    s = re.sub(r'(?<![A-Za-z.])c\.(?![A-Za-z])', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\bca\.\s*', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\babout\s+', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'(\w)\s*c\.\s+', r'\1 约 ', s, flags=re.IGNORECASE)

    # "period" → "年间"
    s = re.sub(r'\bperiod\b', '年间', s, flags=re.IGNORECASE)

    # or later / or earlier / or before（优先于 or→或）
    s = re.sub(r'\bor\s+later\b', '以后', s, flags=re.IGNORECASE)
    s = re.sub(r'\bor\s+earlier\b', '以前', s, flags=re.IGNORECASE)
    s = re.sub(r'\bor\s+before\b', '以前', s, flags=re.IGNORECASE)
    s = re.sub(r'\bor\b', '或', s, flags=re.IGNORECASE)

    # 帝王年号映射
    era_map = {
        'Daoguang': '道光', 'Xianfeng': '咸丰', 'Tongzhi': '同治',
        'Guangxu': '光绪', 'Kangxi': '康熙', 'Yongzheng': '雍正',
        'Qianlong': '乾隆', 'Jiajing': '嘉靖', 'Wanli': '万历',
        'Yongle': '永乐', 'Chenghua': '成化', 'Zhengde': '正德',
        'Hongwu': '洪武', 'Xuande': '宣德', 'Hongzhi': '弘治',
        'Jiaqing': '嘉庆',
    }
    for eng, cn in sorted(era_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'\b{re.escape(eng)}\s*period\b', cn + '年间', s, flags=re.IGNORECASE)
        s = re.sub(rf'\b{re.escape(eng)}\s*', cn, s, flags=re.IGNORECASE)

    # 朝代映射
    dynasty_map = dict(sorted({
        'Northern Song': '北宋', 'Southern Song': '南宋',
        'Northern Wei': '北魏', 'Western Han': '西汉', 'Eastern Han': '东汉',
        'Western Zhou': '西周', 'Eastern Zhou': '东周',
        'Northern Zhou': '北周', 'Northern Qi': '北齐',
        'Warring States': '战国', 'Spring and Autumn': '春秋',
        'Later Jin': '后晋', 'Five Dynasties': '五代',
        'Eastern Wei': '东魏', 'Western Wei': '西魏',
        'Six Dynasties': '六朝', 'S. Sung': '南宋', 'N. Sung': '北宋',
        'Qing': '清', 'Ming': '明', 'Yuan': '元', 'Song': '宋',
        'Tang': '唐', 'Sui': '隋', 'Han': '汉', 'Qin': '秦',
        'Shang': '商', 'Zhou': '周', 'Xia': '夏', 'Jin': '金', 'Liao': '辽',
        'Xin': '新', 'Wei': '魏',
    }.items(), key=lambda x: -len(x[0])))

    def extract_bracket_years(s):
        cn_names = sorted(set(dynasty_map.values()), key=len, reverse=True)
        names_pattern = '|'.join(cn_names)
        s = re.sub(rf'({names_pattern})朝?[\(（]\s*(\d[^\)）]*?)\s*[\)）]', r'\2\1', s)
        return s
    s = extract_bracket_years(s)

    for eng, cn in dynasty_map.items():
        suffix = '' if len(cn) > 1 else '朝'
        s = re.sub(rf'\b{re.escape(eng)}\s*[Dd]ynasty', cn + suffix, s)
        s = re.sub(rf'\b{re.escape(eng)}\s*[Dd]ynastry', cn + suffix, s)

    # probably / possibly → 约
    s = re.sub(r'\bprobably\b', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\bpossibly\b', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\bdated\b', '', s, flags=re.IGNORECASE)

    # ── 2. 噪音词清理（必须 BEFORE 年代规则）──
    s = re.sub(r'\bof\s+the\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bpainting\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bembroidery\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bborders\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(?:with|additions)\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bfrom\s+.*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bmid[\s—\-]*to\s+', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bto\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bmid[\s—\-]*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bof\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bthe\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'1st\s*quarter', '初', s, flags=re.IGNORECASE)

    # ── 3a. half + Century（噪音清理后 first half 14th Century）──
    s = re.sub(r'[Ss]econd\s+half\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪下半叶', s)
    s = re.sub(r'[Ff]irst\s+half\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪上半叶', s)

    # ── 3b. Century → 世纪（含 early/late/mid 前缀）──
    s = re.sub(r'[Ll]ate\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪末', s)
    s = re.sub(r'[Ee]arly\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪初', s)
    s = re.sub(r'[Mm]id[\s—\-]+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪中', s)
    s = re.sub(r'(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪', s)

    # first/second half + 纯数字（噪音清理后，如 "first half 1300s"）
    def _half_century(m, suffix='上半叶'):
        num = m.group(1)
        if len(num) >= 4:
            century = str(int(num[:2]) + 1)
        elif len(num) == 3:
            century = str(int(num[:1]) + 1)
        else:
            return m.group(0)
        return century + '世纪' + suffix
    s = re.sub(r'[Ff]irst\s+half\s+(\d+)(?:s)?\b',
               lambda m: _half_century(m, '上半叶'), s)
    s = re.sub(r'[Ss]econd\s+half\s+(\d+)(?:s)?\b',
               lambda m: _half_century(m, '下半叶'), s)

    def replace_year_s(m, prefix=''):
        num = m.group(1)
        if len(num) >= 4:
            century = str(int(num[:2]) + 1)
        elif len(num) == 3:
            century = str(int(num[:1]) + 1)
        else:
            return m.group(0)
        suf_map = {'late': '末', 'early': '初', 'mid': '中'}
        suf = suf_map.get(prefix, '')
        return century + '世纪' + suf if suf else century + '世纪'

    # ── 3c. before/after + 数字s（必须在纯数字+s之前）──
    def _before_after_s(m):
        ba, num = m.group(1).lower(), m.group(2)
        if len(num) >= 3:
            century = str(int(num[:2]) + 1) if len(num) >= 4 else str(int(num[:1]) + 1)
            return century + '世纪' + ('以前' if ba == 'before' else '以后')
        return m.group(0)
    s = re.sub(r'\b(before|after)\s+(\d+)s\b', _before_after_s, s, flags=re.IGNORECASE)
    # before/after + 纯数字
    s = re.sub(r'\b(before|after)\s+(\d+)\b',
               lambda m: f'{m.group(2)}年{"以前" if m.group(1).lower()=="before" else "以后"}',
               s, flags=re.IGNORECASE)

    # ── 3d. late/early/mid + 数字s ──
    for prefix in ['late', 'early', 'mid']:
        s = re.sub(rf'(?i){prefix}\s+(\d+)s\b',
                   lambda m, p=prefix: replace_year_s(m, p), s)
    # late/early + 3~4位数字（无s）→ 世纪
    for prefix in ['late', 'early']:
        s = re.sub(rf'(?i){prefix}\s+(\d{{3,4}})\b(?!\d)',
                   lambda m, p=prefix: replace_year_s(m, p), s)

    # ── 3e. 纯数字+s（≥3位）──
    s = re.sub(r'\b(\d{3,})s\b', replace_year_s, s)

    # 补齐简写年份 / 年代
    s = re.sub(r'\b(\d{4})—(\d{2})\b',
               lambda m: f'{m.group(1)}—{m.group(1)[:2]}{m.group(2)}' if len(m.group(2)) == 2 else m.group(0), s)
    s = re.sub(r'\b(\d{4})—(\d{2})s\b', lambda m: f'{m.group(1)}—{m.group(1)[:2]}{m.group(2)}年代', s)

    # ── 3f. before/after + 已转换世纪 ──
    s = re.sub(r'\b(before|after)\s+(\d+世纪)', r'\2\1', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+世纪)(before)', r'\1以前', s)
    s = re.sub(r'(\d+世纪)(after)', r'\1以后', s)

    # 分号/逗号后信息清理
    s = re.sub(r';.*', '', s)
    s = re.sub(r',.*', '', s)

    # ── 4. 去空格 ──
    s = re.sub(r'[\(\)（）\[\]]', '', s)
    s = s.replace('?', '')
    s = re.sub(r'\s+', '', s)
    s = re.sub(r'约—+', '约', s)

    # ── 5. 紧凑匹配（去空格后）──
    compact_map = {'SixDynasties': '六朝', 'S.Sung': '南宋', 'N.Sung': '北宋'}
    for eng, cn in compact_map.items():
        s = re.sub(rf'{re.escape(eng)}', cn, s)
    for eng, cn in sorted(dynasty_map.items(), key=lambda x: -len(x[0])):
        compact = eng.replace(' ', '').replace('.', '').replace('-', '')
        if compact != eng:
            s = re.sub(rf'{re.escape(compact)}', cn, s)

    # 清理残余 th/st/nd/rd
    s = re.sub(r'(\d+)(?:th|st|nd|rd)', r'\1', s)

    # ── 6. 清理残余英文标记 ──
    s = re.sub(r'B\.?\s*C\.?\s*E?\.?', '前', s, flags=re.IGNORECASE)
    s = re.sub(r'C\.?\s*E\.?', '后', s, flags=re.IGNORECASE)
    for suf in ['世纪', '世纪初', '世纪末', '世纪中', '世纪下半叶', '世纪上半叶']:
        s = re.sub(rf'(\d+)({suf})(前)', r'公元前\1\2', s)
        s = re.sub(rf'({suf})(前)', r'公元前\1', s)
        s = re.sub(rf'(\d+)({suf})(后)', r'公元\1\2', s)
        s = re.sub(rf'({suf})(后)', r'公元\1', s)

    # 兜底：清理粘在中文前的英文前缀
    s = re.sub(r'\b(late|early|mid)\s*(公元前|公元|\d+世纪)', r'\2', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(before|after|late|early|mid|secondhalf|firsthalf)\b', '', s, flags=re.IGNORECASE)

    s = s.strip('—').strip()
    return s if s else 'unknown'


# 1b. 类型中文映射
type_mapping = {
    'Ceramic': '陶瓷', 'Jade': '玉器', 'Glass': '玻璃器',
    'Miscellaneous': '其他', 'Photograph': '摄影', 'Wood': '木器',
    'Print': '版画/印刷品', 'Stone': '石器', 'Textile': '纺织品',
    'Painting': '绘画', 'Jewelry': '珠宝饰品', 'Silver': '银器',
    'Metalwork': '金属工艺', 'Sculpture': '雕塑',
    'Arms and Armor': '兵器/甲胄', 'Garment': '服饰',
    'Furniture and woodwork': '家具/木作', 'Enamel': '珐琅器',
    'Embroidery': '刺绣', 'Lacquer': '漆器', 'Bound Volume': '古籍/册页',
    'Calligraphy': '书法', 'Coins': '钱币', 'Ivory': '象牙器',
    'Glyptic': '雕刻/宝石', 'Mixed Media': '混合媒材',
    'Rock crystal': '水晶', 'Tapestry': '挂毯', 'Velvet': '天鹅绒',
    'Musical Instrument': '乐器', 'Drawing': '线稿',
}

# 执行标准化
if 'period' in df_cleaned.columns:
    df_cleaned['period'] = df_cleaned['period'].apply(normalize_period)
    print("\n年代标准化示例：")
    print(df['period'].head(10).tolist())
    print("→")
    print(df_cleaned['period'].head(10).tolist())

if 'type' in df_cleaned.columns:
    df_cleaned['type_cn'] = df_cleaned['type'].map(type_mapping).fillna(df_cleaned['type'])
    unmatched = df_cleaned[df_cleaned['type_cn'] == df_cleaned['type']]['type'].unique()
    if len(unmatched) > 0:
        print(f"\n⚠️ 未匹配中文分类的类型：{unmatched}")

# 文本列去空白
for col in ['title', 'material', 'credit_line', 'museum', 'location']:
    if col in df_cleaned.columns:
        df_cleaned[col] = df_cleaned[col].astype(str).str.strip()

# 填充缺失值
fill_values = {
    'title': 'unknown', 'period': 'unknown', 'type': 'unknown',
    'material': 'unknown', 'description': 'unknown', 'dimensions': 'unknown',
    'credit_line': 'unknown', 'accession_number': 'unknown',
}
for col, val in fill_values.items():
    if col in df_cleaned.columns:
        df_cleaned[col] = df_cleaned[col].fillna(val)

df_cleaned = df_cleaned.replace(['', 'nan', 'none', 'null', 'no_image'], 'unknown')


# 2. 智能去重
print(f"\n{'='*50}")
print("开始去重处理...")
before_dedup = len(df_cleaned)

# 2a. 基于 accession_number 精确去重
if 'accession_number' in df_cleaned.columns:
    dup_mask = df_cleaned['accession_number'].duplicated(keep='first')
    dup_count = dup_mask.sum()
    if dup_count > 0:
        print(f"  发现 {dup_count} 条基于 accession_number 的重复记录")
        for acc in df_cleaned.loc[dup_mask, 'accession_number'].unique()[:5]:
            rows = df_cleaned[df_cleaned['accession_number'] == acc]
            print(f"    重复 accession={acc}：{rows['title'].tolist()}")
        df_cleaned = df_cleaned[~dup_mask]
    else:
        print("  无 accession_number 重复记录")

# 2b. 无编号时按标题去重
if 'title' in df_cleaned.columns and 'period' in df_cleaned.columns:
    df_cleaned['_title_norm'] = df_cleaned['title'].astype(str).str.lower().str.strip()
    df_cleaned['_title_norm'] = df_cleaned['_title_norm'].str.replace(r'[^a-z0-9\u4e00-\u9fff]', '', regex=True)
    acc_unknown_mask = df_cleaned['accession_number'] == 'unknown'
    title_dup = df_cleaned[acc_unknown_mask]['_title_norm'].duplicated(keep='first')
    if title_dup.sum() > 0:
        print(f"  发现 {title_dup.sum()} 条无编号但标题重复的记录")
        df_cleaned = df_cleaned[~(acc_unknown_mask & title_dup)]
    df_cleaned = df_cleaned.drop(columns=['_title_norm'])

after_dedup = len(df_cleaned)
print(f"  去重完成：{before_dedup} → {after_dedup} 条（去除 {before_dedup - after_dedup} 条）")


# 3. 重置索引 & Object ID
df_cleaned = df_cleaned.reset_index(drop=True)
df_cleaned['object id'] = df_cleaned.index + 1


# 4. 图片链接有效性验证
print(f"\n{'='*50}")
print("开始图片链接有效性验证...")

def check_server_reachable(url):
    try:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        r = requests.head(f'https://{domain}', timeout=5, verify=False,
                          headers={'User-Agent': 'Mozilla/5.0'})
        return r.status_code < 500
    except Exception:
        return False

def check_single_image(row_idx, url):
    if pd.isna(url) or str(url).strip() in ('', 'unknown'):
        return row_idx, False
    url = str(url).strip()
    try:
        resp = requests.head(
            url, timeout=5, allow_redirects=True, verify=False,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; DataCleaner/1.0)'}
        )
        return row_idx, resp.status_code == 200
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
        return row_idx, False
    except Exception:
        return row_idx, False

if 'image_url' in df_cleaned.columns:
    sample_urls = df_cleaned['image_url'].dropna().head(3).tolist()
    if sample_urls and any('unknown' not in str(u) for u in sample_urls):
        server_ok = any(check_server_reachable(u) for u in sample_urls if str(u).startswith('http'))
        if not server_ok:
            print("  ⚠️ 图片服务器不可达，跳过图片有效性验证")
            df_with_valid_images = df_cleaned.copy()
        else:
            urls = df_cleaned['image_url']
            total = len(urls)
            valid, invalid = 0, []

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(check_single_image, i, u): i for i, u in enumerate(urls)}
                for f in as_completed(futures):
                    idx, ok = f.result()
                    if ok:
                        valid += 1
                    else:
                        invalid.append(idx)

            print(f"  图片链接总计：{total}")
            print(f"  ✅ 有效：{valid}")
            print(f"  ❌ 无效：{len(invalid)}")

            df_cleaned['image_valid'] = False
            df_cleaned.loc[[i for i in range(total) if i not in invalid], 'image_valid'] = True
            df_with_valid_images = df_cleaned[df_cleaned['image_valid']].drop(columns=['image_valid'])
            print(f"  📸 有效图片记录数：{len(df_with_valid_images)}")
    else:
        print("  ⚠️ 无有效图片链接，跳过验证")
        df_with_valid_images = df_cleaned.copy()
else:
    print("  ⚠️ 无 image_url 列，跳过图片验证")
    df_with_valid_images = df_cleaned.copy()


# 5. 保存结果
output_dir = "D:/清洗后的数据"
os.makedirs(output_dir, exist_ok=True)

cleaned_path = os.path.join(output_dir, "cleveland_cleaned_v3.csv")
images_path = os.path.join(output_dir, "cleveland_images_v3.csv")

df_cleaned_for_output = df_cleaned.drop(columns=['image_valid', 'type_cn'], errors='ignore')
df_cleaned_for_output.to_csv(cleaned_path, index=False, encoding='utf-8-sig')
df_with_valid_images.to_csv(images_path, index=False, encoding='utf-8-sig')

print(f"\n{'='*50}")
print("✅ 数据清洗完成")
print(f"📁 {cleaned_path}（{len(df_cleaned_for_output)} 条）")
print(f"📁 {images_path}（{len(df_with_valid_images)} 条）")
print(f"{'='*50}")

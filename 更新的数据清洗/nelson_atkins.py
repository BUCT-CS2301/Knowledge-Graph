import pandas as pd
import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# 读取数据
file_path = r"D:\清洗后的数据\nelson-atkins_museum.csv"
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

    # n.d. / no date
    if re.match(r'^n\.?d\.?$', s, re.IGNORECASE):
        return 'unknown'

    s = re.sub(r'\s+', ' ', s)
    s = s.replace('–', '—').replace('-', '—')
    s = re.sub(r'\s*—\s*', '—', s)

    # 公元前/公元 转换（优先于"约"规则，防 C. 被误匹配）
    s = re.sub(r'(\d+)\s*B\.?\s*C\.?\s*E?\.?', r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*C\.?\s*E\.?', r'公元\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*A\.?\s*D\.?', r'公元\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*B\.?\s*C\.?\b', r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*[Bb]\.?\s*[Cc]\.?\s*[Ee]?\.?', r'公元前\1年', s)
    s = re.sub(r'(\d+)\s*[Cc]\.?\s*[Ee]\.?', r'公元\1年', s)

    # "c." / "ca." / "about" → 约
    s = re.sub(r'(?<![A-Za-z.])c\.(?![A-Za-z])', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\bca\.\s*', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\babout\s+', '约', s, flags=re.IGNORECASE)

    # "period" / "era" → "年间"
    s = re.sub(r'\bperiod\b', '年间', s, flags=re.IGNORECASE)
    s = re.sub(r'\bera\b', '年间', s, flags=re.IGNORECASE)

    # "or" → "或"
    s = re.sub(r'\bor\b', '或', s, flags=re.IGNORECASE)

    # 帝王年号映射（优先于朝代映射）
    era_map = {
        'Daoguang': '道光', 'Xianfeng': '咸丰', 'Tongzhi': '同治',
        'Guangxu': '光绪', 'Kangxi': '康熙', 'Yongzheng': '雍正',
        'Yongzhen': '雍正', 'Qianlong': '乾隆', 'Qianglong': '乾隆',
        'Jiajing': '嘉靖', 'Wanli': '万历', 'Yongle': '永乐',
        'Chenghua': '成化', 'Zhengde': '正德', 'Hongwu': '洪武',
        'Xuande': '宣德', 'Tianshun': '天顺', 'Jingtai': '景泰',
        'Hongzhi': '弘治', 'Longqing': '隆庆', 'Chongzhen': '崇祯',
        'Shunzhi': '顺治', 'Jiaqing': '嘉庆',
    }
    for eng, cn in sorted(era_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'\b{re.escape(eng)}\s*period\b', cn + '年间', s, flags=re.IGNORECASE)
        s = re.sub(rf'\b{re.escape(eng)}\s*', cn, s, flags=re.IGNORECASE)

    # 朝代映射（按名称长度降序，防 Song 优先于 Northern Song）
    dynasty_map = dict(sorted({
        'Northern Song': '北宋', 'Southern Song': '南宋',
        'Northern Wei': '北魏', 'Western Han': '西汉', 'Eastern Han': '东汉',
        'Western Zhou': '西周', 'Eastern Zhou': '东周',
        'Northern Zhou': '北周', 'Northern Qi': '北齐',
        'Warring States': '战国', 'Spring and Autumn': '春秋',
        'Later Jin': '后晋', 'Five Dynasties': '五代',
        'Eastern Wei': '东魏', 'Western Wei': '西魏',
        'Eastern Zhou': '东周', 'Western Zhou': '西周',
        'Qing': '清', 'Ming': '明', 'Yuan': '元', 'Song': '宋',
        'Tang': '唐', 'Sui': '隋', 'Han': '汉', 'Qin': '秦',
        'Shang': '商', 'Zhou': '周', 'Xia': '夏', 'Jin': '金', 'Liao': '辽',
        'Xin': '新', 'Wei': '魏', 'Qi': '齐',
    }.items(), key=lambda x: -len(x[0])))

    # 提取括号内年份：清朝(1644-1911) → 1644—1911清
    def extract_bracket_years(s):
        cn_names = sorted(set(dynasty_map.values()), key=len, reverse=True)
        names_pattern = '|'.join(cn_names)
        s = re.sub(rf'({names_pattern})朝?[\(（]\s*(\d+[—～]\d+)\s*[\)）]', r'\2\1', s)
        s = re.sub(rf'({names_pattern})朝?[\(（]\s*(\d+)\s*[\)）]', r'\2\1', s)
        return s
    s = extract_bracket_years(s)

    # 单字朝代加"朝"，多字不加
    for eng, cn in dynasty_map.items():
        suffix = '' if len(cn) > 1 else '朝'
        s = re.sub(rf'\b{re.escape(eng)}\s*[Dd]ynasty', cn + suffix, s)
        s = re.sub(rf'\b{re.escape(eng)}\s*[Dd]ynastry', cn + suffix, s)

    # "probably" → "约"
    s = re.sub(r'\bprobably\b', '约', s, flags=re.IGNORECASE)

    # "before" / "after"
    s = re.sub(r'before\s+(\d+)', r'\1年以前', s, flags=re.IGNORECASE)
    s = re.sub(r'after\s+(\d+)', r'\1年以后', s, flags=re.IGNORECASE)

    # Century → 世纪
    s = re.sub(r'[Ll]ate\s+(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪末', s)
    s = re.sub(r'[Ee]arly\s+(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪初', s)
    s = re.sub(r'[Mm]id[\s-]+(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪中', s)
    s = re.sub(r'(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪', s)
    s = re.sub(r'centutury', '世纪', s, flags=re.IGNORECASE)

    # second/first half
    s = re.sub(r'second\s+half\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪下半叶', s, flags=re.IGNORECASE)
    s = re.sub(r'first\s+half\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪上半叶', s, flags=re.IGNORECASE)

    # early/late/mid + 1800s
    s = re.sub(r'(late|early|mid)\s+(\d{4})s\b',
               lambda m: {'late': '末', 'early': '初', 'mid': '中'}
               .get(m.group(1).lower(), '') + str(int(m.group(2)[:2])+1) + '世纪',
               s, flags=re.IGNORECASE)

    # 1800s → 19世纪
    s = re.sub(r'\b(\d{4})s[—–-]\s*(\d{4})s\b',
               lambda m: f'{int(m.group(1)[:2])+1}世纪—{int(m.group(2)[:2])+1}世纪', s)
    s = re.sub(r'\b(\d{4})s\b', lambda m: f'{int(m.group(1)[:2])+1}世纪', s)

    # 补齐简写年份：1910—20 → 1910—1920
    s = re.sub(r'\b(\d{4})—(\d{2})\b',
               lambda m: f'{m.group(1)}—{m.group(1)[:2]}{m.group(2)}' if len(m.group(2)) == 2 else m.group(0), s)

    # 清理括号、问号、空白
    s = re.sub(r'[\(\)（）\[\]]', '', s)
    s = s.replace('?', '')
    s = re.sub(r'\s+', '', s)

    # 清理残留 B.C.E. / C.E. → 前/后
    s = re.sub(r'B\.?\s*C\.?\s*E?\.?', '前', s, flags=re.IGNORECASE)
    s = re.sub(r'C\.?\s*E\.?', '后', s, flags=re.IGNORECASE)
    # 世纪前后缀还原：12世纪前 → 公元前12世纪
    for suf in ['世纪', '世纪初', '世纪末', '世纪中', '世纪下半叶', '世纪上半叶']:
        s = re.sub(rf'(\d+)({suf})(前)', r'公元前\1\2', s)
        s = re.sub(rf'({suf})(前)', r'公元前\1', s)
        s = re.sub(rf'(\d+)({suf})(后)', r'公元\1\2', s)
        s = re.sub(rf'({suf})(后)', r'公元\1', s)

    # 重新处理去空格后残留的 "1400s"
    s = re.sub(r'约(\d{4})s', lambda m: f'约{int(m.group(1)[:2])+1}世纪', s)
    s = re.sub(r'(\d{4})s(?!\d)', lambda m: f'{int(m.group(1)[:2])+1}世纪', s)

    # 去空格后清理紧贴数字的 th/st/nd/rd
    s = re.sub(r'(\d+)(?:th|st|nd|rd)', r'\1', s)

    # 去空格后匹配紧凑的朝代名：WarringStates→战国
    for eng, cn in sorted(dynasty_map.items(), key=lambda x: -len(x[0])):
        compact = eng.replace(' ', '')
        if compact != eng:
            s = re.sub(rf'{re.escape(compact)}', cn, s)

    # 无 dynasty 关键词的朝代名：Liao907→辽907
    for eng, cn in sorted(dynasty_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'{re.escape(eng)}(?=\d)', cn, s)
    # Ming—清朝 → 明朝—清朝
    for eng, cn in sorted(dynasty_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'\b{re.escape(eng)}(?=[—～])', cn + '朝' if len(cn) == 1 else cn, s)

    # 处理无空格连接的 late/early/mid + 数字：late10→10世纪末
    s = re.sub(r'late(\d+)(?!\w)', lambda m: f'{m.group(1)}世纪末', s, flags=re.IGNORECASE)
    s = re.sub(r'early(\d+)(?!\w)', lambda m: f'{m.group(1)}世纪初', s, flags=re.IGNORECASE)
    s = re.sub(r'mid(\d+)(?!\w)', lambda m: f'{m.group(1)}世纪中', s, flags=re.IGNORECASE)

    # 兜底清理残留 Late/SecondHalf 等
    s = re.sub(r'late|early|mid|secondhalf|firsthalf', '', s, flags=re.IGNORECASE)

    # 清理 "5或6" 的 or 残留
    s = re.sub(r'(\d+)或(\d+)(?=公元|公元前|世纪)', lambda m: m.group(1), s)

    s = s.strip('—').strip()
    return s if s else 'unknown'


# 1b. 类型标准化
def extract_base_type(t):
    if pd.isna(t):
        return 'unknown'
    t = str(t).strip()
    parts = t.split(',')
    return parts[0].strip()

type_mapping = {
    'Painting': '绘画', 'Ceramics': '陶瓷', 'Sculpture': '雕塑',
    'Jade': '玉器', 'Glass': '玻璃器', 'Wood': '木器', 'Stone': '石器',
    'Textile': '纺织品', 'Photograph': '摄影', 'Photography': '摄影',
    'Print': '版画/印刷品', 'Prints': '版画/印刷品',
    'Scroll': '卷轴', 'Calligraphy': '书法', 'Drawing': '线稿',
    'Jewelry': '珠宝饰品', 'Silver': '银器', 'Metalwork': '金属工艺',
    'Bronze': '青铜器', 'Ironwork': '铁器', 'Ivory': '象牙器',
    'Arms and Armor': '兵器/甲胄', 'Garment': '服饰', 'Costume': '服饰',
    'Furniture': '家具', 'Furniture and woodwork': '家具/木作',
    'Lacquer': '漆器', 'Enamel': '珐琅器', 'Embroidery': '刺绣',
    'Book': '古籍/册页', 'Rubbing': '拓片', 'Snuff Bottle': '鼻烟壶',
    'Decorative Arts': '装饰艺术', 'Container': '容器', 'Implement': '器具',
    'Woodcarving': '木雕', 'Architectural element': '建筑构件',
    'Musical Instrument': '乐器', 'Rug': '毯子', 'Doll': '人偶',
    'Miscellaneous': '其他',
}

# 执行标准化
if 'period' in df_cleaned.columns:
    df_cleaned['period'] = df_cleaned['period'].apply(normalize_period)
    print("\n年代标准化示例：")
    for orig, norm in zip(df['period'].head(8), df_cleaned['period'].head(8)):
        print(f"  {orig}  →  {norm}")

if 'type' in df_cleaned.columns:
    df_cleaned['type_base'] = df_cleaned['type'].apply(extract_base_type)
    df_cleaned['type_cn'] = df_cleaned['type_base'].map(type_mapping).fillna(df_cleaned['type_base'])
    unmatched = df_cleaned[df_cleaned['type_cn'] == df_cleaned['type_base']]['type_base'].unique()
    if len(unmatched) > 0:
        print(f"\n⚠️ 未匹配中文分类的类型：{unmatched}")

# 文本列去空白
for col in ['title', 'material', 'description', 'dimensions', 'credit_line']:
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

if 'title' in df_cleaned.columns:
    df_cleaned['_title_norm'] = df_cleaned['title'].astype(str).str.lower().str.strip()
    df_cleaned['_title_norm'] = df_cleaned['_title_norm'].str.replace(r'[^a-z0-9\u4e00-\u9fff]', '', regex=True)

    if 'accession_number' in df_cleaned.columns:
        dup_id = df_cleaned['accession_number'].duplicated(keep='first') & (df_cleaned['accession_number'] != 'unknown')
        if dup_id.sum() > 0:
            print(f"  发现 {dup_id.sum()} 条编号重复记录")
            df_cleaned = df_cleaned[~dup_id]

    acc_unknown = df_cleaned['accession_number'] == 'unknown'
    title_dup = df_cleaned[acc_unknown]['_title_norm'].duplicated(keep='first')
    if title_dup.sum() > 0:
        print(f"  发现 {title_dup.sum()} 条无编号但标题重复的记录")
        df_cleaned = df_cleaned[~(acc_unknown & title_dup)]

    df_cleaned = df_cleaned.drop(columns=['_title_norm'])

before = len(df_cleaned)
df_cleaned = df_cleaned.drop_duplicates()
if len(df_cleaned) < before:
    print(f"  全行重复：{before - len(df_cleaned)} 条")

after_dedup = len(df_cleaned)
print(f"  去重完成：{before_dedup} → {after_dedup} 条（去除 {before_dedup - after_dedup} 条）")


# 3. 重置索引 & ID
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
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
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
output_dir = r"D:\清洗后的数据"
os.makedirs(output_dir, exist_ok=True)

cleaned_path = os.path.join(output_dir, "nelson_atkins_cleaned_v3.csv")
images_path = os.path.join(output_dir, "nelson_atkins_with_images_v3.csv")

df_cleaned_for_output = df_cleaned.drop(columns=['image_valid', 'type_base'], errors='ignore')
df_cleaned_for_output.to_csv(cleaned_path, index=False, encoding='utf-8-sig')
df_with_valid_images.to_csv(images_path, index=False, encoding='utf-8-sig')

print(f"\n{'='*50}")
print("✅ 数据清洗完成")
print(f"📁 {cleaned_path}（{len(df_cleaned_for_output)} 条）")
print(f"📁 {images_path}（{len(df_with_valid_images)} 条）")
print(f"{'='*50}")

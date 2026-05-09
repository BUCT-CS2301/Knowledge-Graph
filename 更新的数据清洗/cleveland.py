import pandas as pd
import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# 读取数据
file_path = "D:/清洗后的数据/cleveland_museum.csv"
df = pd.read_csv(file_path)
print("当前列名：", df.columns.tolist())
print(f"原始数据行数：{len(df)}")
df_cleaned = df.copy()

# ========================
# 1. 字段标准化
# ========================

# 1a. 年代标准化
def normalize_period(s):
    """period 统一为中文年代表述"""
    if pd.isna(s):
        return 'unknown'
    s = str(s).strip()

    # 统一连接符
    s = re.sub(r'\s+', ' ', s)
    s = s.replace('–', '—')
    s = s.replace('-', '—')
    s = re.sub(r'\s*—\s*', '—', s)

    # 公元前/公元转换
    s = re.sub(r'(\d+)\s*BCE', r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*BC',  r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*CE',  r'公元\1年',   s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)\s*AD',  r'公元\1年',   s, flags=re.IGNORECASE)

    # "c." / "ca." / "about" → 约（需带点号，避免误匹配 Century）
    s = re.sub(r'\b(c\.|ca\.|about)\s*', '约', s, flags=re.IGNORECASE)

    # Century → 世纪（先处理前缀，再处理普通形式）
    s = re.sub(r'late\s+(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪末', s)
    s = re.sub(r'early\s+(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪初', s)
    s = re.sub(r'mid[\s-]+(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪中', s)
    s = re.sub(r'(\d+)(st|nd|rd|th)?\s*[Cc]entury', r'\1世纪', s)

    # early/late + 1800s 形式
    s = re.sub(r'(late)\s+(\d{4})s\b', lambda m: f'{int(m.group(2)[:2])+1}世纪末', s, flags=re.IGNORECASE)
    s = re.sub(r'(early)\s+(\d{4})s\b', lambda m: f'{int(m.group(2)[:2])+1}世纪初', s, flags=re.IGNORECASE)
    s = re.sub(r'(mid)\s+(\d{4})s\b', lambda m: f'{int(m.group(2)[:2])+1}世纪中', s, flags=re.IGNORECASE)

    # Dynasty → 朝
    dynasty_map = {
        'Qing': '清', 'Ming': '明', 'Yuan': '元', 'Song': '宋',
        'Tang': '唐', 'Sui': '隋', 'Han': '汉', 'Qin': '秦',
        'Shang': '商', 'Zhou': '周', 'Xia': '夏', 'Jin': '金', 'Liao': '辽',
        'Western Zhou': '西周', 'Eastern Zhou': '东周',
        'Warring States': '战国', 'Spring and Autumn': '春秋',
    }
    for eng, cn in dynasty_map.items():
        s = re.sub(rf'\b{re.escape(eng)}\s*[Dd]ynasty', cn + '朝', s)
        s = re.sub(rf'\b{re.escape(eng)}\s*[Dd]ynastry', cn + '朝', s)

    # probably → 约
    s = re.sub(r'\bprobably\b', '约', s, flags=re.IGNORECASE)

    # before / after
    s = re.sub(r'before\s+(\d+)', r'\1年以前', s, flags=re.IGNORECASE)
    s = re.sub(r'after\s+(\d+)', r'\1年以后', s, flags=re.IGNORECASE)

    # 1800s → 19世纪（优先处理范围形式）
    def replace_century_pair(m):
        c1 = int(m.group(1)[:2]) + 1
        c2 = int(m.group(2)[:2]) + 1
        return f'{c1}世纪—{c2}世纪'
    s = re.sub(r'\b(\d{4})s[—–-]\s*(\d{4})s\b', replace_century_pair, s)

    def replace_century_single(m):
        return f'{int(m.group(1)[:2]) + 1}世纪'
    s = re.sub(r'\b(\d{4})s\b', replace_century_single, s)

    # 补齐简写年份：1736—95 → 1736—1795
    def expand_short_year(m):
        start, end = m.group(1), m.group(2)
        if len(end) == 2:
            return f'{start}—{start[:2]}{end}'
        return m.group(0)
    s = re.sub(r'\b(\d{4})—(\d{2})\b', expand_short_year, s)

    s = re.sub(r'\s+', '', s)           # 中文格式去空格
    s = s.strip('—').strip()
    return s if s else 'unknown'


# 1b. 文物类型中文映射
type_mapping = {
    'Ceramic': '陶瓷', 'Jade': '玉器', 'Glass': '玻璃器',
    'Miscellaneous': '其他', 'Photograph': '摄影', 'Wood': '木器',
    'Print': '版画/印刷品', 'Stone': '石器', 'Textile': '纺织品',
    'Painting': '绘画', 'Jewelry': '珠宝饰品', 'Silver': '银器',
    'Metalwork': '金属工艺', 'Sculpture': '雕塑',
    'Arms and Armor': '兵器/甲胄', 'Garment': '服饰',
    'Furniture and woodwork': '家具/木作', 'Enamel': '珐琅器',
    'Embroidery': '刺绣', 'Lacquer': '漆器', 'Bound Volume': '古籍/册页',
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


# ========================
# 2. 智能去重
# ========================
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


# ========================
# 3. 重置索引 & Object ID
# ========================
df_cleaned = df_cleaned.reset_index(drop=True)
df_cleaned['object id'] = df_cleaned.index + 1


# ========================
# 4. 图片链接有效性验证
# ========================
print(f"\n{'='*50}")
print("开始图片链接有效性验证...")

def check_single_image(row_idx, url):
    """检查图片链接是否可访问"""
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
    print("  ⚠️ 无 image_url 列，跳过图片验证")
    df_with_valid_images = df_cleaned.copy()


# ========================
# 5. 保存结果
# ========================
output_dir = "D:/清洗后的数据"
os.makedirs(output_dir, exist_ok=True)

cleaned_path = os.path.join(output_dir, "cleaned_data_v2.csv")
images_path = os.path.join(output_dir, "cleaned_data_with_images_v2.csv")

df_cleaned_for_output = df_cleaned.drop(columns=['image_valid'], errors='ignore')
df_cleaned_for_output.to_csv(cleaned_path, index=False, encoding='utf-8-sig')
df_with_valid_images.to_csv(images_path, index=False, encoding='utf-8-sig')

print(f"\n{'='*50}")
print("✅ 数据清洗完成")
print(f"📁 {cleaned_path}（{len(df_cleaned_for_output)} 条）")
print(f"📁 {images_path}（{len(df_with_valid_images)} 条）")
print(f"{'='*50}")

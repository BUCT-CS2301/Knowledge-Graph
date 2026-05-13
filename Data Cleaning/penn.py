import pandas as pd
import os
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# 读取数据
file_path = r"D:\清洗后的数据\penn_museum.csv"
df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
print("当前列名：", df.columns.tolist())
print(f"原始数据行数：{len(df)}")
df_cleaned = df.copy()

# 1. 字段标准化

# 1a. 年代标准化
def normalize_period(s):
    """period 统一为中文年代表述（Penn Museum 适配版 v2）"""
    if pd.isna(s):
        return 'unknown'
    s = str(s).strip()

    # n.d. / no date
    if re.match(r'^n\.?d\.?$', s, re.IGNORECASE):
        return 'unknown'

    s = re.sub(r'\s+', ' ', s)
    s = s.replace('–', '—').replace('-', '—')
    s = re.sub(r'\s*—\s*', '—', s)

    # ── ⭐ EMPEROR/PERSON 映射（放在噪音清理之前！）──
    # 帝王年号映射
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
        s = re.sub(rf'\b{re.escape(eng)}(?!\w)', cn, s, flags=re.IGNORECASE)

    # Emperor 名号映射（用 (?!\w) 替代 \b 结尾，以支持括号结尾的条目）
    emperor_map = {
        'Emperor Taizu': '太祖', 'Emperor Taizong': '太宗',
        'Emperor Gaozu': '高祖', 'Emperor Gaozong': '高宗',
        'Emperor Zhongzong': '中宗', 'Emperor Ruizong': '睿宗',
        'Emperor Xuanzong': '玄宗', 'Emperor Suzong': '肃宗',
        'Emperor Daizong': '代宗', 'Emperor Dezong': '德宗',
        'Emperor Shunzong': '顺宗', 'Emperor Xianzong': '宪宗',
        'Emperor Muzong': '穆宗', 'Emperor Jingzong': '敬宗',
        'Emperor Wenzong': '文宗', 'Emperor Wuzong': '武宗',
        'Emperor Wuzong (Kulug Khan)': '武宗（库鲁格汗）',
        'Emperor Xuānzong': '宣宗', 'Emperor Yizong': '懿宗',
        'Emperor Xizong': '僖宗', 'Emperor Zhaozong': '昭宗',
        'Emperor Zhuangzong': '庄宗', 'Emperor Mingzong': '明宗',
        'Emperor Taizu (Zhu Wen)': '太祖（朱温）',
        'Emperor Shizong': '世宗', 'Emperor Shizong (Chai Rong)': '世宗（柴荣）',
        'Emperor Zhenzong': '真宗', 'Emperor Renzong': '仁宗',
        'Emperor Renzong (Ayurbarwada)': '仁宗（爱育黎拔力八达）',
        'Emperor Yingzong': '英宗', 'Emperor Yingzong (Gegeen Khan)': '英宗（格坚汗）',
        'Emperor Shenzong': '神宗', 'Emperor Zhezong': '哲宗',
        'Emperor Huizong': '徽宗',
        'Emperor Huizong (Toghon Temür)': '惠宗（妥懽贴睦尔）',
        'Emperor Qinzong': '钦宗', 'Emperor Duzong': '度宗',
        'Emperor Gongzong': '恭宗', 'Emperor Duanzong': '端宗',
        'Emperor Bing': '帝昺',
        'Emperor Ningzong': '宁宗', 'Emperor Lizong': '理宗',
        'Emperor Xiaozong': '孝宗', 'Emperor Guangzong': '光宗',
        'Emperor Wudi': '武帝', 'Emperor Wen': '文帝',
        'Emperor Wu': '武帝', 'Emperor Xuan': '宣帝',
        'Emperor Chengzu': '成祖', 'Emperor Sizong': '思宗',
        'Emperor Tianzuo': '天祚帝', 'Emperor Daozong': '道宗',
        'Emperor Shengzu': '圣祖',
        'Emperor Zhangzong': '章宗',
        'Emperor Xiangzong (Li Anquan)': '襄宗（李安全）',
        'Emperor Wanyan Liang': '完颜亮',
        'Emperor Liu Yu': '刘豫',
        'Emperor Shizu': '世祖',
        'Emperor Shizu (Kublai Khan)': '世祖（忽必烈）',
    }
    for eng, cn in sorted(emperor_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'\b{re.escape(eng)}(?!\w)', cn, s, flags=re.IGNORECASE)

    # 个别人名映射（同样在噪音清理前处理）
    person_map = {
        'Xu Shouhui': '徐寿辉', 'Li Zicheng': '李自成',
        'Zhang Xianzhong': '张献忠',
        'Wu Shifan (Prince of Zhou)': '吴世璠（周王）',
        'Wu Sangui (Prince of Zhou)': '吴三桂（周王）',
        'Zhu Yujian (Prince of Tang)': '朱聿键（唐王）',
        'Zhu Yousong (Prince of Fu)': '朱由崧（福王）',
        'Zhu Yuanzhang, as Prince of Wu': '朱元璋（吴王）',
        'Zhu Yuanzhang as Prince of Wu': '朱元璋（吴王）',
        'Zhang Shicheng': '张士诚',
        'Wang Jian': '王建', 'Wang Yan': '王衍',
        'Wang Yanzheng': '王延政', 'Wang Yanxi': '王延羲',
        'Shi Siming': '史思明', 'Nurhachi': '努尔哈赤',
        'Supreme Commander Ma Yin': '马殷',
        'Juqu Mengxun': '沮渠蒙逊',
        'Wang Mang': '王莽',
    }
    for eng, cn in sorted(person_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'\b{re.escape(eng)}(?!\w)', cn, s, flags=re.IGNORECASE)

    # ── 1. 噪音词清理 ──
    s = re.sub(r'\bof\s+the\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bdated\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bto\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bthe\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\bof\b', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\ba\s*[Dd]ate\b', '', s, flags=re.IGNORECASE)  # "a date" noise
    s = s.rstrip('.')
    s = s.strip()

    # ── 1b. 特殊格式预处理 ──
    # "Catalogued 1948" → 1948
    s = re.sub(r'\b[Cc]atalogued\s+(\d{4})\b', r'\1', s)
    # "pre-1895" → 1895年以前
    s = re.sub(r'\bpre[\s—\-]+(\d+)', r'\1年以前', s, flags=re.IGNORECASE)
    # "-1945"（无起始边界）→ 1945年以前
    s = re.sub(r'^—(\d{4})$', r'\1年以前', s)
    # "Modern" → 现代
    s = re.sub(r'\bModern\b', '现代', s, flags=re.IGNORECASE)
    # "Formative Period" → 形成期
    s = re.sub(r'\bFormative\s*[Pp]eriod\b', '形成期', s)
    # "Yangshao" → 仰韶
    s = re.sub(r'\bYangshao\b', '仰韶', s, flags=re.IGNORECASE)

    # ── 1c. "Possibly" → 约 ──
    s = re.sub(r'\b[Pp]ossibly\b', '约', s)

    # ── 1d. 0bc/770BC-255BC 格式 ──
    s = re.sub(r'(\d+)\s*[Bb][Cc]\b', r'公元前\1年', s)
    s = re.sub(r'(\d+)\s*[Cc][Ee]\b', r'公元\1年', s)

    # ── 1e. month day, year 日期格式 ──
    # "October 31st, 1824" → 1824年10月31日
    month_map = {
        'January': '1月', 'February': '2月', 'March': '3月', 'April': '4月',
        'May': '5月', 'June': '6月', 'July': '7月', 'August': '8月',
        'September': '9月', 'October': '10月', 'November': '11月', 'December': '12月',
    }
    def _parse_date(m):
        month = m.group(1)
        day = m.group(2)
        year = m.group(3)
        cn_month = month_map.get(month.capitalize(), month)
        return f'{year}年{cn_month}{day}日'
    s = re.sub(
        r'([A-Z][a-z]+)\s+(\d+)(?:st|nd|rd|th)?,\s*(\d{4})',
        _parse_date, s)

    # "November 1892" → 1892年11月
    def _parse_month_year(m):
        month = m.group(1)
        year = m.group(2)
        cn_month = month_map.get(month.capitalize(), month)
        return f'{year}年{cn_month}'
    s = re.sub(
        r'([A-Z][a-z]+)\s+(\d{4})',
        _parse_month_year, s)

    # "12/21/1648" → 1648年12月21日
    s = re.sub(r'(\d{1,2})/(\d{1,2})/(\d{4})', r'\3年\1月\2日', s)

    # ── 1f. "1930's" → "1930s"（去掉撇号，便于后续匹配）──
    s = re.sub(r"\b(\d{4})'s\b", r'\1s', s)

    # ── 2. BCE/CE 带 late/early/mid 前缀 ──
    s = re.sub(
        r'[Ll]ate\s+(\d+)(?:st|nd|rd|th)?\s*(?:(?:BCE?)|(?:CE?))\b',
        r'公元前\1年末', s)
    s = re.sub(
        r'[Ee]arly\s+(\d+)(?:st|nd|rd|th)?\s*(?:(?:BCE?)|(?:CE?))\b',
        r'公元前\1年初', s)
    s = re.sub(
        r'[Mm]id[\s—\-]+(\d+)(?:st|nd|rd|th)?\s*(?:(?:BCE?)|(?:CE?))\b',
        r'公元前\1年中', s)

    # ── 3. BCE/CE 转换 ──
    s = re.sub(r'(\d+)(?:st|nd|rd|th)?\s*B\.?\s*C\.?\s*E?\.?\b',
               r'公元前\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)(?:st|nd|rd|th)?\s*C\.?\s*E\.?\b',
               r'公元\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'(\d+)(?:st|nd|rd|th)?\s*A\.?\s*D\.?\b',
               r'公元\1年', s, flags=re.IGNORECASE)
    s = re.sub(r'\bB\.?\s*C\.?\s*E?\.?\b', '前', s, flags=re.IGNORECASE)
    s = re.sub(r'\bC\.?\s*E\.?\b', '后', s, flags=re.IGNORECASE)

    # ── 4. "c." / "ca." / "about" → 约 ──
    s = re.sub(r'(?<![A-Za-z.])c\.(?![A-Za-z])', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\bca\.\s*', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\babout\s+', '约', s, flags=re.IGNORECASE)

    # ── 5. "period" / "era" → "年间" ──
    s = re.sub(r'\bperiod\b', '年间', s, flags=re.IGNORECASE)
    s = re.sub(r'\bera\b', '年间', s, flags=re.IGNORECASE)

    # ── 6. "or" → "或" ──
    s = re.sub(r'\bor\s+later\b', '以后', s, flags=re.IGNORECASE)
    s = re.sub(r'\bor\s+earlier\b', '以前', s, flags=re.IGNORECASE)
    s = re.sub(r'\bor\s+before\b', '以前', s, flags=re.IGNORECASE)
    s = re.sub(r'\bor\b', '或', s, flags=re.IGNORECASE)

    # ── 8. 朝代映射 ──
    dynasty_map = dict(sorted({
        'Northern Song': '北宋', 'Southern Song': '南宋',
        'Northern Wei': '北魏', 'Western Han': '西汉', 'Eastern Han': '东汉',
        'Western Zhou': '西周', 'Eastern Zhou': '东周',
        'Northern Zhou': '北周', 'Northern Qi': '北齐',
        'Warring States': '战国', 'Spring and Autumn': '春秋',
        'Later Jin': '后晋', 'Later Tang': '后唐',
        'Five Dynasties': '五代',
        'Eastern Wei': '东魏', 'Western Wei': '西魏',
        'Six Dynasties': '六朝',
        'Qing': '清', 'Ming': '明', 'Yuan': '元', 'Song': '宋',
        'Tang': '唐', 'Sui': '隋', 'Han': '汉', 'Qin': '秦',
        'Shang': '商', 'Zhou': '周', 'Xia': '夏', 'Jin': '金', 'Liao': '辽',
        'Xin': '新', 'Wei': '魏', 'Qi': '齐',
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
        # 也匹配裸朝代名（无 Dynasty 后缀）
        s = re.sub(rf'\b{re.escape(eng)}\b', cn + suffix if len(cn) <= 1 else cn, s)

    s = re.sub(r'\bprobably\b', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'\bpossibly\b', '约', s, flags=re.IGNORECASE)
    s = re.sub(r'before\s+(\d+)', r'\1年以前', s, flags=re.IGNORECASE)
    s = re.sub(r'after\s+(\d+)', r'\1年以后', s, flags=re.IGNORECASE)

    # ── 9. half + Century ──
    s = re.sub(r'[Ss]econd\s+half\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury',
               r'\1世纪下半叶', s)
    s = re.sub(r'[Ff]irst\s+half\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury',
               r'\1世纪上半叶', s)

    # ── 9b. 复合前缀处理 ──
    s = re.sub(r'[Ee]arly[\s—\-]+[Mm]id[\s—\-]+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury',
               r'\1世纪初—中', s)
    s = re.sub(r'[Ll]ate[\s—\-]+[Mm]id[\s—\-]+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury',
               r'\1世纪末—中', s)
    s = re.sub(r'[Ee]arly[\s—\-]+[Ll]ate[\s—\-]+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury',
               r'\1世纪初—末', s)
    s = re.sub(r'[Mm]id[\s—\-]+[Ll]ate[\s—\-]+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury',
               r'\1世纪中—末', s)

    # ── 10. Century → 世纪 ──
    s = re.sub(r'[Ll]ate\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪末', s)
    s = re.sub(r'[Ee]arly\s+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪初', s)
    s = re.sub(r'[Mm]id[\s—\-]+(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪中', s)
    s = re.sub(r'(\d+)(?:st|nd|rd|th)?\s*[Cc]entury', r'\1世纪', s)

    # ── 10b. Late/Early + 朝代（去空格前处理）──
    # "Late Song Dynasty" → 宋末; "Early Tang Dynasty" → 唐初
    cn_names = sorted(set(dynasty_map.values()), key=len, reverse=True)
    for cn in cn_names:
        suf = '' if len(cn) > 1 else '朝'
        s = re.sub(rf'[Ll]ate\s+{re.escape(cn)}{suf}', cn + '末', s)
        s = re.sub(rf'[Ee]arly\s+{re.escape(cn)}{suf}', cn + '初', s)
        s = re.sub(rf'[Mm]id\s+{re.escape(cn)}{suf}', cn + '中', s)

    # ── 11. "X century BC"/"X century AD" ──
    s = re.sub(r'(\d+)\s*[Cc]entury\s*[Bb][Cc]\b', r'公元前\1世纪', s)
    s = re.sub(r'(\d+)\s*[Cc]entury\s*[Aa][Dd]\b', r'公元\1世纪', s)

    # ── 12. half + 世纪（兜底）──
    s = re.sub(r'[Ss]econd\s*[Hh]alf\s*(\d+)世纪', r'\1世纪下半叶', s)
    s = re.sub(r'[Ff]irst\s*[Hh]alf\s*(\d+)世纪', r'\1世纪上半叶', s)

    # ── 13. 数字s → 世纪/年代 ──
    s = re.sub(r'(late|early|mid)\s+(\d{4})s\b',
               lambda m: {'late': '末', 'early': '初', 'mid': '中'}
               .get(m.group(1).lower(), '') + str(int(m.group(2)[:2])+1) + '世纪',
               s, flags=re.IGNORECASE)
    s = re.sub(r'\b(\d{4})s[—–-]\s*(\d{4})s\b',
               lambda m: f'{int(m.group(1)[:2])+1}世纪—{int(m.group(2)[:2])+1}世纪', s)
    s = re.sub(r'\b(\d{4})s\b',
               lambda m: f'{int(m.group(1)[:2])+1}世纪{m.group(1)[2:4]}年代'
               if m.group(1)[2:] != '00' else f'{int(m.group(1)[:2])+1}世纪', s)

    def _years_to_century_decade(m):
        num = m.group(1)
        if len(num) >= 4:
            century = str(int(num[:2]) + 1)
            decade = num[2:4]
        elif len(num) == 3:
            century = str(int(num[:1]) + 1)
            decade = num[1:3]
        else:
            return m.group(0)
        if decade == '00':
            return century + '世纪'
        return century + '世纪' + decade + '年代'
    s = re.sub(r'(?<!\d)(\d{3,4})s\b', _years_to_century_decade, s)

    # ── 14. 补齐简写年份 ──
    s = re.sub(r'(?<!\d)(\d{3,4})—(\d{2})(?!\d)',
               lambda m: f'{m.group(1)}—{m.group(1)[:2]}{m.group(2)}'
               if len(m.group(2)) == 2 else m.group(0), s)
    s = re.sub(r'(?<!\d)(\d{4})—(\d{2})s\b',
               lambda m: f'{m.group(1)}—{m.group(1)[:2]}{m.group(2)}年代', s)

    # ── 15. 清理括号、问号、空白 ──
    s = re.sub(r'[\(\)（）\[\]]', '', s)
    s = s.replace('?', '')
    s = re.sub(r'\s+', '', s)

    # ── 16. 去空格后 BCE/CE 清理 ──
    s = re.sub(r'B\.?\s*C\.?\s*E?\.?', '前', s, flags=re.IGNORECASE)
    s = re.sub(r'C\.?\s*E\.?', '后', s, flags=re.IGNORECASE)
    s = re.sub(r'[Bb][Cc]\b', '前', s)
    for suf in ['世纪', '世纪初', '世纪末', '世纪中', '世纪下半叶', '世纪上半叶']:
        s = re.sub(rf'(\d+)({suf})(前)', r'公元前\1\2', s)
        s = re.sub(rf'({suf})(前)', r'公元前\1', s)
        s = re.sub(rf'(\d+)({suf})(后)', r'公元\1\2', s)
        s = re.sub(rf'({suf})(后)', r'公元\1', s)

    # ── 17. 去空格后残余 th/st/nd/rd ──
    s = re.sub(r'(\d+)(?:th|st|nd|rd)', r'\1', s)

    # ── 17b. 修复 "X或公元Y世纪" → "公元X或Y世纪" ──
    def _fix_or_ce(m):
        return f'公元{m.group(1)}或{m.group(2)}{m.group(3)}'
    s = re.sub(
        r'(\d+)或公元(\d+)(世纪|世纪初|世纪末|世纪中|世纪下半叶|世纪上半叶)',
        _fix_or_ce, s)
    def _fix_or_bce(m):
        return f'公元前{m.group(1)}或{m.group(2)}{m.group(3)}'
    s = re.sub(
        r'(\d+)或公元前(\d+)(年|世纪)',
        _fix_or_bce, s)

    # ── 17c. 修复 "数字—公元前Y世纪" 中第一个数字缺"世纪"──
    s = re.sub(r'(?<![a-zA-Z])(\d+)—(公元前|公元)(\d+世纪)', r'\1世纪—\2\3', s)

    # ── 18. 紧凑朝代名 ──
    compact_map = {'SixDynasties': '六朝', 'FiveDynasties': '五代',
                   'LaterTang': '后唐', 'TangDynasty-FiveDynasties': '唐—五代',
                   'SongDynasty-MingDynasty': '宋—明',
                   'TangDynasty-SongDynasty': '唐—宋',
                   'ShangDynasty-ZhouDynasty': '商—周',
                   'ShangDynasty-Modern': '商—现代',
                   'SouthernSongDynasty-YuanDynasty': '南宋—元',
                   'LateSongDynasty-YuanDynasty': '宋末—元'}
    for eng, cn in compact_map.items():
        s = re.sub(rf'{re.escape(eng)}', cn, s)
    for eng, cn in sorted(dynasty_map.items(), key=lambda x: -len(x[0])):
        compact = eng.replace(' ', '')
        if compact != eng:
            s = re.sub(rf'{re.escape(compact)}', cn, s)
    for eng, cn in sorted(dynasty_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'{re.escape(eng)}(?=\d)', cn, s)
    for eng, cn in sorted(dynasty_map.items(), key=lambda x: -len(x[0])):
        s = re.sub(rf'\b{re.escape(eng)}(?=[—～])', cn + '朝' if len(cn) == 1 else cn, s)

    # ── 18b. Late/Early + 朝代（去空格后黏连式）──
    s = re.sub(r'late(\s*)([\u4e00-\u9fff]+)',
               lambda m: m.group(2) + '末' if len(m.group(2)) > 0 else m.group(0),
               s, flags=re.IGNORECASE)
    s = re.sub(r'early(\s*)([\u4e00-\u9fff]+)',
               lambda m: m.group(2) + '初' if len(m.group(2)) > 0 else m.group(0),
               s, flags=re.IGNORECASE)

    # ── 19. 去空格后 late/early/mid + 数字 ──
    s = re.sub(r'late(\d+)(?![a-zA-Z0-9])',
               lambda m: f'{m.group(1)}世纪末', s, flags=re.IGNORECASE)
    s = re.sub(r'early(\d+)(?![a-zA-Z0-9])',
               lambda m: f'{m.group(1)}世纪初', s, flags=re.IGNORECASE)
    s = re.sub(r'mid(\d+)(?![a-zA-Z0-9])',
               lambda m: f'{m.group(1)}世纪中', s, flags=re.IGNORECASE)

    # ── 19b. 纯数字 → 年份 ──
    # 如果只剩下纯数字，加 "年" 后缀（支持3-4位，如839→839年）
    s = re.sub(r'^(\d{3,4})$', r'\1年', s)

    # ── 20. 兜底清理残余英文 ──
    s = re.sub(r'\b(late|early|mid)\s*(公元前|公元|\d+世纪)', r'\2', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(late)\s*([\u4e00-\u9fff]+)朝',
               lambda m: m.group(2) + '末', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(early)\s*([\u4e00-\u9fff]+)朝',
               lambda m: m.group(2) + '初', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(mid)\s*([\u4e00-\u9fff]+)朝',
               lambda m: m.group(2) + '中', s, flags=re.IGNORECASE)
    s = re.sub(r'\b(before|after|late|early|mid|secondhalf|firsthalf|second|first|half)\b',
               '', s, flags=re.IGNORECASE)

    # ── 21. or 残留清理 ──
    s = re.sub(r'(?<!\d)或(?!\d)', '', s)

    # ── 22. 最终清理 ──
    s = s.strip('—').strip()
    s = re.sub(r'(?<=[\u4e00-\u9fff])\.', '', s)
    s = s.strip('.')
    return s if s else 'unknown'


# 1b. 类型标准化（Penn：工艺/技法类型以 | 分隔）
def extract_base_type(t):
    if pd.isna(t):
        return 'unknown'
    t = str(t).strip()
    # 取第一个 | 前的部分作为主分类
    parts = t.split('|')
    return parts[0].strip()

type_mapping = {
    'Carved': '雕刻', 'Painted': '彩绘', 'Glazed': '施釉',
    'Woven': '编织', 'Embroidered': '刺绣', 'Gilded': '鎏金',
    'Fired': '烧制', 'Ceramic': '陶瓷',
    'Molded': '模制', 'Mold Made': '模制',
    'Inscribed': '刻铭', 'Cast': '铸造', 'Engraved': '雕刻',
    'Appliqued': '贴花', 'Inlaid': '镶嵌',
    'Relief': '浮雕', 'Low Relief': '浅浮雕', 'High Relief': '高浮雕',
    'Lacquered': '漆器', 'Cloisonne': '珐琅',
    'Gilt': '鎏金', 'Dyed': '染色', 'Sewn': '缝制',
    'Fresco': '壁画', 'Printed': '印刷', 'Stamped': '钤印',
    'Brocaded': '织锦', 'Polished': '抛光', 'Incised': '刻划',
    'Stained': '着色', 'Unglazed': '无釉',
    'Blanc de Chine': '德化白瓷',
    'Stoneware': '炻器', 'Earthenware': '陶器',
    'Batik': '蜡染', 'Bound': '装订',
    'Woodblock Printed': '木版印刷',
    'Bichrome': '双色',
    'Photography': '摄影',
    'American': '美国', 'Asian': '亚洲', 'Egyptian': '埃及', 'Near Eastern': '近东',
    'Rubbing': '拓片',
    'Bas Relief': '浅浮雕',
    'Gesso': '底料',
    'Three Color Glaze': '三彩',
    'Dehua Ware': '德化瓷',
    'Mural': '壁画',
    'Twisted': '扭绞', 'Braided': '编织',
    'Gum': '涂胶',
    'Block Printed': '木版印刷',
    'Embossed': '浮雕',
}

# 执行标准化
if 'period' in df_cleaned.columns:
    df_cleaned['period'] = df_cleaned['period'].apply(normalize_period)
    print("\n年代标准化示例：")
    for orig, norm in zip(df['period'].head(10), df_cleaned['period'].head(10)):
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

cleaned_path = os.path.join(output_dir, "penn_cleaned_v3.csv")
images_path = os.path.join(output_dir, "penn_with_images_v3.csv")

df_cleaned_for_output = df_cleaned.drop(columns=['image_valid', 'type_base'], errors='ignore')
df_cleaned_for_output.to_csv(cleaned_path, index=False, encoding='utf-8-sig')
df_with_valid_images.to_csv(images_path, index=False, encoding='utf-8-sig')

print(f"\n{'='*50}")
print("✅ 数据清洗完成")
print(f"📁 {cleaned_path}（{len(df_cleaned_for_output)} 条）")
print(f"📁 {images_path}（{len(df_with_valid_images)} 条）")
print(f"{'='*50}")

import requests
from bs4 import BeautifulSoup
import time
import csv
from urllib.parse import urljoin
import re
import random
import pandas as pd
import json
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from bs4 import MarkupResemblesLocatorWarning
import warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)


# 配置信息
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edge/112.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edge/96.0.1054.53',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:64.0) Gecko/20100101 Firefox/64.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'
]


session = requests.Session()
base_url = 'https://www.penn.museum/'
search_url = 'https://www.penn.museum/collections/search.php'
search_terms = ['china', 'chinese']
PROGRESS_FILE = "progress_penn.json"
MAX_TOTAL = 2500


# 断点续爬功能
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"skip": 0, "count": 0}


def save_progress(skip, count):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"skip": skip, "count": count}, f)


# 文本清洗
def clean_html(text):
    if not text:
        return ""
    text = str(text)
    text = BeautifulSoup(text, "html.parser").get_text()
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def clean(value):
    if value is None:
        return ""
    if isinstance(value, list):
        return " | ".join([clean(i) for i in value])
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return clean_html(value)



# 字段提取
def extract_material(item):
    return clean(
        item.get("material")
        or item.get("materials")
        or item.get("technique")
        or item.get("medium")
        or ""
    )


def extract_type(item):
    return clean(
        item.get("classification")
        or item.get("type")
        or item.get("object_type")
        or item.get("technique")
        or ""
    )


def extract_credit_line(item):
    text = (
        item.get("creditline")
        or item.get("credit_line")
        or item.get("tombstone")
        or ""
    )
    return clean(text)


def get_random_user_agent():
    return random.choice(user_agents)


# 请求
def get_headers():
    return {
        "User-Agent": random.choice(user_agents),
        "Accept-Language": "en-US,en;q=0.9"
    }


def request(url, params=None, retry=3):
    for i in range(retry):
        try:
            r = session.get(url, headers=get_headers(), params=params, timeout=15)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"请求失败 {i+1}/{retry}: {e}")
            time.sleep(2 + i)
    return None



# 图片提取
def extract_real_image(soup, detail_url):
    imgs = soup.find_all("img")
    candidates = []

    for img in imgs:
        src = img.get("src") or ""
        if not src:
            continue
        if "logo" in src:
            continue
        if src.endswith(".svg"):
            continue

        full = urljoin(base_url, src)
        if any(x in full for x in ["collections", "assets", "object"]):
            candidates.append(full)

    for c in candidates:
        if "/assets/" in c:
            return c

    return candidates[0] if candidates else ""



# 图片下载
def download(args):
    url, oid = args
    if not url:
        return "", ""

    os.makedirs("images/penn", exist_ok=True)
    path = f"images/penn/{oid}.jpg"

    if os.path.exists(path):
        return path, url

    try:
        # 先试原图
        r = session.get(url, headers=get_headers(), timeout=15, stream=True)

        if r.status_code == 200:
            real_url = url
        else:
            fallback = re.sub(r'_1600\.jpg', '_800.jpg', url)
            r = session.get(fallback, headers=get_headers(), timeout=15, stream=True)

            if r.status_code != 200:
                return "", ""

            real_url = fallback

        with open(path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)

        return path, real_url

    except:
        return "", url



# 详情页解析
def parse_detail(url):
    data = {
        'object_id': None,
        'current_location': None,
        'culture': None,
        'provenience': None,
        'creator': None,
        'date_made': None,
        'section': None,
        'materials': None,
        'technique': None,
        'description': None,
        'length': None,
        'width': None,
        'height': None,
        'depth': None,
        'credit_line': None,
        'image_url': None,  
        'accession_number': None
    }

    r = request(url)
    if not r:
        return data

    soup = BeautifulSoup(r.text, "html.parser")

    def find(label):
        row = soup.find("td", string=re.compile(f"^{label}$"))
        if row:
            val = row.find_next_sibling("td")
            if val:
                return val.get_text(" ", strip=True)
        return ""

    data['object_id'] = find("Object Number")
    data['accession_number'] = find("Accession Number")
    data['current_location'] = find("Current Location")
    data['culture'] = find("Culture")
    data['provenience'] = find("Provenience")
    data['creator'] = find("Creator")
    data['date_made'] = find("Date Made")
    data['section'] = find("Section")
    data['materials'] = find("Materials")
    data['technique'] = find("Technique")
    data['length'] = find("Length")
    data['width'] = find("Width")
    data['height'] = find("Height")
    data['depth'] = find("Depth")
    data['credit_line'] = find("Credit Line")

    desc = find("Description")
    if not desc:
        script = soup.find("script", type="application/ld+json")
        if script:
            try:
                j = json.loads(script.string)
                desc = j.get("description", "")
            except:
                pass
    data["description"] = desc

    img = extract_real_image(soup, url)
    if img and "_800" in img:
        img = img.replace("_800", "_1600")
    data["image_url"] = img

    return data



# 主爬取函数
def crawl_penn():
    data = []
    seen = set()
    progress = load_progress()
    skip = progress["skip"]
    count = progress["count"]

    museum_name = "University of Pennsylvania Museum of Archaeology and Anthropology"
    location = "Philadelphia, Pennsylvania, United States"

    print("\n[Penn Museum] 开始爬取中国文物...\n")

    for term in search_terms:
        page = 1
        params = {
            'term': term,
            'submit_term': 'Submit',
            'type[]': '1',
            'page': 1
        }

        while True:
            if count >= MAX_TOTAL:
                print(f"\n已达到最大爬取数量 {MAX_TOTAL}，爬取结束。")
                break

            params['page'] = page
            print(f"\n正在爬取关键字 '{term}' 的第 {page} 页...")

            try:
                headers = {
                    'User-Agent': get_random_user_agent(),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
                response = request(search_url, params=params)
                if not response:
                    print("页面请求失败，可能被屏蔽，等待后重试...")
                    time.sleep(random.uniform(5, 10))
                    continue

                soup = BeautifulSoup(response.text, 'html.parser')
                items = soup.select('div.card')
                if not items:
                    print(f"关键字 '{term}' 的第 {page} 页没有找到文物，该关键字爬取结束。")
                    break

                batch_records = []
                download_tasks = []

                for item in items:
                    if count >= MAX_TOTAL:
                        break

                    title_element = item.select_one('h2 a')
                    title = title_element.text.strip() if title_element else None
                    link = urljoin(base_url, title_element['href']) if title_element else None
                    object_id = item.select_one('p.text-sm').text.strip() if item.select_one('p.text-sm') else None
                    object_type = item.select('div.label p.text-sm')[1].text.strip() if len(item.select('div.label p.text-sm')) > 1 else None

                    if object_id in seen or link in seen:
                        continue

                    detail_info = parse_detail(link)

                    # 拼接尺寸
                    dim_parts = []
                    if detail_info.get('length'):
                        dim_parts.append(f"L: {detail_info['length']}")
                    if detail_info.get('width'):
                        dim_parts.append(f"W: {detail_info['width']}")
                    if detail_info.get('height'):
                        dim_parts.append(f"H: {detail_info['height']}")
                    if detail_info.get('depth'):
                        dim_parts.append(f"D: {detail_info['depth']}")
                    dimensions = ", ".join(dim_parts)

                    record = {
                        "object_id": clean(object_id or detail_info.get('object_id')),
                        "title": clean(title),
                        "period": clean(detail_info.get('date_made')),
                        "type": clean(
                            detail_info.get("technique") or 
                            detail_info.get("section") or 
                            object_type
                            ),
                        "material": clean(detail_info.get("materials") or detail_info.get("technique")),
                        "description": clean(detail_info.get('description')),
                        "dimensions": clean(dimensions),
                        "museum": museum_name,
                        "location": location,
                        "detail_url": clean(link),
                        "image_url": clean(detail_info.get('image_url')),
                        "image_path": "",
                        "credit_line": clean(detail_info.get('credit_line')),
                        "accession_number": clean(object_id or detail_info.get('object_id')),
                        "crawl_date": datetime.now().strftime("%Y-%m-%d")
                    }

                    batch_records.append(record)
                    download_tasks.append((record['image_url'], record['object_id']))

                    seen.add(object_id)
                    seen.add(link)

                    count += 1
                    print(f"[{count}] {record['title']}")

                # 多线程下载图片
                if download_tasks:
                    print(f"\n开始下载本批次 {len(download_tasks)} 张图片...")
                    with ThreadPoolExecutor(max_workers=4) as executor:
                        results = list(executor.map(download, download_tasks))

                    success_count = 0

                    for i in range(len(batch_records)):
                        path, real_url = results[i]
                        batch_records[i]["image_path"] = path
                        batch_records[i]["image_url"] = real_url

                        if path:
                            success_count += 1
                    print(f"本批次图片下载完成，成功 {success_count} 张\n")
                
                data.extend(batch_records)
                skip = page
                save_progress(skip, count)

                time.sleep(random.uniform(1, 2))
                page += 1

            except requests.exceptions.RequestException as e:
                print(f"请求出错: {e}")
                time.sleep(random.uniform(3, 5))
                continue

    df = pd.DataFrame(data).drop_duplicates(subset=["object_id"], keep="first")
    return df



# 统计报告
def generate_stats(df):
    total = len(df)
    if total == 0:
        return {"museum": "University of Pennsylvania Museum of Archaeology and Anthropology", "total": 0}

    stats = {
        "museum": "University of Pennsylvania Museum of Archaeology and Anthropology",
        "total": total,
        "image_success_rate": round((df["image_path"] != "").sum() / total * 100, 2),
        "field_completeness": {
            "object_id": f"{round((df['object_id'] != '').sum() / total * 100, 2)}%",
            "title": f"{round((df['title'] != '').sum() / total * 100, 2)}%",
            "period": f"{round((df['period'] != '').sum() / total * 100, 2)}%",
            "type": f"{round((df['type'] != '').sum() / total * 100, 2)}%",
            "material": f"{round((df['material'] != '').sum() / total * 100, 2)}%",
            "description": f"{round((df['description'] != '').sum() / total * 100, 2)}%",
            "dimensions": f"{round((df['dimensions'] != '').sum() / total * 100, 2)}%",
            "museum": f"{round((df['museum'] != '').sum() / total * 100, 2)}%",
            "location": f"{round((df['location'] != '').sum() / total * 100, 2)}%",
            "detail_url": f"{round((df['detail_url'] != '').sum() / total * 100, 2)}%",
            "image_url": f"{round((df['image_url'] != '').sum() / total * 100, 2)}%",
            "image_path": f"{round((df['image_path'] != '').sum() / total * 100, 2)}%",
            "credit_line": f"{round((df['credit_line'] != '').sum() / total * 100, 2)}%",
            "accession_number": f"{round((df['accession_number'] != '').sum() / total * 100, 2)}%",
            "crawl_date": f"{round((df['crawl_date'] != '').sum() / total * 100, 2)}%"
        }
    }
    return stats


if __name__ == "__main__":
    df = crawl_penn()
    output_file = "penn_museum.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n所有数据已保存到 {output_file}")

    stats = generate_stats(df)
    print("\n")
    print("爬取数据统计报告")
    print(f"博物馆名称: {stats['museum']}")
    print(f"成功爬取文物总数: {stats['total']} 件")
    if stats['total'] > 0:
        print(f"图片下载成功率: {stats['image_success_rate']}%")
        print("\n各字段完整率:")
        for field, rate in stats['field_completeness'].items():
            print(f"  {field}: {rate}")

import os
import csv
import uuid
import pymysql
import argparse
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '643114514',
    'database': 'admin_platform',
    'charset': 'utf8mb4'
}

CSV_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Translation')

MUSEUM_INFO = {
    'cleveland': {
        'name': 'Cleveland Museum of Art',
        'name_cn': '克利夫兰艺术博物馆',
        'location': '美国俄亥俄州克利夫兰',
        'website': 'https://clevelandart.org'
    },
    'nelson_atkins': {
        'name': 'Nelson-Atkins Museum of Art',
        'name_cn': '尼尔森-阿特金斯艺术博物馆',
        'location': '美国堪萨斯城',
        'website': 'https://art.nelson-atkins.org'
    },
    'penn': {
        'name': 'Penn Museum',
        'name_cn': '宾夕法尼亚大学考古与人类学博物馆',
        'location': '美国宾夕法尼亚州费城',
        'website': 'https://www.penn.museum'
    }
}

def get_museum_id(cursor, museum_key):
    info = MUSEUM_INFO[museum_key]
    cursor.execute("SELECT object_id FROM museum WHERE name = %s", (info['name'],))
    result = cursor.fetchone()
    if result:
        return result[0]

    museum_id = str(uuid.uuid4())
    cursor.execute(
        """INSERT INTO museum (object_id, name, name_cn, location, website)
           VALUES (%s, %s, %s, %s, %s)""",
        (museum_id, info['name'], info['name_cn'], info['location'], info['website'])
    )
    return museum_id

def parse_crawl_date(date_str):
    if not date_str or not date_str.strip():
        return datetime.now().date()
    date_str = date_str.strip()
    date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y/%m/%d', '%Y年%m月%d日']
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return datetime.now().date()

def clean_period(period_str):
    if not period_str:
        return ''
    lines = [line.strip() for line in period_str.replace('\r\n', '\n').split('\n') if line.strip()]
    return lines[0][:200] if lines else ''

def safe_str(val, max_len=None):
    if not val:
        return ''
    val = str(val).strip()
    return val[:max_len] if max_len else val

def insert_artifact(cursor, row, museum_id):
    object_id = str(uuid.uuid4())  # 主键自动生成

    img_id = str(list(row.values())[0]).strip()

    image_url = f"http://39.106.231.119/images/{img_id}.jpg"
    image_path = f"/var/www/image/{img_id}.jpg"

    title = safe_str(row.get('title', ''))
    period = clean_period(row.get('period', ''))
    type_cn = safe_str(row.get('type_cn', ''), 100)
    type_val = type_cn if type_cn else safe_str(row.get('type', ''), 100)
    material = safe_str(row.get('material', ''), 200)
    description = safe_str(row.get('description', ''))
    dimensions = safe_str(row.get('dimensions', ''), 300)
    detail_url = safe_str(row.get('detail_url', ''), 1000) or 'unknown'
    credit_line = safe_str(row.get('credit_line', ''), 500)
    accession_number = safe_str(row.get('accession_number', ''), 100)
    crawl_date = parse_crawl_date(row.get('crawl_date', ''))

    cursor.execute(
        """INSERT INTO artifact
           (object_id, title, period, type, material, description, dimensions,
            museum_id, detail_url, image_url, image_path, credit_line,
            accession_number, crawl_date)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (object_id, title, period, type_val, material, description, dimensions,
         museum_id, detail_url, image_url, image_path, credit_line,
         accession_number, crawl_date)
    )
    return True

def process_csv_file(cursor, filename):
    filepath = os.path.join(CSV_FOLDER, filename)
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return 0, 0

    museum_key = None
    if 'cleveland' in filename.lower():
        museum_key = 'cleveland'
    elif 'nelson_atkins' in filename.lower() or 'nelson-atkins' in filename.lower():
        museum_key = 'nelson_atkins'
    elif 'penn' in filename.lower():
        museum_key = 'penn'

    if not museum_key:
        print(f"Unknown museum in filename: {filename}")
        return 0, 0

    museum_id = get_museum_id(cursor, museum_key)
    success_count = 0
    fail_count = 0

    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            accession_number = safe_str(row.get('accession_number', ''))
            if accession_number:
                cursor.execute(
                    "SELECT 1 FROM artifact WHERE accession_number = %s",
                    (accession_number,)
                )
                if cursor.fetchone():
                    continue

            try:
                if insert_artifact(cursor, row, museum_id):
                    success_count += 1
            except Exception as e:
                fail_count += 1
                title = safe_str(row.get('title', ''))
                print(f"Error inserting artifact '{title}': {e}")

    print(f"Processed {filename}: {success_count} 成功, {fail_count} 失败")
    return success_count, fail_count

def main():
    parser = argparse.ArgumentParser(description='Import CSV data into MySQL')
    parser.add_argument('--keep', action='store_true', help='保留数据库现有数据，不清空表')
    args = parser.parse_args()

    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        print("Connected to database successfully")

        cursor = conn.cursor()
        cursor.execute("SET NAMES utf8mb4")

        if not args.keep:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("TRUNCATE TABLE artifact")
            cursor.execute("TRUNCATE TABLE museum")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            print("Database tables cleared")
        else:
            print("Keeping existing data (--keep)")

        csv_files = [
            'translated_cleveland.csv',
            'translated_nelson_atkins.csv',
            'translated_penn.csv'
        ]

        total_success = 0
        total_fail = 0

        for csv_file in csv_files:
            success, fail = process_csv_file(cursor, csv_file)
            total_success += success
            total_fail += fail

        conn.commit()
        print(f"\nTotal: {total_success} 条插入成功, {total_fail} 条失败")

    except pymysql.MySQLError as e:
        if conn:
            conn.rollback()
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == '__main__':
    main()
import pandas as pd
import os

# ========================
# 1. 读取数据
# ========================
file_path = r"D:\清洗后的数据\nelson-atkins_museum.csv"
df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)

# 复制一份防止污染原数据
df_cleaned = df.copy()

# ========================
# 2. 统一字段（只处理存在的列，避免报错）
# ========================
text_cols = [
    'title', 'period', 'type', 'material',
    'description', 'dimensions', 'museum',
    'location', 'credit_line', 'accession_number'
]

for col in text_cols:
    if col in df_cleaned.columns:
        df_cleaned[col] = df_cleaned[col].fillna('unknown')
        df_cleaned[col] = df_cleaned[col].astype(str).str.strip().str.lower()

# image_url 单独处理
if 'image_url' in df_cleaned.columns:
    df_cleaned['image_url'] = df_cleaned['image_url'].fillna('no_image')

# ========================
# 3. 清理异常空字符串
# ========================
df_cleaned = df_cleaned.replace(['', 'nan', 'none', 'null'], 'unknown')

# ========================
# 4. 删除重复行
# ========================
df_cleaned = df_cleaned.drop_duplicates()

# ========================
# 5. 生成统一 ID
# ========================
df_cleaned = df_cleaned.reset_index(drop=True)
df_cleaned['id'] = df_cleaned.index + 1

# ========================
# 6. 只保留有图片的数据（可选）
# ========================
df_with_images = df_cleaned[df_cleaned['image_url'] != 'no_image']

# ========================
# 7. 输出目录
# ========================
output_dir = r"D:\清洗后的数据"
os.makedirs(output_dir, exist_ok=True)

# ========================
# 8. 保存文件
# ========================
df_cleaned.to_csv(os.path.join(output_dir, "nelson_atkins_cleaned.csv"),
                   index=False, encoding='utf-8-sig')

df_with_images.to_csv(os.path.join(output_dir, "nelson_atkins_with_images.csv"),
                      index=False, encoding='utf-8-sig')

print("✅ 清洗完成，已保存到 D:\\清洗后的数据")
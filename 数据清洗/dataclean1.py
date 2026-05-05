import pandas as pd
import os

# 读取数据
file_path = "D:/清洗后的数据/cleveland_museum.csv"
df = pd.read_csv(file_path)

# ========================
# ✅ 关键：清洗列名（防止 KeyError）
# ========================
df.columns = df.columns.str.strip().str.lower()
print("当前列名：", df.columns.tolist())

# 复制数据
df_cleaned = df.copy()

# ========================
# 1. 填充缺失值（只对存在的列操作）
# ========================
fill_values = {
    'title': 'unknown',
    'period': 'unknown',
    'medium': 'unknown',
    'image': 'no_image',
    'image download link': 'unknown',
    'artist': 'unknown'
}

for col, val in fill_values.items():
    if col in df_cleaned.columns:
        df_cleaned[col] = df_cleaned[col].fillna(val)

# ========================
# 2. 文本标准化（安全版）
# ========================
text_cols = ['title', 'period', 'medium', 'artist']

for col in text_cols:
    if col in df_cleaned.columns:
        df_cleaned[col] = (
            df_cleaned[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace({'unknown ': 'unknown', 'Unknown': 'unknown'})
        )
    else:
        print(f"⚠️ 列不存在，已跳过: {col}")

# ========================
# 3. 去重
# ========================
df_cleaned = df_cleaned.drop_duplicates()

# ========================
# 4. 重置索引 + 添加 Object ID
# ========================
df_cleaned = df_cleaned.reset_index(drop=True)
df_cleaned['object id'] = df_cleaned.index + 1

# ========================
# 5. 提取有图像数据（安全版）
# ========================
if 'image' in df_cleaned.columns:
    df_with_images = df_cleaned[
        (df_cleaned['image'].notna()) &
        (df_cleaned['image'] != 'no_image') &
        (df_cleaned['image'] != '')
    ]
else:
    print("⚠️ 没有 image 列，无法筛选图片数据")
    df_with_images = df_cleaned.copy()

# ========================
# 6. 保存到指定路径
# ========================
output_dir = "D:/清洗后的数据"
os.makedirs(output_dir, exist_ok=True)

cleaned_path = os.path.join(output_dir, "cleaned_data.csv")
images_path = os.path.join(output_dir, "cleaned_data_with_images.csv")

df_cleaned.to_csv(cleaned_path, index=False, encoding='utf-8')
df_with_images.to_csv(images_path, index=False, encoding='utf-8')

print("✅ 数据清洗完成")
print(f"📁 保存路径: {output_dir}")
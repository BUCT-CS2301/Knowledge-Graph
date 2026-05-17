import requests
import hashlib
import random
import pandas as pd
import time
import os
from tqdm import tqdm  # 用于显示进度条

# 百度翻译 API 账号信息
appid = "20260516002614250"
secret_key = "xgHY_Lv7B3q1GD9CwBkK"


# 批量翻译函数（支持多句）
def baidu_batch_translate(sentences, from_lang="auto", to_lang="zh"):
    if not sentences:
        return []

    # 拼接为换行文本
    q = "\n".join(sentences)
    salt = str(random.randint(32768, 65536))
    sign_str = appid + q + salt + secret_key
    sign = hashlib.md5(sign_str.encode()).hexdigest()

    url = "http://api.fanyi.baidu.com/api/trans/vip/translate"
    params = {
        "q": q,
        "from": from_lang,
        "to": to_lang,
        "appid": appid,
        "salt": salt,
        "sign": sign,
    }

    try:
        response = requests.get(url, params=params, timeout=8)
        result = response.json()
        if "trans_result" in result:
            return [item["dst"] for item in result["trans_result"]]
        else:
            print("❌ 翻译失败:", result)
            return sentences  # 返回原文，避免崩溃
    except Exception as e:
        print("⚠️ 请求异常:", e)
        return sentences


# === 参数配置 ===
input_file = "Data Cleaning/penn.csv"
output_file = "translated_penn.csv"
columns_to_translate = ["title","period","type","material","description","dimensions","museum","location","credit_line"]  # 可添加多个列名
batch_size = 100
sleep_time = 0.8
# === 加载 CSV 文件 ===
df = pd.read_csv(input_file)

# === 加载已翻译文件（断点续传支持） ===
if os.path.exists(output_file):
    df_translated = pd.read_csv(output_file)
    df.update(df_translated)
    print("📄 已加载已有翻译进度，支持断点续传")

# === 执行翻译 ===
for col in columns_to_translate:
    print(f"\n🔄 开始翻译列：{col}")
    for start in tqdm(range(0, len(df), batch_size)):
        end = min(start + batch_size, len(df))
        row_indices = df.index[start:end]

        # 准备这一批需要翻译的文本
        to_translate = []
        valid_indices = []

        for idx in row_indices:
            val = str(df.at[idx, col]) if pd.notna(df.at[idx, col]) else ""
            if val.strip() != "":
                to_translate.append(val)
                valid_indices.append(idx)

        if not to_translate:
            continue  # 本批都是空的，跳过

        translated = baidu_batch_translate(to_translate)

        # 安全赋值（防止翻译失败或越界）
        for i, idx in enumerate(valid_indices):
            if i < len(translated):
                df.at[idx, col] = translated[i]

        # 定期保存
        if start % (batch_size * 2) == 0:
            df.to_csv(output_file, index=False)
            print(f"✅ 已保存前 {end} 行")

        time.sleep(sleep_time)

# === 最终保存 ===
df.to_csv(output_file, index=False)
print("\n🎉 所有翻译完成！结果保存到：", output_file)

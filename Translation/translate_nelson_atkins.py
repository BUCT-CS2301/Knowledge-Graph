# -*- coding: utf-8 -*-
import pandas as pd
from tqdm import tqdm
import time
import os
import json
from tencentcloud.common import credential
from tencentcloud.common.exception.tencent_cloud_sdk_exception import TencentCloudSDKException
from tencentcloud.tmt.v20180321 import tmt_client, models

# ================= 配置区域 =================
# 请替换成你自己的密钥（建议用环境变量，不要硬编码！）
SECRET_ID = "SECRET_ID"
SECRET_KEY = "SECRET_KEY"
REGION = "ap-beijing"          # 地域
INPUT_FILE = "Translation/translated_nelson_atkins.csv"
OUTPUT_FILE = "translated_output_nelson_atkins.csv"
COLUMNS_TO_TRANSLATE = ["credit_line"]
SLEEP_TIME = 0.2              # 单条间隔，满足5次/秒限制（0.2秒=5次/秒）
SAVE_INTERVAL = 50             # 每翻译多少行保存一次
# ===========================================

def translate_single(text, source_lang="auto", target_lang="zh"):
    """单条翻译，返回译文，失败返回原文"""
    if not isinstance(text, str) or not text.strip():
        return text
    try:
        cred = credential.Credential(SECRET_ID, SECRET_KEY)
        client = tmt_client.TmtClient(cred, REGION)
        req = models.TextTranslateRequest()
        params = {
            "SourceText": text,
            "Source": source_lang,
            "Target": target_lang,
            "ProjectId": 0
        }
        req.from_json_string(json.dumps(params))
        resp = client.TextTranslate(req)
        return resp.TargetText
    except TencentCloudSDKException as err:
        print(f"\n翻译失败: {text[:30]}... 错误码: {err.code} 信息: {err.message}")
        return text
    except Exception as e:
        print(f"\n未知错误: {e}")
        return text

def main():
    # 1. 加载CSV
    print(f"📂 加载文件: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)

    # 2. 断点续传：如果输出文件已存在，加载已翻译的部分
    if os.path.exists(OUTPUT_FILE):
        print("🔄 发现已有输出文件，执行断点续传...")
        df_existing = pd.read_csv(OUTPUT_FILE)
        # 只更新需要翻译的列（确保行数一致）
        for col in COLUMNS_TO_TRANSLATE:
            if col in df_existing.columns:
                # 用已有文件中的译文覆盖原DataFrame的对应列（按行索引对齐）
                df[col] = df_existing[col]
        print("   续传加载完成")

    # 3. 逐列翻译（每个单元格单独处理）
    for col in COLUMNS_TO_TRANSLATE:
        print(f"\n🌐 开始翻译列: {col}")
        # 定位需要翻译的行（非空字符串且不是纯数字/空格）
        need_translate = []
        for idx, val in df[col].items():
            if pd.isna(val):
                continue
            s = str(val).strip()
            if s and not s.isdigit():   # 简单过滤纯数字（可根据需要调整）
                need_translate.append((idx, s))
        if not need_translate:
            print(f"   ⏭️ 列 '{col}' 没有需要翻译的文本，跳过")
            continue

        # 逐条翻译，带进度条
        for i, (idx, text) in enumerate(tqdm(need_translate, desc=col, unit="条")):
            translated = translate_single(text)
            df.at[idx, col] = translated

            # 定期保存
            if (i + 1) % SAVE_INTERVAL == 0:
                df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
                print(f"   💾 已保存至第 {i+1} 条")

            time.sleep(SLEEP_TIME)   # 频率控制

        # 每列翻译完成后立即保存一次
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"   ✅ 列 '{col}' 翻译完成并保存")

    # 4. 最终保存
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\n🎉 所有翻译完成！结果保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
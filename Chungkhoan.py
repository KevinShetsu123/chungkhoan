import os
import base64
import json
import pandas as pd
from pdf2image import convert_from_path
from mistralai import Mistral
from dotenv import load_dotenv
from io import BytesIO

# ======================
# CONFIG
# ======================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PDF_PATH = "GMD_21CN_BCTC_HNKT.pdf"
OUTPUT_DIR = "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

load_dotenv(os.path.join(BASE_DIR, ".env"), override=True)
API_KEY = os.getenv("MISTRAL_API_KEY")


client = Mistral(api_key=API_KEY)

# ======================
# STEP 1: PDF → IMAGE
# ======================
def pdf_to_images(pdf_path):
    return convert_from_path(
        pdf_path,
        dpi=300
    )

# # ======================
# # STEP 2: OCR + AI → JSON
# # ======================
# def ocr_to_json(images):
#     results = []

#     for img in images:
#         b64 = img_to_base64(img)

#         resp = client.chat.complete(
#             model="mistral-small-latest",
#             temperature=0,
#             messages=[
#                 {
#                     "role": "system",
#                     "content": "Bạn là chuyên gia đọc báo cáo tài chính Việt Nam."
#                 },
#                 {
#                     "role": "user",
#                     "content": [
#                         {
#                             "type": "text",
#                             "text": """
# Đọc ảnh báo cáo tài chính (scan).
# Trích xuất 3 bảng:
# 1. Bảng cân đối kế toán hợp nhất
# 2. Báo cáo KQHĐKD
# 3. Báo cáo lưu chuyển tiền tệ

# Trả về JSON:

# {
#  "bang_can_doi_ke_toan": [
#    {"chi_tieu": "", "so_tien": "", "ky": ""}
#  ],
#  "ket_qua_kinh_doanh": [
#    {"chi_tieu": "", "so_tien": "", "ky": ""}
#  ],
#  "luu_chuyen_tien_te": [
#    {"chi_tieu": "", "so_tien": "", "ky": ""}
#  ]
# }

# Chỉ JSON.
# """
#                         },
#                         {
#                             "type": "image_url",
#                             "image_url": f"data:image/png;base64,{b64}"
#                         }
#                     ]
#                 }
#             ]
#         )

#         results.append(resp.choices[0].message.content)

#     return results

# # ======================
# # STEP 3: PARSE JSON
# # ======================
# def parse_ai_json(texts):
#     merged = {
#         "bang_can_doi_ke_toan": [],
#         "ket_qua_kinh_doanh": [],
#         "luu_chuyen_tien_te": []
#     }

#     for t in texts:
#         js = json.loads(t[t.find("{"):t.rfind("}")+1])
#         for k in merged:
#             merged[k].extend(js.get(k, []))

#     return merged

# # ======================
# # STEP 4: CHUẨN HÓA CỘT
# # ======================
# COLUMN_MAP = {
#     "chi_tieu": "Item",
#     "so_tien": "Value",
#     "ky": "Period"
# }

# def normalize(df):
#     df = df.rename(columns=COLUMN_MAP)
#     df["Value"] = (
#         df["Value"]
#         .astype(str)
#         .str.replace(".", "", regex=False)
#         .str.replace(",", "", regex=False)
#     )
#     return df

# # ======================
# # STEP 5: SAVE CSV
# # ======================
# def save_csv(data):
#     for name, rows in data.items():
#         df = pd.DataFrame(rows)
#         df = normalize(df)
#         df.to_csv(
#             f"{OUTPUT_DIR}/{name}.csv",
#             index=False,
#             encoding="utf-8-sig"
#         )

# # ======================
# # MAIN
# # ======================
# if __name__ == "__main__":
#     images = pdf_to_images(PDF_PATH)
#     ai_text = ocr_to_json(images)
#     data = parse_ai_json(ai_text)
#     save_csv(data)
#     print("✅ PDF → AI OCR → JSON → CSV HOÀN TẤT")

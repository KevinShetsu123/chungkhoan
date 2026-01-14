import os
import re
import json
import threading
from typing import Dict, List, Optional

import pandas as pd
import requests
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from mistralai import Mistral
from mistralai import models  # SDKError


# =========================
# API KEY (theo yêu cầu: để thẳng trong code)
# Khuyến nghị: đổi sang env MISTRAL_API_KEY sau khi chạy ổn.
# =========================
MISTRAL_API_KEY = "Ucg2pUYmpXOQyVmicgE9ySKF2hqYcFFT"

# OCR model
OCR_MODEL = "mistral-ocr-latest"  # Mistral Document AI OCR :contentReference[oaicite:2]{index=2}

# Chat model để trích xuất bảng
CHAT_MODEL_DEFAULT = "mistral-small-latest"

TABLES = {
    "balance_sheet": {
        "title": "Báo cáo cân đối kế toán hợp nhất (Bảng cân đối kế toán hợp nhất)",
        "filename": "bao_cao_can_doi_ke_toan_hop_nhat.csv",
    },
    "income_statement": {
        "title": "Báo cáo kết quả hoạt động kinh doanh",
        "filename": "bao_cao_ket_qua_hoat_dong_kinh_doanh.csv",
    },
    "cashflow": {
        "title": "Báo cáo lưu chuyển tiền tệ",
        "filename": "bao_cao_luu_chuyen_tien_te.csv",
    },
}


# =========================
# Helpers
# =========================
def normalize_text(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def extract_json_block(s: str) -> str:
    m = re.search(r"```json\s*(\{.*?\})\s*```", s, flags=re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1)
    m2 = re.search(r"(\{.*\})", s, flags=re.DOTALL)
    if m2:
        return m2.group(1)
    return s.strip()


def parse_model_json(s: str) -> dict:
    raw = extract_json_block(s)
    try:
        return json.loads(raw)
    except Exception:
        raw2 = re.sub(r",\s*([}\]])", r"\1", raw)  # trailing commas
        return json.loads(raw2)


def json_to_dataframe(table_json: Dict) -> pd.DataFrame:
    cols = table_json.get("columns", [])
    rows = table_json.get("rows", [])
    if not isinstance(cols, list) or not cols:
        raise ValueError("JSON thiếu 'columns' hoặc columns không hợp lệ.")
    if not isinstance(rows, list):
        raise ValueError("JSON thiếu 'rows' hoặc rows không hợp lệ.")

    fixed_rows = []
    for r in rows:
        if isinstance(r, dict):
            fixed_rows.append({c: (r.get(c, "") if r.get(c, "") is not None else "") for c in cols})

    return pd.DataFrame(fixed_rows, columns=cols)


# =========================
# 1) Mistral OCR: upload PDF -> /v1/ocr -> text
# =========================
def mistral_ocr_pdf_to_text(client: Mistral, api_key: str, pdf_path: str, log_cb=None) -> str:
    """
    Upload file -> get file_id -> call /v1/ocr -> concat pages markdown.
    - Upload endpoint supports purpose="ocr". :contentReference[oaicite:3]{index=3}
    - OCR endpoint accepts document.file_id. :contentReference[oaicite:4]{index=4}
    """
    if log_cb:
        log_cb("Upload PDF lên Mistral Files...")

    # Upload file (SDK)
    # docs: mistral.files.upload(file={"file_name": "...", "content": open(...,"rb")}) :contentReference[oaicite:5]{index=5}
    with open(pdf_path, "rb") as f:
        try:
            up = client.files.upload(
                file={"file_name": os.path.basename(pdf_path), "content": f},
                purpose="ocr",  # theo docs purpose có "ocr" :contentReference[oaicite:6]{index=6}
            )
        except TypeError:
            # nếu SDK version không nhận purpose trong upload, bỏ qua
            f.seek(0)
            up = client.files.upload(file={"file_name": os.path.basename(pdf_path), "content": f})

    file_id = getattr(up, "id", None) or up.get("id")
    if not file_id:
        raise RuntimeError("Upload file thất bại: không nhận được file_id.")

    if log_cb:
        log_cb(f"✅ Upload xong. file_id={file_id}. Gọi OCR...")

    # Call OCR REST (ổn định vì docs minh hoạ rõ file_id) :contentReference[oaicite:7]{index=7}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": OCR_MODEL,
        "document": {"file_id": file_id},  # theo docs /v1/ocr với file_id :contentReference[oaicite:8]{index=8}
    }
    resp = requests.post("https://api.mistral.ai/v1/ocr", headers=headers, json=payload, timeout=300)
    resp.raise_for_status()
    data = resp.json()

    pages = data.get("pages", [])
    if not pages:
        raise RuntimeError("OCR không trả về pages.")

    # OCR trả về markdown theo từng trang (docs có ví dụ pages[].markdown) :contentReference[oaicite:9]{index=9}
    md_parts = []
    for p in pages:
        md = p.get("markdown", "")
        if md:
            md_parts.append(md)

    full_text = normalize_text("\n\n".join(md_parts))
    if log_cb:
        log_cb(f"✅ OCR xong. Độ dài text: {len(full_text):,} ký tự")
    return full_text


# =========================
# 2) Chat extract bảng -> JSON
# =========================
def mistral_extract_table_json(
    client: Mistral,
    model: str,
    full_text: str,
    table_title: str,
    log_cb=None,
) -> Dict:
    system_msg = (
        "Bạn là hệ thống trích xuất bảng tài chính từ văn bản OCR. "
        "Chỉ trả về JSON hợp lệ, KHÔNG giải thích, KHÔNG thêm chữ ngoài JSON."
    )

    user_msg = f"""
Trích xuất bảng: "{table_title}" từ văn bản OCR dưới đây.

BẮT BUỘC trả về 1 JSON object hợp lệ có cấu trúc:
{{
  "table_name": "<tên bảng>",
  "columns": ["Mã số","Chỉ tiêu","Thuyết minh", "...các cột số liệu..."],
  "rows": [
    {{"Mã số":"...","Chỉ tiêu":"...","Thuyết minh":"...","...":"..."}},
    ...
  ]
}}

Quy tắc:
- columns đúng thứ tự cột trong bảng.
- rows giữ ĐỦ mọi dòng; ô trống để "".
- Giữ đầy đủ mã số, thuyết minh/ghi chú, và tất cả các cột số liệu (kỳ này/kỳ trước/...).
- Dòng tiêu đề nhóm (không có mã số) vẫn đưa vào (Mã số="").
- Không bịa dữ liệu. Không đoán.

Văn bản OCR:
\"\"\"{full_text[:220_000]}\"\"\"
""".strip()

    if log_cb:
        log_cb(f"Gọi Mistral Chat để trích xuất: {table_title}")

    res = client.chat.complete(
        model=model,
        messages=[{"role": "system", "content": system_msg},
                  {"role": "user", "content": user_msg}],
        temperature=0.0,
    )
    content = res.choices[0].message.content if res and res.choices else ""
    if not content:
        raise RuntimeError("Chat không trả về nội dung.")
    return parse_model_json(content)


# =========================
# UI
# =========================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Mistral OCR PDF -> Trích xuất 3 bảng BCTC -> CSV")
        self.geometry("980x640")

        self.pdf_path = tk.StringVar()
        self.out_dir = tk.StringVar(value=os.getcwd())
        self.chat_model = tk.StringVar(value=CHAT_MODEL_DEFAULT)
        self.api_key = tk.StringVar(value=MISTRAL_API_KEY)

        self._build_ui()

    def _build_ui(self):
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        r1 = ttk.Frame(frm); r1.pack(fill="x", pady=6)
        ttk.Label(r1, text="PDF:").pack(side="left")
        ttk.Entry(r1, textvariable=self.pdf_path).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(r1, text="Chọn PDF...", command=self.pick_pdf).pack(side="left")

        r2 = ttk.Frame(frm); r2.pack(fill="x", pady=6)
        ttk.Label(r2, text="Thư mục lưu CSV:").pack(side="left")
        ttk.Entry(r2, textvariable=self.out_dir).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(r2, text="Chọn thư mục...", command=self.pick_out_dir).pack(side="left")

        r3 = ttk.Frame(frm); r3.pack(fill="x", pady=6)
        ttk.Label(r3, text="Chat model:").pack(side="left")
        ttk.Entry(r3, textvariable=self.chat_model, width=22).pack(side="left", padx=8)

        r4 = ttk.Frame(frm); r4.pack(fill="x", pady=6)
        ttk.Label(r4, text="Mistral API key:").pack(side="left")
        ent = ttk.Entry(r4, textvariable=self.api_key, show="*", width=60)
        ent.pack(side="left", padx=8, fill="x", expand=True)
        ttk.Button(r4, text="Hiện/Ẩn",
                   command=lambda: ent.config(show="" if ent.cget("show") else "*")).pack(side="left")

        rb = ttk.Frame(frm); rb.pack(fill="x", pady=10)
        self.btn_run = ttk.Button(rb, text="Chạy trích xuất", command=self.run_async)
        self.btn_run.pack(side="left")
        ttk.Button(rb, text="Thoát", command=self.destroy).pack(side="right")

        ttk.Label(frm, text="Log:").pack(anchor="w")
        self.log = tk.Text(frm, height=22, wrap="word")
        self.log.pack(fill="both", expand=True, pady=(6, 0))
        self.log.configure(state="disabled")

    def log_write(self, msg: str):
        self.log.configure(state="normal")
        self.log.insert("end", msg + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")
        self.update_idletasks()

    def pick_pdf(self):
        path = filedialog.askopenfilename(
            title="Chọn file PDF",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if path:
            self.pdf_path.set(path)

    def pick_out_dir(self):
        path = filedialog.askdirectory(title="Chọn thư mục lưu CSV")
        if path:
            self.out_dir.set(path)

    def run_async(self):
        pdf = self.pdf_path.get().strip()
        if not pdf:
            messagebox.showwarning("Thiếu PDF", "Hãy chọn file PDF trước.")
            return
        if not os.path.isfile(pdf):
            messagebox.showerror("Lỗi", "Đường dẫn PDF không tồn tại.")
            return

        self.btn_run.config(state="disabled")
        self.log_write("=== Bắt đầu ===")
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        try:
            pdf = self.pdf_path.get().strip()
            out_dir = self.out_dir.get().strip()
            os.makedirs(out_dir, exist_ok=True)

            api_key = self.api_key.get().strip()
            if not api_key:
                raise ValueError("API key rỗng.")

            client = Mistral(api_key=api_key)
            chat_model = self.chat_model.get().strip() or CHAT_MODEL_DEFAULT

            # 1) OCR bằng Mistral
            ocr_text = mistral_ocr_pdf_to_text(client, api_key, pdf, log_cb=self.log_write)

            # 2) Trích xuất 3 bảng -> CSV
            saved = []
            for key, meta in TABLES.items():
                table_json = mistral_extract_table_json(
                    client=client,
                    model=chat_model,
                    full_text=ocr_text,
                    table_title=meta["title"],
                    log_cb=self.log_write
                )
                df = json_to_dataframe(table_json)
                out_path = os.path.join(out_dir, meta["filename"])
                df.to_csv(out_path, index=False, encoding="utf-8-sig")
                saved.append(out_path)
                self.log_write(f"✅ Saved: {out_path} (rows={len(df)})")

            self.log_write("=== HOÀN TẤT ===")
            messagebox.showinfo("Xong", "Đã xuất 3 file CSV:\n\n" + "\n".join(saved))

        except requests.HTTPError as e:
            self.log_write(f"❌ HTTPError: {e}")
            messagebox.showerror("HTTPError", str(e))
        except models.SDKError as e:
            self.log_write(f"❌ Mistral SDKError: {e}")
            messagebox.showerror("Mistral SDKError", str(e))
        except Exception as e:
            self.log_write(f"❌ Error: {e}")
            messagebox.showerror("Lỗi", str(e))
        finally:
            self.btn_run.config(state="normal")


if __name__ == "__main__":
    App().mainloop()

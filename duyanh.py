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
from mistralai import models

# =========================
# API KEY
# =========================
MISTRAL_API_KEY = "Ucg2pUYmpXOQyVmicgE9ySKF2hqYcFFT"

# OCR model
OCR_MODEL = "mistral-ocr-latest"

# Chat model (Khuyên dùng mistral-large-latest hoặc mistral-small-latest cho task phức tạp này)
CHAT_MODEL_DEFAULT = "mistral-large-latest"

# =========================
# CẤU HÌNH BẢNG & TỪ KHÓA NHẬN DIỆN (UPDATED)
# =========================
TABLES = {
    "balance_sheet": {
        "title": "Bảng Cân đối kế toán (Balance Sheet)",
        "filename": "bang_can_doi_ke_toan.csv",
        "keywords": (
            "Tìm các dòng: TÀI SẢN, TÀI SẢN NGẮN HẠN, TÀI SẢN DÀI HẠN, "
            "TỔNG CỘNG TÀI SẢN, NỢ PHẢI TRẢ, VỐN CHỦ SỞ HỮU, TỔNG CỘNG NGUỒN VỐN. "
            "Cột số liệu thường là Số cuối kỳ và Số đầu năm."
        )
    },
    "income_statement": {
        "title": "Báo cáo Kết quả hoạt động kinh doanh (Income Statement)",
        "filename": "ket_qua_kinh_doanh.csv",
        "keywords": (
            "Tìm các dòng: Doanh thu bán hàng và cung cấp dịch vụ, Doanh thu thuần, "
            "Giá vốn hàng bán, Lợi nhuận gộp, Lợi nhuận thuần, Tổng lợi nhuận kế toán trước thuế, "
            "Lợi nhuận sau thuế thu nhập doanh nghiệp (Mã số 60)."
        )
    },
    "cashflow": {
        "title": "Báo cáo Lưu chuyển tiền tệ (Cash Flow Statement)",
        "filename": "luu_chuyen_tien_te.csv",
        "keywords": (
            "Tìm các dòng: Lưu chuyển tiền từ hoạt động kinh doanh, "
            "Lưu chuyển tiền từ hoạt động đầu tư, Lưu chuyển tiền từ hoạt động tài chính, "
            "Lưu chuyển tiền thuần trong kỳ, Tiền và tương đương tiền cuối kỳ."
        )
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
        # Cố gắng sửa lỗi trailing commas phổ biến trong JSON do AI sinh ra
        raw2 = re.sub(r",\s*([}\]])", r"\1", raw)
        return json.loads(raw2)


def json_to_dataframe(table_json: Dict) -> pd.DataFrame:
    cols = table_json.get("columns", [])
    rows = table_json.get("rows", [])
    
    if not isinstance(cols, list) or not cols:
        # Fallback nếu AI không trả về columns đúng
        cols = ["Chỉ tiêu", "Mã số", "Thuyết minh", "Kỳ này", "Kỳ trước"]
        
    if not isinstance(rows, list):
        raise ValueError("JSON thiếu 'rows' hoặc rows không hợp lệ.")

    fixed_rows = []
    for r in rows:
        if isinstance(r, dict):
            # Đảm bảo thứ tự cột đúng
            row_data = {c: (r.get(c, "") if r.get(c, "") is not None else "") for c in cols}
            fixed_rows.append(row_data)

    return pd.DataFrame(fixed_rows, columns=cols)


# =========================
# 1) Mistral OCR
# =========================
def mistral_ocr_pdf_to_text(client: Mistral, api_key: str, pdf_path: str, log_cb=None) -> str:
    if log_cb:
        log_cb("Upload PDF lên Mistral Files...")

    with open(pdf_path, "rb") as f:
        try:
            up = client.files.upload(
                file={"file_name": os.path.basename(pdf_path), "content": f},
                purpose="ocr",
            )
        except TypeError:
            f.seek(0)
            up = client.files.upload(file={"file_name": os.path.basename(pdf_path), "content": f})

    file_id = getattr(up, "id", None) or up.get("id")
    if not file_id:
        raise RuntimeError("Upload file thất bại: không nhận được file_id.")

    if log_cb:
        log_cb(f"✅ Upload xong. file_id={file_id}. Gọi OCR...")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": OCR_MODEL,
        "document": {"file_id": file_id},
    }
    resp = requests.post("https://api.mistral.ai/v1/ocr", headers=headers, json=payload, timeout=300)
    resp.raise_for_status()
    data = resp.json()

    pages = data.get("pages", [])
    if not pages:
        raise RuntimeError("OCR không trả về pages.")

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
# 2) Chat extract bảng -> JSON (UPDATED WITH PROMPT)
# =========================
def mistral_extract_table_json(
    client: Mistral,
    model: str,
    full_text: str,
    table_info: Dict, # Nhận cả dict info thay vì chỉ title
    log_cb=None,
) -> Dict:
    
    title = table_info["title"]
    keywords = table_info["keywords"]

    # --- PROMPT CHI TIẾT ĐƯỢC TÍCH HỢP VÀO ĐÂY ---
    system_msg = (
        "Bạn là một chuyên gia Kế toán và Phân tích dữ liệu tài chính (Financial Data Analyst) "
        "chuyên về chuẩn mực kế toán Việt Nam (VAS). "
        "Nhiệm vụ của bạn là trích xuất dữ liệu Báo cáo tài chính từ văn bản OCR sang định dạng JSON chính xác."
    )

    user_msg = f"""
    Hãy đọc văn bản OCR và trích xuất bảng: "{title}".

    **Hướng dẫn nhận diện (Key words):**
    {keywords}

    **Quy tắc trích xuất (BẮT BUỘC):**
    1. Chỉ trích xuất đúng bảng yêu cầu.
    2. **Cấu trúc JSON đầu ra:**
       {{
          "table_name": "{title}",
          "columns": ["Chỉ tiêu", "Mã số", "Thuyết minh", "Kỳ này", "Kỳ trước"],
          "rows": [
             {{
                "Chỉ tiêu": "Tên dòng...", 
                "Mã số": "01", 
                "Thuyết minh": "V.01", 
                "Kỳ này": "10.000.000", 
                "Kỳ trước": "9.000.000"
             }},
             ...
          ]
       }}
    3. **Xử lý số liệu:** - Giữ nguyên định dạng số (dấu chấm/phẩy).
       - Số âm trong ngoặc đơn `( )` phải giữ nguyên (ví dụ: `(500)`).
    4. **Cột 'Kỳ này' và 'Kỳ trước':** Thường là cột số liệu đầu tiên và thứ hai (hoặc Số cuối kỳ/Số đầu năm). Hãy tự suy luận dựa vào tiêu đề cột trong văn bản.
    5. **Xử lý lỗi OCR:** Nếu dòng bị lệch, hãy ưu tiên cột "Mã số" (như 01, 10, 11, 20, 60...) để gióng hàng.
    6. Nếu ô trống, hãy để chuỗi rỗng "".
    7. Lấy ĐẦY ĐỦ các dòng từ đầu bảng đến cuối bảng (dựa vào Tổng cộng).

    **Văn bản OCR nguồn:**
    \"\"\"{full_text[:250_000]}\"\"\"
    """.strip()

    if log_cb:
        log_cb(f"Đang phân tích và trích xuất: {title}...")

    res = client.chat.complete(
        model=model,
        messages=[{"role": "system", "content": system_msg},
                  {"role": "user", "content": user_msg}],
        temperature=0.0, # Nhiệt độ 0 để đảm bảo tính nhất quán
        response_format={"type": "json_object"} # Bắt buộc JSON mode (nếu model hỗ trợ)
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
        self.title("Tool Trích xuất BCTC chuẩn VAS (Mistral AI)")
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
        ttk.Label(r1, text="File PDF BCTC:").pack(side="left")
        ttk.Entry(r1, textvariable=self.pdf_path).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(r1, text="Chọn PDF...", command=self.pick_pdf).pack(side="left")

        r2 = ttk.Frame(frm); r2.pack(fill="x", pady=6)
        ttk.Label(r2, text="Lưu CSV tại:").pack(side="left")
        ttk.Entry(r2, textvariable=self.out_dir).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(r2, text="Chọn thư mục...", command=self.pick_out_dir).pack(side="left")

        r3 = ttk.Frame(frm); r3.pack(fill="x", pady=6)
        ttk.Label(r3, text="Model Chat:").pack(side="left")
        ttk.Entry(r3, textvariable=self.chat_model, width=25).pack(side="left", padx=8)
        ttk.Label(r3, text="(Nên dùng mistral-large-latest)").pack(side="left", padx=5)

        r4 = ttk.Frame(frm); r4.pack(fill="x", pady=6)
        ttk.Label(r4, text="Mistral API Key:").pack(side="left")
        ent = ttk.Entry(r4, textvariable=self.api_key, show="*", width=60)
        ent.pack(side="left", padx=8, fill="x", expand=True)

        rb = ttk.Frame(frm); rb.pack(fill="x", pady=10)
        self.btn_run = ttk.Button(rb, text=">>> BẮT ĐẦU TRÍCH XUẤT <<<", command=self.run_async)
        self.btn_run.pack(side="left")
        ttk.Button(rb, text="Thoát", command=self.destroy).pack(side="right")

        ttk.Label(frm, text="Nhật ký xử lý (Logs):").pack(anchor="w")
        self.log = tk.Text(frm, height=20, wrap="word", bg="#f0f0f0")
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
        self.log.configure(bg="black", fg="#00ff00") # Hacker style log
        self.log_write("=== Bắt đầu tiến trình trích xuất BCTC (VAS) ===")
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
                # Truyền toàn bộ meta (bao gồm title và keywords) vào hàm
                table_json = mistral_extract_table_json(
                    client=client,
                    model=chat_model,
                    full_text=ocr_text,
                    table_info=meta, 
                    log_cb=self.log_write
                )
                
                df = json_to_dataframe(table_json)
                
                # Check nhanh dữ liệu
                if df.empty:
                    self.log_write(f"⚠️ Cảnh báo: Bảng {key} không có dữ liệu.")
                else:
                    out_path = os.path.join(out_dir, meta["filename"])
                    df.to_csv(out_path, index=False, encoding="utf-8-sig")
                    saved.append(out_path)
                    self.log_write(f"✅ Đã lưu: {out_path} ({len(df)} dòng)")

            self.log_write("=== HOÀN TẤT TOÀN BỘ ===")
            messagebox.showinfo("Thành công", "Đã xuất xong các file CSV:\n\n" + "\n".join(saved))

        except Exception as e:
            self.log_write(f"❌ Lỗi nghiêm trọng: {e}")
            messagebox.showerror("Lỗi", str(e))
        finally:
            self.btn_run.config(state="normal")
            self.log.configure(bg="#f0f0f0", fg="black")


if __name__ == "__main__":
    App().mainloop()
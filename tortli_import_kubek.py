"""
Tortli.pl → Printerval Importer
- Đọc danh sách URL Kubek từ tortli_products_v2.json
- Lấy HTML từng trang sản phẩm
- POST lên https://printerval.com/pl/crawl-product/create-from-html?debug=1
- Chạy đồng thời 3 request cùng lúc (ThreadPoolExecutor)
"""

import json
import time
import requests
import os
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Cấu hình ────────────────────────────────────────────────
PRINTERVAL_API   = "https://printerval.com/pl/crawl-product/create-from-html?debug=1"
USER_EMAIL       = "duytungnguyen.bkhn.95@gmail.com"
USER_TOKEN       = "4a5747260b5614a86d6fb70f1012ad19"
BRAND_ID         = 2
CATEGORIES       = 35
COUNTRY_CODE     = "pl"
GENERATE_VARIANT = 1
IS_OVERWRITE     = 1
REMOVE_SIZE_CHART = True
TAGS             = "5489"

CONCURRENCY      = 3   # Số request đồng thời
PROGRESS_FILE    = "tortli_kubek_progress.json"
RESULT_FILE      = "tortli_kubek_results.json"

HEADERS_HTML = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8",
}

HEADERS_API = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Cookie": "fingerprint=RCqrLviYtVHcvqKKXDoFxS8P1beLbCdOwrB4m1PJ; _ga=GA1.1.1689186747.1768273138; _fbp=fb.1.1768273138375.890768100260677978; __kla_id=eyJjaWQiOiJOMlpsWW1ObVpETXRZVEprTmkwME5XTm1MV0V5WldFdE9HSmxaV1ZoT0ROaE5qUTMifQ==; _scid=jO68HZP1ddMoJaFXVjCWvJPzeJBVfZ5I; _tt_enable_cookie=1; _ttp=01KETMNNNPNDDG2WJCYK8PRED4_.tt.1; __stripe_mid=bd7d6d95-d461-44c4-9895-e3c9bae8bc0f3f63f8; _pin_unauth=dWlkPVlqZzJPVEF6WldNdE1tRm1NaTAwTlRJNExXSm1NVEV0TWpSaE1qQXhNR0ZsWmpNeQ; visit_source=google; datadome=n4z9uT~vEVeB43IHlvj5dQw2EgAnYQxhwVs2KFszA_v8taz2sWnfA2gB3ejBLPzo0gf887O2f3ejFvVgV4fuWpWXqXSdnSWbsMXGGbea5P~q31qWOIMFC7gPouo7FsJv; wb-p-SERVER=wwwb-app242; AwinChannelCookie=direct; _yjsu_yjad=1772174112.650d136e-92d9-4af9-953f-89ce8b1e5ac7; _cfuvid=wiMmWMWWnm4WTIsb9jdWv1.rg4ba5Z2xIgzNwad7_rI-1772174120063-0.0.1.1-604800000; _twpid=tw.1772417673199.691840371330915802; _sctr=1%7C1772384400000; is_valid_select_country=1; _ScCbts=%5B%5D; _gcl_au=1.1.224713083.1768273138.1327437920.1772439042.1772439169; cto_bundle=PmJPrV8lMkJoJTJCTVdaUTk1QUlzaTJ3R1g5bDQ2eCUyRndjQVRNVnRqVnlzbk5tWUVSODRKUG1kdDFSZFZaZUlyNVdMV01hMkJQeml4MGhmRGEzRTZUdHI2ZjF2YTRxZTdwc0hvWE9MUG5NUndIR3pvWU93OGlRQ25sZFFsV0NhYUk2OUl6UWJCVzN5RzVUZWs1ZlZjQ2hDa0RzYzdoemclM0QlM0Q; _scid_r=v268HZP1ddMoJaFXVjCWvJPzeJBVfZ5IESikxw; _rdt_uuid=1768273138309.bce4243b-b00a-4f73-873c-f40a6a761270; _uetsid=8b088e5015dd11f180a63f06460dee96|85igp3|2|g41|0|2252; _uetvid=ceaaf8b0f02b11f0b8e1751351e65873|1002mo0|1772509173858|2|1|bat.bing.com/p/insights/c/j; ttcsid=1772507504985::dEVCIBCGviF2Xh6QcmLK.128.1772509184709.0; ttcsid_CL46DJ3C77U0CK80E320=1772507504985::8srbPA9_s9irD3-_7CST.128.1772509184709.1; _ga_5Q57T3BBYZ=GS2.1.s1772507504$o141$g1$t1772509255$j60$l0$h1189334786; __cf_bm=JmSXCEkXktP27cBelRG_3PLGP6QPfrFC1B6IA.sKPB4-1772510538-1.0.1.1-bgGnOoM5MP5WNcyazIIPyP1BjVhab8.gUxubU9Xs4at7KncI1XnQ3rXfqt8RYVsGiQv9052R.jlPGe8zcXapc4VSmfucA_cmAaEOGo1otFM; user_id=eyJpdiI6IldNenhNdnhFUW1pSHZ4Z3RERU9jRlE9PSIsInZhbHVlIjoid21iYnRMUVdTbGVZM2JoZFIxeWFkUT09IiwibWFjIjoiZDQxMGFjNGQwODg5NTA2NzI1MmNiNDQ3YTBmMDdhZWRhNWM2YWNiYTExY2U0ODE0ZTNkNjc5YzE1YmJkYWI4OCJ9; sso_token=eyJpdiI6IkQxeWNMRVFleUcxdEdtbnU5RzE1TFE9PSIsInZhbHVlIjoiZDFRXC9QTFo3ZXc2bjhicEJMVDZhRWEzWHFiVWl3cXAxVWUxbWFLZXJ3V01tZkJxSnhsc0pFSmpQbkRUZTJyblkiLCJtYWMiOiI1NzEzODVkM2M3YmFhOWI1Mjk1ZmE5YzQxYzc4MDM1ZDhmN2Y4ZGQwNjY2NzZjNjNmMDk3NjAxZDFkNTJjOTJmIn0%3D; remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6InVLOEVEa2VNWjdLSGtGbTA5bHFEZEE9PSIsInZhbHVlIjoiMTI2WHI5T2VxQlFodVhuWGtMRktrUXkwdWQ0U3pjYkkzYjJMSWtoN1ZHYllwamxvaUZUaW55UkNGYVZ4ekljM21lTzBSSzRLeUZXMHlmUXBWSXRaQ1lQSVlxMmdRU3A5VDREY2tobmo1SFE9IiwibWFjIjoiY2NjNDQxN2FlNmQzZDUzN2U4NDE1YWJkZjI4ZjNkMDM3MmQ4Nzg1Y2M0Nzk2MDM4YWIxZDcwMmZlYWRkNjZjOSJ9; laravel_session=eyJpdiI6InhwMlJBa3hPOEN3RGFLb2Y4MCtFK2c9PSIsInZhbHVlIjoiUkZhaXgxT1B2bXVnNkxIakRkRHRRT3BUZ0N2bjBhRkVlbFBJQVlQWThRZmFxS1c2bzFFMzNNK1A0RURzeGxtcEN6XC9saCtCVEUrbkJJNmN3cStMeXlRPT0iLCJtYWMiOiIwZmIxZTM4ZDUyYTlkMTA2MWZlNTMzYjE1ZTM4ZThmMDc1M2MwZTgxNWE3NzM0NmJhZmQxY2I3OGRiNTcwZWNlIn0%3D",
    "origin": "chrome-extension://clkjnbjpinbbodjlagpnecfjjopokbjg",
    "version": "2.2.8",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

# ─── Thread-safe lock cho file I/O ───────────────────────────
_lock = threading.Lock()

# ─── Load / Save ─────────────────────────────────────────────

def load_kubek_urls(json_file="tortli_products_v2.json"):
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    products = data.get("by_product_type", {}).get("Kubek", [])
    print(f"📦 Tổng URLs Kubek: {len(products)}")
    return products


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"done": [], "failed": [], "skipped": []}


def save_progress(progress):
    with _lock:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)


def load_results():
    if os.path.exists(RESULT_FILE):
        with open(RESULT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_results(results):
    with _lock:
        with open(RESULT_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)


# ─── Core functions ───────────────────────────────────────────

def fetch_html(url: str, max_retry=3) -> str | None:
    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.get(url, headers=HEADERS_HTML, timeout=30)
            if resp.status_code == 200:
                return resp.text
            else:
                print(f"    ⚠️  [{url[-40:]}] HTTP {resp.status_code} (attempt {attempt})")
        except Exception as e:
            print(f"    ❌ [{url[-40:]}] fetch error attempt {attempt}: {e}")
        if attempt < max_retry:
            time.sleep(1)
    return None


def post_to_printerval(product_url: str, html: str, max_retry=3):
    payload = {
        "brand_id": BRAND_ID,
        "categories": CATEGORIES,
        "country_code": COUNTRY_CODE,
        "generate_variant": GENERATE_VARIANT,
        "html": html,
        "is_overwrite_product": IS_OVERWRITE,
        "remove_size_chart": REMOVE_SIZE_CHART,
        "tags": TAGS,
        "url": product_url,
        "user_email": USER_EMAIL,
        "user_token": USER_TOKEN,
    }
    for attempt in range(1, max_retry + 1):
        try:
            resp = requests.post(
                PRINTERVAL_API,
                json=payload,
                headers=HEADERS_API,
                timeout=120
            )
            is_json = "application/json" in resp.headers.get("content-type", "")
            return {
                "status_code": resp.status_code,
                "response": resp.json() if is_json else resp.text[:500],
            }
        except Exception as e:
            print(f"    ❌ POST error attempt {attempt}: {e}")
            if attempt < max_retry:
                time.sleep(2)
    return {"status_code": 0, "response": "Max retries exceeded"}


# ─── Worker (chạy trong thread) ───────────────────────────────

def process_product(product, idx, total, progress, results):
    url   = product["url"]
    title = product["title"]
    tag   = f"[{idx}/{total}]"

    print(f"\n{tag} 🛒 {title[:60]}")
    print(f"  URL: {url}")

    # 1) Fetch HTML
    html = fetch_html(url)
    if not html:
        print(f"  {tag} ❌ Không lấy được HTML")
        with _lock:
            progress["failed"].append(url)
        save_progress(progress)
        return

    html_size = len(html)
    print(f"  {tag} 📥 HTML: {html_size:,} bytes → POST...")

    # 2) POST
    api_result = post_to_printerval(url, html)
    status = api_result["status_code"]
    resp   = api_result["response"]

    record = {
        "url": url,
        "title": title,
        "html_size": html_size,
        "api_status": status,
        "api_response": resp,
        "timestamp": datetime.now().isoformat(),
    }

    with _lock:
        results.append(record)

    save_results(results)

    if status == 200:
        success = True
        if isinstance(resp, dict):
            if resp.get("success") is False or resp.get("error"):
                success = False
                msg = resp.get("error") or resp.get("message", "")
                print(f"  {tag} ⚠️  API 200 nhưng lỗi: {str(msg)[:100]}")
            else:
                print(f"  {tag} ✅ Thành công! {str(resp)[:100]}")
        else:
            print(f"  {tag} ✅ HTTP 200: {str(resp)[:100]}")

        with _lock:
            if success:
                progress["done"].append(url)
            else:
                progress["failed"].append(url)
    else:
        print(f"  {tag} ❌ HTTP {status}: {str(resp)[:120]}")
        with _lock:
            progress["failed"].append(url)

    save_progress(progress)


# ─── Main ─────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("🚀 TORTLI → PRINTERVAL Importer (Kubek) — Concurrency x3")
    print(f"⏰ Bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    products = load_kubek_urls()
    progress = load_progress()
    results  = load_results()

    done_urls = set(progress["done"] + progress["failed"] + progress["skipped"])
    pending   = [p for p in products if p["url"] not in done_urls]

    print(f"✅ Đã xử lý : {len(progress['done'])} thành công | "
          f"❌ {len(progress['failed'])} thất bại | "
          f"⏭️  {len(progress['skipped'])} bỏ qua")
    print(f"⏳ Còn lại  : {len(pending)} sản phẩm\n")

    if not pending:
        print("🎉 Tất cả sản phẩm đã được xử lý!")
        return

    total = len(pending)

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {
            executor.submit(process_product, product, idx, total, progress, results): product
            for idx, product in enumerate(pending, 1)
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                product = futures[future]
                print(f"  💥 Lỗi không mong đợi [{product['url']}]: {e}")
                with _lock:
                    progress["failed"].append(product["url"])
                save_progress(progress)

    # Tổng kết
    print(f"\n{'=' * 65}")
    print(f"🏁 HOÀN THÀNH — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 65}")
    print(f"✅ Thành công : {len(progress['done'])}")
    print(f"❌ Thất bại   : {len(progress['failed'])}")
    print(f"⏭️  Bỏ qua     : {len(progress['skipped'])}")
    print(f"📁 Kết quả    : {RESULT_FILE}")
    print(f"📁 Tiến trình : {PROGRESS_FILE}")


if __name__ == "__main__":
    main()

"""
Printerval Import Bot — Telegram
======================================
Nhận link trang danh sách sản phẩm bất kỳ, tự động:
  1. Scroll + click "more" đến khi load hết sản phẩm (Crawl4AI + browser)
  2. Gom toàn bộ link sản phẩm
  3. Lấy HTML từng trang sản phẩm
  4. POST lên Printerval create-from-html API

Cú pháp:
  /crawl {url}     → Crawl toàn bộ sản phẩm từ trang danh sách
  /status          → Xem job đang chạy
  /cancel          → Huỷ job đang chạy
  Hoặc gửi URL trực tiếp (không cần /crawl)
"""

import asyncio
import json
import os
import re
import time
import threading
import requests
from urllib.parse import urlparse, urljoin
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from playwright.async_api import async_playwright
# Mới thêm các thư viện này ở gần phần import trên cùng:
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"Bot is alive!")
def dummy_web_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    server.serve_forever()

load_dotenv(".env")

# ─── Config ──────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8590410345:AAF5Xgq6GT6FHZ0vyfe_SCIjZsq_k1dew1I")
CRAWLED_FILE       = "crawled_urls.txt"
PRINTERVAL_API     = "https://printerval.com/crawl-product/create-from-html?debug=1"
USER_EMAIL         = "duytungnguyen.bkhn.95@gmail.com"
USER_TOKEN         = "4a5747260b5614a86d6fb70f1012ad19"
BRAND_ID           = 9
DEFAULT_CATEGORIES = 35   # dùng nếu không truyền trong lệnh
COUNTRY_CODE       = "us"
GENERATE_VARIANT   = 1
IS_OVERWRITE       = 1
REMOVE_SIZE_CHART  = True
TAGS               = "14"
CONCURRENCY        = 3   # Số luồng import song song

HEADERS_HTML = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}
HEADERS_API = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Cookie": "fingerprint=RCqrLviYtVHcvqKKXDoFxS8P1beLbCdOwrB4m1PJ; _ga=GA1.1.1689186747.1768273138; _fbp=fb.1.1768273138375.890768100260677978; __kla_id=eyJjaWQiOiJOMlpsWW1ObVpETXRZVEprTmkwME5XTm1MV0V5WldFdE9HSmxaV1ZoT0ROaE5qUTMifQ==; _scid=jO68HZP1ddMoJaFXVjCWvJPzeJBVfZ5I; _tt_enable_cookie=1; _ttp=01KETMNNNPNDDG2WJCYK8PRED4_.tt.1; __stripe_mid=bd7d6d95-d461-44c4-9895-e3c9bae8bc0f3f63f8; visit_source=google; wb-p-SERVER=wwwb-app242; user_id=eyJpdiI6IldNenhNdnhFUW1pSHZ4Z3RERU9jRlE9PSIsInZhbHVlIjoid21iYnRMUVdTbGVZM2JoZFIxeWFkUT09IiwibWFjIjoiZDQxMGFjNGQwODg5NTA2NzI1MmNiNDQ3YTBmMDdhZWRhNWM2YWNiYTExY2U0ODE0ZTNkNjc5YzE1YmJkYWI4OCJ9; sso_token=eyJpdiI6IkQxeWNMRVFleUcxdEdtbnU5RzE1TFE9PSIsInZhbHVlIjoiZDFRXC9QTFo3ZXc2bjhicEJMVDZhRWEzWHFiVWl3cXAxVWUxbWFLZXJ3V01tZkJxSnhsc0pFSmpQbkRUZTJyblkiLCJtYWMiOiI1NzEzODVkM2M3YmFhOWI1Mjk1ZmE5YzQxYzc4MDM1ZDhmN2Y4ZGQwNjY2NzZjNjNmMDk3NjAxZDFkNTJjOTJmIn0%3D; laravel_session=eyJpdiI6InhwMlJBa3hPOEN3RGFLb2Y4MCtFK2c9PSIsInZhbHVlIjoiUkZhaXgxT1B2bXVnNkxIakRkRHRRT3BUZ0N2bjBhRkVlbFBJQVlQWThRZmFxS1c2bzFFMzNNK1A0RURzeGxtcEN6XC9saCtCVEUrbkJJNmN3cStMeXlRPT0iLCJtYWMiOiIwZmIxZTM4ZDUyYTlkMTA2MWZlNTMzYjE1ZTM4ZThmMDc1M2MwZTgxNWE3NzM0NmJhZmQxY2I3OGRiNTcwZWNlIn0%3D",
    "origin": "chrome-extension://clkjnbjpinbbodjlagpnecfjjopokbjg",
    "version": "2.2.8",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
}

# ─── Job tracking ─────────────────────────────────────────────
active_jobs: dict[int, dict] = {}  # chat_id → job info

crawled_urls = set()
crawled_lock = threading.Lock()

def load_crawled_urls():
    if os.path.exists(CRAWLED_FILE):
        with open(CRAWLED_FILE, "r", encoding="utf-8") as f:
            for line in f:
                crawled_urls.add(line.strip())

def add_crawled_url(url):
    with crawled_lock:
        if url not in crawled_urls:
            crawled_urls.add(url)
            with open(CRAWLED_FILE, "a", encoding="utf-8") as f:
                f.write(url + "\n")


# ══════════════════════════════════════════════════════════════
# 1. THU THẬP URL SẢN PHẨM (scroll + click more via Crawl4AI)
# ══════════════════════════════════════════════════════════════

# JS: scroll hết trang + click nút "more/load more" đến khi hết
SCROLL_AND_LOAD_JS = """
(async () => {
    await new Promise(r => setTimeout(r, 2000));

    // Tập hợp chứa TẤT CẢ url sản phẩm thu thập được
    const allCollectedLinks = new Set();
    const baseUrl = window.location.origin;
    const currentPath = window.location.pathname;

    // Hàm xử lý và lưu links trên DOM hiện tại
    function harvestLinks() {
        const links = document.querySelectorAll('a[href]');
        for (let i = 0; i < links.length; i++) {
            let href = links[i].href;
            try {
                href = new URL(href, baseUrl).href.split('?')[0].split('#')[0];
                if (!href || !href.startsWith(baseUrl)) continue;
                if (href === baseUrl + currentPath || href === baseUrl + '/') continue;
                
                const skip = ['/cart','/account','/login','/register','/search',
                              '/wishlist','/checkout','/blog','/news','/contact',
                              '/about','/faq','/shipping','/returns','/privacy',
                              '/sitemap','/404','.pdf','.jpg','.png','.gif',
                              'javascript:','mailto:','tel:'];
                if (skip.some(s => href.includes(s))) continue;
                
                const path = new URL(href).pathname;
                if (path.length > 5 && path !== '/') {
                    allCollectedLinks.add(href);
                }
            } catch(e) { }
        }
    }

    async function loadAll() {
        let maxTries = 80;
        let count = 0;
        let lastLinkCount = 0;
        let stagnateCount = 0;

        while (count < maxTries) {
            window.scrollTo(0, document.body.scrollHeight);
            harvestLinks(); // Thu hoạch link liên tục mỗi lần scroll
            await new Promise(r => setTimeout(r, 1000));

            const moreSelectors = [
                '.more.btn', '.btn-more', '.load-more', '.loadmore',
                '[data-action="more"]', '[class*="load-more"]',
                '[class*="show-more"]', 'button[class*="more"]',
                'a[class*="more"]', '.pagination__next', '.next-page',
                'button.more', 'a.more', '.see-more', '[class*="see-more"]',
                '.btn-loadmore', '[class*="loadMore"]',
            ];

            let clicked = false;
            for (const sel of moreSelectors) {
                const btn = document.querySelector(sel);
                if (btn && btn.offsetParent !== null) {
                    btn.click();
                    await new Promise(r => setTimeout(r, 2000));
                    clicked = true;
                    break;
                }
            }
            
            // Xoá bớt các DOM Node (Cấu trúc giao diện sản phẩm) để đỡ tràn RAM
            try {
                const items = document.querySelectorAll('.product-item, .grid-item, .product-card, li.item');
                // Giữ lại 40 item cuối cùng để trang web không bị gãy JS
                if (items.length > 40) {
                    for (let i = 0; i < items.length - 40; i++) {
                        items[i].remove();
                    }
                }
            } catch(e) {}

            // Điều kiện thoát nếu không thấy link mới
            if (allCollectedLinks.size === lastLinkCount && !clicked) {
                stagnateCount++;
                if (stagnateCount >= 2) break; // Thử 2 lần không có gì mới thì dừng
            } else {
                stagnateCount = 0;
            }
            
            lastLinkCount = allCollectedLinks.size;
            count++;
        }
    }

    await loadAll();
    harvestLinks(); // Thu hoạch lần cuối

    return JSON.stringify([...allCollectedLinks]);
})()
"""


async def collect_product_urls_crawl4ai(listing_url: str) -> list[str]:
    """
    Dùng Playwright để scroll, click 'more' và gom toàn bộ link.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(listing_url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(2000)

        # Scroll + click more nhiều lần
        max_tries = 60
        last_count = 0
        for _ in range(max_tries):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)

            more_selectors = [
                '.more.btn', '.btn-more', '.load-more', '.loadmore',
                '[data-action="more"]', 'button.more', 'a.more',
                '.see-more', '.btn-loadmore', '.pagination__next',
                '.next-page', '[class*="load-more"]', '[class*="show-more"]',
                '[class*="loadMore"]', '[class*="see-more"]',
            ]
            clicked = False
            for sel in more_selectors:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=500):
                        await btn.click()
                        await page.wait_for_timeout(2000)
                        clicked = True
                        break
                except Exception:
                    pass

            # Nếu không click được và số link không tăng → dừng
            cur_count = await page.evaluate("document.querySelectorAll('a[href]').length")
            if cur_count == last_count and not clicked:
                break
            last_count = cur_count

        # Lấy tất cả link
        links_json = await page.evaluate(SCROLL_AND_LOAD_JS)
        await browser.close()

    try:
        return json.loads(links_json)
    except Exception:
        return []


def get_base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def filter_product_links(links: list[str], listing_url: str) -> list[str]:
    """
    Lọc để chỉ giữ link có khả năng là sản phẩm.
    Đặc thù web Callie: link sản phẩm có slug bắt đầu bằng "/personalized"
    """
    base = get_base_url(listing_url)
    listing_path = urlparse(listing_url).path.rstrip("/")

    product_links = []

    for link in links:
        # Bỏ qua nếu là link trang hiện tại
        link_path = urlparse(link).path.rstrip("/")
        if link_path == listing_path or link_path == "":
            continue
        if not link.startswith(base):
            continue

        # Chỉ lấy slug bắt đầu bằng /personalized
        if link_path.startswith('/personalized'):
            product_links.append(link)

    return list(dict.fromkeys(product_links))


# ══════════════════════════════════════════════════════════════
# 2. PRINTERVAL IMPORT
# ══════════════════════════════════════════════════════════════

def fetch_html(url: str, retries: int = 3) -> str | None:
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS_HTML, timeout=30)
            if r.status_code == 200:
                return r.text
        except Exception:
            pass
        if i < retries - 1:
            time.sleep(1)
    return None


def post_to_printerval(product_url: str, html: str, categories: int = DEFAULT_CATEGORIES) -> dict:
    payload = {
        "brand_id": BRAND_ID,
        "categories": categories,
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
    for i in range(3):
        try:
            r = requests.post(PRINTERVAL_API, json=payload, headers=HEADERS_API, timeout=120)
            is_json = "application/json" in r.headers.get("content-type", "")
            return {"status": r.status_code, "body": r.json() if is_json else r.text[:300]}
        except Exception as e:
            if i == 2:
                return {"status": 0, "body": str(e)}
            time.sleep(2)
    return {"status": 0, "body": "max retries"}


def process_one(url: str, counters: dict, lock: threading.Lock, done_links: list, categories: int = DEFAULT_CATEGORIES):
    if url in crawled_urls:
        with lock:
            counters["skipped"] += 1
        return

    html = fetch_html(url)
    if not html:
        with lock:
            counters["failed"] += 1
        return

    result = post_to_printerval(url, html, categories=categories)
    
    # --- DEBUG LOG ---
    try:
        import json
        body_log = json.dumps(result["body"], ensure_ascii=False) if isinstance(result["body"], dict) else str(result["body"])
        print(f"👉 [DEBUG API] {url}\n   Status: {result['status']} | Body: {body_log[:500]}")
    except Exception as e:
        print(f"👉 [DEBUG API] Lấy log thất bại: {e}")
    # -----------------
    
    with lock:
        if result["status"] == 200:
            body = result["body"]
            
            prod_id = None
            if isinstance(body, dict):
                data = body.get("data")
                if isinstance(data, dict):
                    prod_id = data.get("id") or data.get("product_id")
                if not prod_id:
                    prod_id = body.get("id") or body.get("product_id") or body.get("productId")
                    
            if isinstance(body, dict) and body.get("status") == "successful":
                counters["done"] += 1
                add_crawled_url(url)
                if prod_id:
                    done_links.append(f"https://printerval.com/s-p{prod_id}")
            elif isinstance(body, dict) and (body.get("success") is False or body.get("error")):
                counters["failed"] += 1
            else:
                counters["done"] += 1
                add_crawled_url(url)
                if prod_id:
                    done_links.append(f"https://printerval.com/s-p{prod_id}")
        else:
            counters["failed"] += 1


def run_import_job(product_urls: list[str], chat_id: int, loop, bot, categories: int = DEFAULT_CATEGORIES):
    """Chạy import trong background thread với ThreadPoolExecutor"""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    total = len(product_urls)
    counters = {"done": 0, "failed": 0, "skipped": 0}
    done_links = []
    lock = threading.Lock()

    job = active_jobs.get(chat_id, {})
    job["total"] = total
    job["counters"] = counters
    job["cancelled"] = False
    active_jobs[chat_id] = job

    def send(msg):
        asyncio.run_coroutine_threadsafe(
            bot.send_message(chat_id=chat_id, text=msg), loop
        )

    last_report = [0]

    def worker(url: str):
        if active_jobs.get(chat_id, {}).get("cancelled"):
            return
        process_one(url, counters, lock, done_links, categories=categories)
        done_now = counters["done"] + counters["failed"] + counters["skipped"]
        # Báo cáo mỗi 10 SP hoặc lúc hoàn thành
        if done_now - last_report[0] >= 10 or done_now == total:
            last_report[0] = done_now
            elapsed = time.time() - job["start_time"]
            speed = done_now / elapsed * 60 if elapsed > 0 else 0
            pct = done_now / total * 100
            send(
                f"⏳ [{done_now}/{total}] {pct:.0f}%\n"
                f"✅ Thành công: {counters['done']} | ❌ Thất bại: {counters['failed']} | ⏭️ Bỏ qua: {counters['skipped']}\n"
                f"🚀 ~{speed:.0f} SP/phút"
            )

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = [ex.submit(worker, url) for url in product_urls]
        for f in as_completed(futures):
            if active_jobs.get(chat_id, {}).get("cancelled"):
                break

    elapsed = time.time() - job["start_time"]
    cancelled = job.get("cancelled", False)
    send(
        f"{'⛔ Đã huỷ!' if cancelled else '🏁 HOÀN TẤT!'}\n\n"
        f"✅ Thành công : {counters['done']}\n"
        f"❌ Thất bại   : {counters['failed']}\n"
        f"⏭️ Bỏ qua     : {counters['skipped']}\n"
        f"⏱️ Thời gian  : {elapsed / 60:.1f} phút"
    )

    if done_links:
        async def send_doc():
            import io
            from urllib.parse import urlparse
            
            # Tạo tên file từ url
            source_url = job.get("url", "")
            p = urlparse(source_url)
            domain = p.netloc.replace("www.", "").split(".")[0] if p.netloc else "printerval"
            path_part = p.path.strip("/").replace("/", "-") if p.path else ""
            file_name = f"{domain}-{path_part}.txt" if path_part else f"{domain}.txt"
            
            # 1. Lưu ra file cục bộ
            try:
                with open(file_name, "w", encoding="utf-8") as f:
                    f.write("\n".join(done_links))
                print(f"Đã lưu file: {file_name}")
            except Exception as e:
                print(f"Lỗi khi lưu file local: {e}")

            # 2. Gửi qua Telegram
            bio = io.BytesIO("\n".join(done_links).encode("utf-8"))
            bio.name = file_name
            try:
                await bot.send_document(chat_id=chat_id, document=bio, caption=f"Danh sách link sản phẩm đã import ({file_name})")
            except Exception as e:
                print(f"Lỗi gửi file: {e}")
        asyncio.run_coroutine_threadsafe(send_doc(), loop)

    active_jobs.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════
# 3. TELEGRAM HANDLERS
# ══════════════════════════════════════════════════════════════

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    await update.message.reply_text(
        "🤖 *Printerval Import Bot*\n\n"
        "Gửi link trang *danh sách sản phẩm* — bot sẽ:\n"
        "1️⃣ Mở browser, scroll & click *Load More* đến hết\n"
        "2️⃣ Gom toàn bộ link sản phẩm\n"
        "3️⃣ Import từng sản phẩm lên *Printerval* (3 luồng song song)\n\n"
        "📌 *Cú pháp:*\n"
        "`/crawl {url} {category\_id}`\n"
        "`/crawl {url}` _(dùng category mặc định: 35)_\n\n"
        "📌 *Ví dụ:*\n"
        "`/crawl https://example.com/products 33`\n"
        "`/crawl https://example.com/products 45`\n"
        "`/crawl https://example.com/products`\n\n"
        "⚙️ `/status` — tiến độ | `/cancel` — huỷ",
        parse_mode="Markdown"
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    job = active_jobs.get(chat_id)
    if not job:
        await update.message.reply_text("✅ Không có job nào đang chạy.")
        return
    c = job.get("counters", {})
    total = job.get("total", 0)
    done = c.get("done", 0) + c.get("failed", 0) + c.get("skipped", 0)
    elapsed = time.time() - job.get("start_time", time.time())
    pct = done / total * 100 if total else 0
    speed = done / elapsed * 60 if elapsed > 0 else 0
    await update.message.reply_text(
        f"⏳ *Job đang chạy*\n"
        f"🌐 `{job.get('url', '')}`\n"
        f"🗂️ Category ID: `{job.get('categories', DEFAULT_CATEGORIES)}`\n\n"
        f"📦 {done}/{total} ({pct:.0f}%)\n"
        f"✅ Thành công: {c.get('done', 0)} | ❌ Thất bại: {c.get('failed', 0)} | ⏭️ Bỏ qua: {c.get('skipped', 0)}\n"
        f"⏱️ Đã chạy: {elapsed / 60:.1f} phút\n"
        f"🚀 ~{speed:.0f} SP/phút",
        parse_mode="Markdown"
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = update.effective_chat.id
    if chat_id in active_jobs:
        active_jobs[chat_id]["cancelled"] = True
        await update.message.reply_text("⛔ Đang huỷ job...")
    else:
        await update.message.reply_text("❌ Không có job nào đang chạy.")


async def do_crawl(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str, categories: int = DEFAULT_CATEGORIES):
    """Logic chính: nhận URL → crawl → import"""
    chat_id = update.effective_chat.id

    if chat_id in active_jobs:
        await update.message.reply_text(
            "⚠️ Đang có job chạy! Dùng `/status` hoặc `/cancel`.",
            parse_mode="Markdown"
        )
        return

    if not url.startswith("http"):
        await update.message.reply_text("❌ URL không hợp lệ!")
        return

    await update.message.reply_text(
        f"🌐 URL: `{url}`\n"
        f"🗂️ Category ID: `{categories}`\n\n"
        f"⏳ *Bước 1/3:* Đang mở browser & scroll trang...\n"
        f"_(Có thể mất 1-2 phút nếu trang có nhiều sản phẩm)_",
        parse_mode="Markdown"
    )

    # Bước 1: Thu thập link (async / browser)
    try:
        all_links = await collect_product_urls_crawl4ai(url)
    except Exception as e:
        await update.message.reply_text(f"❌ Lỗi khi crawl trang:\n`{str(e)[:200]}`", parse_mode="Markdown")
        return

    if not all_links:
        await update.message.reply_text("❌ Không tìm thấy link nào trên trang này!")
        return

    # Bước 2: Lọc link sản phẩm
    product_urls = filter_product_links(all_links, url)

    if not product_urls:
        # Nếu không lọc được, dùng toàn bộ link
        product_urls = all_links
        
    # Ghi file danh sách urls ngay lập tức
    from urllib.parse import urlparse
    p = urlparse(url)
    domain = p.netloc.replace("www.", "").split(".")[0] if p.netloc else "domain"
    path_part = p.path.strip("/").replace("/", "-") if p.path else "home"
    list_file_name = f"list-urls-{domain}-{path_part}.txt"
    try:
        with open(list_file_name, "w", encoding="utf-8") as f:
            f.write("\n".join(product_urls))
        print(f"Đã lưu danh sách URL vào: {list_file_name}")
    except Exception as e:
        print(f"Lỗi khi lưu list urls: {e}")

    await update.message.reply_text(
        f"✅ *Bước 1/3 xong!*\n"
        f"📊 Tổng link tìm thấy: {len(all_links)}\n"
        f"🎯 Link sản phẩm (sau lọc): *{len(product_urls)}*\n"
        f"💾 Đã lưu danh sách vào file: `{list_file_name}`\n\n"
        f"🚀 *Bước 2/3:* Bắt đầu import lên Printerval...\n"
        f"_(Báo cáo mỗi 10 sản phẩm)_",
        parse_mode="Markdown"
    )

    # Bước 3: Import (background thread)
    loop = asyncio.get_event_loop()
    active_jobs[chat_id] = {
        "url": url,
        "categories": categories,
        "total": len(product_urls),
        "start_time": time.time(),
        "cancelled": False,
        "counters": {"done": 0, "failed": 0, "skipped": 0},
    }

    thread = threading.Thread(
        target=run_import_job,
        args=(product_urls, chat_id, loop, context.bot, categories),
        daemon=True
    )
    thread.start()


async def crawl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/crawl {url} [category_id]"""
    if not update.message or not update.message.text:
        return
    parts = update.message.text.split(maxsplit=2)  # ['/crawl', url, category_id?]
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ Cú pháp: `/crawl {url} {category\_id}`\n"
            "Ví dụ: `/crawl https://example.com/products 33`",
            parse_mode="Markdown"
        )
        return

    url = parts[1].strip()
    categories = DEFAULT_CATEGORIES
    if len(parts) >= 3:
        cat_str = parts[2].strip()
        if cat_str.isdigit():
            categories = int(cat_str)
        else:
            await update.message.reply_text(
                f"⚠️ Category ID phải là số nguyên, nhận được: `{cat_str}`\n"
                f"Dùng category mặc định: `{DEFAULT_CATEGORIES}`",
                parse_mode="Markdown"
            )
    await do_crawl(update, context, url, categories=categories)


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gửi URL + category_id trực tiếp (không cần /crawl)"""
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    parts = text.split(maxsplit=1)

    if parts[0].startswith("http://") or parts[0].startswith("https://"):
        url = parts[0]
        categories = DEFAULT_CATEGORIES
        if len(parts) >= 2 and parts[1].strip().isdigit():
            categories = int(parts[1].strip())
        elif len(parts) >= 2:
            await update.message.reply_text(
                f"⚠️ Tham số thứ 2 phải là category ID (số)\n"
                f"Dùng category mặc định: `{DEFAULT_CATEGORIES}`",
                parse_mode="Markdown"
            )
        await do_crawl(update, context, url, categories=categories)
    else:
        await update.message.reply_text(
            "💬 Cú pháp:\n"
            "`/crawl {url} {category_id}`\n\n"
            "Hoặc gửi: `{url} {category_id}`\n"
            "Gõ /start để xem hướng dẫn.",
            parse_mode="Markdown"
        )


# ══════════════════════════════════════════════════════════════
# 4. MAIN
# ══════════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log lỗi toàn cục, không crash bot."""
    import traceback
    err = context.error
    print(f"[ERROR] {err}")
    traceback.print_exception(type(err), err, err.__traceback__)


def main():
    # --- THÊM DÒNG NÀY ĐỂ RENDER KHÔNG TẮT WEB SERVICE ---
    threading.Thread(target=dummy_web_server, daemon=True).start()

    load_crawled_urls()
    print("🤖 Printerval Import Bot đang khởi động...")
    print(f"📦 Đã tải {len(crawled_urls)} URL đã crawl từ trước.")
    print(f"🔑 Token: {TELEGRAM_BOT_TOKEN[:20]}...")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",  start_command))
    app.add_handler(CommandHandler("help",   start_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("crawl",  crawl_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    app.add_error_handler(error_handler)

    print("✅ Bot đang lắng nghe... (Ctrl+C để dừng)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

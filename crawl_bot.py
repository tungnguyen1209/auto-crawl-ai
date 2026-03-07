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
import random
from urllib.parse import urlparse, urljoin
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import litellm

litellm.drop_params = True

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

# ─── Config & Auto Crawl ──────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8590410345:AAF5Xgq6GT6FHZ0vyfe_SCIjZsq_k1dew1I")

# -- Cấu hình cho Auto Crawl liên tục ---
AUTO_CRAWL_ENABLED = False # Mặc định tắt, chỉ chạy khi chat /start_auto_crawl
AUTO_CRAWL_INTERVAL_HOURS = float(os.environ.get("AUTO_CRAWL_INTERVAL_HOURS", "1")) # 1 tiếng vòng quay
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "") # Nhập Chat ID của bạn để cấu hình nhận báo cáo

# (Đã xoá AUTO_CRAWL_LIST do tích hợp AI tự động quét website)

CRAWLED_FILE       = "crawled_urls.txt"
PRINTERVAL_API     = "https://printerval.com/crawl-product/create-from-html?debug=1"
USER_EMAIL         = "duytungnguyen.bkhn.95@gmail.com"
USER_TOKEN         = "4a5747260b5614a86d6fb70f1012ad19"
BRAND_ID           = 9
DEFAULT_CATEGORIES = -1   # -1 để bật AI tự động dự đoán danh mục cho mọi sản phẩm
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

SEARCH_URL_MAP = {
    "callie": "https://callie.com/category/index?search={}",
    "allegro": "https://allegro.pl/listing?string={}",
    "amazon": "https://www.amazon.com/s?k={}",
    "etsy": "https://www.etsy.com/search?q={}",
    "redbubble": "https://www.redbubble.com/shop?query={}",
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
# 1. AI CHỌN CATEGORY & TỰ ĐỘNG KHÁM PHÁ WEBSITE
# ══════════════════════════════════════════════════════════════

def predict_category_with_ai(product_url: str) -> int:
    """Gọi LLM (Gemini/OpenAI) để phân tích slug URL và tự động chọn Category ID."""
    try:
        # Load cache json một lần
        if not hasattr(predict_category_with_ai, "_cat_json"):
            with open("us_categories.json", "r", encoding="utf-8") as f:
                cats = json.load(f)
            # Rút gọn token để bot đọc nhanh hơn: id, name
            predict_category_with_ai._cat_json = json.dumps([{"id": c["id"], "name": c["name"]} for c in cats])

        slug = product_url.split("/")[-1].replace("-", " ")
        prompt = f"""
Sản phẩm e-commerce (slug/tên): '{slug}'
Danh sách id + tên danh mục:
{predict_category_with_ai._cat_json}

Trả về CHỈ một con số nguyên (integer) là `id` của danh mục phù hợp nhất với sản phẩm này. KHÔNG kèm bất kỳ thông báo hay text nào khác. Nếu không có gì thực sự khớp, trả về 35.
"""
        model_name = os.environ.get("AI_MODEL_NAME", "gemini/gemini-2.5-flash-lite") # Mặc định dùng Gemini
        response = litellm.completion(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0,
            api_key=os.environ.get("GEMINI_API_KEY", os.environ.get("OPENAI_API_KEY", ""))
        )
        # Log toàn bộ response để debug nếu không có content
        raw_content = None
        if hasattr(response, 'choices') and len(response.choices) > 0:
            choice = response.choices[0]
            if hasattr(choice, 'message') and hasattr(choice.message, 'content'):
                raw_content = choice.message.content
        
        if raw_content is not None:
            content = str(raw_content).strip()
            match = re.search(r'\d+', content)
            if match:
                return int(match.group(0))
        else:
            print(f"[AI Category Warn] AI trả về đối tượng không hợp lệ cho URL: {product_url}")
            print(f"Log từ LiteLLM object: {str(response)}")
            
    except Exception as e:
        print(f"[AI Category Error] {product_url} -> {e}")
    return 35 # Trả về 35 mặc định nếu cả AI cũng sụp lỗi (fallback an toàn cuối cùng)


async def auto_discover_callie_categories(website: str = "callie.com") -> list[str]:
    """Tự động gọi API hunter.printerval.com để lấy các từ khoá tìm kiếm"""
    print("[AUTO CRAWL] Đang lấy từ khóa từ API hunter.printerval.com...")
    
    url = 'https://hunter.printerval.com/api/research'
    headers = {
        'accept': '*/*',
        'accept-language': 'vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5',
        'content-type': 'application/json',
        'origin': 'https://hunter.printerval.com',
        'referer': 'https://hunter.printerval.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        'cookie': '_ga=GA1.1.1689186747.1768273138; _fbp=fb.1.1768273138375.890768100260677978; _scid=jO68HZP1ddMoJaFXVjCWvJPzeJBVfZ5I; _tt_enable_cookie=1; _ttp=01KETMNNNPNDDG2WJCYK8PRED4_.tt.1; __stripe_mid=bd7d6d95-d461-44c4-9895-e3c9bae8bc0f3f63f8; _pin_unauth=dWlkPVlqZzJPVEF6WldNdE1tRm1NaTAwTlRJNExXSm1NVEV0TWpSaE1qQXhNR0ZsWmpNeQ; datadome=n4z9uT~vEVeB43IHlvj5dQw2EgAnYQxhwVs2KFszA_v8taz2sWnfA2gB3ejBLPzo0gf887O2f3ejFvVgV4fuWpWXqXSdnSWbsMXGGbea5P~q31qWOIMFC7gPouo7FsJv; AwinChannelCookie=direct; _yjsu_yjad=1772174112.650d136e-92d9-4af9-953f-89ce8b1e5ac7; _twpid=tw.1772417673199.691840371330915802; _sctr=1%7C1772384400000; _ScCbts=%5B%5D; _gcl_au=1.1.224713083.1768273138.1327437920.1772439042.1772439169; _cfuvid=uV8CFXJ3HjQRmxTOlaleXjvg_31Gb7.2HleUBxH4Sv8-1772778976500-0.0.1.1-604800000; cto_bundle=sZI46V8lMkJoJTJCTVdaUTk1QUlzaTJ3R1g5bDQ2JTJCJTJGMUdzTFh3TkxlelNDV0lMem1kQ01OeklIJTJCUHNRekNYNmZOc0Q0S0F6WXY2Mk1uVUpxUWkyd0JvTzNMZ3JyJTJGQkFiMkcxNlhFJTJGMTB4TTNnQiUyQmp6cndpaEpQcExLRXJDbnJWNUI1MG9mZ1I1b0ZFUFBFYW13WmxYTGpQJTJGd0dPQlElM0QlM0Q; _rdt_uuid=1768273138309.bce4243b-b00a-4f73-873c-f40a6a761270; _uetsid=8b088e5015dd11f180a63f06460dee96|85igp3|2|g44|0|2252; _scid_r=om68HZP1ddMoJaFXVjCWvJPzeJBVfZ5IESilnA; ttcsid=1772781490691::HocCBz4JeJFTVsdny2fM.144.1772781502592.0; ttcsid_CL46DJ3C77U0CK80E320=1772781490690::Fhf05tVsI05fylC6rn72.144.1772781502592.1; _uetvid=ceaaf8b0f02b11f0b8e1751351e65873|vlt4kl|1772781978981|5|1|bat.bing.com/p/insights/c/j; _ga_5Q57T3BBYZ=GS2.1.s1772781490$o157$g1$t1772782491$j60$l0$h1535647158; __cf_bm=rRWcHNKay1DLLY0jlmLwMKUHo77ptmjmKVIrbYPYkU0-1772848729-1.0.1.1-a1qBLzJ6NmPKqiWqlyuBuKPWZ.U133LPawssNBAcfVA4KSrj1xLuE6K_Eipw2bwCCJd6bre2nkw.HGZGrL.HhUohAA.APfS.chU7ofhNlOc; __stripe_sid=45621a1f-e604-4e97-9cb3-77dd6b6a6f497c40c8; cf_clearance=fMArZwoyE26_jH9RW4hnbXALpRURAZDTI_M5LT6.kl4-1772849257-1.2.1.1-CgXmDjfhqlbsjZ9UFWyU.XpFBMli35T10eTJRnAAUZwZp3GKcSgS.q6esQ05CAE9pnVl7DMWVf_Da.daB.bL0jmtixR3rYRgik_FT..W8OaLU9JYotfStpxqvVEAnISzX2LQeATQmUqi_9lE4WsAoFCLgTc0EClHgXBuchFeayJ614s96euSg5LrZJIgnmgcxPKGXJ1Dq10b4Pb7eLYlfr3mm63zcKL9R_ZlNbWoIjU'
    }
    payload = {
        "category": "pod",
        "customPrompt": "",
        "region": "US"
    }
    
    keywords = []
    try:
        # Sử dụng requests để lấy data (chạy đồng bộ trong hàm async tạm thời)
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code == 200:
            try:
                result = response.json()
                
                # Ưu tiên lấy theo đúng cấu trúc JSON trả về (data.trending_keywords / data.etsy_suggestions)
                if result.get("success") and isinstance(result.get("data"), dict):
                    data = result["data"]
                    if "trending_keywords" in data and isinstance(data["trending_keywords"], list):
                        for item in data["trending_keywords"]:
                            if isinstance(item, dict) and "keyword" in item:
                                keywords.append(item["keyword"])
                                
                    if "etsy_suggestions" in data and isinstance(data["etsy_suggestions"], list):
                        for item in data["etsy_suggestions"]:
                            if isinstance(item, dict) and "keyword" in item:
                                keywords.append(item["keyword"])
                
                # Fallback: Trích xuất đệ quy nếu format API thay đổi
                if not keywords:
                    def extract_keywords_recursive(obj):
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                if k == 'keyword' and isinstance(v, str):
                                    keywords.append(v)
                                else:
                                    extract_keywords_recursive(v)
                        elif isinstance(obj, list):
                            for item in obj:
                                extract_keywords_recursive(item)
                    extract_keywords_recursive(result)

            except Exception as e:
                print(f"[AUTO CRAWL ERR] Lỗi parse JSON: {e}")
        else:
            print(f"[AUTO CRAWL ERR] Status {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[AUTO CRAWL ERR] Lỗi gọi API: {e}")
        
    # Loại bỏ các từ trống
    keywords = [kw for kw in keywords if str(kw).strip()]
    
    if not keywords:
        print("[AUTO CRAWL WARN] Không tìm thấy từ khóa nào, dùng từ khóa phòng hờ.")
        keywords = ["custom photo gifts", "personalized mug", "funny cat t-shirt"]
    else:
        print(f"[AUTO CRAWL] Thu thập được {len(keywords)} từ khoá.")
    
    # Ráp thành URL tìm kiếm
    import urllib.parse
    valid_categories = []
    
    # Xác định hàm build URL theo chuẩn của script (sử dụng SEARCH_URL_MAP)
    matched_key = next((key for key in SEARCH_URL_MAP.keys() if key in website.lower()), None)
    
    for kw in keywords:
        encoded_kw = urllib.parse.quote(str(kw))
        if matched_key:
            search_link = SEARCH_URL_MAP[matched_key].format(encoded_kw)
        else:
            base_site = website if website.startswith("http") else f"https://{website}"
            search_link = f"{base_site}/search?q={encoded_kw}"
            
        valid_categories.append(search_link)
                
    random.shuffle(valid_categories)
    return valid_categories

# ══════════════════════════════════════════════════════════════
# 2. THU THẬP URL SẢN PHẨM (scroll + click more via Crawl4AI)
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
    Đối với trang hỗ trợ phân trang (như allegro), vòng lặp sẽ chạy qua nhiều trang.
    """
    import re
    all_extracted_links = set()
    is_allegro = "allegro.pl" in listing_url or "allegro" in listing_url
    pages_to_crawl = 5 if is_allegro else 1
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Sử dụng context để dễ dàng quản lý state hơn nếu cần
        context = await browser.new_context()

        for page_num in range(1, pages_to_crawl + 1):
            current_url = listing_url
            if is_allegro:
                # Xoá param p= cũ nếu có để tránh trùng lặp
                base_url_clean = re.sub(r'([?&])p=\d+', '', listing_url)
                sep = "&" if "?" in base_url_clean else "?"
                current_url = f"{base_url_clean}{sep}p={page_num}"
                print(f"[CRAWL] Đang tải trang {page_num}: {current_url}")

            # Tạo trang độc lập cho mỗi vòng lặp để tránh lỗi Navigation interrupted
            page = await context.new_page()

            try:
                await page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(2000)
                
                # Ghi HTML trang hiện tại ở vòng lặp đầu tiên ra file để dễ test và debug DOM
                if page_num == 1:
                    try:
                        html_content = await page.content()
                        import urllib.parse
                        domain = urllib.parse.urlparse(listing_url).netloc.replace("www.", "").split(".")[0]
                        debug_file = f"debug_dom_{domain}.html"
                        with open(debug_file, "w", encoding="utf-8") as f:
                            f.write(html_content)
                        print(f"👉 [DEBUG] Đã lưu DOM trang tìm kiếm ra {debug_file}")
                    except Exception as e:
                        print(f"Lỗi ghi file debug HTML: {e}")

            except Exception as e:
                print(f"[CRAWL ERR] Lỗi load URL {current_url}: {e}")
                await page.close()
                continue

            # Scroll + click more nhiều lần
            max_tries = 60 if not is_allegro else 15
            last_count = 0
            for _ in range(max_tries):
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    pass
                await page.wait_for_timeout(1000)

                clicked = False
                
                # CHỈ click "Load more" nếu KHÔNG phải web phân trang thủ công qua URL (như allegro)
                # Vì click next page bằng selector có thể gây conflict với logic page_num++ ở trên
                if not is_allegro:
                    more_selectors = [
                        '.more.btn', '.btn-more', '.load-more', '.loadmore',
                        '[data-action="more"]', 'button.more', 'a.more',
                        '.see-more', '.btn-loadmore', '.pagination__next',
                        '.next-page', '[class*="load-more"]', '[class*="show-more"]',
                        '[class*="loadMore"]', '[class*="see-more"]',
                    ]
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

                # Nếu không click được và số link không tăng → dừng scroll trang hiện tại
                try:
                    cur_count = await page.evaluate("document.querySelectorAll('a[href]').length")
                    if cur_count == last_count and not clicked:
                        break
                    last_count = cur_count
                except Exception:
                    break

            # Lấy tất cả link trang này
            try:
                links_json = await page.evaluate(SCROLL_AND_LOAD_JS)
                parsed = json.loads(links_json)
                all_extracted_links.update(parsed)
            except Exception:
                pass
                
            # Đóng page hiện tại
            await page.close()

        await browser.close()

    return list(all_extracted_links)


def get_base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def filter_product_links(links: list[str], listing_url: str, website: str = "callie.com") -> list[str]:
    """
    Lọc để chỉ giữ link có khả năng là sản phẩm.
    Hỗ trợ rule tĩnh cho một số trang đã biết, và rule heuristic động cho các trang bất kì.
    """
    from collections import Counter
    from urllib.parse import urlparse
    
    base = get_base_url(listing_url)
    listing_path = urlparse(listing_url).path.rstrip("/")
    if not website:
        website = base

    product_links = []
    
    # Từ khóa nhận diện các trang cấu trúc/chức năng (không phải trang sản phẩm)
    skip_keywords = [
        '/category', '/categories', '/collection', '/collections', '/search', 
        '/pages/', '/about', '/contact', '/policy', '/terms', '/privacy', 
        '/cart', '/checkout', '/account', '/login', '/register', '/faq', 
        '/blog', '/news', '/help', '/shipping', '/returns', '/sitemap'
    ]

    for link in links:
        parsed = urlparse(link)
        link_path = parsed.path.rstrip("/")
        
        # Bỏ qua nếu là link trang hiện tại, trống hoặc không nằm trên cùng domain
        if link_path == listing_path or link_path == "":
            continue
        if not link.startswith(base):
            continue

        link_lower = link.lower()
        
        # --- Rule đặc thù cho các trang cố định ---
        if "callie" in website.lower() or "callie.com" in base:
            if link_path.startswith('/personalized'):
                product_links.append(link)
            continue
            
        if "allegro" in website.lower() or "allegro.pl" in base:
            if "/oferta/" in link_lower or "/produkt/" in link_lower:
                product_links.append(link)
            continue
            
        if "amazon" in website.lower() or "amazon.com" in base:
            if "/dp/" in link_lower or "/gp/product/" in link_lower:
                product_links.append(link)
            continue
            
        if "etsy" in website.lower() or "etsy.com" in base:
            if "/listing/" in link_lower:
                product_links.append(link)
            continue
            
        if "redbubble" in website.lower() or "redbubble.com" in base:
            if "/i/" in link_lower:
                product_links.append(link)
            continue

        # --- Generic Heuristic cho MỌI WEBSITE khác ---
        # 1. Bỏ qua các đường dẫn chức năng / danh mục
        if any(skip_kw in link_lower for skip_kw in skip_keywords):
            continue
            
        # 2. Heuristic độ dài: Slug sản phẩm đa phần khác biệt và dài hơn 5 kí tự
        if len(link_path) > 5 and link_path.count('/') > 0:
            product_links.append(link)

    # 3. Lọc trùng URL và xoá query parameters không cần thiết
    unique_links = list(dict.fromkeys(product_links))
    
    is_known_site = any(x in website.lower() for x in ["callie", "allegro", "amazon", "etsy", "redbubble"])
    
    # 4. Phân tích cụm cấu trúc phổ biến nhất (Smart Grouping Heuristic)
    # Tại trang tìm kiếm, các link sản phẩm xuất hiện phần lớn và cùng cấu trúc path.
    if not is_known_site and len(unique_links) > 10:
        # Gom nhóm dựa theo cấu trúc cha (base directory của từng link)
        path_groups = Counter()
        for link in unique_links:
            path = urlparse(link).path
            base_dir = "/".join(path.split("/")[:-1]) # /products/ao-thun -> /products
            path_groups[base_dir] += 1
            
        valid_links = []
        for link in unique_links:
            path = urlparse(link).path
            base_dir = "/".join(path.split("/")[:-1])
            # Chỉ lấy các cấu trúc lập lại ít nhất 3 lần để loại trừ link mồ côi
            if path_groups[base_dir] >= 3: 
                valid_links.append(link)
                
        if valid_links:
            return valid_links

    return unique_links


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


def post_to_printerval(product_url: str, html: str, categories: int = DEFAULT_CATEGORIES, market: str = "us") -> dict:
    # Chèn biến market vào sau printerval.com
    api_url = PRINTERVAL_API.replace("printerval.com", f"printerval.com/{market}")

    payload = {
        "brand_id": BRAND_ID,
        "categories": categories,
        "country_code": market,
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
            r = requests.post(api_url, json=payload, headers=HEADERS_API, timeout=120)
            is_json = "application/json" in r.headers.get("content-type", "")
            return {"status": r.status_code, "body": r.json() if is_json else r.text[:300]}
        except Exception as e:
            if i == 2:
                return {"status": 0, "body": str(e)}
            time.sleep(2)
    return {"status": 0, "body": "max retries"}


def process_one(url: str, counters: dict, lock: threading.Lock, done_links: list, categories: int = DEFAULT_CATEGORIES, market: str = "us"):

    html = fetch_html(url)
    if not html:
        with lock:
            counters["failed"] += 1
        return

    # Nếu Category được set là -1 (Tự động bởi AI) - Mặc định hiện tại luôn là -1
    final_category = categories
    if final_category == -1:
        final_category = predict_category_with_ai(url)
        
    # Bỏ qua không đăng nếu AI trả về giá trị -2
    if final_category == -2:
        print(f"[SKIP] Bỏ qua sản phẩm vì kịch bản AI không tìm được Category: {url}")
        with lock:
            counters["skipped"] += 1
        return

    result = post_to_printerval(url, html, categories=final_category, market=market)
    
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


def run_import_job(product_urls: list[str], chat_id: int, loop, bot, categories: int = DEFAULT_CATEGORIES, market: str = "us", is_auto: bool = False):
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
        if is_auto and not ADMIN_CHAT_ID:
            print(f"[AUTO MSG] {msg.replace(chr(10), ' | ')}")
            return
            
        target_id = int(str(ADMIN_CHAT_ID).strip()) if is_auto and str(ADMIN_CHAT_ID).strip() else chat_id
        if target_id == -1:
            print(f"[AUTO MSG] {msg.replace(chr(10), ' | ')}")
            return
            
        try:
            asyncio.run_coroutine_threadsafe(
                bot.send_message(chat_id=target_id, text=msg), loop
            )
        except Exception as e:
            print(f"[LỖI GỬI TIN TELEGRAM] {e}")

    last_report = [0]

    def worker(url: str):
        if active_jobs.get(chat_id, {}).get("cancelled"):
            return
        process_one(url, counters, lock, done_links, categories=categories, market=market)
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
            target_id_for_doc = int(str(ADMIN_CHAT_ID).strip()) if is_auto and str(ADMIN_CHAT_ID).strip() else chat_id
            if target_id_for_doc != -1:
                try:
                    await bot.send_document(chat_id=target_id_for_doc, document=bio, caption=f"Danh sách link sản phẩm đã import ({file_name})")
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
        "Gửi *Từ khóa* hoặc *Link danh sách*, bot sẽ:\n"
        "1️⃣ Mở browser tìm kiếm, scroll & click *Load More* đến hết\n"
        "2️⃣ Gom toàn bộ link sản phẩm\n"
        "3️⃣ Import từng sản phẩm lên *Printerval*\n\n"
        "📌 *Cú pháp:*\n"
        "`/crawl {từ khoá/url} [category_id] [website] [market]`\n"
        "_(Các tham số đằng sau từ khóa/url là tuỳ chọn)_\n\n"
        "📌 *Ví dụ:*\n"
        "`/crawl https://allegro.pl/listing us`\n"
        "`/crawl Easter Bunny 33 allegro.pl pl`\n"
        "`/crawl wallet` (mặc định crawl callie.com vào market us)\n\n"
        "⚙️ `/status` — tiến độ | `/cancel` — huỷ\n"
        "🤖 `/start_auto_crawl` | `/stop_auto_crawl`",
        parse_mode="Markdown"
    )


# Biến giữ tham chiếu đến task chạy ngầm của bot để không tạo task trùng lặp
_auto_crawl_task = None

async def start_auto_crawl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global AUTO_CRAWL_ENABLED, _auto_crawl_task
    if not update.message:
        return
    if AUTO_CRAWL_ENABLED:
        await update.message.reply_text("⚠️ Auto Crawl hiện đang chạy rồi.")
        return
        
    text = update.message.text.replace("/start_auto_crawl", "", 1).strip()
    
    # Tính năng mới: Truyền text rỗng vào để lấy arguments args
    # VD: /start_auto_crawl etsy us
    _, _categories, website, market = parse_command_args(text if text else "")

    AUTO_CRAWL_ENABLED = True
    await update.message.reply_text(
        f"✅ Đã BẬT Auto Crawl!\n"
        f"🌍 Website logic: `{website}`\n"
        f"🇺🇸 Market: `{market}`\n"
        f"Bot sẽ tự động fetch keywords từ API theo vòng lặp ngầm.", parse_mode="Markdown"
    )
    
    # Khởi chạy Task ngầm nếu chưa có
    if _auto_crawl_task is None or _auto_crawl_task.done():
        _auto_crawl_task = asyncio.create_task(auto_crawl_runner(context.bot, website, market))


async def stop_auto_crawl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global AUTO_CRAWL_ENABLED, _auto_crawl_task
    if not update.message:
        return
    if not AUTO_CRAWL_ENABLED:
        await update.message.reply_text("⚠️ Auto Crawl hiện đã TẮT sẵn.")
        return
        
    AUTO_CRAWL_ENABLED = False
    
    # Huỷ task nếu nó đang đợi sleep
    if _auto_crawl_task and not _auto_crawl_task.done():
        _auto_crawl_task.cancel()
        _auto_crawl_task = None
        
    await update.message.reply_text("⛔ Đã TẮT Auto Crawl. Vòng lặp ngầm đã bị huỷ.", parse_mode="Markdown")


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


async def do_crawl(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str, categories: int = DEFAULT_CATEGORIES, website: str = "callie.com", market: str = "us"):
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
        f"🗂️ Category ID: `{categories}`\n"
        f"🌍 Website logic: `{website}`\n"
        f"🇺🇸 Market: `{market}`\n\n"
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
    product_urls = filter_product_links(all_links, url, website)

    if not product_urls:
        # Nếu không lọc được, dùng toàn bộ link
        product_urls = all_links
        
    # Giới hạn số lượng link sản phẩm tối đa (Chống tràn RAM/API Rate Limit)
    MAX_LINKS_PER_CRAWL = 2000 # <-- Bạn có thể chỉnh lại số này
    if len(product_urls) > MAX_LINKS_PER_CRAWL:
        product_urls = product_urls[:MAX_LINKS_PER_CRAWL]
        
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
        f"🎯 Lấy {len(product_urls)} sản phẩm để Import (đã giới hạn {MAX_LINKS_PER_CRAWL})\n"
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
        kwargs={
            "product_urls": product_urls,
            "chat_id": chat_id,
            "loop": loop,
            "bot": context.bot,
            "categories": categories,
            "market": market,
            "is_auto": False
        },
        daemon=True
    )
    thread.start()


def parse_command_args(text: str):
    parts = text.split()
    market = "us"
    website = "callie.com"
    categories = -1
    
    found_market = False
    found_website = False
    found_category = False

    while len(parts) > 1:
        last = parts[-1].lower()
        if not found_market and len(last) == 2 and last.isalpha():
            market = last
            found_market = True
            parts.pop()
        elif not found_category and (last.isdigit() or (last.startswith('-') and last[1:].isdigit())):
            categories = int(last)
            found_category = True
            parts.pop()
        elif not found_website and ('.' in last or last.startswith('http')):
            website = last
            found_website = True
            parts.pop()
        else:
            break

    keyword_or_url = " ".join(parts).strip()
    return keyword_or_url, categories, website, market


def build_crawling_url(keyword_or_url: str, website: str) -> (str, str):
    if keyword_or_url.startswith("http://") or keyword_or_url.startswith("https://"):
        return keyword_or_url, keyword_or_url
    
    import urllib.parse
    encoded_kw = urllib.parse.quote(keyword_or_url)
    
    # Tìm link map tương ứng với tên website bằng list comprehension
    matched_key = next((key for key in SEARCH_URL_MAP.keys() if key in website.lower()), None)
    
    if matched_key:
        url = SEARCH_URL_MAP[matched_key].format(encoded_kw)
    else:
        # Fallback nếu không khớp map
        base_site = website if website.startswith("http") else f"https://{website}"
        url = f"{base_site}/search?q={encoded_kw}"
        
    return url, website

async def crawl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/crawl {keyword_or_url} [category_id] [website] [market]"""
    if not update.message or not update.message.text:
        return
    text = update.message.text.replace("/crawl", "", 1).strip()
    if not text:
        await update.message.reply_text(
            "❌ Cú pháp: `/crawl {từ khoá/url} [category_id] [website] [market]`\n"
            "Ví dụ: `/crawl Easter Bunny 33 callie.com us`\n"
            "Ví dụ: `/crawl https://allegro.pl/listing uk`",
            parse_mode="Markdown"
        )
        return

    keyword_or_url, categories, website, market = parse_command_args(text)
    url, website = build_crawling_url(keyword_or_url, website)

    await do_crawl(update, context, url, categories=categories, website=website, market=market)


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gửi url hoặc từ khoá trực tiếp (không cần /crawl)"""
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    if not text:
        return

    keyword_or_url, categories, website, market = parse_command_args(text)
    url, website = build_crawling_url(keyword_or_url, website)

    await do_crawl(update, context, url, categories=categories, website=website, market=market)


# ══════════════════════════════════════════════════════════════
# 4. MAIN
# ══════════════════════════════════════════════════════════════

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log lỗi toàn cục, không crash bot."""
    import traceback
    err = context.error
    print(f"[ERROR] {err}")
    traceback.print_exception(type(err), err, err.__traceback__)


async def auto_crawl_runner(bot, website: str = "callie.com", market: str = "us"):
    """Tiến trình ngầm chạy auto liên tục theo vòng lặp, tự động lấy link kết hợp AI."""
    print("🔄 Bắt đầu tiến trình nhánh Auto Crawl Runner...")
    print(f"🌍 Cấu hình Market: {market} | Website: {website}")
    
    while AUTO_CRAWL_ENABLED:
        print("\n" + "="*50)
        print(f"BẮT ĐẦU VÒNG AUTO CRAWL MỚI [{website} - {market}]")
        print("="*50)

        # Truyền tham số website vào hàm tạo keywords
        auto_categories = await auto_discover_callie_categories(website)
        print(f"[AUTO CRAWL] 🤖 Tìm thấy {len(auto_categories)} URL search ngẫu nhiên. Bắt đầu quét...")
        
        # Fix lỗi IndexError nếu không tìm thấy link nào thì sleep đợi rồi tìm lại
        if not auto_categories:
            print(f"[AUTO CRAWL ERR] ❌ Cảnh báo! Tính năng auto không tạo được url search nào.")
            print(f"Sẽ sleep {AUTO_CRAWL_INTERVAL_HOURS} giờ rồi thử quét trang chủ lại...")
            await asyncio.sleep(AUTO_CRAWL_INTERVAL_HOURS * 3600)
            continue
            
        # Chọn thẻ random thay vì duyệt foreach (Tiết kiệm CPU/Ram mỗi giờ chạy 1 link)
        url = random.choice(auto_categories)
        
        print(f"\n[AUTO CRAWL] 🤖 Đã chọn ngẫu nhiên URL danh mục: {url}")
        
        # Nếu bot có quyền nhắn tin cho Admin
        if str(ADMIN_CHAT_ID).strip():
            try:
                await bot.send_message(
                    chat_id=int(str(ADMIN_CHAT_ID).strip()), 
                    text=f"🔄 *[AUTO CRAWL]* Vòng lặp mới ({website}-{market}).\nĐã chọn tự động danh mục: {url}\nSẽ tự động crawl trang và dùng AI chọn ID chuyên mục...",
                    parse_mode="Markdown"
                )
            except: pass

        # Category ID -1 là cờ để bật tính năng AI predict lúc đăng SP
        categories = -1 
        
        chat_id = -1
        if str(ADMIN_CHAT_ID).strip():
            try:
                chat_id = int(str(ADMIN_CHAT_ID).strip())
            except:
                pass
        
        job_id_key = chat_id if chat_id != -1 else -1

        # Nếu user vừa gọi /cancel, dọn job rỗng
        if job_id_key in active_jobs and active_jobs[job_id_key].get("cancelled"):
            active_jobs.pop(job_id_key, None)
            
            if job_id_key in active_jobs:
                print(f"[AUTO CRAWL] Đang có job khác chạy cho ID {job_id_key}, bỏ qua và chờ lượt sau...")
                await asyncio.sleep(60)
                continue
                
        # 1. Thu thập links
        try:
            all_links = await collect_product_urls_crawl4ai(url)
        except Exception as e:
            print(f"[AUTO CRAWL ERR] Lỗi khi thu thập URL tại {url}: {e}")
            await asyncio.sleep(AUTO_CRAWL_INTERVAL_HOURS * 3600)
            continue
            
        product_urls = filter_product_links(all_links, url, website)
        if not product_urls:
            product_urls = all_links
            
        if not product_urls:
            print(f"[AUTO CRAWL] Không tìm thấy link sản phẩm nào cho {url}")
            await asyncio.sleep(AUTO_CRAWL_INTERVAL_HOURS * 3600)
            continue
            
        print(f"[AUTO CRAWL] Tìm thấy {len(product_urls)} sản phẩm cho {url}. Bắt đầu gửi qua AI & Import...")
        
        # Giới hạn số lượng link sản phẩm tối đa cho bot auto
        MAX_LINKS_PER_CRAWL = 2000
        if len(product_urls) > MAX_LINKS_PER_CRAWL:
            product_urls = product_urls[:MAX_LINKS_PER_CRAWL]
        
        if chat_id != -1:
            try:
                await bot.send_message(
                    chat_id=chat_id, 
                    text=f"🔄 *[AUTO CRAWL]* Quét danh mục mới:\n`{url}`\nTìm thấy: {len(product_urls)} SP (Giới hạn {MAX_LINKS_PER_CRAWL}).\n⚡ Dùng AI để khớp `category`.",
                    parse_mode="Markdown"
                )
            except Exception as e:
                pass

        loop = asyncio.get_running_loop()
        active_jobs[job_id_key] = {
            "url": url,
            "categories": "AI Mode",
            "total": len(product_urls),
            "start_time": time.time(),
            "cancelled": False,
            "counters": {"done": 0, "failed": 0, "skipped": 0},
        }
        
        thread = threading.Thread(
            target=run_import_job,
            kwargs={
                "product_urls": product_urls,
                "chat_id": job_id_key,
                "loop": loop,
                "bot": bot,
                "categories": categories,
                "market": market,
                "is_auto": True
            },
            daemon=True
        )
        thread.start()
        
        # Đợi job chạy xong
        while job_id_key in active_jobs:
            if active_jobs[job_id_key].get("cancelled"):
                break
            await asyncio.sleep(5)
            
        print(f"[AUTO CRAWL] Xong vòng lập hiện tại.")

        if str(ADMIN_CHAT_ID).strip():
            try:
                await bot.send_message(
                    chat_id=int(str(ADMIN_CHAT_ID).strip()), 
                    text=f"🏁 *[AUTO CRAWL]* Đã hoàn tất job tự động hiện tại!\nSẽ tìm Category khác & Crawl tiếp sau {AUTO_CRAWL_INTERVAL_HOURS} giờ.",
                    parse_mode="Markdown"
                )
            except: pass
            
        try:
            await asyncio.sleep(AUTO_CRAWL_INTERVAL_HOURS * 3600)
        except asyncio.CancelledError:
            print("[AUTO CRAWL] ⏹️ Tiến trình sleep đã bị ngắt do người dùng TẮT Auto Crawl.")
            break
            
    print("[AUTO CRAWL] ⏹️ Tiến trình Runner đã kết thúc.")


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
    app.add_handler(CommandHandler("start_auto_crawl", start_auto_crawl_command))
    app.add_handler(CommandHandler("stop_auto_crawl", stop_auto_crawl_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    app.add_error_handler(error_handler)

    print("✅ Bot đang lắng nghe... (Ctrl+C để dừng)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

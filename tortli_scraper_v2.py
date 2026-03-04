"""
Tortli.pl Product Link Scraper - V2 (Nhanh hơn)
Lấy tất cả link sản phẩm từ https://tortli.pl/ và phân loại theo product_type
"""

import requests
import json
import csv
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
import time

BASE_URL = "https://tortli.pl"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

def fetch_all_products_api():
    """Lấy tất cả sản phẩm từ Shopify products.json API"""
    print("🛍️  Đang lấy tất cả sản phẩm từ API...")
    all_products = []
    page = 1
    limit = 250
    
    while True:
        url = f"{BASE_URL}/products.json?limit={limit}&page={page}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f"  ⚠️  API trả về {resp.status_code}")
                break
            data = resp.json()
            items = data.get("products", [])
            if not items:
                break
            for p in items:
                all_products.append({
                    "id": p["id"],
                    "title": p["title"],
                    "handle": p["handle"],
                    "url": f"{BASE_URL}/products/{p['handle']}",
                    "product_type": p.get("product_type", "").strip() or "Inne",
                    "tags": ", ".join(p.get("tags", [])) if isinstance(p.get("tags"), list) else str(p.get("tags", "")),
                    "vendor": p.get("vendor", ""),
                })
            print(f"  ✅ Trang {page}: {len(items)} sản phẩm (tổng: {len(all_products)})")
            if len(items) < limit:
                break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"  ❌ Lỗi trang {page}: {e}")
            break
    
    return all_products


def main():
    print("=" * 60)
    print("🚀 TORTLI.PL Product Scraper V2")
    print(f"⏰ Bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Lấy tất cả sản phẩm
    all_products = fetch_all_products_api()
    
    if not all_products:
        print("❌ Không lấy được sản phẩm!")
        return
    
    # Phân loại theo product_type
    by_type = defaultdict(list)
    for p in all_products:
        cat = p["product_type"] if p["product_type"] else "Nie sklasyfikowane"
        by_type[cat].append(p)
    
    # In kết quả tổng quan
    print(f"\n{'=' * 60}")
    print(f"📊 TỔNG KẾT")
    print(f"{'=' * 60}")
    print(f"✅ Tổng sản phẩm: {len(all_products)}")
    print(f"📂 Số loại sản phẩm (product_type): {len(by_type)}")
    print()
    
    for cat in sorted(by_type.keys()):
        prods = by_type[cat]
        print(f"\n{'─' * 50}")
        print(f"📁 {cat} ({len(prods)} sản phẩm)")
        print(f"{'─' * 50}")
        for p in prods[:3]:
            print(f"  • {p['title']}")
            print(f"    → {p['url']}")
        if len(prods) > 3:
            print(f"  ... và {len(prods) - 3} sản phẩm khác")
    
    # Lưu JSON đầy đủ
    output = {
        "scraped_at": datetime.now().isoformat(),
        "base_url": BASE_URL,
        "total_products": len(all_products),
        "total_categories": len(by_type),
        "by_product_type": {
            cat: [
                {"title": p["title"], "url": p["url"], "vendor": p["vendor"], "tags": p["tags"]}
                for p in prods
            ]
            for cat, prods in sorted(by_type.items())
        }
    }
    
    json_file = "tortli_products_v2.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n💾 JSON: {json_file}")
    
    # Lưu CSV
    csv_file = "tortli_products_v2.csv"
    with open(csv_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["product_type", "title", "url", "vendor", "tags"])
        writer.writeheader()
        for cat in sorted(by_type.keys()):
            for p in by_type[cat]:
                writer.writerow({
                    "product_type": cat,
                    "title": p["title"],
                    "url": p["url"],
                    "vendor": p["vendor"],
                    "tags": p["tags"],
                })
    print(f"💾 CSV: {csv_file}")
    
    # Lưu TXT report dễ đọc
    txt_file = "tortli_products_v2_report.txt"
    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(f"TORTLI.PL PRODUCT LINKS REPORT\n")
        f.write(f"Ngày lấy: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Tổng sản phẩm: {len(all_products)}\n")
        f.write(f"Số category (product_type): {len(by_type)}\n")
        f.write("=" * 60 + "\n")
        
        for cat in sorted(by_type.keys()):
            prods = by_type[cat]
            f.write(f"\n{'=' * 60}\n")
            f.write(f"CATEGORY: {cat} ({len(prods)} sản phẩm)\n")
            f.write(f"{'=' * 60}\n")
            for p in prods:
                f.write(f"  [{p['title']}]\n")
                f.write(f"  {p['url']}\n\n")
    
    print(f"💾 Report: {txt_file}")
    print("\n✅ Hoàn thành!")
    print("=" * 60)
    
    return output


if __name__ == "__main__":
    result = main()

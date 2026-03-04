"""
Tortli.pl Product Link Scraper
Lấy tất cả link sản phẩm từ https://tortli.pl/ và phân loại theo category
Sử dụng Shopify JSON API + sitemap XML
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

def fetch_all_collections():
    """Lấy tất cả collections (categories) từ Shopify API"""
    print("📂 Đang lấy danh sách collections...")
    collections = {}
    page = 1
    limit = 250
    
    while True:
        url = f"{BASE_URL}/collections.json?limit={limit}&page={page}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f"  ⚠️  Collections API trả về {resp.status_code}, thử endpoint khác...")
                break
            data = resp.json()
            items = data.get("collections", [])
            if not items:
                break
            for c in items:
                collections[c["id"]] = {
                    "id": c["id"],
                    "title": c["title"],
                    "handle": c["handle"],
                    "url": f"{BASE_URL}/collections/{c['handle']}"
                }
            print(f"  ✅ Trang {page}: {len(items)} collections")
            if len(items) < limit:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"  ❌ Lỗi lấy collections: {e}")
            break
    
    return collections


def fetch_products_from_collection(collection_handle, collection_title):
    """Lấy tất cả sản phẩm từ một collection"""
    products = []
    page = 1
    limit = 250
    
    while True:
        url = f"{BASE_URL}/collections/{collection_handle}/products.json?limit={limit}&page={page}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                break
            data = resp.json()
            items = data.get("products", [])
            if not items:
                break
            for p in items:
                products.append({
                    "id": p["id"],
                    "title": p["title"],
                    "handle": p["handle"],
                    "url": f"{BASE_URL}/products/{p['handle']}",
                    "product_type": p.get("product_type", ""),
                    "tags": p.get("tags", []),
                    "vendor": p.get("vendor", ""),
                    "category": collection_title,
                    "collection_handle": collection_handle,
                })
            if len(items) < limit:
                break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"    ❌ Lỗi: {e}")
            break
    
    return products


def fetch_all_products_api():
    """Lấy tất cả sản phẩm từ Shopify products.json API"""
    print("\n🛍️  Đang lấy tất cả sản phẩm từ API...")
    all_products = {}
    page = 1
    limit = 250
    
    while True:
        url = f"{BASE_URL}/products.json?limit={limit}&page={page}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                print(f"  ⚠️  Products API trả về {resp.status_code}")
                break
            data = resp.json()
            items = data.get("products", [])
            if not items:
                break
            for p in items:
                all_products[p["id"]] = {
                    "id": p["id"],
                    "title": p["title"],
                    "handle": p["handle"],
                    "url": f"{BASE_URL}/products/{p['handle']}",
                    "product_type": p.get("product_type", ""),
                    "tags": p.get("tags", []),
                    "vendor": p.get("vendor", ""),
                    "categories": [],
                }
            print(f"  ✅ Trang {page}: {len(items)} sản phẩm (tổng: {len(all_products)})")
            if len(items) < limit:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"  ❌ Lỗi: {e}")
            break
    
    return all_products


def fetch_sitemap_products():
    """Lấy danh sách sản phẩm từ sitemap XML"""
    print("\n🗺️  Đang đọc sitemap sản phẩm...")
    sitemap_url = f"{BASE_URL}/sitemap_products_1.xml?from=6610601214144&to=15451337589062"
    try:
        resp = requests.get(sitemap_url, headers={**HEADERS, "Accept": "text/xml"}, timeout=60)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        root = ET.fromstring(resp.content)
        urls = []
        for url_elem in root.findall("sm:url", ns):
            loc = url_elem.find("sm:loc", ns)
            if loc is not None and "/products/" in loc.text:
                # Lấy URL chính (không có variant)
                url = loc.text.split("?")[0]
                if url not in urls:
                    urls.append(url)
        print(f"  ✅ Tìm thấy {len(urls)} URL sản phẩm trong sitemap")
        return urls
    except Exception as e:
        print(f"  ❌ Lỗi đọc sitemap: {e}")
        return []


def main():
    print("=" * 60)
    print("🚀 TORTLI.PL Product Link Scraper")
    print(f"⏰ Bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Lấy tất cả collections
    collections = fetch_all_collections()
    print(f"\n📊 Tổng số collections: {len(collections)}")
    for cid, cinfo in collections.items():
        print(f"  - [{cinfo['handle']}] {cinfo['title']}")

    # 2. Lấy tất cả sản phẩm từ API chung
    all_products = fetch_all_products_api()
    
    # 3. Lấy sản phẩm từng collection để biết category
    print("\n🔗 Đang lấy sản phẩm theo từng collection...")
    category_products = defaultdict(list)
    product_categories = defaultdict(set)  # product_id -> set of categories
    
    for cid, cinfo in collections.items():
        handle = cinfo["handle"]
        title = cinfo["title"]
        print(f"  📁 [{handle}] {title}...")
        prods = fetch_products_from_collection(handle, title)
        print(f"     → {len(prods)} sản phẩm")
        
        for p in prods:
            product_categories[p["id"]].add(title)
            if p["id"] in all_products:
                cat_list = list(product_categories[p["id"]])
                all_products[p["id"]]["categories"] = cat_list
            category_products[title].append({
                "id": p["id"],
                "title": p["title"],
                "url": p["url"],
                "vendor": p["vendor"],
            })
        time.sleep(0.5)

    # 4. Cũng lấy từ sitemap để đảm bảo không thiếu sản phẩm
    sitemap_urls = fetch_sitemap_products()
    sitemap_handles = set()
    for url in sitemap_urls:
        handle = url.split("/products/")[-1].rstrip("/")
        sitemap_handles.add(handle)
    
    # Tìm sản phẩm trong sitemap nhưng không có trong collections
    known_handles = {p["handle"] for p in all_products.values()}
    uncategorized_handles = sitemap_handles - known_handles
    
    # Thêm sản phẩm chưa phân loại
    uncategorized = []
    for handle in sitemap_handles:
        handle_found = False
        for pid, pinfo in all_products.items():
            if pinfo["handle"] == handle:
                handle_found = True
                if not pinfo["categories"]:
                    pinfo["categories"] = ["Uncategorized"]
                break
        if not handle_found:
            uncategorized.append({
                "id": None,
                "title": handle,
                "url": f"{BASE_URL}/products/{handle}",
                "categories": ["Uncategorized"]
            })
    
    # 5. Tổng hợp kết quả
    print("\n" + "=" * 60)
    print("📊 TỔNG KẾT")
    print("=" * 60)
    
    # Thống kê
    total_products = len(all_products) + len(uncategorized)
    print(f"✅ Tổng số sản phẩm: {total_products}")
    print(f"📂 Tổng số categories: {len(collections)}")
    print()
    
    # Hiển thị theo category
    result_by_category = defaultdict(list)
    
    for pid, pinfo in all_products.items():
        cats = pinfo["categories"] if pinfo["categories"] else ["Uncategorized"]
        for cat in cats:
            result_by_category[cat].append({
                "id": pinfo["id"],
                "title": pinfo["title"],
                "url": pinfo["url"],
                "vendor": pinfo.get("vendor", ""),
                "product_type": pinfo.get("product_type", ""),
            })
    
    for p in uncategorized:
        result_by_category["Uncategorized"].append({
            "id": p["id"],
            "title": p["title"],
            "url": p["url"],
            "vendor": "",
            "product_type": "",
        })
    
    # In kết quả
    for cat, products in sorted(result_by_category.items()):
        # Deduplicate
        seen_urls = set()
        unique_products = []
        for p in products:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                unique_products.append(p)
        result_by_category[cat] = unique_products
        print(f"\n📁 {cat} ({len(unique_products)} sản phẩm):")
        for p in unique_products[:5]:
            print(f"  - {p['url']}")
        if len(unique_products) > 5:
            print(f"  ... và {len(unique_products) - 5} sản phẩm khác")
    
    # 6. Lưu kết quả ra file JSON
    output_json = {
        "scraped_at": datetime.now().isoformat(),
        "base_url": BASE_URL,
        "total_products": total_products,
        "total_categories": len(result_by_category),
        "categories": {}
    }
    
    all_urls_flat = []
    for cat, products in result_by_category.items():
        output_json["categories"][cat] = [
            {"title": p["title"], "url": p["url"], "vendor": p.get("vendor", "")}
            for p in products
        ]
        for p in products:
            all_urls_flat.append({
                "category": cat,
                "title": p["title"],
                "url": p["url"],
                "vendor": p.get("vendor", ""),
                "product_type": p.get("product_type", ""),
            })
    
    # Lưu JSON
    json_file = "tortli_products.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(output_json, f, ensure_ascii=False, indent=2)
    print(f"\n💾 Đã lưu JSON: {json_file}")
    
    # Lưu CSV
    csv_file = "tortli_products.csv"
    with open(csv_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["category", "title", "url", "vendor", "product_type"])
        writer.writeheader()
        writer.writerows(all_urls_flat)
    print(f"💾 Đã lưu CSV: {csv_file}")
    
    # Lưu text report
    txt_file = "tortli_products_report.txt"
    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(f"TORTLI.PL PRODUCT LINKS REPORT\n")
        f.write(f"Scrape time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total products: {total_products}\n")
        f.write(f"Total categories: {len(result_by_category)}\n")
        f.write("=" * 60 + "\n\n")
        
        for cat, products in sorted(result_by_category.items()):
            f.write(f"\n{'=' * 60}\n")
            f.write(f"CATEGORY: {cat} ({len(products)} sản phẩm)\n")
            f.write(f"{'=' * 60}\n")
            for p in products:
                f.write(f"  [{p['title']}]\n")
                f.write(f"  → {p['url']}\n\n")
    
    print(f"💾 Đã lưu Report: {txt_file}")
    print("\n✅ Hoàn thành!")
    print("=" * 60)
    
    return result_by_category


if __name__ == "__main__":
    result = main()

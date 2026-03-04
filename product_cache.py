"""
Quản lý cache các product_id đã crawl để tránh chạy lặp lại.
Lưu vào file crawled_ids.json cùng thư mục.
"""

import json
import os

CACHE_FILE = os.path.join(os.path.dirname(__file__), "crawled_ids.json")


def load_crawled_ids() -> set:
    """Đọc danh sách product_id đã crawl từ file JSON"""
    if not os.path.exists(CACHE_FILE):
        return set()
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data.get("product_ids", []))
    except Exception:
        return set()


def save_crawled_ids(ids: set) -> None:
    """Ghi danh sách product_id vào file JSON"""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"product_ids": list(ids)}, f, indent=2)
    except Exception as e:
        print(f"[Cache] Lỗi ghi cache: {e}")


def add_crawled_ids(new_ids: list[str]) -> None:
    """Cộng thêm các ID mới vào cache hiện có"""
    existing = load_crawled_ids()
    existing.update(new_ids)
    save_crawled_ids(existing)


def filter_new_products(products: list[dict]) -> tuple[list[dict], int]:
    """
    Lọc ra những sản phẩm chưa crawl.
    Trả về (danh_sách_mới, số_lượng_bị_bỏ_qua)
    """
    crawled = load_crawled_ids()
    new_products = []
    skipped = 0
    for p in products:
        pid = str(p.get("product_id", "")).strip()
        if pid and pid in crawled:
            skipped += 1
        else:
            new_products.append(p)
    return new_products, skipped


def mark_products_done(products: list[dict]) -> None:
    """Đánh dấu các sản phẩm đã xử lý vào cache"""
    ids = [str(p.get("product_id", "")).strip() for p in products if p.get("product_id")]
    if ids:
        add_crawled_ids(ids)
        print(f"[Cache] Đã lưu {len(ids)} product_id mới vào cache. Tổng: {len(load_crawled_ids())}")

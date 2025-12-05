import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import os
from datetime import datetime

BASE_URL = "https://wowtale.net/latest-news/"

def get_investment_article_urls(max_pages=4):
    urls = set()

    for page in range(1, max_pages + 1):
        if page == 1:
            url = BASE_URL
        else:
            url = f"{BASE_URL}?_paged={page}"

        print(f"[INFO] 크롤링 중 - page {page}: {url}")

        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if res.status_code == 404:
            print(f"[INFO] page {page} 에서 404 → 여기서 중단")
            break

        soup = BeautifulSoup(res.text, "html.parser")

        for a in soup.find_all("a", href=True):
            text = (a.get_text() or "").strip()
            if "유치" in text: 
                full_url = urljoin(url, a["href"])
                urls.add(full_url)

    return sorted(list(urls))


def save_to_csv(urls, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["index", "url"])
        for i, u in enumerate(urls, start=1):
            writer.writerow([i, u])

if __name__ == "__main__":
    # 기본: 4페이지까지, 파일 이름은 고정으로 하나 (매번 덮어쓰기)
    max_pages = 4
    project_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(project_dir, "wowtale_latest.csv")

    print("=== Wowtale '투자 유치' 자동 수집기 (no prompt) ===")
    urls = get_investment_article_urls(max_pages=max_pages)

    if not urls:
        print("[INFO] '투자 유치' 기사 없음. CSV는 만들지 않습니다.")
    else:
        save_to_csv(urls, csv_path)
        print(f"[INFO] 총 {len(urls)}개 URL 저장 완료 → {csv_path}")



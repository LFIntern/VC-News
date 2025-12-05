import requests
from bs4 import BeautifulSoup
import csv
import os
from datetime import datetime
from typing import List, Set

BASE_URL = "https://www.thebell.co.kr/free/content/article.asp"
LINKS_CSV = "lp_news_links.csv"
KEYWORDS = ["LP Radar"]  # 필요하면 ["LP Radar", "LP", "출자"] 이런 식으로 늘려도 됨


def get_lp_radar_urls(max_pages: int = 3) -> List[str]:
    urls: Set[str] = set()

    for page in range(1, max_pages + 1):
        params = {
            "page": page,
            "svccode": "03",  # 인베스트 섹션
        }
        print(f"[INFO] 크롤링 중 - page {page}: {BASE_URL} {params}")

        res = requests.get(BASE_URL, params=params, headers={"User-Agent": "Mozilla/5.0"})
        if res.status_code == 404:
            print(f"[INFO] page {page} 에서 404 → 여기서 중단")
            break

        soup = BeautifulSoup(res.text, "html.parser")

        # 기사 상세로 가는 링크 (ArticleView.asp) 중에서 제목에 키워드 포함되는 것만
        for a in soup.select("a[href*='ArticleView.asp']"):
            text = (a.get_text() or "").strip()
            if not text:
                continue
            if not any(kw in text for kw in KEYWORDS):
                continue

            href = a.get("href")
            if not href:
                continue

            # 절대경로로 변환
            if href.startswith("/"):
                full_url = "https://www.thebell.co.kr" + href
            elif href.startswith("http"):
                full_url = href
            else:
                full_url = "https://www.thebell.co.kr/free/content/" + href.lstrip("./")

            urls.add(full_url)

    # 정렬해서 반환 (안 해도 되지만 버그 디버깅할 때 눈에 보기 좋음)
    return sorted(list(urls))


def load_existing_urls_and_max_deal(csv_path: str):
    """
    기존 CSV에서 url 셋 + 최대 deal_number를 가져옴.
    CSV 없으면 (빈 경우) → (set(), 0) 반환.
    """
    if not os.path.exists(csv_path):
        return set(), 0

    existing_urls: Set[str] = set()
    max_deal = 0

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url")
            if url:
                existing_urls.add(url)
            dn = row.get("deal_number")
            if dn:
                try:
                    n = int(dn)
                    if n > max_deal:
                        max_deal = n
                except ValueError:
                    continue

    return existing_urls, max_deal


def append_links_to_csv(new_urls: List[str], csv_path: str, start_deal_number: int):
    """
    새 URL 리스트에 대해 deal_number를 순차 부여해서 CSV에 append.
    """
    if not new_urls:
        return

    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["deal_number", "url"])

        deal_num = start_deal_number
        for u in new_urls:
            writer.writerow([deal_num, u])
            deal_num += 1


if __name__ == "__main__":
    print("=== thebell 'LP Radar' 링크 수집기 ===")

    project_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(project_dir, LINKS_CSV)

    # 1) 기존 CSV에서 url 셋 + 최대 deal_number 읽기
    existing_urls, max_deal = load_existing_urls_and_max_deal(csv_path)
    print(f"[INFO] 기존 URL 개수: {len(existing_urls)}, 기존 최대 Deal Number: {max_deal}")

    # 2) 웹에서 최신 LP Radar 기사 URL 목록 가져오기
    urls = get_lp_radar_urls(max_pages=3)

    if not urls:
        print("[INFO] 'LP Radar' 기사 없음.")
    else:
        # 3) 기존에 없는 URL만 추림
        new_urls = [u for u in urls if u not in existing_urls]
        print(f"[INFO] 새로 추가할 URL 개수: {len(new_urls)}")

        if not new_urls:
            print("[INFO] 새로 추가할 링크 없음. CSV 수정 안 함.")
        else:
            append_links_to_csv(new_urls, csv_path, start_deal_number=max_deal + 1)
            print(f"[INFO] 총 {len(new_urls)}개 URL 추가 완료 → {csv_path}")
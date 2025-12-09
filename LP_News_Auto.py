import requests
from bs4 import BeautifulSoup
import csv
import os
from datetime import datetime
from typing import List, Set

BASE_URL = "https://www.thebell.co.kr/free/content/article.asp"
LINKS_CSV = "lp_news_links.csv"
SUMMARIES_CSV = "lp_news_summaries.csv"
MASTER_CSV = "lp_news_master_log.csv"
KEYWORDS = ["LP Radar", "펀드 결성",
    "조합 결성",
    "펀드 결성 나선다",
    "펀드레이징",
    "1차 클로징",
    "2차 클로징",
    "멀티클로징"]  # 필요하면 ["LP Radar", "LP", "출자"] 이런 식으로 늘려도 됨

def get_newstopkorea_fund_urls() -> List[str]:
    """
    뉴스톱코리아 VC/PE 섹션에서 펀드 관련 기사 URL 수집.
    페이지 구조에 따라 selector는 나중에 조금 손봐줘야 할 수도 있음.
    """
    urls: Set[str] = set()

    list_url = "https://www.newstopkorea.com/news/articleList.html?sc_section_code=S1N44&view_type=sm"
    print(f"[INFO] 크롤링 중 - newstopkorea: {list_url}")

    res = requests.get(list_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    # 기사 리스트 구조 예시:
    # <li class="altlist-webzine-item">
    #   <div class="altlist-webzine-content">
    #     <h2 class="altlist-subject">
    #       <a href="...articleView.html?idxno=...">제목...</a>
    #     </h2>
    #   </div>
    # </li>
    # → 제목 a 태그만 선택
    for a in soup.select("div.altlist-webzine-content h2.altlist-subject a"):
        text = (a.get_text() or "").strip()
        if not text:
            continue
        if not any(kw in text for kw in KEYWORDS):
            continue

        href = a.get("href")
        if not href:
            continue

        if href.startswith("/"):
            full_url = "https://www.newstopkorea.com" + href
        elif href.startswith("http"):
            full_url = href
        else:
            full_url = "https://www.newstopkorea.com" + "/" + href.lstrip("./")

        urls.add(full_url)

    print(f"[INFO] newstopkorea에서 수집한 URL 개수: {len(urls)}")
    return sorted(list(urls))

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

    # 2) newstopkorea 소스 추가
    try:
        nk_urls = get_newstopkorea_fund_urls()
        for u in nk_urls:
            urls.add(u)
    except Exception as e:
        print(f"[WARN] newstopkorea 크롤링 실패: {e}")

    # 정렬해서 반환 (안 해도 되지만 버그 디버깅할 때 눈에 보기 좋음)
    return sorted(list(urls))


def load_existing_urls_and_max_deal(links_csv_path: str, summaries_csv_path: str, master_csv_path: str):
    """
    기존 링크 CSV + 요약 CSV + 마스터 로그 CSV에서
    - url 셋 (이미 본 URL 전체)
    - 최대 deal_number/Deal ID
    를 가져옴.
    CSV 없으면 해당 파일은 무시하고, 전체적으로 (set(), 0) 반환 가능.
    """
    existing_urls: Set[str] = set()
    max_deal = 0

    def update_from_csv(path: str):
        nonlocal max_deal
        if not os.path.exists(path):
            return
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url")
                if url:
                    existing_urls.add(url)
                dn = row.get("Deal ID") or row.get("deal_number") or row.get("deal_id")
                if dn:
                    try:
                        n = int(dn)
                        if n > max_deal:
                            max_deal = n
                    except ValueError:
                        continue

    # 큐(링크), 요약, 마스터 로그 파일 순서로 모두 반영
    update_from_csv(links_csv_path)
    update_from_csv(summaries_csv_path)
    update_from_csv(master_csv_path)

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


def load_processed_urls() -> set:
    """
    마스터 로그에서 이미 처리된 URL들을 읽어온다.
    (펀드/비펀드 모두 포함)
    """
    urls = set()
    if os.path.exists(MASTER_CSV):
        with open(MASTER_CSV, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url")
                if url:
                    urls.add(url)
    return urls


def append_master_log(rows: List[dict]):
    fieldnames = [
        "Deal ID",
        "기사 제목",
        "기사 작성일",
        "url",
        "is_fundraising",
        "status",
    ]
    file_exists = os.path.exists(MASTER_CSV)
    with open(MASTER_CSV, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


if __name__ == "__main__":
    print("=== Fund news link collector (thebell + newstopkorea) ===")

    project_dir = os.path.dirname(os.path.abspath(__file__))
    links_csv_path = os.path.join(project_dir, LINKS_CSV)
    summaries_csv_path = os.path.join(project_dir, SUMMARIES_CSV)
    master_csv_path = os.path.join(project_dir, MASTER_CSV)

    # 1) 기존 URL/Deal ID 읽기: 큐 + 요약 + 마스터 로그 전체에서 가져옴
    existing_urls, max_deal = load_existing_urls_and_max_deal(
        links_csv_path, summaries_csv_path, master_csv_path
    )
    print(f"[INFO] 기존 URL 개수: {len(existing_urls)}, 기존 최대 Deal Number: {max_deal}")

    # 2) 웹에서 최신 LP Radar 기사 URL 목록 가져오기
    urls = get_lp_radar_urls(max_pages=3)

    if not urls:
        print("[INFO] 수집할 펀드 관련 기사 없음.")
    else:
        # 3) 기존에 없는 URL만 추림
        new_urls = [u for u in urls if u not in existing_urls]
        print(f"[INFO] 새로 추가할 URL 개수: {len(new_urls)}")

        if not new_urls:
            print("[INFO] 새로 추가할 링크 없음. CSV 수정 안 함.")
        else:
            append_links_to_csv(new_urls, links_csv_path, start_deal_number=max_deal + 1)
            print(f"[INFO] 총 {len(new_urls)}개 URL 추가 완료 → {links_csv_path}")
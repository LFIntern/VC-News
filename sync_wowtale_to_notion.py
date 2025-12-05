import os
import csv
import requests
from datetime import datetime
from time import sleep

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

CSV_PATH = "wowtale_deals.csv"  # 깃에 올라갈 CSV 파일 이름/경로


def parse_date(date_str: str):
    """
    CSV의 '기사 날짜' 형식이 '2025.11.27' 이런 식이라 가정하고
    Notion이 요구하는 'YYYY-MM-DD' 형태로 변환.
    """
    if not date_str:
        return None

    date_str = date_str.strip()
    # 2025.11.27 형태 처리
    for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.date().isoformat()  # 'YYYY-MM-DD'
        except ValueError:
            continue
    # 못 파싱하면 None
    return None


def safe_get(row: dict, key: str) -> str:
    """
    CSV row에서 값 꺼내고, None/빈값/NaN 같은 것들을 공백 문자열로 정리.
    """
    value = row.get(key, "")
    if value is None:
        return ""
    value = str(value).strip()
    if value.lower() in ("nan", "none"):
        return ""
    return value


def build_notion_properties(row: dict) -> dict:
    """
    CSV 한 줄(row) → Notion properties JSON.
    노션 DB에서 속성 이름은 CSV 헤더와 동일하다고 가정.
    """
    properties = {}

    # 1) Deal ID (Number)
    deal_id_str = safe_get(row, "Deal ID")
    if deal_id_str:
        try:
            deal_id_num = int(float(deal_id_str))
            properties["Deal ID"] = {
                "number": deal_id_num
            }
        except ValueError:
            pass

    # 2) 투자 받는 회사 (Title)
    startup_name = safe_get(row, "투자 받는 회사 (Target / Startup)")
    properties["투자 받는 회사 (Target / Startup)"] = {
        "title": [
            {"text": {"content": startup_name or "미상 스타트업"}}
        ]
    }

    # 3) 투자사 (Rich text)
    investor = safe_get(row, "투자사 (Investor)")
    if investor:
        properties["투자사 (Investor)"] = {
            "rich_text": [
                {"text": {"content": investor}}
            ]
        }

    # 4) 투자 금액 (Rich text)
    amount = safe_get(row, "투자 금액")
    if amount:
        properties["투자 금액"] = {
            "rich_text": [
                {"text": {"content": amount}}
            ]
        }

    # 5) 라운드 (Rich text 또는 Select)
    round_ = safe_get(row, "라운드")
    if round_:
        properties["라운드"] = {
            "rich_text": [
                {"text": {"content": round_}}
            ]
        }

    # 6) 사업 섹터 (Multi-select)
    sector = safe_get(row, "사업 섹터")
    if sector:
        # 콤마/슬래시로 여러 태그 들어있는 경우 쪼개기
        raw = sector.replace("/", ",")
        options = [s.strip() for s in raw.split(",") if s.strip()]

        properties["사업 섹터"] = {
            "multi_select": [
                {"name": opt} for opt in options
            ]
        }

    # 7) 주요 사업부문 (Rich text)
    biz_detail = safe_get(row, "주요 사업부문")
    if biz_detail:
        properties["주요 사업부문"] = {
            "rich_text": [
                {"text": {"content": biz_detail}}
            ]
        }

    # 8) 기사 날짜 (Rich text)
    article_date_str = safe_get(row, "기사 날짜")
    if article_date_str:
        iso_date = parse_date(article_date_str)  # 2025-11-27 형식으로 정리
        text_value = iso_date if iso_date else article_date_str

        properties["기사 날짜"] = {
            "rich_text": [
                {"text": {"content": text_value}}
            ]
        }

    # 9) 기사 출처 (Rich text)
    source = safe_get(row, "기사 출처")
    if source:
        properties["기사 출처"] = {
            "rich_text": [
                {"text": {"content": source}}
            ]
        }

    # 10) 비고 (Rich text)
    note = safe_get(row, "비고")
    if note:
        properties["비고"] = {
            "rich_text": [
                {"text": {"content": note}}
            ]
        }

    # 11) 기사 링크 (URL)
    url = safe_get(row, "기사 링크")
    if url:
        properties["기사 링크"] = {
            "url": url
        }

    return properties


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def find_page_by_deal_id(deal_id_num: int):
    """
    Deal ID(number)로 노션 DB에서 기존 페이지가 있는지 조회.
    있으면 page_id 리턴, 없으면 None.
    """
    url = f"{NOTION_API_BASE}/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Deal ID",
            "number": {
                "equals": deal_id_num
            }
        },
        "page_size": 1,
    }

    resp = requests.post(url, headers=notion_headers(), json=payload)
    if resp.status_code >= 400:
        print(f"[WARN] query 실패 (Deal ID={deal_id_num}): {resp.status_code} {resp.text}")
        return None

    results = resp.json().get("results", [])
    if not results:
        return None

    return results[0]["id"]


def create_page_in_notion(row: dict):
    """
    새 페이지 생성.
    """
    url = f"{NOTION_API_BASE}/pages"
    data = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": build_notion_properties(row),
    }

    resp = requests.post(url, headers=notion_headers(), json=data)
    if resp.status_code >= 400:
        print(f"[ERROR][CREATE] {resp.status_code} - {resp.text}")
    else:
        print(f"[OK][CREATE] {safe_get(row, '투자 받는 회사 (Target / Startup)')}")


def update_page_in_notion(page_id: str, row: dict):
    """
    기존 페이지의 속성만 업데이트.
    (다른 속성이나 코멘트, relation 등은 그대로 유지됨)
    """
    url = f"{NOTION_API_BASE}/pages/{page_id}"
    data = {
        "properties": build_notion_properties(row),
    }

    resp = requests.patch(url, headers=notion_headers(), json=data)
    if resp.status_code >= 400:
        print(f"[ERROR][UPDATE] {resp.status_code} - {resp.text}")
    else:
        print(f"[OK][UPDATE] {safe_get(row, '투자 받는 회사 (Target / Startup)')}")


def sync_csv_to_notion(csv_path: str):
    """
    CSV 전체를 읽어서 Notion DB로 upsert.
    - Deal ID가 있는 row:
      - 해당 Deal ID가 이미 있으면 UPDATE
      - 없으면 CREATE
    - Deal ID가 비어 있으면: 무조건 CREATE
    """
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            print(f"\n=== Row {i} ===")

            deal_id_str = safe_get(row, "Deal ID")
            page_id = None
            deal_id_num = None

            if deal_id_str:
                try:
                    deal_id_num = int(float(deal_id_str))
                    page_id = find_page_by_deal_id(deal_id_num)
                except ValueError:
                    print(f"[WARN] Deal ID 정수 변환 실패: {deal_id_str}")

            if page_id:
                # 이미 있는 딜 → 건너뜀
                print(f"[SKIP][EXISTS] Deal ID={deal_id_num} 이미 있어서 건너뜀")
            else:
                # 없는 딜 → 새로 생성
                create_page_in_notion(row)

            # 노션 API rate limit 조금 여유 있게
            sleep(0.3)


if __name__ == "__main__":
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {CSV_PATH}")

    print(f"CSV → Notion 동기화 시작: {CSV_PATH}")
    print(f"Database ID: {NOTION_DATABASE_ID}")
    sync_csv_to_notion(CSV_PATH)
    print("동기화 완료.")

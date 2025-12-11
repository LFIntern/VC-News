import os
import csv
import requests
from datetime import datetime
from time import sleep

"""
lp_news_summaries.csv → Notion 데이터베이스 동기화 스크립트

요구 헤더:
  Deal ID,
  기사 제목,
  LP,
  운용사,
  펀드명,
  펀드규모,
  펀드유형,
  투자섹터,
  조성상태,
  요약,
  코멘트,
  url

 환경 변수
  - NOTION_API_KEY       : Notion 통합에서 발급받은 시크릿 토큰
  - NOTION_LP_NEWS_DB    : 동기화 대상 데이터베이스 ID
"""

NOTION_TOKEN = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID = os.environ["NOTION_LP_NEWS_DB"]

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

# 깃에 올라갈 CSV 파일 이름/경로 (필요하면 여기만 바꿔서 사용)
CSV_PATH = "lp_news_summaries.csv"


# ------------------------
# 유틸 함수
# ------------------------

def safe_get(row: dict, key: str) -> str:
    """CSV row에서 key를 안전하게 꺼내고, 스트립한 문자열로 반환"""
    return (row.get(key) or "").strip()


def parse_date(date_str: str):
    """summarized_at 등을 Notion date 형식으로 바꾸기 위한 파서"""
    if not date_str:
        return None

    date_str = date_str.strip()
    if not date_str:
        return None

    # 가능한 포맷들 몇 가지 시도
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
    ]
    dt = None
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            break
        except ValueError:
            continue

    # 파싱 실패 시 그냥 오늘 날짜로 처리할 수도 있고, None으로 둘 수도 있음
    if dt is None:
        return None

    # Notion date는 ISO 8601 문자열
    return dt.isoformat()


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


# ------------------------
# Notion Page 생성/수정용 Property 빌더
# ------------------------

def build_properties_from_row(row: dict) -> dict:
    """
    CSV 한 줄(row)을 받아서 Notion page properties 형태(dict)로 변환
    Notion 데이터베이스에서 다음과 같은 프로퍼티 이름을 맞춰줘야 함:
      - Deal ID (rich_text)
      - 기사 제목 (title)
      - LP (rich_text)
      - 운용사 (rich_text)
      - 펀드명 (rich_text)
      - 펀드규모 (rich_text)
      - 펀드유형 (rich_text)
      - 투자섹터 (rich_text)
      - 조성상태 (rich_text)
      - 요약 (rich_text)
      - 코멘트 (rich_text)
      - url (url)
    """
    deal_id = safe_get(row, "Deal ID")
    article_title = safe_get(row, "기사 제목") or "(제목 없음)"
    lp = safe_get(row, "LP")
    gp = safe_get(row, "운용사")
    fund_name = safe_get(row, "펀드명")
    fund_size_txt = safe_get(row, "펀드규모")
    fund_type = safe_get(row, "펀드유형")
    sector = safe_get(row, "투자섹터")
    formation_status = safe_get(row, "조성상태")
    summary = safe_get(row, "요약")
    comment = safe_get(row, "코멘트")
    url = safe_get(row, "url")

    properties: dict = {}

    # 1) 제목: 기사 제목 (title 타입)
    properties["기사 제목"] = {
        "title": [
            {
                "type": "text",
                "text": {"content": article_title},
            }
        ]
    }

    # 2) Deal ID (rich_text)
    if deal_id:
        properties["Deal ID"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": deal_id},
                }
            ]
        }

    # 3) LP
    if lp:
        properties["LP"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": lp},
                }
            ]
        }

    # 4) 운용사
    if gp:
        properties["운용사"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": gp},
                }
            ]
        }

    # 5) 펀드명
    if fund_name:
        properties["펀드명"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": fund_name},
                }
            ]
        }

    # 6) 펀드규모
    if fund_size_txt:
        properties["펀드규모"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": fund_size_txt},
                }
            ]
        }

    # 7) 펀드유형
    if fund_type:
        properties["펀드유형"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": fund_type},
                }
            ]
        }

    # 8) 투자섹터
    if sector:
        properties["투자섹터"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": sector},
                }
            ]
        }

    # 9) 조성상태
    if formation_status:
        properties["조성상태"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": formation_status},
                }
            ]
        }

    # 10) 요약
    if summary:
        properties["요약"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": summary},
                }
            ]
        }

    # 11) 코멘트
    if comment:
        properties["코멘트"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": comment},
                }
            ]
        }

    # 13) url (url 타입)
    if url:
        properties["url"] = {
            "url": url
        }

    return properties


# ------------------------
# Notion API helpers
# ------------------------

def find_page_by_deal_id(deal_id: str):
    """
    Deal ID가 같은 페이지가 Notion DB에 이미 있는지 조회.
    - Notion DB에서 'Deal ID' 프로퍼티는 rich_text 타입이어야 함.
    """
    if not deal_id:
        return None

    url = f"{NOTION_API_BASE}/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Deal ID",
            "rich_text": {
                "equals": deal_id
            },
        },
        "page_size": 1,
    }

    resp = requests.post(url, headers=notion_headers(), json=payload)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    if not results:
        return None
    return results[0]  # 첫 번째 결과 반환


def create_page_in_notion(row: dict):
    properties = build_properties_from_row(row)
    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": properties,
    }
    url = f"{NOTION_API_BASE}/pages"
    resp = requests.post(url, headers=notion_headers(), json=payload)
    resp.raise_for_status()
    return resp.json()


def update_page_in_notion(page_id: str, row: dict):
    properties = build_properties_from_row(row)
    payload = {
        "properties": properties,
    }
    url = f"{NOTION_API_BASE}/pages/{page_id}"
    resp = requests.patch(url, headers=notion_headers(), json=payload)
    resp.raise_for_status()
    return resp.json()


# ------------------------
# 메인 동기화 로직
# ------------------------

def sync_csv_to_notion(csv_path: str):
    """
    CSV 전체를 훑으면서:
      - Deal ID가 이미 있는 페이지는 UPDATE
      - 없으면 새로 CREATE
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        total = 0
        created = 0
        updated = 0

        for row in reader:
            total += 1
            deal_id = safe_get(row, "Deal ID")

            if not deal_id:
                print(f"[SKIP] Deal ID 없음 (row {total})")
                continue

            # Notion에서 기존 페이지 존재 여부 확인
            existing_page = find_page_by_deal_id(deal_id)

            if existing_page:
                page_id = existing_page["id"]
                print(f"[UPDATE] Deal ID={deal_id} (page_id={page_id})")
                update_page_in_notion(page_id, row)
                updated += 1
            else:
                print(f"[CREATE] Deal ID={deal_id}")
                create_page_in_notion(row)
                created += 1

            # rate limit 대비 약간의 딜레이
            sleep(0.3)

        print(f"\n총 {total}개 행 처리 완료 (생성 {created}개, 업데이트 {updated}개)")


if __name__ == "__main__":
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {CSV_PATH}")

    print(f"CSV → Notion 동기화 시작: {CSV_PATH}")
    print(f"Database ID: {NOTION_DATABASE_ID}")
    sync_csv_to_notion(CSV_PATH)
    print("동기화 완료.")

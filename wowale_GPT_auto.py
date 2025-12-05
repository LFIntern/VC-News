# wowtale_gpt_auto.py
import os
import csv
import json
import requests
from bs4 import BeautifulSoup
from openai import OpenAI

# OpenAI 클라이언트 (환경변수에 OPENAI_API_KEY 필요)
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

LATEST_CSV = "wowtale_latest.csv"
SUMMARY_CSV = "wowtale_deals.csv"  # 엑셀 예시 형태의 요약 테이블


# ----------------------------------------------------
# 1) 최신 크롤링 결과 로드
# ----------------------------------------------------
def load_latest_rows():
    rows = []
    with open(LATEST_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


# ----------------------------------------------------
# 2) 이미 처리된 기사 URL 로드 (요약 CSV의 "기사 링크" 기준)
# ----------------------------------------------------
def load_processed_urls():
    if not os.path.exists(SUMMARY_CSV):
        return set()
    urls = set()
    with open(SUMMARY_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        link_field = "기사 링크"
        for row in reader:
            url = row.get(link_field)
            if url:
                urls.add(url)
    return urls


# ----------------------------------------------------
# 3) 이미 존재하는 요약 개수 (Deal ID 시작 번호 계산용)
# ----------------------------------------------------
def load_existing_count():
    if not os.path.exists(SUMMARY_CSV):
        return 0
    with open(SUMMARY_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return sum(1 for _ in reader)


# ----------------------------------------------------
# 4) 기사 본문 크롤링
# ----------------------------------------------------
def fetch_article_text(url: str) -> str:
    """주어진 기사 URL에서 본문 텍스트를 최대한 많이 가져오는 함수.

    - 와우테일 워드프레스 구조를 고려해서 여러 CSS 셀렉터를 시도한 뒤,
      가장 텍스트가 긴 노드를 본문으로 간주한다.
    - 그래도 안 잡히면 main/body 전체 텍스트를 fallback으로 사용한다.
    - 토큰 폭발을 막기 위해 8,000자에서 잘라낸다.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        )
    }

    res = requests.get(url, headers=headers, timeout=10)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    # 와우테일 본문에 자주 쓰일 만한 후보 셀렉터들을 순서대로 시도
    candidates = [
        "article .tdb-block-inner",      # Newspaper/tdb 계열 본문 래퍼
        "article .td-post-content",      # 구형/다른 스킨에서의 본문
        "article .entry-content",        # 워드프레스 기본
        "article",                       # 그래도 안 잡히면 article 전체
        ".td-post-content",              # article 태그가 없을 때
        ".entry-content",                # fallback 1
    ]

    best_node = None
    best_selector = None
    best_len = 0

    # 각 셀렉터에서 나온 모든 노드를 검사하여, 텍스트 길이가 가장 긴 노드를 선택
    for sel in candidates:
        nodes = soup.select(sel)
        for idx, node in enumerate(nodes):
            text_candidate = node.get_text("\n", strip=True)
            l = len(text_candidate)
            if l > best_len:
                best_len = l
                best_node = node
                best_selector = f"{sel}[{idx}]"

    # 그래도 못 찾으면 main/body 전체 텍스트 사용
    if best_node is None:
        fallback = soup.select_one("main") or soup.body
        if fallback:
            text = fallback.get_text("\n", strip=True)
            best_selector = "main/body (fallback)"
        else:
            text = soup.get_text("\n", strip=True)
            best_selector = "full document (soup.get_text)"
    else:
        text = best_node.get_text("\n", strip=True)

    # 토큰 폭발 방지용으로 적당히 자르기
    text = text[:8000]

    # 디버그용: 길이 + 사용한 셀렉터 + 앞부분 200자 출력
    preview = text[:200].replace("\n", " ")
    print(f"[DEBUG] Fetched article from {url} selector={best_selector} length={len(text)}")
    print(f"[DEBUG] Preview: {preview}")

    return text


# ----------------------------------------------------
# 5) GPT로 요약 & 투자 정보 추출
#   - 출력: JSON 문자열
# ----------------------------------------------------
def summarize_with_gpt(row):
    url = row["url"]
    title = row.get("title", "")
    article_text = fetch_article_text(url)

    prompt = f"""
당신은 벤처캐피털 리서치 애널리스트입니다.
아래는 스타트업/기업의 투자·펀딩·인수(M&A) 관련 기사입니다.
이 기사에서 핵심이 되는 투자/인수 "한 건"에 대한 정보를 추출해 주세요.

※ 만약 기사에 투자·펀딩·인수(M&A) 관련 내용이 전혀 없다면,
어떤 정보도 임의로 추정하지 말고, is_deal 값을 false로 설정한 JSON 한 줄만 출력하세요.

[출력 형식]
반드시 아래 필드를 포함하는 JSON 한 줄만 출력하세요. 그 외 설명, 텍스트는 절대 출력하지 마세요.

필드:
- deal_id: 항상 null로 두세요 (파이썬에서 자동으로 채웁니다)
- is_deal: 이 기사가 실제 투자/인수(M&A) 딜을 다루는 기사이면 true, 아니면 false
- target: 투자 받는 회사 또는 인수 대상 회사명 (문자열, is_deal=false인 경우 빈 문자열)
- investors: 참여 투자사를 쉼표(,)로 구분한 하나의 문자열 (예: "카카오벤처스, 알토스벤처스", is_deal=false인 경우 빈 문자열)
- amount: 기사에 나온 투자 금액을 통화/단위를 포함해 그대로 적기 (예: "50억 원", "10M USD", "3 billion yen", 없으면 "미공개")
- round: 기사에 명시된 라운드를 그대로 사용 (예: "Seed", "Pre-A", "Series A", "전략적 투자", "합병", 없으면 "미공개")
- sector: 아래 여섯 개 중 하나로만 선택
  · "Biotech & Healthcare"
  · "Interactive Contents & Media"
  · "Consumer Internet & Fintech"
  · "ICT & Digitalization"
  · "Semiconductor & Industrial"
  · "ETC"
- business_summary: 회사의 주요 사업을 한 줄로 요약 (한국어, is_deal=false인 경우 빈 문자열)
- article_date: 기사에 표기된 날짜 (없으면 "확인 불가")
- article_source: 기사 출처(매체명) (없으면 "확인 불가")
- notes: 특이사항이 있을 때만 한 줄로 간단히 (없으면 빈 문자열)

JSON 예시는 아래와 같습니다.

{{
  "deal_id": null,
  "is_deal": true,
  "target": "예시회사",
  "investors": "카카오벤처스, 알토스벤처스",
  "amount": "50억 원",
  "round": "Series A",
  "sector": "ICT & Digitalization",
  "business_summary": "소상공인을 위한 클라우드 기반 결제·정산 SaaS를 제공",
  "article_date": "2025.12.02",
  "article_source": "와우테일",
  "notes": "정부 펀드 참여"
}}

[기사 정보]
기사 제목: {title}
기사 본문:
{article_text}
"""

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": "당신은 벤처캐피털 애널리스트입니다."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )

    content = resp.choices[0].message.content.strip()
    return content  # JSON 문자열이라고 가정


# ----------------------------------------------------
# 6) 요약 CSV 헤더 보장 (엑셀 예시 형식)
# ----------------------------------------------------
def ensure_summary_header():
    if os.path.exists(SUMMARY_CSV):
        return
    with open(SUMMARY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Deal ID",
                "투자 받는 회사 (Target / Startup)",
                "투자사 (Investor)",
                "투자 금액",
                "라운드",
                "사업 섹터",
                "주요 사업부문",
                "기사 날짜",
                "기사 출처",
                "비고",
                "기사 링크",
            ],
        )
        writer.writeheader()


# ----------------------------------------------------
# 7) 요약 CSV에 한 줄 추가
# ----------------------------------------------------
def append_summary(json_str: str, deal_id: int, base_row: dict):
    data = json.loads(json_str)

    # GPT가 준 값 우선 사용, 없으면 latest CSV 값으로 보완
    gpt_article_date = data.get("article_date") or ""
    gpt_article_source = data.get("article_source") or ""

    article_date = gpt_article_date or base_row.get("date", "")
    article_source = gpt_article_source or base_row.get("source", "와우테일")
    article_url = base_row.get("url", "")

    row_dict = {
        "Deal ID": deal_id,
        "투자 받는 회사 (Target / Startup)": data.get("target", ""),
        "투자사 (Investor)": data.get("investors", ""),
        "투자 금액": data.get("amount", ""),
        "라운드": data.get("round", ""),
        "사업 섹터": data.get("sector", ""),
        "주요 사업부문": data.get("business_summary", ""),
        "기사 날짜": article_date,
        "기사 출처": article_source,
        "비고": data.get("notes", ""),
        "기사 링크": article_url,
    }

    with open(SUMMARY_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "Deal ID",
                "투자 받는 회사 (Target / Startup)",
                "투자사 (Investor)",
                "투자 금액",
                "라운드",
                "사업 섹터",
                "주요 사업부문",
                "기사 날짜",
                "기사 출처",
                "비고",
                "기사 링크",
            ],
        )
        writer.writerow(row_dict)


# ----------------------------------------------------
# 8) 메인 로직: 새 기사만 골라서 GPT 돌리고 요약 CSV에 append
# ----------------------------------------------------

def main():
    ensure_summary_header()

    latest_rows = load_latest_rows()
    processed_urls = load_processed_urls()

    new_rows = [r for r in latest_rows if r.get("url") and r["url"] not in processed_urls]

    print(f"[INFO] 새로 처리할 기사 {len(new_rows)}개")

    existing_count = load_existing_count()
    next_id = existing_count + 1

    for row in new_rows:
        try:
            json_str = summarize_with_gpt(row)
            data = json.loads(json_str)

            # 투자/인수 기사가 아닌 경우 스킵
            if data.get("is_deal") is False:
                print(f"[SKIP] 투자/인수 기사 아님: {row.get('title', '')}")
                continue

            append_summary(json_str, deal_id=next_id, base_row=row)
            print(f"[OK] {row.get('title', '')} 요약 완료 (Deal ID={next_id})")
            next_id += 1
        except Exception as e:
            print(f"[ERROR] {row.get('url')} 처리 실패: {e}")



if __name__ == "__main__":
    main()
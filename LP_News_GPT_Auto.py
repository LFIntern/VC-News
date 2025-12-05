import os
import csv
import json
from datetime import datetime
from typing import List, Dict, Any

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

LINKS_CSV = "lp_news_links.csv"
SUMMARIES_CSV = "lp_news_summaries.csv"

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
너는 한국 벤처캐피탈/PE 시장을 잘 아는 애널리스트이자 데이터 정제 봇이다.
입력으로 주어진 한국어 기사는 'LP 출자, 펀드 결성, 벤처캐피탈/PE, 모태펀드, 성장금융, 연기금' 등과 관련된 뉴스이다.

기사에서 다음 정보를 최대한 구조화해서 뽑아라:
- LP(출자 기관) 목록
- VC/PE 운용사(GP) 목록
- 펀드 이름
- 펀드 규모(텍스트/숫자/단위/통화)
- 주요 투자 섹터/스테이지
- 기사 핵심 요약
- VC 리서치 관점에서 한 줄 코멘트

반드시 JSON 객체 한 개만 반환하고, 다른 텍스트는 절대 섞지 마라.
"""

USER_PROMPT_TEMPLATE = """
다음은 벤처캐피탈 출자/펀드 관련 한국어 기사 전문이다.

[기사 제목]
{title}

[기사 본문]
{body}

위 기사를 분석해서 아래 스키마에 맞는 JSON 객체 한 개만 출력해라.

스키마:
{{
  "lp_institutions": ["string | LP 이름 리스트, 예: '성장금융', '산업은행', '모태펀드' 등"],
  "vc_firms": ["string | VC/PE 운용사(GP) 이름 리스트"],
  "fund_name": "string | 펀드명, 없으면 null",
  "fund_size": {{
    "raw": "string | 기사에 나온 펀드 규모 표현 (예: '약 1,000억 원')",
    "value": 0,
    "unit": "string | '억원', '조원', 'billion', 'million' 등, 없으면 null",
    "currency": "string | 'KRW', 'USD', 'JPY' 등, 알 수 없으면 null"
  }},
  "fund_sector": ["string | 바이오, 헬스케어, AI, 소재/부품, 친환경/에너지 등 섹터 리스트"],
  "fund_stage": ["string | 초기, 시리즈A/B, 그로스, Pre-IPO 등 스테이지 리스트"],
  "summary": "string | 기사 내용 핵심을 2~3문장으로 요약",
  "analysis_comment": "string | VC/PE 리서치 관점에서 이 뉴스의 의미를 한두 문장으로 정리"
}}
"""


def load_links() -> List[dict]:
    if not os.path.exists(LINKS_CSV):
        return []
    with open(LINKS_CSV, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_summarized_urls() -> set:
    if not os.path.exists(SUMMARIES_CSV):
        return set()
    urls = set()
    with open(SUMMARIES_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url")
            if url:
                urls.add(url)
    return urls


def extract_article_text(url: str) -> (str, str):
    """
    기사 제목 + 본문 텍스트 추출.
    더벨 구조에 맞게 selector는 필요에 따라 조정 가능.
    """
    res = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    # 제목 후보
    title_tag = soup.select_one("h1, h2, div.title, div#article-title")
    title = (title_tag.get_text() or "").strip() if title_tag else ""

    # 본문 후보
    candidates = [
        "div#article-view-content-div",
        "div.article",
        "div#content",
    ]
    texts: List[str] = []
    for sel in candidates:
        parts = soup.select(sel + " p") or soup.select(sel)
        for p in parts:
            t = p.get_text(" ", strip=True)
            if t:
                texts.append(t)
        if texts:
            break

    if not texts:
        body = soup.get_text(" ", strip=True)
    else:
        body = "\n".join(texts)

    # 너무 길면 앞부분만
    return title, body[:8000]


def call_openai(title: str, body: str) -> Dict[str, Any]:
    user_prompt = USER_PROMPT_TEMPLATE.format(title=title, body=body)

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )
    content = resp.choices[0].message.content
    return json.loads(content)


def append_summaries(rows: List[dict]):
    fieldnames = [
        "deal_number",
        "url",
        "article_title",
        "lp_institutions",
        "vc_firms",
        "fund_name",
        "fund_size_raw",
        "fund_size_value",
        "fund_size_unit",
        "fund_size_currency",
        "fund_sector",
        "fund_stage",
        "summary",
        "analysis_comment",
        "summarized_at",
    ]
    file_exists = os.path.exists(SUMMARIES_CSV)
    with open(SUMMARIES_CSV, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


if __name__ == "__main__":
    print("=== LP News GPT 요약기 ===")

    links = load_links()
    summarized_urls = load_summarized_urls()
    print(f"[INFO] 링크 CSV 로딩: {len(links)}건, 기존 요약 URL: {len(summarized_urls)}건")

    new_summary_rows: List[dict] = []

    for row in links:
        deal_number = row.get("deal_number")
        url = row.get("url")
        if not url:
            continue
        if url in summarized_urls:
            continue

        try:
            print(f"[INFO] 요약 중: deal {deal_number}, url={url}")
            title, body = extract_article_text(url)
            data = call_openai(title=title, body=body)

            fund_size = data.get("fund_size") or {}

            new_summary_rows.append(
                {
                    "deal_number": deal_number,
                    "url": url,
                    "article_title": title,
                    "lp_institutions": ", ".join(data.get("lp_institutions") or []),
                    "vc_firms": ", ".join(data.get("vc_firms") or []),
                    "fund_name": data.get("fund_name"),
                    "fund_size_raw": fund_size.get("raw"),
                    "fund_size_value": fund_size.get("value"),
                    "fund_size_unit": fund_size.get("unit"),
                    "fund_size_currency": fund_size.get("currency"),
                    "fund_sector": ", ".join(data.get("fund_sector") or []),
                    "fund_stage": ", ".join(data.get("fund_stage") or []),
                    "summary": data.get("summary"),
                    "analysis_comment": data.get("analysis_comment"),
                    "summarized_at": datetime.utcnow().isoformat(),
                }
            )
        except Exception as e:
            print(f"[WARN] 요약 실패 (deal {deal_number}): {e}")

    if not new_summary_rows:
        print("[INFO] 새로 요약할 URL 없음.")
    else:
        append_summaries(new_summary_rows)
        print(f"[INFO] 총 {len(new_summary_rows)}건 요약 추가 → {SUMMARIES_CSV}")
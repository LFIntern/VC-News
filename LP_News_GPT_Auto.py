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
- 펀드 규모(텍스트/숫자/단위)
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
  "LP": ["string | 출자자(LP) 이름 리스트"],
  "운용사": ["string | VC/PE 운용사(GP) 이름 리스트"],
  "펀드명": "string | 펀드명, 없으면 null",
  "펀드규모_텍스트": "string | 기사에 나온 펀드 규모 표현 (예: '약 1,000억 원')",
  "펀드규모_숫자": 0,
  "펀드규모_단위": "string | '억', '조', 'billion', 'million' 등, 없으면 null",
  "펀드유형": ["string | 벤처, 그로스, 세컨더리, 바이아웃, 프로젝트 등"],
  "투자섹터": ["string | AI, 헬스케어, 소재/부품, 콘텐츠, 친환경/에너지 등"],
  "투자단계": ["string | 초기, 시리즈A/B, 그로스, Pre-IPO 등"],
  "투자지역": ["string | 국내, 글로벌, 미국, 유럽, 아시아 등"],
  "조성상태": "string | 신규결성, 1차 클로징, 멀티클로징, 모집중 등",
  "요약": "string | 기사 내용 핵심을 2~3문장으로 요약",
  "코멘트": "string | VC/PE 리서치 관점에서 이 뉴스의 의미를 한두 문장으로 정리"
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
    """기사 제목 + 본문 텍스트 추출.

    - thebell: div#article-view-content-div, div.article 등
    - newstopkorea: article.atlview-grid-body (기사 본문 영역)
    """
    res = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    # 제목 후보 (공통)
    # - thebell의 경우 상단에 '전체기사' 같은 헤더가 먼저 나오고,
    #   실제 기사 제목은 다른 태그(h3/h4 또는 별도 div)에 있는 경우가 있어
    #   조금 더 폭넓게 잡고, 이후에 <meta>, <title>로도 보정한다.
    title_tag = soup.select_one(
        "h1, h2, h3, h4, div.title, div#article-title, div#articleTitle, div#article_title"
    )
    title = (title_tag.get_text() or "").strip() if title_tag else ""

    # thebell 등에서 헤더 텍스트('전체기사', '뉴스홈')만 잡히거나
    # 아무것도 못 잡은 경우에 대한 보정
    if not title or title in ("전체기사", "뉴스홈"):
        # 1순위: og:title 메타 태그
        og_meta = soup.find("meta", attrs={"property": "og:title"}) or soup.find(
            "meta", attrs={"name": "title"}
        )
        if og_meta and og_meta.get("content"):
            title = og_meta["content"].strip()

    # 그래도 실패하면 <title> 태그에서 사이트명 이전까지만 사용
    if (not title) and soup.title:
        raw_title = soup.title.get_text(strip=True)
        # 예: "[thebell desk]직원들이 회사를 샀다 - thebell"
        if " - " in raw_title:
            title = raw_title.split(" - ", 1)[0].strip()
        else:
            title = raw_title

    # 본문候補 기본값 (thebell 등)
    candidates = [
        "div#article-view-content-div",
        "div.article",
        "div#content",
    ]

    # newstopkorea 도메인의 경우 기사 본문 article.atlview-grid-body를 우선 시도
    if "newstopkorea.com" in url:
        candidates = [
            "article.atlview-grid-body",
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
        "Deal ID",
        "기사 제목",
        "LP",
        "운용사",
        "펀드명",
        "펀드규모_텍스트",
        "펀드유형",
        "투자섹터",
        "조성상태",
        "요약",
        "코멘트",
        "summarized_at",
        "url",
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

            new_summary_rows.append(
                {
                    "Deal ID": deal_number,
                    "url": url,
                    "기사 제목": title,
                    "LP": ", ".join(data.get("LP") or []),
                    "운용사": ", ".join(data.get("운용사") or []),
                    "펀드명": data.get("펀드명"),
                    "펀드규모_텍스트": data.get("펀드규모_텍스트"),
                    "펀드유형": ", ".join(data.get("펀드유형") or []),
                    "투자섹터": ", ".join(data.get("투자섹터") or []),
                    "조성상태": data.get("조성상태"),
                    "요약": data.get("요약"),
                    "코멘트": data.get("코멘트"),
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
import os
import csv
import json
from typing import List, Dict, Any

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

LINKS_CSV = "lp_news_links.csv"
SUMMARIES_CSV = "lp_news_summaries.csv"
MASTER_CSV = "lp_news_master_log.csv"

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

SYSTEM_PROMPT = """
너는 VC/PE 리서치팀에서 일하는 애널리스트 어시스턴트이자 데이터 정제 봇이다.

입력으로 한국어 또는 영어 기사 제목과 본문이 주어진다.
너의 역할은 두 가지다.

1) 이 기사 안에 ‘신규 펀드레이징(Fundraising) 또는 출자사업’에 대한 구체적인 프로그램/펀드 정보(규모, 주체, 진행 상태 등)가 존재하는지 VERY STRICT하게 판별한다.
2) 그런 정보가 하나라도 존재한다면, 그 중 이번 기사에서 가장 핵심적으로 다루는 대표적인 1개의 펀드/출자 프로그램을 골라, 그 펀드/출자사업 관련 핵심 정보를 구조화해서 JSON으로 정리한다.

[is_fundraising = true 인 경우]
다음과 같은 기사들은 is_fundraising = true 로 판단한다.
- 공공 또는 민간 LP(예: 모태펀드, 한국벤처투자, 한국성장금융, 연기금, 공제회, 보험사, 은행 등)가
  - 출자사업을 공고/진행/마감했다
  - 위탁운용사(GP)를 선정했다
  - 자펀드를 결성한다/결성할 예정이다
  - 블라인드펀드, 프로젝트펀드, 세컨더리펀드 등을 새로 조성한다/조성할 예정이다
  와 같이, 새로운 펀드나 자펀드에 자금을 넣는 행위 자체가 기사 핵심인 경우.
- 기존 펀드의 추가 클로징(1차/2차/최종 클로징 등)으로 AUM이 증가하는 내용이 기사 핵심인 경우.
이때 is_fundraising = true 는 “기사 전체가 펀드레이징 기사냐”가 아니라, “기사 안에 위 조건을 만족하는 **특정 펀드/출자 프로그램에 대한 구체 정보 블록이 존재한다”는 뜻이다. JSON 필드(펀드명, 펀드규모, 조성상태 등)는 그 대표적인 1개의 프로그램을 기준으로 채운다.

[is_fundraising = false 인 경우]
다음과 같은 기사들은 is_fundraising = false 로 판단한다.
- 개별 기업/스타트업/포트폴리오에 대한 ‘투자 집행’ 기사 (펀드 자체가 아니라 투자 대상 기업이 중심인 경우)
- 과거에 이미 진행된 출자사업/펀드 조성의 성과나 히스토리를 설명하는 기사 (성과 리뷰, 통계, 유니콘 사례 소개 등)
- 펀드 운용성과/리턴/엑시트, 포트폴리오 성과 위주의 기사
- 기관/VC의 전략, 조직개편, 해외 진출, IR, 정책 방향 등 일반 전략·동향 기사
- 출자사업/펀드가 잠깐 언급되더라도, 이번 기사에서 새로 진행되는 출자/펀드 결성 또는 자펀드 결성이 아니다.
- 예컨대 “[LP Radar]IBK기업은행, 내년 VC 출자 규모 키운다”처럼, 특정 차수/연도 출자사업의 공고·선정·클로징 결과나 새로운 프로그램의 구체적인 규모·GP·자펀드 구조를 제시하지 않고, 단순히 향후 출자 규모를 ‘키운다/확대할 계획’처럼 추상적으로 설명하는 전략·계획 기사는 is_fundraising = false 로 본다. 다만, 같은 기사 안에 별도로 특정 출자사업/펀드(예: ‘2025년 하반기 글로벌펀드 출자사업, 총 7,000억 규모, GP 6곳 선정’)에 대한 구체 정보 블록이 존재한다면, 그 블록을 기준으로 is_fundraising = true 로 보고 정보를 추출해야 한다.

판정 원칙:
- 모호한 경우에는 항상 is_fundraising = false 로 두고 보수적으로 판단하라.
- is_fundraising = true 로 판단한다는 것은 “기사 전체가 펀드레이징 기사”라는 뜻이 아니라, 기사 안에 위 조건을 만족하는 **구체적인 펀드/출자 프로그램 정보 블록이 최소 1개 존재한다”는 뜻이다.
- is_fundraising = true 로 판단하려면, 기사 안에
  (a) 특정 차수/연도/프로그램명이 붙은 출자사업 또는 펀드가 명시되고,
  (b) 그 출자사업 또는 펀드에 대해 구체적인 금액/자펀드 규모, 위탁운용사 선정, 클로징 완료/마감 등의 사실이 포함되어 있어야 한다.
  이 두 가지 조건이 모두 충족되지 않으면 is_fundraising = false 로 본다.
- 기사에 과거/현재/미래의 여러 출자사업/펀드가 같이 언급되더라도,
  이번 기사에서 새롭게 발표된 출자·펀드 조성/자펀드 결성의 “결과 또는 구체 계획(규모·GP·자펀드 구조 등)”이 없다면 is_fundraising = false 로 본다.
- 특정 연도·차수의 출자사업 결과(예: ‘2025년 하반기 글로벌펀드 출자사업, 6개 GP 선정, 모태펀드 768억 출자, 자펀드 7,214억 조성 예정’)처럼
  이번 차수 출자와 자펀드 결성 규모가 기사 핵심이면 is_fundraising = true이다.

출력 형식:
- 항상 JSON 객체 한 개만 반환한다.
- is_fundraising 이 false인 경우에는, 아래 스키마 중 is_fundraising 외 필드는 모두 null 또는 빈 배열로 두거나 생략하고,
  is_fundraising 필드만 정확히 false로 설정한다.
- is_fundraising 이 true인 경우에만 나머지 필드를 최대한 채운다.

반드시 JSON 이외의 텍스트(설명, 주석, 자연어 문장)는 출력하지 마라.
"""

USER_PROMPT_TEMPLATE = """
다음은 벤처캐피탈/PE와 관련된 한국어 기사 전문이다.

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
  "펀드규모": "string | 기사에 나온 펀드 또는 자펀드 규모 표현 (예: '약 1,000억 원')",
  "펀드유형": ["string | 벤처, 그로스, 세컨더리, 바이아웃, 프로젝트 등, 없으면 빈 리스트"],
  "투자섹터": ["string | Biotech & Healthcare, Interactive Contents & Media, Consumer Internet & Fintech, ICT & Digitalization, Semiconductor & Industrial, ETC 중 해당되는 것들. 애매하면 'ETC'만 넣을 것."],
  "조성상태": "string | 신규결성, 1차 클로징, 멀티클로징, 모집중, 위탁운용사 선정 등, 기사 맥락에 맞는 한 단어. 애매하면 null",
  "요약": "string | (is_fundraising=true 인 경우에만) 기사 내용 중 펀드레이징/출자사업 관련 핵심을 2~3문장으로 요약",
  "is_fundraising": "boolean | 이 기사가 '신규 펀드 결성/자펀드 결성/출자사업(선정, 공고, 클로징 등)'에 대한 기사이면 true, 그 외에는 false"
}}
"""


def load_links() -> List[dict]:
    if not os.path.exists(LINKS_CSV):
        return []
    with open(LINKS_CSV, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_processed_urls() -> set:
    """
    마스터 로그(있으면)와 요약 CSV에서 이미 처리된 URL들을 읽어온다.
    (펀드/비펀드 모두 포함)
    """
    urls = set()
    # 마스터 로그 기준
    if os.path.exists(MASTER_CSV):
        with open(MASTER_CSV, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url")
                if url:
                    urls.add(url)
    # 과거 버전 호환: 마스터 로그가 없던 시절 요약만 된 URL들도 포함
    if os.path.exists(SUMMARIES_CSV):
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

    # 본문 기본값 (thebell 등)
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
        "기사 작성일",
        "LP",
        "운용사",
        "펀드명",
        "펀드규모",
        "펀드유형",
        "투자섹터",
        "조성상태",
        "요약",
        "url",
    ]
    file_exists = os.path.exists(SUMMARIES_CSV)
    with open(SUMMARIES_CSV, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


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


def get_last_deal_id() -> int:
    """
    SUMMARIES_CSV 기준으로 마지막 Deal ID를 읽어와서 정수로 반환한다.
    파일이 없거나 유효한 Deal ID가 없으면 0을 반환한다.
    """
    if not os.path.exists(SUMMARIES_CSV):
        return 0

    last_id = 0
    with open(SUMMARIES_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = row.get("Deal ID") or row.get("deal_number") or row.get("deal_id")
            if not raw:
                continue
            try:
                num = int(str(raw).strip())
            except ValueError:
                # 숫자로 변환 불가한 Deal ID는 무시
                continue
            if num > last_id:
                last_id = num
    return last_id


if __name__ == "__main__":
    print("=== LP News GPT 요약기 ===")

    links = load_links()
    processed_urls = load_processed_urls()
    print(f"[INFO] 링크 CSV 로딩: {len(links)}건, 기존 처리 URL: {len(processed_urls)}건")

    # 써머리 CSV 기준 마지막 Deal ID를 불러와, 그 다음 번호부터 사용
    last_deal_id = get_last_deal_id()
    next_deal_id = last_deal_id + 1
    print(f"[INFO] 마지막 Deal ID(요약 기준): {last_deal_id}, 다음 시작 Deal ID: {next_deal_id}")

    new_summary_rows: List[dict] = []  # 새로 요약할 펀드레이징 기사
    master_rows: List[dict] = []       # 마스터 로그용 처리 이력

    for row in links:
        # Deal ID는 링크 CSV가 아니라, 써머리 CSV 기준 마지막 번호 + 1부터 순차 부여
        url = row.get("url")
        if not url:
            continue
        if url in processed_urls:
            continue

        try:
            print(f"[INFO] 요약 중: url={url}")
            title, body = extract_article_text(url)
            data = call_openai(title=title, body=body)

            # is_fundraising 플래그 해석
            is_fundraising = data.get("is_fundraising")
            if isinstance(is_fundraising, str):
                is_fundraising_normalized = is_fundraising.strip().lower()
                is_fundraising = is_fundraising_normalized in ("true", "1", "yes", "y")
            else:
                is_fundraising = bool(is_fundraising)

            article_date = row.get("기사 작성일") or row.get("article_date") or row.get("date") or ""

            if is_fundraising:
                deal_id = str(next_deal_id)
                next_deal_id += 1

                new_summary_rows.append(
                    {
                        "Deal ID": deal_id,
                        "기사 제목": title,
                        "기사 작성일": article_date,
                        "LP": ", ".join(data.get("LP") or []),
                        "운용사": ", ".join(data.get("운용사") or []),
                        "펀드명": data.get("펀드명"),
                        "펀드규모": data.get("펀드규모"),
                        "펀드유형": ", ".join(data.get("펀드유형") or []),
                        "투자섹터": ", ".join(data.get("투자섹터") or []),
                        "조성상태": data.get("조성상태"),
                        "요약": data.get("요약"),
                        "url": url,
                    }
                )
                master_rows.append(
                    {
                        "Deal ID": deal_id,
                        "기사 제목": title,
                        "기사 작성일": article_date,
                        "url": url,
                        "is_fundraising": True,
                        "status": "fundraising_saved",
                    }
                )
            else:
                master_rows.append(
                    {
                        "Deal ID": "",
                        "기사 제목": title,
                        "기사 작성일": article_date,
                        "url": url,
                        "is_fundraising": False,
                        "status": "non_fundraising",
                    }
                )

        except Exception as e:
            print(f"[WARN] 요약 실패 (url={url}): {e}")

    if not new_summary_rows:
        print("[INFO] 새로 요약할 URL 없음.")
    else:
        append_summaries(new_summary_rows)
        print(f"[INFO] 총 {len(new_summary_rows)}건 요약 추가 → {SUMMARIES_CSV}")

    if master_rows:
        append_master_log(master_rows)
        print(f"[INFO] 총 {len(master_rows)}건 처리 결과 기록 → {MASTER_CSV}")
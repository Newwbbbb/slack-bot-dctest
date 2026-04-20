# app.py — DCInside ONLY 안정 버전 (GitHub Actions 호환)
# 주요 수정:
#  1. 실제 Chrome 수준의 전체 헤더 (Sec-Fetch-*, Accept, Referer 등)
#  2. requests.Session() 으로 쿠키 유지 + 메인 페이지 선방문
#  3. 재시도(backoff) + 차단 페이지 감지
#  4. 진단 로깅 강화 (status, 응답 길이, 본문 일부)
#  5. 실패 시 Slack에 원인을 함께 전달

import os
import re
import html
import time
import random
import logging
from collections import Counter
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------- 설정 ----------------
DC_GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
DC_BASE_URL = "https://gall.dcinside.com"
DC_MAIN_URL = "https://www.dcinside.com/"

# 실제 Chrome 122 수준의 헤더 세트
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

HANGUL_TOKEN = re.compile(r"[가-힣]{2,}")
STOPWORDS = set(
    "그리고 그러나 그래서 또는 이런 저런 같은 해당 우리 당신 여러분 그냥 매우 너무 좀 진짜 거의 또한 또 더".split()
)

ISSUE_BUCKETS = {
    "장비·강화": ["강화", "연마", "장비"],
    "직업·밸런스": ["소울", "검마", "각성"],
    "콘텐츠": ["레이드", "던전"],
    "경제": ["골드", "과금"],
    "버그": ["버그", "렉"],
}

MAX_RETRIES = 4
BASE_DELAY = 2.0  # seconds

# ---------------- 유틸 ----------------

def clean_text(s):
    return html.unescape(re.sub(r"\s+", " ", s)).strip()

def tokenize(text):
    return [t for t in HANGUL_TOKEN.findall(text) if t not in STOPWORDS]

def bucket_issues(tokens):
    c = Counter(tokens)
    result = {}
    for k, keys in ISSUE_BUCKETS.items():
        score = sum(c[x] for x in keys)
        if score:
            result[k] = score
    return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))

def looks_blocked(text: str) -> bool:
    """DC 차단/점검/빈 페이지 휴리스틱."""
    if not text or len(text) < 3000:
        return True
    lowered = text.lower()
    bad_markers = [
        "access denied",
        "blocked",
        "cloudflare",
        "잠시 후 다시 시도",
        "접근이 차단",
        "점검 중",
        "비정상적인 접근",
    ]
    return any(m in text or m in lowered for m in bad_markers)

# ---------------- DC 수집 ----------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def _warmup(session: requests.Session):
    """메인 페이지를 먼저 방문해서 쿠키 획득."""
    try:
        r = session.get(DC_MAIN_URL, timeout=15)
        logging.info(f"[warmup] status={r.status_code} cookies={len(session.cookies)}")
        time.sleep(random.uniform(0.8, 1.6))
    except Exception as e:
        logging.warning(f"[warmup] 실패 (계속 진행): {e}")

def dc_fetch():
    """DC 갤러리 수집. 실패 시 (posts, error_reason) 튜플 대신 posts만 반환하고
    에러 사유는 전역 로거에 남김. 최종 호출부에서 len(posts)로 판단."""
    session = _make_session()
    _warmup(session)

    # 갤러리 요청 시 Referer 지정 (메인에서 들어온 것처럼)
    req_headers = {"Referer": DC_MAIN_URL}

    last_reason = "unknown"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(DC_GALLERY_URL, headers=req_headers, timeout=20)
            body_len = len(r.text)
            logging.info(
                f"[attempt {attempt}] status={r.status_code} len={body_len}"
            )

            # 명시적 차단 상태코드
            if r.status_code in (403, 429, 503):
                last_reason = f"HTTP {r.status_code} (차단/레이트리밋)"
                logging.warning(last_reason)
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            r.raise_for_status()

            # 본문 기반 차단/빈 페이지 감지
            if looks_blocked(r.text):
                last_reason = f"차단 또는 비정상 응답 (len={body_len})"
                logging.warning(last_reason)
                logging.debug("본문 앞부분: %s", r.text[:500])
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("table.gall_list tbody tr")
            logging.info(f"rows: {len(rows)}")

            if not rows:
                last_reason = "table.gall_list 행 없음 (셀렉터 변경 가능성)"
                logging.warning(last_reason)
                logging.debug("본문 앞부분: %s", r.text[:800])
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            posts = _parse_rows(rows)
            logging.info(f"posts: {len(posts)}")

            if posts:
                return posts

            last_reason = "행은 있으나 파싱 결과 0건"
            logging.warning(last_reason)

        except requests.RequestException as e:
            last_reason = f"요청 예외: {e!r}"
            logging.exception(f"[attempt {attempt}] 요청 실패")
            time.sleep(BASE_DELAY * attempt + random.random())

    logging.error(f"모든 재시도 실패. 마지막 사유: {last_reason}")
    # 에러 사유를 환경변수에 저장해서 build_summary에서 노출
    os.environ["DC_FETCH_ERROR"] = last_reason
    return []

def _parse_rows(rows):
    posts = []
    for tr in rows:
        # 공지 제외
        cls = tr.get("class") or []
        if "notice" in cls or "notice" in str(tr.get("class", "")):
            continue
        if tr.select_one("td.gall_subject") and "공지" in tr.get_text():
            # 보수적 이중 체크
            pass

        cols = tr.find_all("td")
        if len(cols) < 7:
            continue

        title_el = cols[2].select_one("a")
        if not title_el:
            continue

        try:
            href = title_el.get("href", "")
            if not href:
                continue
            posts.append({
                "source": "DC",
                "category": clean_text(cols[1].get_text()),
                "title": clean_text(title_el.get_text()),
                "link": urljoin(DC_BASE_URL, href),
                "views": int(re.sub(r"\D", "", cols[5].get_text()) or 0),
                "up": int(re.sub(r"\D", "", cols[6].get_text()) or 0),
            })
        except Exception:
            continue
    return posts

# ---------------- 요약 ----------------

def build_summary(posts):

    if not posts:
        reason = os.environ.get("DC_FETCH_ERROR", "원인 불명")
        return {
            "text": "DC 수집 실패",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn",
                    "text": f":warning: *DC 데이터 수집 실패*\n사유: `{reason}`"}},
                {"type": "context", "elements": [
                    {"type": "mrkdwn",
                     "text": "GitHub Actions 러너 IP가 DCInside에 차단되었을 가능성이 높습니다. "
                             "셀프호스팅 러너 또는 프록시 사용을 검토하세요."}
                ]},
            ],
        }

    by_cat = Counter(p["category"] for p in posts)
    by_src = Counter(p["source"] for p in posts)

    tokens = []
    for p in posts:
        tokens += tokenize(p["title"])

    top_keywords = Counter(tokens).most_common(10)
    issues = bucket_issues(tokens)

    top_posts = sorted(posts, key=lambda x: (x["views"], x["up"]), reverse=True)[:5]
    top_views = sorted(posts, key=lambda x: x["views"], reverse=True)[:5]
    top_up = sorted(posts, key=lambda x: x["up"], reverse=True)[:5]

    def fmt(p, i):
        return f"{i+1}. <{p['link']}|{p['title']}> · 조회 {p['views']} · 추천 {p['up']}"

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": "*던파M DCInside 요약*"}},
        {"type": "divider"},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*말머리 분포*\n" + "\n".join(f"- {k}: {v}" for k, v in by_cat.most_common())
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*출처 분포*\n" + "\n".join(f"- {k}: {v}" for k, v in by_src.most_common())
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*핵심 키워드*\n" + ", ".join(f"{k}({v})" for k, v in top_keywords)
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*이슈 레이더*\n" + ("\n".join(f"- {k}: {v}" for k, v in issues.items()) or "(감지 없음)")
        }},

        {"type": "divider"},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*인기글 TOP5*\n" + "\n".join(fmt(p, i) for i, p in enumerate(top_posts))
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*조회수 TOP5*\n" + "\n".join(fmt(p, i) for i, p in enumerate(top_views))
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*추천수 TOP5*\n" + "\n".join(fmt(p, i) for i, p in enumerate(top_up))
        }},
    ]

    return {"text": "DC 요약", "blocks": blocks}

# ---------------- Slack ----------------

def post_to_slack(payload):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        raise ValueError("SLACK_WEBHOOK_URL 없음")

    r = requests.post(webhook, json=payload, timeout=15)
    r.raise_for_status()

# ---------------- main ----------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    posts = dc_fetch()
    logging.info(f"수집된 글 수: {len(posts)}")

    summary = build_summary(posts)
    post_to_slack(summary)

if __name__ == "__main__":
    main()

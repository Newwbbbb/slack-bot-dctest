# app.py — DCInside ONLY (어제 게시글만 수집, KST 기준)
# 주요 특징:
#  1. truststore 주입 → Windows 인증서 저장소 사용 (회사 SSL 검사 프록시 대응)
#  2. html.parser 사용 → lxml 의존성 제거
#  3. 실제 Chrome 수준의 전체 헤더 + 세션 + 워밍업
#  4. 재시도/백오프, 오탐 방지된 차단 감지
#  5. ★ KST 기준 "어제" 날짜로 페이지네이션하며 필터링
#  6. 실패 시 Slack에 사유 전달

# === 회사 SSL 프록시 환경에서 Windows 인증서 저장소를 쓰도록 주입 ===
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

import os
import re
import html
import time
import random
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------- 설정 ----------------
DC_GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
DC_BASE_URL = "https://gall.dcinside.com"
DC_MAIN_URL = "https://www.dcinside.com/"

KST = timezone(timedelta(hours=9))

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
MAX_PAGES_DC = int(os.getenv("MAX_PAGES_DC", "10"))
REQUEST_INTERVAL_SEC = float(os.getenv("REQUEST_INTERVAL_SEC", "1.5"))

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

def get_target_date():
    """수집 대상 날짜 = KST 기준 어제."""
    now_kst = datetime.now(KST)
    return (now_kst - timedelta(days=1)).date()

def looks_blocked(text: str) -> bool:
    """DC 차단/점검/빈 페이지 휴리스틱 (오탐 방지)."""
    if not text or len(text) < 3000:
        return True
    normal_markers = ['class="gall_list"', "gall_list", "dcinside"]
    if any(m in text for m in normal_markers):
        return False
    bad_markers = [
        "access denied",
        "잠시 후 다시 시도",
        "접근이 차단",
        "점검 중",
        "비정상적인 접근",
    ]
    return any(m in text for m in bad_markers)

def parse_post_datetime(td_date):
    """td.gall_date의 title 속성 또는 텍스트에서 datetime 추출 (KST)."""
    if td_date is None:
        return None

    # 1) title 속성이 가장 정확 — "2026-04-19 14:32:56" 형태
    title = (td_date.get("title") or "").strip()
    if title:
        try:
            return datetime.strptime(title, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
        except ValueError:
            pass

    # 2) 텍스트 기반 fallback
    text = clean_text(td_date.get_text())
    now = datetime.now(KST)

    # "HH:MM" — 오늘 작성
    if re.match(r"^\d{2}:\d{2}$", text):
        try:
            h, m = map(int, text.split(":"))
            return now.replace(hour=h, minute=m, second=0, microsecond=0)
        except ValueError:
            pass

    # "MM.DD" — 올해의 다른 날
    if re.match(r"^\d{2}\.\d{2}$", text):
        try:
            mo, d = map(int, text.split("."))
            dt = datetime(now.year, mo, d, 12, 0, 0, tzinfo=KST)
            if dt > now:  # 미래면 작년
                dt = dt.replace(year=now.year - 1)
            return dt
        except ValueError:
            pass

    return None

# ---------------- DC 수집 ----------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def _warmup(session: requests.Session):
    try:
        r = session.get(DC_MAIN_URL, timeout=15)
        logging.info(f"[warmup] status={r.status_code} cookies={len(session.cookies)}")
        time.sleep(random.uniform(0.8, 1.6))
    except Exception as e:
        logging.warning(f"[warmup] 실패 (계속 진행): {e}")

def _page_url(page: int) -> str:
    if page <= 1:
        return DC_GALLERY_URL
    return f"{DC_GALLERY_URL}&page={page}"

def _fetch_page_rows(session, page, req_headers):
    """단일 페이지를 재시도 포함해서 가져오고 tr 리스트 반환. 실패 시 (None, reason)."""
    url = _page_url(page)
    last_reason = "unknown"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=req_headers, timeout=20)
            body_len = len(r.text)
            logging.info(f"[page {page} attempt {attempt}] status={r.status_code} len={body_len}")

            if r.status_code in (403, 429, 503):
                last_reason = f"HTTP {r.status_code} (차단/레이트리밋)"
                logging.warning(last_reason)
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            r.raise_for_status()

            if looks_blocked(r.text):
                last_reason = f"차단 또는 비정상 응답 (len={body_len})"
                logging.warning(last_reason)
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("table.gall_list tbody tr")
            logging.info(f"[page {page}] rows: {len(rows)}")

            if not rows:
                last_reason = f"page {page}: table.gall_list 행 없음"
                logging.warning(last_reason)
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            return rows, None

        except requests.RequestException as e:
            last_reason = f"요청 예외: {e!r}"
            logging.exception(f"[page {page} attempt {attempt}] 요청 실패")
            time.sleep(BASE_DELAY * attempt + random.random())

    return None, last_reason

def _parse_row(tr):
    """단일 tr → (datetime, post dict) 또는 None."""
    cls = tr.get("class") or []
    if "notice" in cls or "notice" in str(tr.get("class", "")):
        return None

    cols = tr.find_all("td")
    if len(cols) < 7:
        return None

    title_el = cols[2].select_one("a")
    if not title_el:
        return None

    href = title_el.get("href", "")
    if not href:
        return None

    post_dt = parse_post_datetime(cols[4])

    try:
        post = {
            "source": "DC",
            "category": clean_text(cols[1].get_text()),
            "title": clean_text(title_el.get_text()),
            "link": urljoin(DC_BASE_URL, href),
            "views": int(re.sub(r"\D", "", cols[5].get_text()) or 0),
            "up": int(re.sub(r"\D", "", cols[6].get_text()) or 0),
            "posted_at": post_dt.isoformat() if post_dt else None,
        }
    except Exception:
        return None

    return post_dt, post

def dc_fetch():
    """KST 기준 어제 작성 게시글만 수집."""
    session = _make_session()
    _warmup(session)

    req_headers = {"Referer": DC_MAIN_URL}
    target_date = get_target_date()
    logging.info(f"수집 대상 날짜: {target_date} (KST 기준 어제)")

    collected = []
    stop = False
    last_reason = None

    for page in range(1, MAX_PAGES_DC + 1):
        rows, reason = _fetch_page_rows(session, page, req_headers)

        if rows is None:
            last_reason = reason
            # 첫 페이지 실패면 치명적, 그 외엔 지금까지 모은 것 반환
            if page == 1:
                os.environ["DC_FETCH_ERROR"] = reason or "page 1 실패"
                return []
            logging.warning(f"page {page} 건너뜀: {reason}")
            break

        # 페이지 내 파싱 + 날짜별 카운트
        page_yesterday_count = 0
        page_older_count = 0
        page_newer_count = 0

        for tr in rows:
            parsed = _parse_row(tr)
            if not parsed:
                continue
            dt, post = parsed

            if dt is None:
                # 날짜 파싱 실패한 글은 스킵 (공지/비정상 행일 가능성)
                continue

            d = dt.date()
            if d == target_date:
                collected.append(post)
                page_yesterday_count += 1
            elif d < target_date:
                page_older_count += 1
            else:  # d > target_date (오늘 이후)
                page_newer_count += 1

        logging.info(
            f"[page {page}] 어제={page_yesterday_count} "
            f"이전={page_older_count} 이후={page_newer_count}"
        )

        # 중단 조건: 이 페이지에 어제보다 더 이전 글이 하나라도 보이면,
        # 다음 페이지는 확정적으로 더 오래된 글들이므로 멈춘다.
        if page_older_count > 0:
            logging.info(f"어제 이전 글 발견 → 페이지네이션 종료 (page {page})")
            stop = True

        if stop:
            break

        # 다음 페이지 요청 전 딜레이
        time.sleep(REQUEST_INTERVAL_SEC)

    logging.info(f"최종 수집: {len(collected)}건 (대상일 {target_date})")

    if not collected and last_reason:
        os.environ["DC_FETCH_ERROR"] = last_reason
    elif not collected:
        os.environ["DC_FETCH_ERROR"] = f"{target_date}에 해당하는 글 없음"

    return collected

# ---------------- 요약 ----------------

def build_summary(posts):
    target_date = get_target_date()
    date_str = target_date.strftime("%Y-%m-%d")

    if not posts:
        reason = os.environ.get("DC_FETCH_ERROR", "원인 불명")
        return {
            "text": f"DC 수집 실패 ({date_str})",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn",
                    "text": f":warning: *DC 데이터 수집 실패 ({date_str})*\n사유: `{reason}`"}},
                {"type": "context", "elements": [
                    {"type": "mrkdwn",
                     "text": "러너 네트워크에서 DCInside 접근을 확인하세요 (방화벽, 프록시, 인증서)."}
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
        {"type": "section", "text": {"type": "mrkdwn",
            "text": f"*던파M DCInside 요약 — {date_str}* (총 {len(posts)}건)"}},
        {"type": "divider"},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*말머리 분포*\n" + "\n".join(f"- {k}: {v}" for k, v in by_cat.most_common())
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*출처 분포*\n" + "\n".join(f"- {k}: {v}" for k, v in by_src.most_common())
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*핵심 키워드*\n" + (", ".join(f"{k}({v})" for k, v in top_keywords) or "(없음)")
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

    return {"text": f"DC 요약 — {date_str}", "blocks": blocks}

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

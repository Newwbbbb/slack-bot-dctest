# app.py — DCInside ONLY (어제 게시글만, KST 기준)
# 날짜 표시 형식:
#   - "14:32"       → 오늘 글
#   - "04.19"       → 올해 다른 날
#   - "2024.04.19"  → 작년 이전

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
BASE_DELAY = 2.0
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

# ---------------- 날짜/행 파싱 ----------------

# DC 텍스트 형식 정규식
RE_TIME_ONLY   = re.compile(r"^\s*(\d{1,2}):(\d{1,2})\s*$")                     # 14:32
RE_MONTH_DAY   = re.compile(r"^\s*(\d{1,2})\.(\d{1,2})\s*$")                    # 04.19
RE_YEAR_MD     = re.compile(r"^\s*(\d{4})\.(\d{1,2})\.(\d{1,2})\s*$")           # 2024.04.19
RE_FULL_TITLE  = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})(?:[\sT]+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?")  # title 속성용

def _find_date_td(tr):
    """tr 안에서 날짜 td 찾기 (견고한 다단계)."""
    td = tr.select_one("td.gall_date")
    if td:
        return td
    # 텍스트가 MM.DD / HH:MM / YYYY.MM.DD 중 하나인 td
    for td in tr.find_all("td"):
        t = clean_text(td.get_text())
        if RE_TIME_ONLY.match(t) or RE_MONTH_DAY.match(t) or RE_YEAR_MD.match(t):
            return td
    cols = tr.find_all("td")
    if len(cols) > 4:
        return cols[4]
    return None

def parse_post_datetime(td_date):
    """td에서 datetime 추출 (KST). 텍스트 우선, title 속성은 보조."""
    if td_date is None:
        return None

    now = datetime.now(KST)
    text = clean_text(td_date.get_text())

    # 1) "HH:MM" — 오늘 글
    m = RE_TIME_ONLY.match(text)
    if m:
        try:
            h, mi = map(int, m.groups())
            return now.replace(hour=h, minute=mi, second=0, microsecond=0)
        except ValueError:
            pass

    # 2) "MM.DD" — 올해 다른 날 (★ DC에서 가장 흔한 형식)
    m = RE_MONTH_DAY.match(text)
    if m:
        try:
            mo, d = map(int, m.groups())
            dt = datetime(now.year, mo, d, 12, 0, 0, tzinfo=KST)
            # 미래면 작년으로 보정 (예: 1월에 "12.30"은 작년)
            if dt.date() > now.date():
                dt = dt.replace(year=now.year - 1)
            return dt
        except ValueError:
            pass

    # 3) "YYYY.MM.DD" — 작년 이전
    m = RE_YEAR_MD.match(text)
    if m:
        try:
            y, mo, d = map(int, m.groups())
            return datetime(y, mo, d, 12, 0, 0, tzinfo=KST)
        except ValueError:
            pass

    # 4) title 속성 fallback (텍스트 파싱 실패 시)
    title = (td_date.get("title") or "").strip()
    if title:
        m = RE_FULL_TITLE.search(title)
        if m:
            y, mo, d, h, mi, s = m.groups()
            try:
                return datetime(
                    int(y), int(mo), int(d),
                    int(h or 0), int(mi or 0), int(s or 0),
                    tzinfo=KST,
                )
            except ValueError:
                pass

    return None

def _debug_dump_first_rows(rows, page, n=3):
    """첫 n개 행의 구조와 날짜 파싱 결과 로그."""
    if not rows:
        return
    logging.info(f"[DEBUG page {page}] 총 {len(rows)}행 중 상위 {min(n, len(rows))}행 덤프")
    for idx, tr in enumerate(rows[:n]):
        cols = tr.find_all("td")
        td_date = _find_date_td(tr)
        text = clean_text(td_date.get_text()) if td_date else "(없음)"
        title = (td_date.get("title") if td_date else "") or ""
        dt = parse_post_datetime(td_date)
        cls = " ".join(tr.get("class") or [])
        logging.info(
            f"[DEBUG page {page}] row#{idx} trClass='{cls}' tdCount={len(cols)} "
            f"dateText='{text}' dateTitle='{title[:30]}' parsed={dt}"
        )

def _parse_row(tr):
    """단일 tr → (datetime, post dict) 또는 None."""
    cls = tr.get("class") or []
    if "notice" in cls:
        return None

    cols = tr.find_all("td")
    if len(cols) < 5:
        return None

    title_el = cols[2].select_one("a") if len(cols) > 2 else None
    if not title_el:
        return None

    href = title_el.get("href", "")
    if not href:
        return None

    td_date = _find_date_td(tr)
    post_dt = parse_post_datetime(td_date)

    try:
        views = int(re.sub(r"\D", "", cols[5].get_text()) or 0) if len(cols) > 5 else 0
        up = int(re.sub(r"\D", "", cols[6].get_text()) or 0) if len(cols) > 6 else 0
    except Exception:
        views, up = 0, 0

    post = {
        "source": "DC",
        "category": clean_text(cols[1].get_text()) if len(cols) > 1 else "",
        "title": clean_text(title_el.get_text()),
        "link": urljoin(DC_BASE_URL, href),
        "views": views,
        "up": up,
        "posted_at": post_dt.isoformat() if post_dt else None,
    }
    return post_dt, post

# ---------------- 수집 ----------------

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
    url = _page_url(page)
    last_reason = "unknown"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=req_headers, timeout=20)
            body_len = len(r.text)
            logging.info(f"[page {page} attempt {attempt}] status={r.status_code} len={body_len}")

            if r.status_code in (403, 429, 503):
                last_reason = f"HTTP {r.status_code}"
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            r.raise_for_status()

            if looks_blocked(r.text):
                last_reason = f"차단/비정상 응답 (len={body_len})"
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("table.gall_list tbody tr")
            logging.info(f"[page {page}] rows: {len(rows)}")

            if not rows:
                last_reason = f"page {page}: rows 없음"
                time.sleep(BASE_DELAY * attempt + random.random())
                continue

            return rows, None

        except requests.RequestException as e:
            last_reason = f"요청 예외: {e!r}"
            logging.exception(f"[page {page} attempt {attempt}] 요청 실패")
            time.sleep(BASE_DELAY * attempt + random.random())

    return None, last_reason

def dc_fetch():
    """KST 기준 어제 작성 게시글만 수집."""
    session = _make_session()
    _warmup(session)

    req_headers = {"Referer": DC_MAIN_URL}
    target_date = get_target_date()
    logging.info(f"수집 대상 날짜: {target_date} (KST 어제)")
    logging.info(f"현재 시각(KST): {datetime.now(KST).isoformat()}")

    collected = []
    last_reason = None
    total_rows = 0
    total_fail = 0

    for page in range(1, MAX_PAGES_DC + 1):
        rows, reason = _fetch_page_rows(session, page, req_headers)

        if rows is None:
            last_reason = reason
            if page == 1:
                os.environ["DC_FETCH_ERROR"] = reason or "page 1 실패"
                return []
            logging.warning(f"page {page} 건너뜀: {reason}")
            break

        if page == 1:
            _debug_dump_first_rows(rows, page, n=3)

        page_y = page_o = page_n = page_dnone = 0

        for tr in rows:
            parsed = _parse_row(tr)
            if not parsed:
                continue
            dt, post = parsed
            total_rows += 1

            if dt is None:
                page_dnone += 1
                total_fail += 1
                continue

            d = dt.date()
            if d == target_date:
                collected.append(post)
                page_y += 1
            elif d < target_date:
                page_o += 1
            else:
                page_n += 1

        logging.info(
            f"[page {page}] 어제={page_y} 이전={page_o} 이후={page_n} 날짜파싱실패={page_dnone}"
        )

        # 어제 이전 글이 보이면 종료
        if page_o > 0:
            logging.info(f"어제 이전 글 발견 → 종료 (page {page})")
            break

        # 1페이지에서 전혀 파싱 안 되면 종료
        if page == 1 and page_y == 0 and page_n == 0 and page_dnone > 0:
            logging.error("page 1 모든 행 날짜 파싱 실패 → 종료")
            os.environ["DC_FETCH_ERROR"] = (
                f"날짜 파싱 전부 실패 ({page_dnone}행). "
                f"Actions 로그의 [DEBUG page 1] 줄을 확인하세요."
            )
            return []

        time.sleep(REQUEST_INTERVAL_SEC)

    logging.info(
        f"최종 수집: {len(collected)}건 (대상일 {target_date}, "
        f"총본행 {total_rows}, 파싱실패 {total_fail})"
    )

    if not collected:
        if last_reason:
            os.environ["DC_FETCH_ERROR"] = last_reason
        else:
            os.environ["DC_FETCH_ERROR"] = (
                f"{target_date}에 해당하는 글 없음 "
                f"(총 {total_rows}행 확인, 파싱실패 {total_fail}행)"
            )

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

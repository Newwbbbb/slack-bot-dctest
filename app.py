# app.py — DCInside + Arca Live 통합 요약 봇 (v2)
#
# 주요 수정사항 (v2):
# 1. DCInside 파싱을 인덱스 기반 → 클래스 기반(td.gall_tit, td.gall_date 등)으로 변경
#    → 테이블 컬럼 순서/개수 변동에 강해짐
# 2. td.gall_date의 title 속성(YYYY-MM-DD HH:MM:SS)을 우선 활용
#    → 날짜 파싱 정확도 대폭 향상 ("HH:MM"만으로 추정하던 문제 해결)
# 3. 공지/광고 필터를 us-notice/adunit/adhit 등 실제 클래스명에 맞춤
# 4. Arca Live 파싱을 anchor 매칭 → a.vrow 기반으로 변경
#    → 제목 외 링크(프로필, 댓글) 오탐 제거, time[datetime] 속성으로 정확한 시간 확보
# 5. 세션 사용 + 브라우저 헤더 보강 (Sec-Fetch-* 등)
# 6. 진단 로깅 대폭 강화 — 빈 결과일 때 어느 단계에서 실패했는지 Slack으로 전달
# 7. 일부 수집 실패(한쪽만 실패)해도 다른 쪽 결과로 요약 진행

import os
import re
import time
import html
import json
import logging
from datetime import datetime, timedelta, timezone
from collections import Counter
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------- 공통 설정 ----------------
KST = timezone(timedelta(hours=9))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
# 브라우저 유사 헤더 강화 (Sec-Fetch-*, Sec-Ch-Ua 추가)
COMMON_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

# 속도/예의
REQUEST_INTERVAL_SEC = float(os.getenv("REQUEST_INTERVAL_SEC", "1.5"))
MAX_PAGES_DC = int(os.getenv("MAX_PAGES_DC", "10"))
MAX_LINKS_ARCA = int(os.getenv("MAX_LINKS_ARCA", "120"))

# ---------------- 소스: DCInside ----------------
DC_GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
DC_BASE_URL = "https://gall.dcinside.com"

# ---------------- 소스: Arca Live ----------------
ARCA_BOARD_URL = "https://arca.live/b/mobiledf"
ARCA_BASE_URL = "https://arca.live"

# ---------------- 분석 설정 ----------------
HANGUL_TOKEN = re.compile(r"[가-힣]{2,}")
STOPWORDS = set(
    """
    그리고 그러나 그래서 또는 이런 저런 같은 해당 우리 당신 여러분 그냥 매우 너무 좀 진짜 거의 또한 또 더 좀더
    오늘 어제 내일 이번 지난 다음 지금 지금은 조금 많이 대한 관련 관련해 관련한 대해서 등 등등
    """.split()
)

ISSUE_BUCKETS = {
    "장비·강화": ["강화", "16강", "연마", "장비변환", "돌파", "보장", "악세", "방어구"],
    "직업·밸런스": ["소울", "스핏", "귀참", "검마", "시너지", "무적", "각성", "룬", "액리폼", "여메카", "암제", "소마"],
    "콘텐츠·레이드": ["재해", "흑룡", "안톤", "레이드", "일던", "정예", "결투"],
    "경제·과금": ["현질", "세라", "콘텐츠페이", "골드", "과금"],
    "이벤트·콜라보": ["콜라보", "소아온", "이벤트", "패스"],
    "품질·버그": ["팅김", "멈춤", "버그", "렉", "크래시", "오류"],
    "커뮤니티·정책": ["공지", "파업", "규칙", "운영", "네오플"],
}
WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]

# ---------------- 진단용 전역 상태 ----------------
# 실패 원인을 Slack 메시지에 담기 위해 누적
DIAG = {
    "dc_status": None,      # ok / http_{code} / error / blocked
    "dc_raw_rows": 0,       # 파싱 전 원본 행 수
    "dc_parsed": 0,         # 정상 파싱된 행 수
    "dc_in_window": 0,      # 24h 필터 통과 수
    "dc_error": None,
    "arca_status": None,
    "arca_raw_rows": 0,
    "arca_parsed": 0,
    "arca_in_window": 0,
    "arca_error": None,
}

# ---------------- 공통 유틸 ----------------

def clean_text(s: str) -> str:
    if s is None:
        return ""
    return html.unescape(re.sub(r"\s+", " ", s)).strip()


def tokenize_korean(text: str):
    tokens = [t for t in HANGUL_TOKEN.findall(text)]
    return [t for t in tokens if t not in STOPWORDS and len(t) >= 2]


def bucket_issues(tokens):
    counts = Counter(tokens)
    result = {}
    for bucket, keys in ISSUE_BUCKETS.items():
        score = sum(counts[k] for k in keys)
        if score > 0:
            result[bucket] = score
    return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))


def _to_int(s: str) -> int:
    if not s:
        return 0
    digits = re.sub(r"[^\-0-9]", "", s)
    try:
        return int(digits) if digits and digits != "-" else 0
    except Exception:
        return 0


def make_session(referer: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(COMMON_HEADERS)
    s.headers["Referer"] = referer
    return s


# ---------------- DCInside 수집 ----------------

def parse_dc_datetime(td_date) -> datetime:
    """
    DCInside 날짜 td의 title 속성을 최우선으로 사용.
    title="2026-04-20 15:23:45" (당일/최근 글)
    title="2026-04-20" (오래된 글의 경우)
    텍스트 내용: "15:23" (당일) 또는 "04.19" / "24.11.20" (과거)
    """
    now_kst = datetime.now(KST)
    # 1) title 속성 우선 (가장 정확)
    title_attr = (td_date.get("title") or "").strip()
    if title_attr:
        # "2026-04-20 15:23:45" 형태
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})", title_attr)
        if m:
            y, M, d, hh, mm, ss = map(int, m.groups())
            try:
                return datetime(y, M, d, hh, mm, ss, tzinfo=KST)
            except Exception:
                pass
        # "2026-04-20" 형태
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", title_attr)
        if m:
            y, M, d = map(int, m.groups())
            try:
                return datetime(y, M, d, tzinfo=KST)
            except Exception:
                pass

    # 2) 텍스트 fallback
    raw = clean_text(td_date.get_text())
    if ":" in raw:
        # "15:23" → 오늘
        try:
            hh, mm = map(int, raw.split(":")[:2])
            return now_kst.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except Exception:
            pass
    if "." in raw:
        parts = raw.split(".")
        try:
            if len(parts) == 2:
                # "04.19" → 올해
                M, d = int(parts[0]), int(parts[1])
                candidate = datetime(now_kst.year, M, d, tzinfo=KST)
                # 미래면 작년으로
                if candidate > now_kst + timedelta(days=1):
                    candidate = datetime(now_kst.year - 1, M, d, tzinfo=KST)
                return candidate
            if len(parts) >= 3:
                y = int(parts[0])
                if y < 100:
                    y += 2000
                M = int(parts[1])
                d = int(parts[2])
                return datetime(y, M, d, tzinfo=KST)
        except Exception:
            pass

    # 3) 실패시 24시간 이전으로 (필터에서 배제)
    return now_kst - timedelta(days=365)


def dc_fetch_page(session: requests.Session, page: int):
    url = f"{DC_GALLERY_URL}&page={page}" if page > 1 else DC_GALLERY_URL
    r = session.get(url, timeout=20)
    r.raise_for_status()
    time.sleep(REQUEST_INTERVAL_SEC)
    return r.text


def dc_parse_list(html_text: str):
    """
    DCInside 마이너 갤러리 리스트 파싱 (클래스 기반).
    - 공지/광고 제외
    - td.gall_tit a (첫 번째), td.gall_date, td.gall_count, td.gall_recommend 활용
    """
    soup = BeautifulSoup(html_text, "lxml")
    all_trs = soup.select("table.gall_list tbody tr")
    DIAG["dc_raw_rows"] += len(all_trs)

    rows = []
    for tr in all_trs:
        classes = set(tr.get("class") or [])
        # 공지/광고 제외 (실제 클래스명 기반)
        if classes & {"notice", "us-notice", "adunit", "adhit", "ad"}:
            continue
        # data-type이 ad인 경우도 제외
        if (tr.get("data-type") or "").lower() in ("ad", "notice"):
            continue

        td_num = tr.select_one("td.gall_num")
        td_subject = tr.select_one("td.gall_subject")
        td_tit = tr.select_one("td.gall_tit")
        td_writer = tr.select_one("td.gall_writer")
        td_date = tr.select_one("td.gall_date")
        td_count = tr.select_one("td.gall_count")
        td_recommend = tr.select_one("td.gall_recommend")

        # 최소 요건: 제목 td와 날짜 td는 있어야 함
        if not td_tit or not td_date:
            continue

        # 제목 td 안에서 '첫 번째' a (댓글수 앵커 제외)
        title_anchor = None
        for a in td_tit.find_all("a"):
            # 댓글 수 표시용 anchor(reply_numbox)는 건너뜀
            a_class = set(a.get("class") or [])
            if "reply_numbox" in a_class:
                continue
            # 제목이 있는 anchor
            if a.get_text(strip=True):
                title_anchor = a
                break
        if not title_anchor:
            continue

        # "공지" 텍스트가 번호 자리에 있으면 제외 (class 누락시 대비)
        num_text = clean_text(td_num.get_text()) if td_num else ""
        if num_text in ("공지", "설문", "AD", "공지사항"):
            continue

        title = clean_text(title_anchor.get_text())
        href = title_anchor.get("href")
        link = urljoin(DC_BASE_URL, href) if href else None
        if not title or not link:
            continue

        category = clean_text(td_subject.get_text()) if td_subject else ""
        author = clean_text(td_writer.get_text()) if td_writer else ""
        dt = parse_dc_datetime(td_date)
        views = _to_int(td_count.get_text()) if td_count else 0
        up = _to_int(td_recommend.get_text()) if td_recommend else 0

        rows.append({
            "source": "DCInside",
            "no": num_text,
            "category": category,
            "title": title,
            "link": link,
            "author": author,
            "dt": dt,
            "views": views,
            "up": up,
        })

    DIAG["dc_parsed"] += len(rows)
    return rows


def dc_fetch_last_24h(hours=24):
    cutoff = datetime.now(KST) - timedelta(hours=hours)
    session = make_session(referer="https://gall.dcinside.com/")
    all_posts = []

    # 1회 "워밍업" — 메인 페이지 방문으로 쿠키 취득
    try:
        session.get("https://gall.dcinside.com/", timeout=15)
        time.sleep(0.5)
    except Exception as e:
        logging.info(f"[DC] 워밍업 실패(무시): {e}")

    pages_read = 0
    for page in range(1, MAX_PAGES_DC + 1):
        try:
            html_text = dc_fetch_page(session, page)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            DIAG["dc_status"] = f"http_{code}"
            DIAG["dc_error"] = f"HTTP {code} on page {page}"
            logging.warning(f"[DC] HTTP {code} on page {page}: {e}")
            break
        except Exception as e:
            DIAG["dc_status"] = "error"
            DIAG["dc_error"] = f"{type(e).__name__}: {e}"
            logging.warning(f"[DC] 페이지 {page} 요청 실패: {e}")
            break

        pages_read += 1
        posts = dc_parse_list(html_text)
        if not posts:
            # 파싱 0이면 구조 변경 가능성
            if page == 1:
                DIAG["dc_status"] = DIAG["dc_status"] or "parsed_empty"
                DIAG["dc_error"] = DIAG["dc_error"] or "첫 페이지 파싱 결과 0건 (구조 변경 의심)"
            break

        any_in_window = False
        min_dt = None
        for p in posts:
            if not min_dt or p["dt"] < min_dt:
                min_dt = p["dt"]
            if p["dt"] >= cutoff:
                all_posts.append(p)
                any_in_window = True

        # 다음 페이지가 확실히 24시간 밖이면 중단
        if not any_in_window and min_dt and min_dt < cutoff:
            break

    if DIAG["dc_status"] is None:
        DIAG["dc_status"] = "ok" if all_posts or pages_read else "empty"

    uniq = {p["link"]: p for p in all_posts if p.get("link")}
    DIAG["dc_in_window"] = len(uniq)
    return list(uniq.values())


# ---------------- Arca Live 수집 ----------------

ARCA_ISO_RX = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})")


def parse_arca_datetime(time_el):
    """
    Arca Live <time> 요소의 datetime 속성을 우선 사용.
    datetime="2026-04-20T15:23:45.000Z" 또는 "2026-04-20T15:23:45+09:00"
    """
    if time_el is None:
        return None
    dt_attr = (time_el.get("datetime") or "").strip()
    if dt_attr:
        m = ARCA_ISO_RX.search(dt_attr)
        if m:
            y, M, d, hh, mm, ss = map(int, m.groups())
            try:
                # Arca는 일반적으로 Z(UTC) 또는 +09:00으로 오는데,
                # Z일 경우 UTC → KST 변환 필요
                if dt_attr.endswith("Z") or "+00:00" in dt_attr:
                    naive = datetime(y, M, d, hh, mm, ss, tzinfo=timezone.utc)
                    return naive.astimezone(KST)
                # +09:00이면 이미 KST
                return datetime(y, M, d, hh, mm, ss, tzinfo=KST)
            except Exception:
                pass
    return None


def arca_parse_list(html_text: str):
    """
    Arca Live 리스트 파싱 — a.vrow 기반.
    - .vrow.notice 등 공지 제외
    - <time datetime="..."> 속성으로 시간 정확 파싱
    - .col-view, .col-rate 로 조회수/추천수
    """
    soup = BeautifulSoup(html_text, "lxml")

    # 1순위: a.vrow 셀렉터
    vrows = soup.select("a.vrow")
    # fallback: a[href^="/b/mobiledf/"] 중 vrow 부모 가진 것
    if not vrows:
        vrows = [a for a in soup.select('a[href^="/b/mobiledf/"]') if a.get("class") and "vrow" in a["class"]]

    DIAG["arca_raw_rows"] += len(vrows)

    items = []
    seen = set()
    for a in vrows:
        a_classes = set(a.get("class") or [])
        if "notice" in a_classes:
            continue

        href = a.get("href", "")
        m = re.match(r"^/b/mobiledf/(\d+)", href)
        if not m:
            continue
        post_id = m.group(1)
        if post_id in seen:
            continue
        seen.add(post_id)

        title_el = a.select_one(".col-title .title") or a.select_one(".col-title") or a.select_one(".title")
        title = clean_text(title_el.get_text()) if title_el else clean_text(a.get_text())
        if not title:
            continue

        category_el = a.select_one(".vrow-category")
        category = clean_text(category_el.get_text()) if category_el else ""

        time_el = a.select_one("time.col-time") or a.select_one("time")
        dt = parse_arca_datetime(time_el)

        view_el = a.select_one(".col-view")
        rate_el = a.select_one(".col-rate")
        views = _to_int(view_el.get_text()) if view_el else 0
        up = _to_int(rate_el.get_text()) if rate_el else 0

        link = urljoin(ARCA_BASE_URL, href)

        items.append({
            "source": "Arca",
            "no": post_id,
            "category": category,
            "title": title,
            "link": link,
            "author": "",
            "dt": dt,
            "views": views,
            "up": up,
        })
        if len(items) >= MAX_LINKS_ARCA:
            break

    DIAG["arca_parsed"] += len(items)
    return items


def arca_fetch(session: requests.Session, url: str):
    r = session.get(url, timeout=20)
    r.raise_for_status()
    time.sleep(REQUEST_INTERVAL_SEC)
    text = r.text
    if any(x in text for x in ["Checking your browser", "cf-challenge", "Please enable JavaScript", "Attention Required"]):
        DIAG["arca_status"] = "blocked_challenge"
        DIAG["arca_error"] = "Cloudflare/보호 페이지 감지"
        logging.warning("[Arca] 보호 페이지 감지")
    return text


def arca_fetch_last_24h(hours=24):
    cutoff = datetime.now(KST) - timedelta(hours=hours)
    session = make_session(referer="https://arca.live/")

    try:
        html_text = arca_fetch(session, ARCA_BOARD_URL)
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        DIAG["arca_status"] = f"http_{code}"
        DIAG["arca_error"] = f"HTTP {code}"
        logging.warning(f"[Arca] HTTP {code}: {e}")
        return []
    except Exception as e:
        DIAG["arca_status"] = "error"
        DIAG["arca_error"] = f"{type(e).__name__}: {e}"
        logging.warning(f"[Arca] 요청 실패: {e}")
        return []

    items = arca_parse_list(html_text)

    # 시간 파싱 실패한 것은 "방금 전"으로 간주(리스트 상위일수록 최신) → 포함
    now_kst = datetime.now(KST)
    result = []
    for it in items:
        dt = it.get("dt")
        if dt is None:
            # 시간 알 수 없는 경우, 안전하게 제외(이상치 방지)
            continue
        if dt >= cutoff:
            result.append(it)

    if DIAG["arca_status"] is None:
        DIAG["arca_status"] = "ok" if result else ("parsed_empty" if not items else "no_recent")

    uniq = {p["link"]: p for p in result if p.get("link")}
    DIAG["arca_in_window"] = len(uniq)
    return list(uniq.values())


# ---------------- 통합 수집 ----------------

def fetch_all_sources_last_24h():
    hours = int(os.getenv("HOURS", "24"))
    results = []

    # DCInside
    if os.getenv("DC_ENABLED", "1") == "1":
        try:
            results.extend(dc_fetch_last_24h(hours=hours))
        except Exception as e:
            DIAG["dc_status"] = DIAG["dc_status"] or "error"
            DIAG["dc_error"] = DIAG["dc_error"] or f"{type(e).__name__}: {e}"
            logging.exception("[DC] 수집 실패")
    else:
        DIAG["dc_status"] = "disabled"

    # Arca Live
    if os.getenv("ARCA_ENABLED", "1") == "1":
        try:
            results.extend(arca_fetch_last_24h(hours=hours))
        except Exception as e:
            DIAG["arca_status"] = DIAG["arca_status"] or "error"
            DIAG["arca_error"] = DIAG["arca_error"] or f"{type(e).__name__}: {e}"
            logging.exception("[Arca] 수집 실패")
    else:
        DIAG["arca_status"] = "disabled"

    return results


# ---------------- 요약/슬랙 ----------------

def build_diagnostic_text() -> str:
    """빈 결과일 때 Slack 메시지에 포함할 진단 정보"""
    lines = []
    # DC
    dc_line = f"• DCInside: 상태=`{DIAG['dc_status']}`, 원본행={DIAG['dc_raw_rows']}, 파싱={DIAG['dc_parsed']}, 24h내={DIAG['dc_in_window']}"
    if DIAG["dc_error"]:
        dc_line += f"\n   └ 오류: {DIAG['dc_error']}"
    lines.append(dc_line)
    # Arca
    arca_line = f"• Arca Live: 상태=`{DIAG['arca_status']}`, 원본행={DIAG['arca_raw_rows']}, 파싱={DIAG['arca_parsed']}, 24h내={DIAG['arca_in_window']}"
    if DIAG["arca_error"]:
        arca_line += f"\n   └ 오류: {DIAG['arca_error']}"
    lines.append(arca_line)
    return "\n".join(lines)


def build_summary(posts):
    now = datetime.now(KST)
    weekday = WEEKDAY_KR[now.weekday()]
    title_text = f"*던파M 커뮤니티 동향 요약* — {now.strftime('%Y-%m-%d')} ({weekday}) {now.strftime('%H:%M')} KST 기준"

    context_links = (
        f"<{DC_GALLERY_URL}|DCInside 던파M> · <{ARCA_BOARD_URL}|Arca 던파M>"
    )

    if not posts:
        diag = build_diagnostic_text()
        return {
            "text": f"수집된 글이 없거나 파싱 실패 ({now.strftime('%Y-%m-%d %H:%M KST')})",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": title_text}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": context_links + " · 지난 24시간"}]},
                {"type": "section", "text": {"type": "mrkdwn", "text": ":warning: *수집 결과가 비어 있습니다.*"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": "*진단 상세*\n" + diag}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": "상태값: `ok`/`http_403`/`parsed_empty`/`blocked_challenge`/`error`/`disabled` · 원본행=테이블 행 수, 파싱=정상 파싱된 글, 24h내=시간 필터 통과"}]},
            ],
        }

    by_cat = Counter((p.get("category") or "기타") for p in posts if p["source"] == "DCInside")
    by_src = Counter(p["source"] for p in posts)
    total = len(posts)

    tokens = []
    for p in posts:
        tokens += tokenize_korean(p["title"])
    top_keywords = Counter(tokens).most_common(10)
    issue_scores = bucket_issues(tokens)
    top_issues = list(issue_scores.items())[:5]

    top_posts = sorted(posts, key=lambda x: (x.get("views", 0), x.get("up", 0)), reverse=True)[:5]

    def mk_list(lines):
        return "\n".join(lines)

    cat_lines = [
        f"- *{cat}*: {cnt}개 ({cnt/(sum(by_cat.values()) or 1)*100:.1f}%)" for cat, cnt in by_cat.most_common()
    ]
    src_lines = [f"- *{src}*: {cnt}개 ({cnt/total*100:.1f}%)" for src, cnt in by_src.most_common()]
    kw_lines = [f"*{k}* ({c})" for k, c in top_keywords]
    iss_lines = [f"- *{k}*: {v}" for k, v in top_issues]
    post_lines = [
        f"{i+1}. <{p['link']}|{p['title']}> · {p['source']} · 조회 {p.get('views',0)} · 추천 {p.get('up',0)} · "
        + (p['dt'].astimezone(KST).strftime('%m/%d %H:%M') if p.get('dt') else '')
        for i, p in enumerate(top_posts)
    ]

    text_fallback = (
        f"총 {total}건 | 출처: " + ", ".join([f"{k}:{v}" for k,v in by_src.most_common()]) +
        " | 키워드: " + ", ".join([k for k,_ in top_keywords[:5]])
    )

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title_text}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": context_links + " · 수집범위: 지난 24시간"}]},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*말머리 분포(DC)*\n" + (mk_list(cat_lines) if cat_lines else "- DCInside 말머리 없음")}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*출처 분포*\n" + mk_list(src_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*핵심 키워드 TOP 10*\n" + (", ".join(kw_lines) if kw_lines else "- 없음")}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*이슈 레이더*\n" + (mk_list(iss_lines) if iss_lines else "- 감지된 주요 이슈 없음")}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*인기 글 TOP 5*\n" + mk_list(post_lines)}},
    ]
    return {"text": text_fallback, "blocks": blocks}


def post_to_slack(payload):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    bot_token = os.getenv("SLACK_BOT_TOKEN")
    channel = os.getenv("SLACK_CHANNEL")

    if webhook:
        r = requests.post(
            webhook,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if r.status_code >= 300:
            raise RuntimeError(f"Webhook error: {r.status_code} {r.text}")
        return "webhook"
    elif bot_token and channel:
        api = "https://slack.com/api/chat.postMessage"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {bot_token}",
        }
        body = {"channel": channel, "text": payload["text"], "blocks": payload["blocks"]}
        r = requests.post(api, headers=headers, data=json.dumps(body), timeout=15)
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(f"Slack API error: {data}")
        return "bot"
    else:
        raise RuntimeError(
            "슬랙 설정이 없습니다. SLACK_WEBHOOK_URL 또는 (SLACK_BOT_TOKEN+SLACK_CHANNEL)을 넣어주세요."
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    try:
        posts = fetch_all_sources_last_24h()
    except Exception:
        logging.exception("수집 중 치명 오류")
        posts = []

    # 최종 진단 로그
    logging.info(f"[DIAG] DC status={DIAG['dc_status']} raw={DIAG['dc_raw_rows']} parsed={DIAG['dc_parsed']} window={DIAG['dc_in_window']} err={DIAG['dc_error']}")
    logging.info(f"[DIAG] Arca status={DIAG['arca_status']} raw={DIAG['arca_raw_rows']} parsed={DIAG['arca_parsed']} window={DIAG['arca_in_window']} err={DIAG['arca_error']}")

    summary = build_summary(posts)
    mode = post_to_slack(summary)
    logging.info(f"Slack 전송 완료 ({mode}) — {len(posts)} posts summarized.")


if __name__ == "__main__":
    main()

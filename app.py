# app.py — DCInside + Arca Live 통합 요약 봇 (patched)
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
# 브라우저 유사 헤더 (아카라이브 보호 페이지 회피 확률↑)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://arca.live/",
}

# 속도/예의
REQUEST_INTERVAL_SEC = float(os.getenv("REQUEST_INTERVAL_SEC", "1.2"))
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

# ---------------- 유틸 ----------------

def clean_text(s: str) -> str:
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


def parse_time_like_dc(raw: str) -> datetime:
    raw = raw.strip()
    now_kst = datetime.now(KST)
    if ":" in raw:
        try:
            hour, minute = map(int, raw.split(":")[:2])
            return now_kst.replace(hour=hour, minute=minute, second=0, microsecond=0)
        except Exception:
            pass
    parts = raw.split(".")
    if len(parts) >= 3:
        try:
            y = int(parts[0])
            if y < 100:
                y += 2000
            m = int(parts[1])
            d = int(parts[2])
            return datetime(y, m, d, tzinfo=KST)
        except Exception:
            pass
    return now_kst - timedelta(days=365)

# ---------------- DCInside 수집 ----------------

def dc_fetch_page(page: int):
    url = f"{DC_GALLERY_URL}&page={page}" if page > 1 else DC_GALLERY_URL
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    time.sleep(REQUEST_INTERVAL_SEC)
    return r.text


def dc_parse_list(html_text: str):
    soup = BeautifulSoup(html_text, "lxml")
    rows = []
    for tr in soup.select("table.gall_list tbody tr"):
        classes = " ".join(tr.get("class") or [])
        if "notice" in classes or "ad" in classes:
            continue
        cols = tr.find_all("td")
        if len(cols) < 7:
            continue
        try:
            no = clean_text(cols[0].get_text())
            category = clean_text(cols[1].get_text())
            title_el = cols[2].select_one("a:nth-of-type(1)")
            title = clean_text(title_el.get_text()) if title_el else clean_text(cols[2].get_text())
            href = title_el["href"] if title_el and title_el.has_attr("href") else None
            link = urljoin(DC_BASE_URL, href) if href else None
            author = clean_text(cols[3].get_text())
            dt_raw = clean_text(cols[4].get_text())
            dt = parse_time_like_dc(dt_raw)
            views = int(re.sub(r"[^\d]", "", cols[5].get_text()) or "0")
            up = int(re.sub(r"[^\d]", "", cols[6].get_text()) or "0")
        except Exception:
            continue
        rows.append({
            "source": "DCInside",
            "no": no, "category": category, "title": title, "link": link,
            "author": author, "dt": dt, "views": views, "up": up
        })
    return rows


def dc_fetch_last_24h(hours=24):
    cutoff = datetime.now(KST) - timedelta(hours=hours)
    all_posts = []
    for page in range(1, MAX_PAGES_DC + 1):
        try:
            html_text = dc_fetch_page(page)
        except Exception as e:
            logging.warning(f"[DC] 페이지 {page} 요청 실패: {e}")
            break
        posts = dc_parse_list(html_text)
        if not posts:
            break
        any_new = False
        min_dt = None
        for p in posts:
            min_dt = p["dt"] if not min_dt or p["dt"] < min_dt else min_dt
            if p["dt"] >= cutoff:
                all_posts.append(p)
                any_new = True
        if not any_new and min_dt and min_dt < cutoff:
            break
    uniq = {p["link"]: p for p in all_posts if p.get("link")}
    return list(uniq.values())

# ---------------- Arca Live 수집 ----------------

ARCA_DATE_RX1 = re.compile(r"(\d{4})[.\-](\d{2})[.\-](\d{2})")
ARCA_DATE_RX2 = re.compile(r"(\d{2}):(\d{2})")
ARCA_VIEWS_RX = re.compile(r"조회수\s*([0-9,]+)")
ARCA_UP_RX    = re.compile(r"추천\s*([\-0-9,]+)")
ARCA_POST_DATETIME_RX = re.compile(r"(\d{4})[.\-](\d{2})[.\-](\d{2})\s+(\d{2}):(\d{2}):(\d{2})")


def arca_fetch(url: str):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    time.sleep(REQUEST_INTERVAL_SEC)
    text = r.text
    # 보호/차단 페이지 휴리스틱 탐지
    if any(x in text for x in ["Checking your browser", "cf-challenge", "Please enable JavaScript"]):
        logging.warning("[Arca] 보호 페이지 감지: 파싱 품질 저하 가능")
    return text


def arca_parse_list(html_text: str):
    soup = BeautifulSoup(html_text, "lxml")
    anchors = soup.select('a[href^="/b/mobiledf/"]')
    seen = set()
    items = []
    for a in anchors:
        href = a.get("href", "")
        m = re.match(r"^/b/mobiledf/(\d+)", href)
        if not m:
            continue
        post_id = m.group(1)
        if post_id in seen:
            continue
        seen.add(post_id)
        title = clean_text(a.get_text())
        link = urljoin(ARCA_BASE_URL, href)

        parent = a.find_parent(["tr", "div", "li"]) or a.parent
        row_text = clean_text(parent.get_text(" ")) if parent else ""

        dt = None
        m1 = ARCA_DATE_RX1.search(row_text)
        if m1:
            y, M, d = map(int, m1.groups())
            dt = datetime(y, M, d, tzinfo=KST)
        else:
            m2 = ARCA_DATE_RX2.search(row_text)
            if m2:
                now = datetime.now(KST)
                h, mi = map(int, m2.groups())
                dt = now.replace(hour=h, minute=mi, second=0, microsecond=0)

        views = 0
        up = 0
        vm = ARCA_VIEWS_RX.search(row_text)
        if vm:
            views = int(vm.group(1).replace(",", ""))
        um = ARCA_UP_RX.search(row_text)
        if um:
            try:
                up = int(um.group(1).replace(",", ""))
            except Exception:
                up = 0

        items.append({
            "source": "Arca",
            "no": post_id,
            "category": "",
            "title": title,
            "link": link,
            "author": "",
            "dt": dt,
            "views": views,
            "up": up,
        })
        if len(items) >= MAX_LINKS_ARCA:
            break
    return items


def arca_fill_detail(item):
    try:
        html_text = arca_fetch(item["link"])
    except Exception as e:
        logging.warning(f"[Arca] 본문 요청 실패: {e}")
        return item
    m = ARCA_POST_DATETIME_RX.search(html_text)
    if m:
        y, M, d, hh, mm, ss = map(int, m.groups())
        item["dt"] = datetime(y, M, d, hh, mm, ss, tzinfo=KST)
    vm = ARCA_VIEWS_RX.search(html_text)
    if vm:
        item["views"] = int(vm.group(1).replace(",", ""))
    um = ARCA_UP_RX.search(html_text)
    if um:
        try:
            item["up"] = int(um.group(1).replace(",", ""))
        except Exception:
            pass
    return item


def arca_fetch_last_24h(hours=24):
    cutoff = datetime.now(KST) - timedelta(hours=hours)
    html_text = arca_fetch(ARCA_BOARD_URL)
    items = arca_parse_list(html_text)

    missing = [it for it in items if not it.get("dt")]
    to_fill = missing[:int(os.getenv("ARCA_DETAIL_LIMIT", "30"))]
    for it in to_fill:
        arca_fill_detail(it)

    result = []
    for it in items:
        if it.get("dt") and it["dt"] >= cutoff:
            result.append(it)
    uniq = {p["link"]: p for p in result if p.get("link")}
    return list(uniq.values())

# ---------------- 통합 수집 ----------------

def fetch_all_sources_last_24h():
    hours = int(os.getenv("HOURS", "24"))
    results = []
    # DCInside는 항상 시도
    try:
        results.extend(dc_fetch_last_24h(hours=hours))
    except Exception:
        logging.exception("[DC] 수집 실패")

    # Arca는 토글 가능
    if os.getenv("ARCA_ENABLED", "1") == "1":
        try:
            results.extend(arca_fetch_last_24h(hours=hours))
        except Exception:
            logging.exception("[Arca] 수집 실패")

    return results

# ---------------- 요약/슬랙 ----------------

def build_summary(posts):
    now = datetime.now(KST)
    weekday = WEEKDAY_KR[now.weekday()]
    title_text = f"*던파M 커뮤니티 동향 요약* — {now.strftime('%Y-%m-%d')} ({weekday}) 09:00 KST 기준"

    context_links = (
        f"<{DC_GALLERY_URL}|DCInside 던파M> · <{ARCA_BOARD_URL}|Arca 던파M>"
    )

    if not posts:
        return {
            "text": f"수집된 글이 없거나 파싱 실패 ({now.strftime('%Y-%m-%d %H:%M KST')})",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": title_text}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": context_links + " · 지난 24시간"}]},
                {"type": "section", "text": {"type": "mrkdwn", "text": ":warning: 수집 결과가 비어 있습니다."}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": "진단 팁: Actions 로그 확인 · 환경변수 HOURS/ARCA_ENABLED 조정 · 요청 간격 증가"}]},
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
        {"type": "section", "text": {"type": "mrkdwn", "text": "*핵심 키워드 TOP 10*\n" + ", ".join(kw_lines)}},
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
    logging.basicConfig(level=logging.INFO)
    try:
        posts = fetch_all_sources_last_24h()
    except Exception:
        logging.exception("수집 중 치명 오류")
        posts = []
    summary = build_summary(posts)
    mode = post_to_slack(summary)
    logging.info(f"Slack 전송 완료 ({mode}) — {len(posts)} posts summarized.")


if __name__ == "__main__":
    main()

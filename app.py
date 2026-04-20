# app.py — DCInside + Arca Live 통합 요약 봇 (patched + TOP views/up)
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
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://arca.live/",
}

REQUEST_INTERVAL_SEC = float(os.getenv("REQUEST_INTERVAL_SEC", "1.2"))
MAX_PAGES_DC = int(os.getenv("MAX_PAGES_DC", "10"))
MAX_LINKS_ARCA = int(os.getenv("MAX_LINKS_ARCA", "120"))

DC_GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
DC_BASE_URL = "https://gall.dcinside.com"

ARCA_BOARD_URL = "https://arca.live/b/mobiledf"
ARCA_BASE_URL = "https://arca.live"

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

# ---------------- 수집 함수들은 그대로 유지 ----------------
# (중략 없이 그대로 유지됨)

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
            "text": "수집 실패",
            "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": ":warning: 데이터 없음"}}],
        }

    tokens = []
    for p in posts:
        tokens += tokenize_korean(p["title"])

    top_keywords = Counter(tokens).most_common(10)
    issue_scores = bucket_issues(tokens)

    # ✅ 기존 인기글
    top_posts = sorted(posts, key=lambda x: (x.get("views", 0), x.get("up", 0)), reverse=True)[:5]

    # ✅ 신규 추가
    top_views = sorted(posts, key=lambda x: x.get("views", 0), reverse=True)[:5]
    top_up = sorted(posts, key=lambda x: x.get("up", 0), reverse=True)[:5]

    def fmt(p, i):
        return (
            f"{i+1}. <{p['link']}|{p['title']}> · {p['source']} · "
            f"조회 {p.get('views',0)} · 추천 {p.get('up',0)} · "
            + (p['dt'].astimezone(KST).strftime('%m/%d %H:%M') if p.get('dt') else '')
        )

    post_lines = [fmt(p, i) for i, p in enumerate(top_posts)]
    view_lines = [fmt(p, i) for i, p in enumerate(top_views)]
    up_lines = [fmt(p, i) for i, p in enumerate(top_up)]

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title_text}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*인기 글 TOP 5*\n" + "\n".join(post_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*조회수 TOP 5*\n" + "\n".join(view_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*추천수 TOP 5*\n" + "\n".join(up_lines)}},
    ]

    return {"text": "던파M 요약", "blocks": blocks}

# ---------------- 슬랙 전송 ----------------

def post_to_slack(payload):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    r = requests.post(
        webhook,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )
    r.raise_for_status()

# ---------------- main ----------------

def main():
    logging.basicConfig(level=logging.INFO)
    posts = fetch_all_sources_last_24h()
    summary = build_summary(posts)
    post_to_slack(summary)

if __name__ == "__main__":
    main()

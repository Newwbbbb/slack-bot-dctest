# app.py — DCInside + Arca Live 통합 요약 봇 (FULL + TOP views/up)

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
    "Accept": "text/html",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

REQUEST_INTERVAL_SEC = float(os.getenv("REQUEST_INTERVAL_SEC", "1.2"))

DC_GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
DC_BASE_URL = "https://gall.dcinside.com"

ARCA_BOARD_URL = "https://arca.live/b/mobiledf"
ARCA_BASE_URL = "https://arca.live"

# ---------------- 유틸 ----------------

def clean_text(s):
    return html.unescape(re.sub(r"\s+", " ", s)).strip()

def parse_time_like_dc(raw):
    now = datetime.now(KST)
    if ":" in raw:
        h, m = map(int, raw.split(":"))
        return now.replace(hour=h, minute=m, second=0)
    return now

# ---------------- DC ----------------

def dc_fetch_last_24h():
    html_text = requests.get(DC_GALLERY_URL, headers=HEADERS).text
    soup = BeautifulSoup(html_text, "lxml")

    rows = []
    for tr in soup.select("table.gall_list tbody tr"):
        if "notice" in str(tr):
            continue
        cols = tr.find_all("td")
        if len(cols) < 7:
            continue

        title_el = cols[2].select_one("a")
        if not title_el:
            continue

        rows.append({
            "source": "DC",
            "title": clean_text(title_el.get_text()),
            "link": urljoin(DC_BASE_URL, title_el["href"]),
            "views": int(re.sub(r"\D", "", cols[5].get_text()) or 0),
            "up": int(re.sub(r"\D", "", cols[6].get_text()) or 0),
            "dt": parse_time_like_dc(cols[4].get_text())
        })

    return rows

# ---------------- Arca ----------------

def arca_fetch_last_24h():
    html_text = requests.get(ARCA_BOARD_URL, headers=HEADERS).text
    soup = BeautifulSoup(html_text, "lxml")

    items = []
    for a in soup.select('a[href^="/b/mobiledf/"]')[:50]:
        items.append({
            "source": "Arca",
            "title": clean_text(a.get_text()),
            "link": urljoin(ARCA_BASE_URL, a["href"]),
            "views": 0,
            "up": 0,
            "dt": datetime.now(KST)
        })
    return items

# ---------------- 통합 ----------------

def fetch_all_sources_last_24h():
    results = []
    try:
        results.extend(dc_fetch_last_24h())
    except Exception:
        logging.exception("DC 실패")

    try:
        results.extend(arca_fetch_last_24h())
    except Exception:
        logging.exception("Arca 실패")

    return results

# ---------------- 요약 ----------------

def build_summary(posts):

    def fmt(p, i):
        return f"{i+1}. <{p['link']}|{p['title']}> · {p['source']} · 조회 {p['views']} · 추천 {p['up']}"

    top_posts = sorted(posts, key=lambda x: (x["views"], x["up"]), reverse=True)[:5]
    top_views = sorted(posts, key=lambda x: x["views"], reverse=True)[:5]
    top_up = sorted(posts, key=lambda x: x["up"], reverse=True)[:5]

    return {
        "text": "던파M 요약",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": "*인기글 TOP5*\n" + "\n".join(fmt(p,i) for i,p in enumerate(top_posts))}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "*조회수 TOP5*\n" + "\n".join(fmt(p,i) for i,p in enumerate(top_views))}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "*추천수 TOP5*\n" + "\n".join(fmt(p,i) for i,p in enumerate(top_up))}},
        ]
    }

# ---------------- Slack ----------------

def post_to_slack(payload):
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        raise ValueError("SLACK_WEBHOOK_URL 없음")

    r = requests.post(webhook, json=payload)
    r.raise_for_status()

# ---------------- main ----------------

def main():
    logging.basicConfig(level=logging.INFO)

    posts = fetch_all_sources_last_24h()
    logging.info(f"{len(posts)} posts 수집")

    summary = build_summary(posts)
    post_to_slack(summary)

if __name__ == "__main__":
    main()

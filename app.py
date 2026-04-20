# app.py — FULL (크롤링 + 분석 + TOP 랭킹)

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

# ---------------- 공통 ----------------
KST = timezone(timedelta(hours=9))

DC_GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
DC_BASE_URL = "https://gall.dcinside.com"

ARCA_BOARD_URL = "https://arca.live/b/mobiledf"
ARCA_BASE_URL = "https://arca.live"

HANGUL_TOKEN = re.compile(r"[가-힣]{2,}")
STOPWORDS = set("그리고 그러나 그래서 또는 이런 저런 같은 해당 우리 당신 여러분 그냥 매우 너무 좀 진짜 거의 또한 또 더".split())

ISSUE_BUCKETS = {
    "장비·강화": ["강화", "연마", "장비"],
    "직업·밸런스": ["소울", "검마", "각성"],
    "콘텐츠": ["레이드", "던전"],
    "경제": ["골드", "과금"],
    "버그": ["버그", "렉"],
}

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

# ---------------- DC ----------------

def dc_fetch():
    html_text = requests.get(DC_GALLERY_URL).text
    soup = BeautifulSoup(html_text, "lxml")

    posts = []
    for tr in soup.select("table.gall_list tbody tr"):
        if "notice" in str(tr):
            continue

        cols = tr.find_all("td")
        if len(cols) < 7:
            continue

        title_el = cols[2].select_one("a")
        if not title_el:
            continue

        posts.append({
            "source": "DC",
            "category": clean_text(cols[1].get_text()),
            "title": clean_text(title_el.get_text()),
            "link": urljoin(DC_BASE_URL, title_el["href"]),
            "views": int(re.sub(r"\D", "", cols[5].get_text()) or 0),
            "up": int(re.sub(r"\D", "", cols[6].get_text()) or 0),
        })
    return posts

# ---------------- Arca ----------------

def arca_fetch():
    html_text = requests.get(ARCA_BOARD_URL).text
    soup = BeautifulSoup(html_text, "lxml")

    posts = []
    for a in soup.select('a[href^="/b/mobiledf/"]')[:50]:
        posts.append({
            "source": "Arca",
            "category": "아카",
            "title": clean_text(a.get_text()),
            "link": urljoin(ARCA_BASE_URL, a["href"]),
            "views": 0,
            "up": 0,
        })
    return posts

# ---------------- 통합 ----------------

def fetch_all():
    posts = []
    try:
        posts += dc_fetch()
    except:
        logging.exception("DC 실패")

    try:
        posts += arca_fetch()
    except:
        logging.exception("Arca 실패")

    return posts

# ---------------- 분석 + 요약 ----------------

def build_summary(posts):

    # 분포
    by_cat = Counter(p["category"] for p in posts)
    by_src = Counter(p["source"] for p in posts)

    # 키워드
    tokens = []
    for p in posts:
        tokens += tokenize(p["title"])

    top_keywords = Counter(tokens).most_common(10)
    issues = bucket_issues(tokens)

    # TOP
    top_posts = sorted(posts, key=lambda x: (x["views"], x["up"]), reverse=True)[:5]
    top_views = sorted(posts, key=lambda x: x["views"], reverse=True)[:5]
    top_up = sorted(posts, key=lambda x: x["up"], reverse=True)[:5]

    def fmt(p, i):
        return f"{i+1}. <{p['link']}|{p['title']}> · {p['source']} · 조회 {p['views']} · 추천 {p['up']}"

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": "*던파M 커뮤니티 요약*"}},
        {"type": "divider"},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*말머리 분포*\n" + "\n".join(f"- {k}: {v}" for k,v in by_cat.most_common())
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*출처 분포*\n" + "\n".join(f"- {k}: {v}" for k,v in by_src.most_common())
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*핵심 키워드*\n" + ", ".join(f"{k}({v})" for k,v in top_keywords)
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*이슈 레이더*\n" + "\n".join(f"- {k}: {v}" for k,v in issues.items())
        }},

        {"type": "divider"},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*인기글 TOP5*\n" + "\n".join(fmt(p,i) for i,p in enumerate(top_posts))
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*조회수 TOP5*\n" + "\n".join(fmt(p,i) for i,p in enumerate(top_views))
        }},

        {"type": "section", "text": {"type": "mrkdwn", "text":
            "*추천수 TOP5*\n" + "\n".join(fmt(p,i) for i,p in enumerate(top_up))
        }},
    ]

    return {"text": "요약", "blocks": blocks}

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

    posts = fetch_all()
    logging.info(f"{len(posts)} posts 수집")

    summary = build_summary(posts)
    post_to_slack(summary)

if __name__ == "__main__":
    main()

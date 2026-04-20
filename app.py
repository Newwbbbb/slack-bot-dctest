# app.py — DCInside ONLY 안정 버전 (분석 + TOP 포함)

import os
import re
import html
import json
import logging
from collections import Counter
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------- 설정 ----------------
DC_GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
DC_BASE_URL = "https://gall.dcinside.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

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

# ---------------- DC 수집 ----------------

def dc_fetch():
    try:
        r = requests.get(DC_GALLERY_URL, headers=HEADERS, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logging.exception("DC 요청 실패")
        return []

    soup = BeautifulSoup(r.text, "lxml")

    rows = soup.select("table.gall_list tbody tr")
    logging.info(f"rows: {len(rows)}")

    posts = []

    for tr in rows:
        if "notice" in str(tr):
            continue

        cols = tr.find_all("td")
        if len(cols) < 7:
            continue

        title_el = cols[2].select_one("a")
        if not title_el:
            continue

        try:
            posts.append({
                "source": "DC",
                "category": clean_text(cols[1].get_text()),
                "title": clean_text(title_el.get_text()),
                "link": urljoin(DC_BASE_URL, title_el["href"]),
                "views": int(re.sub(r"\D", "", cols[5].get_text()) or 0),
                "up": int(re.sub(r"\D", "", cols[6].get_text()) or 0),
            })
        except Exception:
            continue

    logging.info(f"posts: {len(posts)}")
    return posts

# ---------------- 요약 ----------------

def build_summary(posts):

    if not posts:
        return {
            "text": "데이터 없음",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": ":warning: DC 데이터 수집 실패"}}
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

    return {"text": "DC 요약", "blocks": blocks}

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

    posts = dc_fetch()
    logging.info(f"수집된 글 수: {len(posts)}")

    summary = build_summary(posts)
    post_to_slack(summary)

if __name__ == "__main__":
    main()

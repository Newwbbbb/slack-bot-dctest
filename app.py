# app.py
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

# ---------------- Settings ----------------
GALLERY_URL = "https://gall.dcinside.com/mgallery/board/lists/?id=dnfm"
BASE_URL = "https://gall.dcinside.com"
USER_AGENT = (
    "Mozilla/5.0 (compatible; dnfm-slack-bot/1.0; +https://example.com/bot)"
)
HEADERS = {"User-Agent": USER_AGENT}
KST = timezone(timedelta(hours=9))

REQUEST_INTERVAL_SEC = 1.2  # be nice
MAX_PAGES = 10

HANGUL_TOKEN = re.compile(r"[가-힣]{2,}")
STOPWORDS = set("""
그리고 그러나 그래서 또는 이런 저런 같은 해당 우리 당신 여러분 그냥 매우 너무 좀 진짜 거의 또한 또 더 좀더
오늘 어제 내일 이번 지난 다음 지금 지금은 조금 많이 대한 관련 관련해 관련한 대해서 등 등등
""".split())

ISSUE_BUCKETS = {
    "장비·강화": ["강화", "16강", "연마", "장비변환", "돌파", "보장", "악세", "방어구"],
    "직업·밸런스": ["소울", "스핏", "귀참", "검마", "시너지", "무적", "각성", "룬", "액리폼"],
    "콘텐츠·레이드": ["재해", "흑룡", "안톤", "레이드", "일던", "정예", "결투"],
    "경제·과금": ["현질", "세라", "콘텐츠페이", "골드", "과금"],
    "이벤트·콜라보": ["콜라보", "소아온", "이벤트", "패스"],
    "품질·버그": ["팅김", "멈춤", "버그", "렉", "크래시", "오류"],
    "커뮤니티·정책": ["공지", "파업", "규칙", "운영", "네오플"],
}

WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]

# ---------------- Utils ----------------

def parse_datetime_kst(raw: str) -> datetime:
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


def fetch_page(page: int):
    url = f"{GALLERY_URL}&page={page}" if page > 1 else GALLERY_URL
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    time.sleep(REQUEST_INTERVAL_SEC)
    return r.text


def parse_list(html_text: str):
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
            link = urljoin(BASE_URL, href) if href else None
            author = clean_text(cols[3].get_text())
            dt_raw = clean_text(cols[4].get_text())
            dt = parse_datetime_kst(dt_raw)
            views_text = cols[5].get_text()
            up_text = cols[6].get_text()
            views = int(re.sub(r"[^\d]", "", views_text) or "0")
            up = int(re.sub(r"[^\d]", "", up_text) or "0")
        except Exception:
            continue
        rows.append({
            "no": no, "category": category, "title": title, "link": link,
            "author": author, "dt": dt, "views": views, "up": up
        })
    return rows


def fetch_last_24h():
    cutoff = datetime.now(KST) - timedelta(hours=24)
    all_posts = []
    for page in range(1, MAX_PAGES + 1):
        try:
            html_text = fetch_page(page)
        except Exception as e:
            logging.warning(f"페이지 {page} 요청 실패: {e}")
            break
        posts = parse_list(html_text)
        if not posts:
            break
        any_new = False
        min_dt = None
        for p in posts:
            if min_dt is None or p["dt"] < min_dt:
                min_dt = p["dt"]
            if p["dt"] >= cutoff:
                all_posts.append(p)
                any_new = True
        if not any_new and min_dt and min_dt < cutoff:
            break
    # dedupe by no
    uniq = {}
    for p in all_posts:
        uniq[p["no"]] = p
    return list(uniq.values())


def build_summary(posts):
    now = datetime.now(KST)
    weekday = WEEKDAY_KR[now.weekday()]
    title_text = f"*던파M 갤 동향 요약* — {now.strftime('%Y-%m-%d')} ({weekday}) 09:00 KST 기준"

    if not posts:
        payload = {
            "text": f"수집된 글이 없거나 페이지 구조가 변경되었습니다. ({now.strftime('%Y-%m-%d %H:%M KST')})",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": title_text}},
                {"type": "context", "elements": [{"type": "mrkdwn", "text": f"<{GALLERY_URL}|갤러리 목록> · 지난 24시간"}]},
                {"type": "section", "text": {"type": "mrkdwn", "text": ":warning: 수집된 글이 없거나 파서가 실패했습니다."}},
            ],
        }
        return payload

    by_cat = Counter(p.get("category") or "기타" for p in posts)
    total = len(posts)

    tokens = []
    for p in posts:
        tokens += tokenize_korean(p["title"])
    top_keywords = Counter(tokens).most_common(10)

    issue_scores = bucket_issues(tokens)
    top_issues = list(issue_scores.items())[:5]

    top_posts = sorted(posts, key=lambda x: (x["views"], x["up"]), reverse=True)[:5]

    def mrkdwn_list(items):
        return "\n".join(items)

    cat_lines = [
        f"- *{cat}*: {cnt}개 ({cnt/total*100:.1f}%)" for cat, cnt in by_cat.most_common()
    ]
    kw_lines = [f"*{k}* ({c})" for k, c in top_keywords]
    iss_lines = [f"- *{k}*: {v}" for k, v in top_issues]
    post_lines = [
        f"{i+1}. <{p['link']}|{p['title']}>  · 조회 {p['views']} · 추천 {p['up']} · {p['dt'].astimezone(KST).strftime('%m/%d %H:%M')}"
        for i, p in enumerate(top_posts)
    ]

    text_fallback = (
        f"총 {total}건 | 말머리 TOP: "
        + ", ".join([f"{k}:{v}" for k, v in by_cat.most_common(3)])
        + " | 키워드: "
        + ", ".join([k for k, _ in top_keywords[:5]])
    )

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title_text}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"<{GALLERY_URL}|갤러리 목록> · 수집범위: 지난 24시간"}]},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*말머리 분포* \n" + mrkdwn_list(cat_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*핵심 키워드 TOP 10*\n" + ", ".join(kw_lines)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*이슈 레이더*\n" + (mrkdwn_list(iss_lines) if iss_lines else "- 감지된 주요 이슈 없음")}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "*인기 글 TOP 5*\n" + mrkdwn_list(post_lines)}},
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
        posts = fetch_last_24h()
    except Exception as e:
        logging.exception("수집 중 오류")
        posts = []
    summary = build_summary(posts)
    mode = post_to_slack(summary)
    logging.info(f"Slack 전송 완료 ({mode}) — {len(posts)} posts summarized.")


if __name__ == "__main__":
    main()

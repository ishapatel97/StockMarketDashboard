import os
import time
import math
import datetime as dt
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

from database import SessionLocal
from sqlalchemy import text

try:
    from groq import Groq
except Exception:
    Groq = None


def _iso(dtobj: dt.datetime) -> str:
    try:
        return dtobj.isoformat()
    except Exception:
        return str(dtobj)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_symbol_metrics_from_db(symbol: str) -> Optional[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT date, volume FROM stock_prices
            WHERE symbol = :symbol AND volume IS NOT NULL
            ORDER BY date ASC LIMIT 60
        """), {"symbol": symbol}).fetchall()
    finally:
        db.close()

    if not rows or len(rows) < 21:
        return None

    dates  = [r[0] for r in rows]
    vols   = [int(r[1]) for r in rows]
    last20 = vols[-21:-1]
    if len(last20) < 20:
        return None

    avg20     = sum(last20) / 20.0
    today_vol = vols[-1]
    if avg20 <= 0:
        return None
    surge = ((today_vol - avg20) / avg20) * 100.0
    return {
        "today_volume": int(today_vol),
        "avg_20d":      int(round(avg20)),
        "volume_surge": round(surge, 2),
        "as_of":        _iso(dates[-1]),
    }


def get_company_name_from_db(symbol: str) -> Optional[str]:
    db = SessionLocal()
    try:
        row = db.execute(text("""
            SELECT company FROM stock_prices
            WHERE symbol = :symbol AND company IS NOT NULL AND company != ''
            LIMIT 1
        """), {"symbol": symbol}).fetchone()
        return row[0] if row else None
    finally:
        db.close()


# ── News helpers ──────────────────────────────────────────────────────────────

def fetch_news_via_newsapi(symbol: str, api_key: str, page_size: int = 5) -> List[Dict[str, Any]]:
    company = get_company_name_from_db(symbol) or symbol
    url = "https://newsapi.org/v2/everything"
    q = f"{company} stock OR {symbol} stock OR {company} shares OR {company} trading"
    params = {
        "q": q, "sortBy": "publishedAt",
        "language": "en", "pageSize": page_size, "apiKey": api_key,
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        arts = r.json().get("articles", []) or []
        return [{
            "title": a.get("title"), "url": a.get("url"),
            "source": (a.get("source") or {}).get("name"),
            "publishedAt": a.get("publishedAt"), "summary": a.get("description"),
        } for a in arts]
    except Exception:
        return []


def fetch_news_via_google_rss(symbol: str, page_size: int = 5) -> List[Dict[str, Any]]:
    company = get_company_name_from_db(symbol) or symbol
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    # Try multiple query variations for better results
    queries = [
        f"{symbol} stock",
        f"{company} stock",
        f"{symbol} shares trading",
    ]
    for q_raw in queries:
        q = requests.utils.quote(q_raw)
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
            soup  = BeautifulSoup(r.text, "xml")
            items = soup.find_all("item")[:page_size]
            if not items:
                continue
            results = []
            for it in items:
                source_tag = it.find("source")
                results.append({
                    "title": it.title.text if it.title else None,
                    "url":   it.link.text  if it.link  else None,
                    "source": source_tag.text if source_tag else None,
                    "publishedAt": it.pubDate.text if it.pubDate else None,
                })
            if results:
                return results
        except Exception:
            continue
    return []


def fetch_news_via_bing_rss(symbol: str, page_size: int = 5) -> List[Dict[str, Any]]:
    """Bing News RSS — additional fallback when Google RSS fails."""
    company = get_company_name_from_db(symbol) or symbol
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    q = requests.utils.quote(f"{symbol} stock {company}")
    url = f"https://www.bing.com/news/search?q={q}&format=rss"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup  = BeautifulSoup(r.text, "xml")
        items = soup.find_all("item")[:page_size]
        results = []
        for it in items:
            results.append({
                "title": it.title.text if it.title else None,
                "url":   it.link.text  if it.link  else None,
                "source": "Bing News",
                "publishedAt": it.pubDate.text if it.pubDate else None,
            })
        return results
    except Exception:
        return []


# ── Prompt builders ───────────────────────────────────────────────────────────

def build_ai_prompt(symbol: str, metrics: Dict[str, Any], articles: List[Dict[str, Any]]) -> str:
    lines = [
        "You are an equity research assistant. Be concise and factual.\n",
        "Task: Summarize reasons for the recent trading volume surge for the stock below.\n",
        "Requirements:\n- Use only the provided articles.\n- Avoid speculation.\n- Cite sources using bracket indices [1], [2], etc.\n- Provide 2-4 bullets.\n",
        f"Stock: {symbol}\n",
        f"Metrics (as of {metrics.get('as_of')}): today_volume={metrics.get('today_volume'):,}, avg_20d={metrics.get('avg_20d'):,}, volume_surge={metrics.get('volume_surge')}%\n\n",
    ]
    if not articles:
        lines.append("No reliable recent articles found. If no clear reasons are available, say so briefly.\n")
        return "".join(lines)
    lines.append("Articles:\n")
    for idx, a in enumerate(articles, start=1):
        lines.append(f"[{idx}] {a.get('title') or '(no title)'} — {a.get('source') or 'Unknown'} — {a.get('publishedAt') or ''}\n{a.get('url') or ''}\n")
    lines.append("\nNow produce a concise explanation with 2-4 bullets, each ending with the citation index, e.g., [1].\n")
    return "".join(lines)


def build_brief_prompt(symbol: str, company: str, price: float, price_change: float,
                       volume_surge: float, market_cap_billion: float) -> str:
    """Short 1-2 sentence insight + 7-day prediction for the card."""
    direction = "up" if price_change > 0 else "down"
    return (
        f"You are a financial analyst. Give a VERY brief 1-2 sentence insight for this stock.\n"
        f"Include: why volume may be surging and a short 7-day price prediction.\n"
        f"Be direct. No bullet points. No preamble. Just 1-2 sentences max.\n\n"
        f"Stock: {symbol} ({company})\n"
        f"Price: ${price} ({direction} {abs(price_change)}% today)\n"
        f"Volume surge: {volume_surge}% above 20-day average\n"
        f"Market cap: ${market_cap_billion}B\n\n"
        f"Response (1-2 sentences only):"
    )


# ── Groq caller ───────────────────────────────────────────────────────────────

# Track daily rate limit to avoid spamming Groq after exhaustion
_GROQ_DAILY_EXHAUSTED = {"exhausted": False, "reset_after": None}

def _call_groq(prompt: str, max_tokens: int = 120) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return ""
    if Groq is None:
        return ""

    # If daily limit was hit, skip until reset time
    if _GROQ_DAILY_EXHAUSTED["exhausted"]:
        reset = _GROQ_DAILY_EXHAUSTED.get("reset_after")
        if reset and time.time() < reset:
            return ""
        # Reset period passed, try again
        _GROQ_DAILY_EXHAUSTED["exhausted"] = False
        _GROQ_DAILY_EXHAUSTED["reset_after"] = None

    for attempt in range(3):
        try:
            client = Groq(api_key=api_key)
            resp = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": "You are a precise financial analyst. Be extremely concise."},
                    {"role": "user",   "content": prompt},
                ],
                temperature=0.3,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content or ""
        except Exception as e:
            msg = str(e).lower()
            # Daily token limit (TPD) exhausted — stop all calls for 10 min
            if "429" in str(e) and ("tokens per day" in msg or "tpd" in msg):
                print(f"Groq daily token limit reached. Pausing AI calls for 10 min.")
                _GROQ_DAILY_EXHAUSTED["exhausted"] = True
                _GROQ_DAILY_EXHAUSTED["reset_after"] = time.time() + 600  # 10 min
                return ""
            # Per-minute rate limit — short retry
            if "429" in str(e) and attempt < 2:
                wait = (attempt + 1) * 3
                time.sleep(wait)
                continue
            print(f"Groq error: {e}")
            return ""
    return ""


def generate_reason_with_groq(prompt: str) -> str:
    return _call_groq(prompt, max_tokens=300)


# ── Public functions ──────────────────────────────────────────────────────────

def get_brief_insight(symbol: str, price: float, price_change: float,
                      volume_surge: float, market_cap_billion: float) -> str:
    """Called during refresh_calculated_stocks to pre-generate card insight."""
    company = get_company_name_from_db(symbol) or symbol
    prompt  = build_brief_prompt(symbol, company, price, price_change, volume_surge, market_cap_billion)
    return _call_groq(prompt, max_tokens=120)


def get_ai_reason(symbol: str, threshold: float = 1.5, page_size: int = 5) -> Dict[str, Any]:
    """Full AI summary with news sources — used in the modal."""
    metrics = get_symbol_metrics_from_db(symbol)
    if not metrics:
        return {"symbol": symbol, "reason": "Not enough DB data to compute 20-day average.", "sources": [], "surge": None}

    if metrics.get("volume_surge", 0) < threshold:
        return {"symbol": symbol, "reason": f"Volume surge ({metrics['volume_surge']}%) is below threshold ({threshold}%).", "sources": [], "surge": metrics}

    newsapi_key = os.getenv("NEWSAPI_KEY")
    articles = []

    # Try NewsAPI first (works on localhost, blocked on free tier for deployed servers)
    if newsapi_key:
        articles = fetch_news_via_newsapi(symbol, newsapi_key, page_size=page_size)

    # Fallback 1: Google News RSS
    if not articles:
        articles = fetch_news_via_google_rss(symbol, page_size=page_size)

    # Fallback 2: Bing News RSS
    if not articles:
        articles = fetch_news_via_bing_rss(symbol, page_size=page_size)

    prompt = build_ai_prompt(symbol, metrics, articles)
    reason = generate_reason_with_groq(prompt)
    return {"symbol": symbol, "surge": metrics, "reason": reason, "sources": articles}
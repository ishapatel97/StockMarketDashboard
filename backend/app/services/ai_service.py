import os
import math
import datetime as dt
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

from database import SessionLocal
from sqlalchemy import text

# If you install groq python client later, you can import it; for now guard import
try:
    from groq import Groq
except Exception:  # pragma: no cover
    Groq = None  # type: ignore


def _iso(dtobj: dt.datetime) -> str:
    try:
        return dtobj.isoformat()
    except Exception:
        return str(dtobj)


def get_symbol_metrics_from_db(symbol: str) -> Optional[Dict[str, Any]]:
    """Return metrics computed from DB only: today_volume, avg_20d, volume_surge(%)."""
    db = SessionLocal()
    try:
        rows = db.execute(text(
            """
            SELECT date, volume
            FROM stock_prices
            WHERE symbol = :symbol AND volume IS NOT NULL
            ORDER BY date ASC
            LIMIT 60
            """
        ), {"symbol": symbol}).fetchall()
    finally:
        db.close()

    if not rows or len(rows) < 21:
        return None

    # Keep last 21 to compute 20d avg and get today's volume
    dates = [r[0] for r in rows]
    vols = [int(r[1]) for r in rows]
    if len(vols) < 21:
        return None
    last20 = vols[-21:-1] if len(vols) >= 21 else vols[-20:]
    if len(last20) < 20:
        return None

    avg20 = sum(last20) / 20.0
    today_vol = vols[-1]
    if avg20 <= 0:
        return None
    surge = ((today_vol - avg20) / avg20) * 100.0
    return {
        "today_volume": int(today_vol),
        "avg_20d": int(round(avg20)),
        "volume_surge": round(surge, 2),
        "as_of": _iso(dates[-1])
    }

def get_company_name_from_db(symbol: str) -> Optional[str]:
    """Fetch company name from DB for the given symbol."""
    db = SessionLocal()
    try:
        row = db.execute(text(
            """
            SELECT company 
            FROM stock_prices 
            WHERE symbol = :symbol 
            AND company IS NOT NULL 
            LIMIT 1
            """
        ), {"symbol": symbol}).fetchone()
        return row[0] if row else None
    finally:
        db.close()

def fetch_news_via_newsapi(symbol: str, api_key: str, page_size: int = 5) -> List[Dict[str, Any]]:
    """Fetch recent articles from NewsAPI for the given symbol. Requires NEWSAPI_KEY."""
    company = get_company_name_from_db(symbol) or symbol
    url = "https://newsapi.org/v2/everything"
    q = f"{company} stock OR {symbol} company OR {company} shares OR {company} stock news OR {company} stock price OR {company} stock market OR {company} trading OR {symbol} stock"
    params = {
        "q": q,
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": page_size,
        "apiKey": api_key,
    }   
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        arts = data.get("articles", []) or []
        results = []
        for a in arts:
            results.append({
                "title": a.get("title"),
                "url": a.get("url"),
                "source": (a.get("source") or {}).get("name"),
                "publishedAt": a.get("publishedAt"),
                "summary": a.get("description"),
            })
        return results
    except Exception:
        return []


def fetch_news_via_google_rss(symbol: str, page_size: int = 5) -> List[Dict[str, Any]]:
    """Fallback: scrape Google News search RSS for the symbol."""
    company = get_company_name_from_db(symbol) or symbol
    q = requests.utils.quote(f"{company} stock OR {symbol} company OR {company} shares OR {company} stock news OR {company} stock price OR {company} stock market OR {company} trading OR {symbol} stock")

    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "xml")
        items = soup.find_all("item")[:page_size]
        results = []
        for it in items:
            title = it.title.text if it.title else None
            link = it.link.text if it.link else None
            pub = it.pubDate.text if it.pubDate else None
            source_tag = it.find("source")
            source = source_tag.text if source_tag else None
            results.append({
                "title": title,
                "url": link,
                "source": source,
                "publishedAt": pub,
            })
        return results
    except Exception:
        return []


def build_ai_prompt(symbol: str, metrics: Dict[str, Any], articles: List[Dict[str, Any]]) -> str:
    lines = []
    lines.append(f"You are an equity research assistant. Be concise and factual.\n")
    lines.append("Task: Summarize reasons for the recent trading volume surge for the stock below.\n")
    lines.append("Requirements:\n- Use only the provided articles.\n- Avoid speculation.\n- Cite sources using bracket indices [1], [2], etc.\n- Provide 2-4 bullets.\n")

    surge = metrics.get("volume_surge")
    tv = metrics.get("today_volume")
    av = metrics.get("avg_20d")
    as_of = metrics.get("as_of")
    lines.append(f"Stock: {symbol}\n")
    lines.append(f"Metrics (as of {as_of}): today_volume={tv:,}, avg_20d={av:,}, volume_surge={surge}%\n\n")

    if not articles:
        lines.append("No reliable recent articles found. If no clear reasons are available, say so briefly.\n")
        return "".join(lines)

    lines.append("Articles:\n")
    for idx, a in enumerate(articles, start=1):
        title = a.get("title") or "(no title)"
        url = a.get("url") or ""
        src = a.get("source") or "Unknown"
        pub = a.get("publishedAt") or ""
        lines.append(f"[{idx}] {title} — {src} — {pub}\n{url}\n")

    lines.append("\nNow produce a concise explanation with 2-4 bullets, each bullet ending with the appropriate citation index, e.g., [1].\n")
    return "".join(lines)


def generate_reason_with_groq(prompt: str) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "GROQ_API_KEY not set on server."
    if Groq is None:
        return "Groq client is not installed. Please install 'groq' package."
    try:
        client = Groq(api_key=api_key)
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a precise financial assistant. Be concise and cite sources by index."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=300,
        )
        content = resp.choices[0].message.content if resp and resp.choices else None
        return content or "No response from Groq."
    except Exception as e:
        return f"Groq error: {e}"


def get_ai_reason(symbol: str, threshold: float = 1.5, page_size: int = 5) -> Dict[str, Any]:
    metrics = get_symbol_metrics_from_db(symbol)
    if not metrics:
        return {"symbol": symbol, "reason": "Not enough DB data to compute 20-day average.", "sources": [], "surge": None}

    # Screen by threshold: if surge less than threshold, inform user
    if metrics.get("volume_surge", 0) < threshold:
        return {"symbol": symbol, "reason": f"Volume surge ({metrics['volume_surge']}%) is below threshold ({threshold}%).", "sources": [], "surge": metrics}

    # News via NewsAPI if present; else fallback to Google RSS
    newsapi_key = os.getenv("NEWSAPI_KEY")
    articles = []
    if newsapi_key:
        articles = fetch_news_via_newsapi(symbol, newsapi_key, page_size=page_size)
    if not articles:
        articles = fetch_news_via_google_rss(symbol, page_size=page_size)

    prompt = build_ai_prompt(symbol, metrics, articles)
    reason = generate_reason_with_groq(prompt)
    return {
        "symbol": symbol,
        "surge": metrics,
        "reason": reason,
        "sources": articles,
    }

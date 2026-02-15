from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from datetime import datetime
import html as html_lib
import json
from pathlib import Path
import re
import threading
import urllib3
import requests
from typing import Any

from app.capitals import CAPITAL_BY_CODE
from app.config import settings
from app.schemas import (
    AccountApplyRequest,
    AccountApplyResult,
    BankOptionItem,
    BulkApplyResponse,
    CapitalOptionItem,
    IPOItem,
    ResultCheckAccountRequest,
    ResultCheckResponse,
    ResultIpoItem,
    ResultStatus,
    NepseChartPoint,
    NepseChartResponse,
    NepseNewsItem,
    IpoNewsArticleItem,
    IpoAnalysisRequest,
    MarketChatContext,
    MarketChatRequest,
    MarketChatResponse,
    MarketIndexItem,
    MarketMoverItem,
    MarketOverviewResponse,
    MarketSummaryResponse,
    StockAnalysisRequest,
    StockCompanyItem,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://webbackend.cdsc.com.np/api/meroShare"
REQUEST_ERROR_CODE = "NETWORK_ERROR"


class MeroShareError(Exception):
    pass


@dataclass
class SessionContext:
    dp_id: str
    username: str
    password: str

    @property
    def demat(self) -> str:
        return f"130{self.dp_id}{self.username}"


class MeroShareService:
    _MEROLAGANI_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; BulkIpoApply/1.0)"}
    _MEROLAGANI_NEWS_SOURCES = (
        "https://www.merolagani.com/NewsList.aspx?news=nepse",
        "https://www.merolagani.com/NewsList.aspx",
        "https://www.merolagani.com/NewsList.aspx?id=13&type=latest",
        "https://www.merolagani.com/NewsList.aspx?id=25&type=latest",
        "https://www.merolagani.com/NewsList.aspx?id=23&type=latestY",
        "https://www.merolagani.com/NewsList.aspx?id=15&type=latest",
        "https://www.merolagani.com/NewsList.aspx?id=10&type=latest",
        "https://www.merolagani.com/NewsList.aspx?id=17&type=latest",
        "https://www.merolagani.com/NewsList.aspx?id=12&type=latest",
    )
    _SHARESANSAR_IPO_URL = "https://www.sharesansar.com/category/ipo-fpo-news"
    _MARKET_HUB_HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; BulkIpoApply/1.0; +https://github.com)",
    }
    _chat_lock = threading.Lock()
    _chat_buckets: dict[str, tuple[datetime, int]] = {}

    def _as_float(self, value: object) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _extract_nepse_points(self, payload: object, *, limit: int) -> list[NepseChartPoint]:
        # The upstream structure can vary by endpoint; normalize into [date, value].
        if isinstance(payload, dict):
            items = payload.get("content")
            if not isinstance(items, list):
                items = payload.get("data")
        elif isinstance(payload, list):
            items = payload
        else:
            items = []
        points: list[NepseChartPoint] = []
        for raw in items:
            if not isinstance(raw, dict):
                continue
            value = None
            for key in (
                "index",
                "indexValue",
                "nepseIndex",
                "close",
                "closePrice",
                "currentValue",
            ):
                value = self._as_float(raw.get(key))
                if value is not None:
                    break
            if value is None:
                continue
            date_value = (
                raw.get("businessDate")
                or raw.get("asOf")
                or raw.get("date")
                or raw.get("timestamp")
                or ""
            )
            points.append(NepseChartPoint(date=str(date_value), value=value))
        if len(points) > limit:
            points = points[-limit:]
        return points

    def _extract_merolagani_news_blocks(self, html: str) -> list[NepseNewsItem]:
        block_pattern = re.compile(
            r'<div[^>]*class="media-news[^"]*"[\s\S]*?</div>\s*</div>',
            re.IGNORECASE,
        )
        # Use title anchor in h4 so we don't parse image alt text by mistake.
        link_pattern = re.compile(
            r'<h4[^>]*class="media-title"[^>]*>[\s\S]*?<a[^>]+href="(?P<href>/NewsDetail\.aspx\?newsID=\d+)"[^>]*>(?P<title>[\s\S]*?)</a>',
            re.IGNORECASE,
        )
        date_pattern = re.compile(
            r'<span[^>]*class="media-label"[^>]*>(?P<date>[\s\S]*?)</span>',
            re.IGNORECASE,
        )
        tag_pattern = re.compile(r"<[^>]+>")

        items: list[NepseNewsItem] = []
        for block in block_pattern.findall(html):
            link_match = link_pattern.search(block)
            if not link_match:
                continue
            href = link_match.group("href").strip()
            title = html_lib.unescape(tag_pattern.sub("", link_match.group("title"))).strip()
            if not title:
                continue

            date_match = date_pattern.search(block)
            published_at = None
            if date_match:
                published_at = html_lib.unescape(
                    tag_pattern.sub("", date_match.group("date"))
                ).strip() or None

            items.append(
                NepseNewsItem(
                    title=title,
                    url=f"https://www.merolagani.com{href}",
                    published_at=published_at,
                    source="merolagani",
                )
            )
        return items

    def _is_nepse_related_title(self, title: str) -> bool:
        upper = title.upper()
        keywords = (
            "NEPSE",
            "नेप्से",
            "SHARE",
            "शेयर",
            "सेयर",
            "SECONDARY MARKET",
            "IPO",
            "आईपिओ",
        )
        return any(token in title or token in upper for token in keywords)

    def _news_id_from_url(self, url: str) -> int:
        match = re.search(r"newsID=(\d+)", url, re.IGNORECASE)
        if not match:
            return 0
        try:
            return int(match.group(1))
        except ValueError:
            return 0

    def _extract_nepse_points_from_merolagani(self, html: str, *, limit: int) -> list[NepseChartPoint]:
        # Merolagani "Indices.aspx" renders tabular NEPSE history in static HTML.
        # Row cells are: SN, Date(AD), Index Value, +/- change, % change.
        row_pattern = re.compile(r"<tr>\s*(.*?)\s*</tr>", re.IGNORECASE | re.DOTALL)
        cell_pattern = re.compile(r"<td[^>]*>\s*(.*?)\s*</td>", re.IGNORECASE | re.DOTALL)
        tag_pattern = re.compile(r"<[^>]+>")

        points: list[NepseChartPoint] = []
        for row_match in row_pattern.finditer(html):
            row_html = row_match.group(1)
            cells = [tag_pattern.sub("", c).strip() for c in cell_pattern.findall(row_html)]
            if len(cells) < 3:
                continue
            raw_date = cells[1].replace("/", "-")
            value = self._as_float(cells[2])
            if not raw_date or value is None:
                continue
            points.append(NepseChartPoint(date=raw_date, value=value))

        # Page is newest-first; chart should render oldest-first.
        points.reverse()
        if len(points) > limit:
            points = points[-limit:]
        return points

    def _parse_iso_dt(self, raw: str) -> datetime | None:
        candidate = raw.strip().replace("/", "-")
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            return None

    def _pick_first_list(self, payload: object, *, max_depth: int = 3) -> list[object]:
        if max_depth < 0:
            return []
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []

        for key in (
            "content",
            "data",
            "graph",
            "items",
            "points",
            "result",
            "response",
            "payload",
            "records",
            "list",
        ):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return candidate
            if isinstance(candidate, dict):
                nested = self._pick_first_list(candidate, max_depth=max_depth - 1)
                if nested:
                    return nested

        for value in payload.values():
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                nested = self._pick_first_list(value, max_depth=max_depth - 1)
                if nested:
                    return nested
        return []

    def _extract_timewise_points(self, payload: object, *, limit: int) -> list[NepseChartPoint]:
        def to_label(value: object) -> str:
            if value is None:
                return ""
            if isinstance(value, (int, float)):
                # Some APIs return unix epoch (ms or s).
                epoch = float(value)
                if epoch > 10_000_000_000:
                    epoch /= 1000.0
                try:
                    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    return str(value)
            return str(value).strip()

        items = self._pick_first_list(payload)

        points: list[NepseChartPoint] = []
        for raw in items:
            if isinstance(raw, (list, tuple)) and len(raw) >= 2:
                label = to_label(raw[0])
                value = self._as_float(raw[1])
            elif isinstance(raw, dict):
                label = to_label(
                    raw.get("time")
                    or raw.get("dateTime")
                    or raw.get("datetime")
                    or raw.get("businessDate")
                    or raw.get("date")
                    or raw.get("timestamp")
                    or raw.get("asOf")
                    or raw.get("xAxis")
                    or raw.get("label")
                    or raw.get("x")
                )
                value = self._as_float(
                    raw.get("index")
                    or raw.get("indexValue")
                    or raw.get("nepseIndex")
                    or raw.get("value")
                    or raw.get("closePrice")
                    or raw.get("close")
                    or raw.get("ltp")
                    or raw.get("lastTradedPrice")
                    or raw.get("y")
                )
            else:
                continue
            if not label or value is None:
                continue
            points.append(NepseChartPoint(date=label, value=value))

        points.sort(key=lambda p: self._parse_iso_dt(p.date) or datetime.min)
        if len(points) > limit:
            points = points[-limit:]
        return points

    def _fetch_timewise_nepse_points(self, *, index_id: int, limit: int) -> list[NepseChartPoint]:
        # Best-effort intraday source. This may fail outside Nepal network;
        # callers must fallback gracefully.
        try:
            from nepse_scraper import Nepse_scraper  # type: ignore

            scraper = Nepse_scraper()
            payload = scraper.call_nepse_function(
                url=f"https://www.nepalstock.com.np/api/nots/graph/index/{index_id}",
                method="POST",
            )
            points = self._extract_timewise_points(payload, limit=limit)
            if points:
                return points
        except Exception:
            pass
        return []

    def get_nepse_chart(
        self,
        limit: int = 90,
        mode: str = "daily",
        index_id: int = 58,
    ) -> NepseChartResponse:
        if limit <= 0:
            limit = 90
        if index_id <= 0:
            index_id = 58
        clean_mode = mode.strip().lower()
        if clean_mode not in {"daily", "timewise"}:
            clean_mode = "daily"
        if clean_mode == "timewise":
            points = self._fetch_timewise_nepse_points(index_id=index_id, limit=limit)
            if points:
                return NepseChartResponse(
                    source=f"nepse-timewise-{index_id}",
                    mode="timewise",
                    trading_date=(points[-1].date[:10] if points[-1].date else None),
                    points=points,
                )
            return NepseChartResponse(source="unavailable", mode="timewise", points=[])
        try:
            response = requests.get(
                f"https://www.merolagani.com/Indices.aspx?index={index_id}",
                timeout=settings.request_timeout_seconds,
                headers={"User-Agent": "Mozilla/5.0 (compatible; BulkIpoApply/1.0)"},
            )
            if response.ok and response.text:
                points = self._extract_nepse_points_from_merolagani(response.text, limit=limit)
                if points:
                    return NepseChartResponse(
                        source=f"merolagani-{index_id}",
                        mode="daily",
                        trading_date=points[-1].date if points else None,
                        points=points,
                    )
        except Exception:
            pass
        return NepseChartResponse(source="unavailable", mode=clean_mode, points=[])

    def get_nepse_news(self, limit: int = 100) -> list[NepseNewsItem]:
        if limit <= 0:
            limit = 100
        if limit > 300:
            limit = 300
        try:
            results: list[NepseNewsItem] = []
            seen: set[str] = set()
            for source_url in self._MEROLAGANI_NEWS_SOURCES:
                response = requests.get(
                    source_url,
                    timeout=settings.request_timeout_seconds,
                    headers=self._MEROLAGANI_HEADERS,
                )
                if not response.ok or not response.text:
                    continue
                items = self._extract_merolagani_news_blocks(response.text)
                for item in items:
                    if item.url in seen:
                        continue
                    # Keep all headlines from dedicated NEPSE list.
                    # For other Merolagani latest pages, keep NEPSE/market-related only.
                    if "news=nepse" not in source_url and not self._is_nepse_related_title(item.title):
                        continue
                    seen.add(item.url)
                    results.append(item)
                    if len(results) >= limit:
                        break
                if len(results) >= limit:
                    break

            results.sort(key=lambda item: self._news_id_from_url(item.url), reverse=True)
            if len(results) > limit:
                results = results[:limit]
            if results:
                return results

            # Secondary source fallback (Bizshala keyword search).
            try:
                bzr = requests.get(
                    "https://www.bizshala.com/search?keyword=nepse",
                    timeout=settings.request_timeout_seconds,
                    headers=self._MEROLAGANI_HEADERS,
                )
                if bzr.ok and bzr.text:
                    article_link_pattern = re.compile(
                        r'<a[^>]+href="(?P<href>https://www\.bizshala\.com/article/\d+)"[^>]*>(?P<title>[\s\S]*?)</a>',
                        re.IGNORECASE,
                    )
                    tag_pattern = re.compile(r"<[^>]+>")
                    for m in article_link_pattern.finditer(bzr.text):
                        url = m.group("href").strip()
                        if url in seen:
                            continue
                        title = html_lib.unescape(
                            tag_pattern.sub("", m.group("title"))
                        ).strip()
                        if not title or len(title) < 4:
                            continue
                        if not self._is_nepse_related_title(title):
                            continue
                        seen.add(url)
                        results.append(
                            NepseNewsItem(
                                title=title,
                                url=url,
                                published_at=None,
                                source="bizshala",
                            )
                        )
                        if len(results) >= limit:
                            break
            except Exception:
                pass

            if results:
                return results

            # Fallback: official NEPSE pages so users still get actionable links.
            return [
                NepseNewsItem(
                    title="NEPSE News (official)",
                    url="https://nepalstock.com.np/news",
                    published_at=None,
                    source="nepalstock",
                ),
                NepseNewsItem(
                    title="NEPSE Notices (official)",
                    url="https://nepalstock.com.np/notices",
                    published_at=None,
                    source="nepalstock",
                ),
                NepseNewsItem(
                    title="NEPSE Announcement (official)",
                    url="https://nepalstock.com.np/announcement",
                    published_at=None,
                    source="nepalstock",
                ),
            ][:limit]
        except Exception:
            return [
                NepseNewsItem(
                    title="NEPSE News (official)",
                    url="https://nepalstock.com.np/news",
                    published_at=None,
                    source="nepalstock",
                ),
                NepseNewsItem(
                    title="NEPSE Notices (official)",
                    url="https://nepalstock.com.np/notices",
                    published_at=None,
                    source="nepalstock",
                ),
                NepseNewsItem(
                    title="NEPSE Announcement (official)",
                    url="https://nepalstock.com.np/announcement",
                    published_at=None,
                    source="nepalstock",
                ),
            ]

    def _lang_code(self, language: str) -> str:
        code = (language or "en").strip().lower()
        return "ne" if code.startswith("ne") else "en"

    def _translate_if_needed(self, text: str, *, language: str) -> str:
        if not text.strip():
            return text
        if self._lang_code(language) != "ne":
            return text
        try:
            from deep_translator import GoogleTranslator  # type: ignore

            return GoogleTranslator(source="auto", target="ne").translate(text)
        except Exception:
            return text

    def _safe_text(self, value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _new_nepse_client(self) -> object:
        try:
            from nepse import Nepse  # type: ignore
        except Exception as exc:
            raise MeroShareError(
                "Missing 'nepse' dependency. Install backend requirements."
            ) from exc
        api = Nepse()
        try:
            api.setTLSVerification(False)
        except Exception:
            pass
        return api

    def _run_with_timeout(self, func, *, timeout_seconds: int):
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(func)
        try:
            return future.result(timeout=max(1, timeout_seconds))
        except FuturesTimeoutError as exc:
            future.cancel()
            raise MeroShareError("Market data provider timed out.") from exc
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _to_market_movers(self, rows: object) -> list[MarketMoverItem]:
        if not isinstance(rows, list):
            return []
        output: list[MarketMoverItem] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            symbol = self._safe_text(
                raw.get("symbol") or raw.get("stockSymbol") or raw.get("script")
            )
            if not symbol:
                continue
            output.append(
                MarketMoverItem(
                    symbol=symbol,
                    ltp=self._as_float(raw.get("ltp") or raw.get("lastTradedPrice")),
                    point_change=self._as_float(
                        raw.get("pointChange")
                        or raw.get("change")
                        or raw.get("point_change")
                    ),
                    percentage_change=self._as_float(
                        raw.get("percentageChange")
                        or raw.get("perChange")
                        or raw.get("percentChange")
                        or raw.get("percentage_change")
                    ),
                    turnover=self._as_float(raw.get("turnover") or raw.get("totalTurnover")),
                )
            )
        return output

    def _to_market_indices(self, rows: object) -> list[MarketIndexItem]:
        if not isinstance(rows, list):
            return []
        output: list[MarketIndexItem] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            name = self._safe_text(raw.get("index") or raw.get("name"))
            if not name:
                continue
            output.append(
                MarketIndexItem(
                    index=name,
                    current_value=self._as_float(
                        raw.get("currentValue")
                        or raw.get("indexValue")
                        or raw.get("value")
                    ),
                    change=self._as_float(raw.get("change") or raw.get("pointChange")),
                    per_change=self._as_float(
                        raw.get("perChange")
                        or raw.get("percentageChange")
                        or raw.get("percentChange")
                    ),
                )
            )
        return output

    def _fetch_market_overview_live(self) -> MarketOverviewResponse:
        api = self._new_nepse_client()
        status_payload = api.getMarketStatus() or {}
        gainers = self._to_market_movers(api.getTopGainers() or [])
        losers = self._to_market_movers(api.getTopLosers() or [])
        turnover = self._to_market_movers(api.getTopTenTurnoverScrips() or [])
        raw_indices: list[dict[str, object]] = []

        nepse_index_rows = api.getNepseIndex() or []
        if isinstance(nepse_index_rows, dict):
            raw_indices.append(nepse_index_rows)
        elif isinstance(nepse_index_rows, list):
            raw_indices.extend([x for x in nepse_index_rows if isinstance(x, dict)])
        sub_index_rows = api.getNepseSubIndices() or []
        if isinstance(sub_index_rows, list):
            raw_indices.extend([x for x in sub_index_rows if isinstance(x, dict)])

        dedup: dict[str, dict[str, object]] = {}
        for row in raw_indices:
            name = self._safe_text(row.get("index") or row.get("name"))
            if not name or name in dedup:
                continue
            dedup[name] = row
        status = self._safe_text(status_payload.get("status")) or "Unknown"

        return MarketOverviewResponse(
            status=status,
            gainers=gainers,
            losers=losers,
            turnover=turnover,
            indices=self._to_market_indices(list(dedup.values())),
        )

    def get_market_overview(self) -> MarketOverviewResponse:
        timeout_seconds = min(max(5, settings.request_timeout_seconds), 15)
        try:
            return self._run_with_timeout(
                self._fetch_market_overview_live,
                timeout_seconds=timeout_seconds,
            )
        except Exception:
            # Avoid upstream 504 on slow third-party APIs by returning a safe fallback.
            return MarketOverviewResponse(
                status="Unknown",
                gainers=[],
                losers=[],
                turnover=[],
                indices=[],
            )

    def _fallback_market_summary(self, overview: MarketOverviewResponse) -> str:
        top_gainer = overview.gainers[0].symbol if overview.gainers else "N/A"
        top_loser = overview.losers[0].symbol if overview.losers else "N/A"
        top_turnover = overview.turnover[0].symbol if overview.turnover else "N/A"
        status_line = f"Market status: {overview.status}."
        movement = (
            f"Top movers: gainer {top_gainer}, loser {top_loser}, highest turnover {top_turnover}."
        )
        takeaway = (
            "Use this as a quick snapshot only and verify fundamentals before decisions."
        )
        return f"{status_line}\n\n{movement}\n\n{takeaway}"

    def _collect_gemini_api_keys(self) -> list[str]:
        def parse_keys(raw: str) -> list[str]:
            if not raw.strip():
                return []
            return [
                token.strip()
                for token in re.split(r"[\n,;]+", raw)
                if token.strip()
            ]

        ordered: list[str] = []
        seen: set[str] = set()

        inline_raw = f"{settings.gemini_api_keys}\n{settings.gemini_api_key}"
        for key in parse_keys(inline_raw):
            if key in seen:
                continue
            seen.add(key)
            ordered.append(key)

        file_path_raw = settings.gemini_api_keys_file.strip()
        if file_path_raw:
            configured = Path(file_path_raw)
            candidates: list[Path]
            if configured.is_absolute():
                candidates = [configured]
            else:
                project_root = Path(__file__).resolve().parents[2]
                candidates = [Path.cwd() / configured, project_root / configured]

            used_paths: set[Path] = set()
            for path in candidates:
                if path in used_paths:
                    continue
                used_paths.add(path)
                try:
                    if not path.exists():
                        continue
                    file_keys = parse_keys(path.read_text(encoding="utf-8"))
                    for key in file_keys:
                        if key in seen:
                            continue
                        seen.add(key)
                        ordered.append(key)
                except OSError:
                    # Ignore file read errors and continue with inline keys.
                    continue

        return ordered

    def _call_llm(self, prompt: str) -> str:
        api_keys = self._collect_gemini_api_keys()
        if not api_keys:
            raise MeroShareError(
                "Gemini API key is not configured. Set GEMINI_API_KEYS/GEMINI_API_KEY "
                "or put keys in app/api.txt."
            )
        try:
            from google import genai  # type: ignore
        except Exception as exc:
            raise MeroShareError(f"LLM client initialization failed: {exc}") from exc

        failures: list[str] = []
        for idx, api_key in enumerate(api_keys, start=1):
            try:
                client = genai.Client(api_key=api_key)
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                )
                text = getattr(response, "text", None)
                if isinstance(text, str) and text.strip():
                    return text.strip()
                rendered = str(response).strip()
                if rendered:
                    return rendered
                failures.append(f"key#{idx}: empty response")
            except Exception as exc:
                failures.append(f"key#{idx}: {exc}")
                continue

        if failures:
            failure_summary = " | ".join(failures[:3])
            raise MeroShareError(
                f"LLM generation failed for all Gemini keys ({len(api_keys)} tried): {failure_summary}"
            )
        raise MeroShareError("LLM generation failed for all Gemini keys.")

    def _ai_or_fallback(
        self,
        *,
        prompt: str,
        fallback: str,
        language: str,
    ) -> tuple[str, str, bool]:
        try:
            text = self._call_llm(prompt)
            text = self._translate_if_needed(text, language=language)
            if text.strip():
                return text, "gemini", False
        except Exception:
            pass
        fallback_text = self._translate_if_needed(fallback, language=language)
        return fallback_text, "fallback", True

    def get_market_summary(self, language: str = "en") -> MarketSummaryResponse:
        overview = self.get_market_overview()
        clean_lang = self._lang_code(language)
        prompt = (
            "You are a NEPSE market analyst. "
            "Generate a short daily briefing with sections: overall sentiment, "
            "sector trend, notable movers, and a practical takeaway. "
            "Do not provide buy/sell advice. "
            f"Respond in {'Nepali' if clean_lang == 'ne' else 'English'}.\n\n"
            f"Market status: {overview.status}\n"
            f"Gainers: {[x.model_dump() for x in overview.gainers[:8]]}\n"
            f"Losers: {[x.model_dump() for x in overview.losers[:8]]}\n"
            f"Turnover: {[x.model_dump() for x in overview.turnover[:8]]}\n"
            f"Indices: {[x.model_dump() for x in overview.indices[:12]]}\n"
        )
        text, provider, used_fallback = self._ai_or_fallback(
            prompt=prompt,
            fallback=self._fallback_market_summary(overview),
            language=clean_lang,
        )
        return MarketSummaryResponse(
            summary=text,
            language=clean_lang,
            provider=provider,
            used_fallback=used_fallback,
        )

    def _extract_article_text(self, url: str) -> str:
        try:
            from bs4 import BeautifulSoup  # type: ignore
        except Exception as exc:
            raise MeroShareError(
                "Missing 'beautifulsoup4' dependency. Install backend requirements."
            ) from exc

        response = requests.get(
            url,
            headers=self._MARKET_HUB_HEADERS,
            timeout=settings.request_timeout_seconds,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        content = soup.select_one("#newsdetail-content")
        if content is None:
            content = soup.find("article")
        if content is None:
            return ""
        text = content.get_text(separator="\n", strip=True)
        return re.sub(r"\n{2,}", "\n\n", text).strip()

    def get_ipo_news_articles(self, limit: int = 5) -> list[IpoNewsArticleItem]:
        if limit <= 0:
            limit = 5
        if limit > 20:
            limit = 20
        try:
            from bs4 import BeautifulSoup  # type: ignore
        except Exception as exc:
            raise MeroShareError(
                "Missing 'beautifulsoup4' dependency. Install backend requirements."
            ) from exc

        try:
            list_response = requests.get(
                self._SHARESANSAR_IPO_URL,
                headers=self._MARKET_HUB_HEADERS,
                timeout=settings.request_timeout_seconds,
            )
            list_response.raise_for_status()
            soup = BeautifulSoup(list_response.content, "html.parser")
            cards = soup.find_all("div", class_="featured-news-list")
            output: list[IpoNewsArticleItem] = []

            for card in cards[:limit]:
                title_tag = card.find("h4", class_="featured-news-title")
                link_tag = card.find("a")
                date_tag = card.find("span", class_="text-org")
                if not title_tag or not link_tag:
                    continue
                title = title_tag.get_text(strip=True)
                link = self._safe_text(link_tag.get("href"))
                date = date_tag.get_text(strip=True) if date_tag else ""
                if not title or not link:
                    continue
                content = ""
                try:
                    content = self._extract_article_text(link)
                except Exception:
                    content = ""
                output.append(
                    IpoNewsArticleItem(
                        title=title,
                        date=date,
                        link=link,
                        content=content,
                    )
                )
            return output
        except Exception as exc:
            raise MeroShareError(f"Failed to scrape IPO articles: {exc}") from exc

    def _fallback_ipo_analysis(self, req: IpoAnalysisRequest) -> str:
        compact = re.sub(r"\s+", " ", req.content).strip()
        preview = compact[:550]
        if len(compact) > 550:
            preview = f"{preview}..."
        return (
            f"IPO article reviewed for: {req.title}\n\n"
            "Summary:\n"
            f"{preview}\n\n"
            "Educational note:\n"
            "- Verify issue manager, opening/closing dates, and share type from official notices.\n"
            "- For heavily subscribed IPOs in Nepal, many retail investors apply the minimum lot size.\n"
            "- Treat this summary as informational only, not financial advice."
        )

    def analyze_ipo_article(self, request: IpoAnalysisRequest) -> MarketSummaryResponse:
        clean_lang = self._lang_code(request.language)
        prompt = (
            "You are an expert NEPSE IPO analyst. "
            "Analyze the given IPO news article and produce: "
            "1) key details (issue size, dates, issue manager, rating), "
            "2) company/sector context, "
            "3) beginner-friendly educational strategy notes, "
            "4) explicit disclaimer that this is not financial advice. "
            f"Respond in {'Nepali' if clean_lang == 'ne' else 'English'}.\n\n"
            f"Title: {request.title}\n\n"
            f"Article:\n{request.content}\n"
        )
        text, provider, used_fallback = self._ai_or_fallback(
            prompt=prompt,
            fallback=self._fallback_ipo_analysis(request),
            language=clean_lang,
        )
        return MarketSummaryResponse(
            summary=text,
            language=clean_lang,
            provider=provider,
            used_fallback=used_fallback,
        )

    def get_market_companies(self) -> list[StockCompanyItem]:
        def load_companies() -> list[StockCompanyItem]:
            api = self._new_nepse_client()
            company_rows = api.getCompanyList() or []
            if not isinstance(company_rows, list):
                return []
            output: list[StockCompanyItem] = []
            for row in company_rows:
                if not isinstance(row, dict):
                    continue
                symbol = self._safe_text(
                    row.get("symbol") or row.get("stockSymbol") or row.get("script")
                )
                if not symbol:
                    continue
                output.append(
                    StockCompanyItem(
                        symbol=symbol,
                        name=self._safe_text(
                            row.get("securityName")
                            or row.get("companyName")
                            or row.get("name")
                        )
                        or None,
                        sector=self._safe_text(
                            row.get("sectorName") or row.get("sector")
                        )
                        or None,
                    )
                )
            output.sort(key=lambda x: x.symbol)
            return output

        try:
            timeout_seconds = min(max(5, settings.request_timeout_seconds), 15)
            return self._run_with_timeout(
                load_companies,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            raise MeroShareError(f"Failed to fetch company list: {exc}") from exc

    def get_market_company_details(self, symbol: str) -> dict[str, Any]:
        clean_symbol = symbol.strip().upper()
        if not clean_symbol:
            raise MeroShareError("Stock symbol is required.")

        def load_details() -> dict[str, Any]:
            api = self._new_nepse_client()
            details = api.getCompanyDetails(clean_symbol)
            if isinstance(details, dict):
                return details
            if isinstance(details, list) and details and isinstance(details[0], dict):
                return details[0]
            return {"symbol": clean_symbol, "raw": details}

        try:
            timeout_seconds = min(max(5, settings.request_timeout_seconds), 15)
            return self._run_with_timeout(
                load_details,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            raise MeroShareError(
                f"Failed to fetch company details for {clean_symbol}: {exc}"
            ) from exc

    def _fallback_stock_analysis(self, request: StockAnalysisRequest) -> str:
        details = request.details
        ltp = (
            details.get("ltp")
            or details.get("lastTradedPrice")
            or details.get("closePrice")
            or "N/A"
        )
        high = details.get("high") or details.get("highPrice") or "N/A"
        low = details.get("low") or details.get("lowPrice") or "N/A"
        week52_high = (
            details.get("fiftyTwoWeekHigh")
            or details.get("fiftyTwoWeeksHigh")
            or details.get("week52High")
            or "N/A"
        )
        week52_low = (
            details.get("fiftyTwoWeekLow")
            or details.get("fiftyTwoWeeksLow")
            or details.get("week52Low")
            or "N/A"
        )
        return (
            f"Snapshot for {request.symbol.upper()}:\n\n"
            f"- LTP: {ltp}\n"
            f"- Day high/low: {high} / {low}\n"
            f"- 52-week high/low: {week52_high} / {week52_low}\n\n"
            "Interpretation:\n"
            "Use this as a quick market snapshot and cross-check full fundamentals, "
            "financial reports, and sector trend before investing."
        )

    def analyze_stock_details(self, request: StockAnalysisRequest) -> MarketSummaryResponse:
        clean_lang = self._lang_code(request.language)
        serialized_details = json.dumps(request.details, ensure_ascii=False)[:12000]
        prompt = (
            "You are a NEPSE equity analyst. "
            "From the provided stock JSON data, produce a structured markdown report with: "
            "company overview, price/performance, fundamental snapshot, and beginner interpretation. "
            "Do not provide direct buy/sell advice. "
            f"Respond in {'Nepali' if clean_lang == 'ne' else 'English'}.\n\n"
            f"Symbol: {request.symbol.upper()}\n"
            f"Raw JSON: {serialized_details}\n"
        )
        text, provider, used_fallback = self._ai_or_fallback(
            prompt=prompt,
            fallback=self._fallback_stock_analysis(request),
            language=clean_lang,
        )
        return MarketSummaryResponse(
            summary=text,
            language=clean_lang,
            provider=provider,
            used_fallback=used_fallback,
        )

    def _chat_rate_limit(self, key: str) -> tuple[bool, int]:
        now = datetime.utcnow()
        max_messages = max(1, settings.market_hub_chat_messages_per_minute)
        with self._chat_lock:
            started_at, count = self._chat_buckets.get(key, (now, 0))
            elapsed = (now - started_at).total_seconds()
            if elapsed >= 60:
                started_at = now
                count = 0
                elapsed = 0
            if count >= max_messages:
                retry_after = max(1, int(60 - elapsed))
                return False, retry_after
            self._chat_buckets[key] = (started_at, count + 1)
        return True, 0

    def _fallback_chat_response(self, message: str, context: MarketChatContext | None) -> str:
        if context is None:
            return (
                "I can help with NEPSE basics, IPO process, and market terms. "
                "For specific stock or IPO details, open Stock Analysis or IPO Center and ask again."
            )
        if context.type.upper() == "IPO":
            return (
                f"You are asking about IPO context: {context.name}. "
                "Please verify issue dates, issue manager, and eligibility from official notices before applying."
            )
        if context.type.upper() == "STOCK":
            return (
                f"You are asking about stock context: {context.name}. "
                "Use the displayed company details, compare sector trend, and avoid decisions from one data point."
            )
        return (
            f"Context loaded for {context.name}. "
            "I can explain terms and summarize available context data."
        )

    def ask_market_chat(
        self,
        request: MarketChatRequest,
        *,
        client_key: str,
    ) -> MarketChatResponse:
        allowed, retry_after = self._chat_rate_limit(client_key)
        if not allowed:
            raise MeroShareError(
                f"Rate limit exceeded. Max {settings.market_hub_chat_messages_per_minute} messages/minute. "
                f"Try again in about {retry_after} seconds."
            )

        clean_lang = self._lang_code(request.language)
        context_text = ""
        if request.context is not None:
            context_text = (
                f"\nContext type: {request.context.type}\n"
                f"Context name: {request.context.name}\n"
                f"Context data:\n{request.context.data[:12000]}\n"
            )

        prompt = (
            "You are NEPSE Sahayogi, an assistant for Nepali stock market learners. "
            "Answer clearly and briefly. "
            "Do not give direct buy/sell advice. "
            "If context exists, prioritize it. "
            f"Respond in {'Nepali' if clean_lang == 'ne' else 'English'}.\n"
            f"{context_text}\n"
            f"User question: {request.message}\n"
        )
        fallback = self._fallback_chat_response(request.message, request.context)
        text, provider, used_fallback = self._ai_or_fallback(
            prompt=prompt,
            fallback=fallback,
            language=clean_lang,
        )
        return MarketChatResponse(
            response=text,
            language=clean_lang,
            provider=provider,
            used_fallback=used_fallback,
        )

    def get_capitals(self) -> list[CapitalOptionItem]:
        try:
            response = requests.get(
                f"{BASE_URL}/capital/",
                verify=False,
                timeout=settings.request_timeout_seconds,
            )
            if not response.ok:
                raise MeroShareError("Unable to fetch DP capital list")
            payload = response.json()
            capitals = [
                CapitalOptionItem(
                    code=str(item.get("code", "")).strip(),
                    name=str(item.get("name", "")).strip(),
                    client_id=int(item.get("id", 0) or 0),
                )
                for item in payload
                if str(item.get("code", "")).strip()
            ]
            capitals.sort(key=lambda item: item.code)
            return capitals
        except requests.RequestException as exc:
            raise MeroShareError(f"Unable to fetch DP capital list: {exc}") from exc

    def _apply_result(
        self,
        boid: str,
        success: bool,
        code: str,
        message: str,
        upstream_status_code: int | None = None,
    ) -> AccountApplyResult:
        return AccountApplyResult(
            boid=boid,
            success=success,
            code=code,
            message=message,
            upstream_status_code=upstream_status_code,
        )

    def _to_ipo_items(self, issues: list[dict]) -> list[IPOItem]:
        output: list[IPOItem] = []
        for item in issues:
            is_ipo = item.get("shareTypeName") == "IPO"
            is_ordinary = item.get("shareGroupName") == "Ordinary Shares"
            if not (is_ipo and is_ordinary):
                continue

            output.append(
                IPOItem(
                    company_share_id=item.get("companyShareId", 0),
                    company_name=item.get("companyName", ""),
                    issue_manager=item.get("assignedToClientName") or "",
                    min_quantity=int(item.get("minimumKitta", 10) or 10),
                    max_quantity=int(item.get("maximumKitta", 10) or 10),
                    is_applied=item.get("action") == "edit",
                )
            )
        return output

    def _normalize_dp(self, dp_id: str) -> str:
        only_digits = "".join(c for c in dp_id.strip() if c.isdigit())
        if len(only_digits) < 5:
            raise MeroShareError(f"Invalid dp_id '{dp_id}'")
        # Accept plain DPID (e.g. 10600), BOID/Demat style value (e.g. 1301060000...)
        if len(only_digits) == 5:
            return only_digits
        if only_digits.startswith("130") and len(only_digits) >= 8:
            return only_digits[3:8]
        return only_digits[:5]

    def _extract_username(self, boid_or_username: str, dp_id: str) -> str:
        value = "".join(c for c in boid_or_username.strip() if c.isdigit())
        demat_prefix = f"130{dp_id}"
        if value.startswith(demat_prefix):
            username = value[len(demat_prefix):]
            if not username:
                raise MeroShareError("Invalid BOID/Demat format")
            return username
        return value

    def _build_context(self, dp_id: str, boid_or_username: str, password: str) -> SessionContext:
        clean_dp = self._normalize_dp(dp_id)
        username = self._extract_username(boid_or_username, clean_dp)
        if clean_dp not in CAPITAL_BY_CODE:
            raise MeroShareError(f"Unsupported dp_id '{clean_dp}'")
        return SessionContext(dp_id=clean_dp, username=username, password=password)

    def _error_code_from_exception(self, exc: Exception) -> str:
        msg = str(exc).lower()
        if "invalid dp_id" in msg:
            return "INVALID_DPID"
        if "unsupported dp_id" in msg:
            return "UNSUPPORTED_DPID"
        if "invalid boid" in msg or "invalid boid/demat format" in msg:
            return "INVALID_BOID"
        if "authentication failed" in msg:
            return "AUTH_FAILED"
        if "token missing" in msg:
            return "AUTH_TOKEN_MISSING"
        if "bank info" in msg:
            return "BANK_INFO_UNAVAILABLE"
        if "branch info" in msg:
            return "BRANCH_INFO_UNAVAILABLE"
        return "INTERNAL_ERROR"

    def _auth_headers(self, ctx: SessionContext) -> dict[str, str]:
        client_id = CAPITAL_BY_CODE[ctx.dp_id]
        r = requests.post(
            f"{BASE_URL}/auth/",
            json={"clientId": client_id, "username": ctx.username, "password": ctx.password},
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if not r.ok:
            raise MeroShareError("Authentication failed")

        token = r.headers.get("Authorization")
        if not token:
            raise MeroShareError("Authentication token missing")
        return {"Authorization": token}

    def _bank_options_raw(self, headers: dict[str, str]) -> list[dict]:
        banks = requests.get(
            f"{BASE_URL}/bank/",
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if not banks.ok or not banks.json():
            raise MeroShareError("Unable to fetch bank info")
        return banks.json()

    def _branch_info(self, headers: dict[str, str], bank_id: int | None = None) -> dict:
        banks = self._bank_options_raw(headers)
        if bank_id is None:
            bank = banks[0]
        else:
            bank = next((item for item in banks if item.get("id") == bank_id), None)
            if not bank:
                raise MeroShareError(f"Selected bank id {bank_id} not found for this account")

        branches = requests.get(
            f"{BASE_URL}/bank/{bank['id']}",
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if not branches.ok or not branches.json():
            raise MeroShareError("Unable to fetch branch info")

        info = branches.json()[0]
        info["bankId"] = bank["id"]
        return info

    def _open_issues_raw(self, headers: dict[str, str]) -> list[dict]:
        payload = {
            "filterFieldParams": [
                {"key": "companyIssue.companyISIN.script", "alias": "Scrip"},
                {"key": "companyIssue.companyISIN.company.name", "alias": "Company Name"},
                {
                    "key": "companyIssue.assignedToClient.name",
                    "value": "",
                    "alias": "Issue Manager",
                },
            ],
            "filterDateParams": [
                {"key": "minIssueOpenDate", "condition": "", "alias": "", "value": ""},
                {"key": "maxIssueCloseDate", "condition": "", "alias": "", "value": ""},
            ],
            "page": 1,
            "size": 50,
            "searchRoleViewConstants": "VIEW_APPLICABLE_SHARE",
        }

        r = requests.post(
            f"{BASE_URL}/companyShare/applicableIssue/",
            json=payload,
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if not r.ok:
            raise MeroShareError("Unable to fetch open issues")
        return r.json().get("object", [])

    def _customer_apply_status(
        self,
        headers: dict[str, str],
        company_share_id: int,
        demat: str,
    ) -> tuple[bool, str]:
        r = requests.get(
            f"{BASE_URL}/applicantForm/customerType/{company_share_id}/{demat}",
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if not r.ok:
            return False, f"Customer type check failed ({r.status_code})"
        message = str((r.json() or {}).get("message") or "").strip()
        return message == "Customer can apply.", message

    def _is_reapply_required_message(self, message: str) -> bool:
        text = (message or "").strip().lower()
        if not text:
            return False
        return (
            "cannot edit rejected" in text
            or "rejected applicant" in text
            or "reapply" in text
            or "rejected" in text
        )

    def _submit_reapply(
        self,
        *,
        headers: dict[str, str],
        company_share_id: int,
        payload: dict[str, str],
    ) -> tuple[bool, str, int | None]:
        r = requests.post(
            f"{BASE_URL}/applicantForm/reapply/{company_share_id}",
            json=payload,
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if r.ok:
            return True, "Reapply submitted", r.status_code
        backend_message = ""
        try:
            body = r.json()
            backend_message = str(body.get("message") or body.get("detail") or "").strip()
        except Exception:
            backend_message = r.text.strip()
        return False, (backend_message or f"Reapply rejected by upstream ({r.status_code})"), r.status_code

    def _find_active_applicant_form_id(
        self,
        *,
        headers: dict[str, str],
        company_share_id: int,
    ) -> int | None:
        payload = {
            "filterFieldParams": [
                {"key": "companyShare.companyIssue.companyISIN.script", "alias": "Scrip"},
                {"key": "companyShare.companyIssue.companyISIN.company.name", "alias": "Company Name"},
            ],
            "page": 1,
            "size": 50,
            "searchRoleViewConstants": "VIEW_APPLICANT_FORM_COMPLETE",
            "filterDateParams": [],
        }
        r = requests.post(
            f"{BASE_URL}/applicantForm/active/search/",
            json=payload,
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if not r.ok:
            return None
        forms = r.json().get("object", [])
        if not isinstance(forms, list):
            return None
        match = next(
            (
                item
                for item in forms
                if int(item.get("companyShareId", 0) or 0) == company_share_id
                and item.get("applicantFormId")
            ),
            None,
        )
        if not match:
            return None
        try:
            return int(match.get("applicantFormId", 0) or 0) or None
        except Exception:
            return None

    def _submit_edit(
        self,
        *,
        headers: dict[str, str],
        applicant_form_id: int,
        payload: dict[str, str],
    ) -> tuple[bool, str, int | None]:
        r = requests.put(
            f"{BASE_URL}/applicantForm/share/edit/{applicant_form_id}",
            json=payload,
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if r.ok:
            return True, "Application edited", r.status_code
        backend_message = ""
        try:
            body = r.json()
            backend_message = str(body.get("message") or body.get("detail") or "").strip()
        except Exception:
            backend_message = r.text.strip()
        return False, (backend_message or f"Edit rejected by upstream ({r.status_code})"), r.status_code

    def get_open_ipos(self) -> list[IPOItem]:
        if not (settings.default_dp_id and settings.default_boid_or_username and settings.default_password):
            return []

        ctx = self._build_context(
            settings.default_dp_id,
            settings.default_boid_or_username,
            settings.default_password,
        )
        headers = self._auth_headers(ctx)
        issues = self._open_issues_raw(headers)
        return self._to_ipo_items(issues)

    def get_open_ipos_for_account(self, dp_id: str, boid_or_username: str, password: str) -> list[IPOItem]:
        ctx = self._build_context(dp_id, boid_or_username, password)
        headers = self._auth_headers(ctx)
        issues = self._open_issues_raw(headers)
        return self._to_ipo_items(issues)

    def get_banks_for_account(self, dp_id: str, boid_or_username: str, password: str) -> list[BankOptionItem]:
        ctx = self._build_context(dp_id, boid_or_username, password)
        headers = self._auth_headers(ctx)
        banks = self._bank_options_raw(headers)
        return [
            BankOptionItem(
                bank_id=bank.get("id", 0),
                bank_name=bank.get("name", ""),
            )
            for bank in banks
        ]

    def _resolve_accounts_csv_path(self) -> Path:
        configured = Path(settings.accounts_csv_path).expanduser()
        if configured.is_absolute():
            return configured
        return Path.cwd() / configured

    def _load_apply_accounts_from_csv(self) -> list[AccountApplyRequest]:
        csv_path = self._resolve_accounts_csv_path()
        if not csv_path.exists():
            raise MeroShareError(
                f"Accounts CSV not found at '{csv_path}'. "
                "Create it with columns: user,dp,username,password,crn,pin."
            )

        out: list[AccountApplyRequest] = []
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_index, row in enumerate(reader, start=2):
                quantity_raw = str(row.get("quantity", "")).strip()
                quantity = settings.default_apply_quantity
                if quantity_raw:
                    try:
                        quantity = int(quantity_raw)
                    except ValueError as exc:
                        raise MeroShareError(
                            f"Invalid quantity in '{csv_path}' at line {row_index}: '{quantity_raw}'"
                        ) from exc

                try:
                    out.append(
                        AccountApplyRequest(
                            dp_id=str(row.get("dp", "")).strip(),
                            boid=str(row.get("username", "")).strip(),
                            password=str(row.get("password", "")).strip(),
                            quantity=quantity,
                            crn_number=str(row.get("crn", "")).strip(),
                            transaction_pin=str(row.get("pin", "")).strip(),
                        )
                    )
                except Exception as exc:
                    raise MeroShareError(
                        f"Invalid account in '{csv_path}' at line {row_index}: {exc}"
                    ) from exc

        if not out:
            raise MeroShareError(f"No account rows found in '{csv_path}'.")
        return out

    def _load_result_accounts_from_csv(self) -> list[ResultCheckAccountRequest]:
        csv_path = self._resolve_accounts_csv_path()
        if not csv_path.exists():
            raise MeroShareError(
                f"Accounts CSV not found at '{csv_path}'. "
                "Create it with columns: user,dp,username,password,crn,pin."
            )

        out: list[ResultCheckAccountRequest] = []
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_index, row in enumerate(reader, start=2):
                try:
                    out.append(
                        ResultCheckAccountRequest(
                            dp_id=str(row.get("dp", "")).strip(),
                            boid=str(row.get("username", "")).strip(),
                            password=str(row.get("password", "")).strip(),
                        )
                    )
                except Exception as exc:
                    raise MeroShareError(
                        f"Invalid account in '{csv_path}' at line {row_index}: {exc}"
                    ) from exc

        if not out:
            raise MeroShareError(f"No account rows found in '{csv_path}'.")
        return out

    def apply_bulk(self, company_share_id: int, accounts: list[AccountApplyRequest]) -> BulkApplyResponse:
        if not accounts:
            accounts = self._load_apply_accounts_from_csv()
        results: list[AccountApplyResult] = []

        for account in accounts:
            try:
                ctx = self._build_context(account.dp_id, account.boid, account.password)
                headers = self._auth_headers(ctx)
                branch = self._branch_info(headers, account.bank_id)
                issues = self._open_issues_raw(headers)

                candidate = next(
                    (
                        item
                        for item in issues
                        if item.get("companyShareId") == company_share_id
                        and item.get("shareGroupName") == "Ordinary Shares"
                    ),
                    None,
                )

                if not candidate:
                    results.append(
                        self._apply_result(
                            boid=account.boid,
                            success=False,
                            code="ISSUE_NOT_FOUND",
                            message=f"Company share ID {company_share_id} not found in applicable ordinary issues",
                        )
                    )
                    continue

                action_is_edit = candidate.get("action") == "edit"

                min_kitta = int(candidate.get("minimumKitta", 10) or 10)
                max_kitta = int(candidate.get("maximumKitta", min_kitta) or min_kitta)
                if account.quantity < min_kitta or account.quantity > max_kitta:
                    results.append(
                        self._apply_result(
                            boid=account.boid,
                            success=False,
                            code="QUANTITY_OUT_OF_RANGE",
                            message=f"Quantity must be between {min_kitta} and {max_kitta}",
                        )
                    )
                    continue

                payload = {
                    "demat": ctx.demat,
                    "boid": ctx.username,
                    "accountNumber": branch["accountNumber"],
                    "customerId": branch["id"],
                    "accountBranchId": branch["accountBranchId"],
                    "accountTypeId": branch["accountTypeId"],
                    "appliedKitta": str(account.quantity),
                    "crnNumber": account.crn_number,
                    "transactionPIN": account.transaction_pin,
                    "companyShareId": str(company_share_id),
                    "bankId": branch["bankId"],
                }

                if action_is_edit:
                    applicant_form_id = self._find_active_applicant_form_id(
                        headers=headers,
                        company_share_id=company_share_id,
                    )
                    if applicant_form_id is None:
                        results.append(
                            self._apply_result(
                                boid=account.boid,
                                success=False,
                                code="EDIT_FORM_NOT_FOUND",
                                message=(
                                    "Could not resolve applicantFormId for edit. "
                                    "Expected endpoint format: "
                                    f"/applicantForm/share/edit/{{applicantFormId}} for companyShareId {company_share_id}."
                                ),
                            )
                        )
                        continue

                    ok, message, status_code = self._submit_edit(
                        headers=headers,
                        applicant_form_id=applicant_form_id,
                        payload=payload,
                    )
                    if (not ok) and self._is_reapply_required_message(message):
                        reapply_ok, reapply_message, reapply_status_code = self._submit_reapply(
                            headers=headers,
                            company_share_id=company_share_id,
                            payload=payload,
                        )
                        results.append(
                            self._apply_result(
                                boid=account.boid,
                                success=reapply_ok,
                                code="REAPPLIED" if reapply_ok else "REAPPLY_REJECTED",
                                message=reapply_message,
                                upstream_status_code=reapply_status_code,
                            )
                        )
                        continue
                    code = "EDITED" if ok else "EDIT_REJECTED"
                    results.append(
                        self._apply_result(
                            boid=account.boid,
                            success=ok,
                            code=code,
                            message=message,
                            upstream_status_code=status_code,
                        )
                    )
                    continue

                can_apply, can_apply_message = self._customer_apply_status(
                    headers=headers,
                    company_share_id=company_share_id,
                    demat=ctx.demat,
                )
                if not can_apply:
                    if self._is_reapply_required_message(can_apply_message):
                        ok, message, status_code = self._submit_reapply(
                            headers=headers,
                            company_share_id=company_share_id,
                            payload=payload,
                        )
                        results.append(
                            self._apply_result(
                                boid=account.boid,
                                success=ok,
                                code="REAPPLIED" if ok else "REAPPLY_REJECTED",
                                message=message,
                                upstream_status_code=status_code,
                            )
                        )
                    else:
                        results.append(
                            self._apply_result(
                                boid=account.boid,
                                success=False,
                                code="CANNOT_APPLY",
                                message=can_apply_message or "Customer cannot apply",
                            )
                        )
                    continue

                r = requests.post(
                    f"{BASE_URL}/applicantForm/share/apply",
                    json=payload,
                    headers=headers,
                    verify=False,
                    timeout=settings.request_timeout_seconds,
                )

                if r.ok:
                    results.append(
                        self._apply_result(
                            boid=account.boid,
                            success=True,
                            code="APPLIED",
                            message="Application submitted",
                            upstream_status_code=r.status_code,
                        )
                    )
                    continue

                backend_message = ""
                try:
                    body = r.json()
                    backend_message = body.get("message") or body.get("detail") or ""
                except Exception:
                    backend_message = r.text.strip()

                message = backend_message or f"Apply rejected by upstream ({r.status_code})"
                if self._is_reapply_required_message(message):
                    ok, reapply_message, status_code = self._submit_reapply(
                        headers=headers,
                        company_share_id=company_share_id,
                        payload=payload,
                    )
                    results.append(
                        self._apply_result(
                            boid=account.boid,
                            success=ok,
                            code="REAPPLIED" if ok else "REAPPLY_REJECTED",
                            message=reapply_message,
                            upstream_status_code=status_code,
                        )
                    )
                    continue
                results.append(
                    self._apply_result(
                        boid=account.boid,
                        success=False,
                        code="APPLY_REJECTED",
                        message=message,
                        upstream_status_code=r.status_code,
                    )
                )
            except requests.RequestException as exc:
                results.append(
                    self._apply_result(
                        boid=account.boid,
                        success=False,
                        code=REQUEST_ERROR_CODE,
                        message=str(exc),
                    )
                )
            except Exception as exc:
                results.append(
                    self._apply_result(
                        boid=account.boid,
                        success=False,
                        code=self._error_code_from_exception(exc),
                        message=str(exc),
                    )
                )

        success_count = sum(1 for item in results if item.success)
        failure_count = len(results) - success_count

        return BulkApplyResponse(
            company_share_id=company_share_id,
            total=len(results),
            success_count=success_count,
            failure_count=failure_count,
            results=results,
        )

    def check_results(
        self,
        accounts: list[ResultCheckAccountRequest],
        company_share_id: int | None = None,
    ) -> ResultCheckResponse:
        if not accounts:
            accounts = self._load_result_accounts_from_csv()
        output: list[ResultStatus] = []

        for account in accounts:
            try:
                ctx = self._build_context(account.dp_id, account.boid, account.password)
                headers = self._auth_headers(ctx)

                payload = {
                    "filterFieldParams": [
                        {"key": "companyShare.companyIssue.companyISIN.script", "alias": "Scrip"},
                        {"key": "companyShare.companyIssue.companyISIN.company.name", "alias": "Company Name"},
                    ],
                    "page": 1,
                    "size": 20,
                    "searchRoleViewConstants": "VIEW_APPLICANT_FORM_COMPLETE",
                    "filterDateParams": [],
                }

                r = requests.post(
                    f"{BASE_URL}/applicantForm/active/search/",
                    json=payload,
                    headers=headers,
                    verify=False,
                    timeout=settings.request_timeout_seconds,
                )
                if not r.ok:
                    raise MeroShareError("Failed to fetch application report")

                forms = r.json().get("object", [])
                if not forms:
                    output.append(
                        ResultStatus(
                            boid=account.boid,
                            status="NO_APPLICATION",
                            allotted_quantity=0,
                            message="No recent applications found",
                            company_share_id=company_share_id,
                        )
                    )
                    continue

                latest = None
                if company_share_id is not None:
                    latest = next(
                        (item for item in forms if int(item.get("companyShareId", 0) or 0) == company_share_id),
                        None,
                    )
                else:
                    latest = forms[0]

                if latest is None:
                    output.append(
                        ResultStatus(
                            boid=account.boid,
                            status="NO_APPLICATION",
                            allotted_quantity=0,
                            message=f"No application found for share ID {company_share_id}",
                            company_share_id=company_share_id,
                        )
                    )
                    continue

                status_name = latest.get("statusName", "UNKNOWN")
                allotted = 0
                message = status_name

                application_id = latest.get("applicantFormId")
                if application_id and status_name in {"TRANSACTION_SUCCESS", "APPROVED"}:
                    detail = requests.get(
                        f"{BASE_URL}/applicantForm/report/detail/{application_id}",
                        headers=headers,
                        verify=False,
                        timeout=settings.request_timeout_seconds,
                    )
                    if detail.ok:
                        detail_body = detail.json()
                        message = detail_body.get("statusName", status_name)
                        allotted = int(detail_body.get("allotedKitta", 0) or 0)

                output.append(
                    ResultStatus(
                        boid=account.boid,
                        status=status_name,
                        allotted_quantity=allotted,
                        message=message,
                        company_share_id=int(latest.get("companyShareId", 0) or 0),
                        company_name=(latest.get("companyName") or latest.get("companyShareName")),
                    )
                )
            except Exception as exc:
                output.append(
                    ResultStatus(
                        boid=account.boid,
                        status="ERROR",
                        allotted_quantity=0,
                        message=str(exc),
                        company_share_id=company_share_id,
                    )
                )

        return ResultCheckResponse(total=len(output), results=output)

    def list_result_ipos(
        self,
        dp_id: str,
        boid_or_username: str,
        password: str,
    ) -> list[ResultIpoItem]:
        ctx = self._build_context(dp_id, boid_or_username, password)
        headers = self._auth_headers(ctx)

        payload = {
            "filterFieldParams": [
                {"key": "companyShare.companyIssue.companyISIN.script", "alias": "Scrip"},
                {"key": "companyShare.companyIssue.companyISIN.company.name", "alias": "Company Name"},
            ],
            "page": 1,
            "size": 200,
            "searchRoleViewConstants": "VIEW_APPLICANT_FORM_COMPLETE",
            "filterDateParams": [],
        }

        r = requests.post(
            f"{BASE_URL}/applicantForm/active/search/",
            json=payload,
            headers=headers,
            verify=False,
            timeout=settings.request_timeout_seconds,
        )
        if not r.ok:
            raise MeroShareError("Failed to fetch listed IPO/FPO for results")

        forms = r.json().get("object", [])
        dedup: dict[int, ResultIpoItem] = {}
        for item in forms:
            share_id = int(item.get("companyShareId", 0) or 0)
            if share_id <= 0:
                continue
            if share_id in dedup:
                continue
            company_name = str(
                item.get("companyName")
                or item.get("companyShareName")
                or item.get("scrip")
                or share_id
            )
            dedup[share_id] = ResultIpoItem(
                company_share_id=share_id,
                company_name=company_name,
                status=(item.get("statusName") or None),
            )

        return list(dedup.values())

    def list_result_ipos_default(self) -> list[ResultIpoItem]:
        if not (
            settings.default_dp_id
            and settings.default_boid_or_username
            and settings.default_password
        ):
            return []
        return self.list_result_ipos(
            dp_id=settings.default_dp_id,
            boid_or_username=settings.default_boid_or_username,
            password=settings.default_password,
        )


meroshare_service = MeroShareService()

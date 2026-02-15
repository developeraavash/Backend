from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from app.capitals import CAPITAL_BY_CODE
from app.config import settings
from app.schemas import (
    BankOptionItem,
    BulkApplyRequest,
    BulkApplyResponse,
    CapitalOptionItem,
    IPOItem,
    IpoFetchRequest,
    ResultCheckRequest,
    ResultCheckResponse,
    ResultIpoItem,
    NepseChartResponse,
    NepseNewsItem,
    IpoAnalysisRequest,
    IpoNewsArticleItem,
    MarketChatRequest,
    MarketChatResponse,
    MarketOverviewResponse,
    MarketSummaryResponse,
    StockAnalysisRequest,
    StockCompanyItem,
)
from app.service import MeroShareError, meroshare_service

app = FastAPI(title=settings.app_name)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok', 'env': settings.app_env}


@app.get(f'{settings.api_prefix}/dpids', response_model=list[str])
def get_supported_dpids() -> list[str]:
    return sorted(CAPITAL_BY_CODE.keys())


@app.get(f'{settings.api_prefix}/capitals', response_model=list[CapitalOptionItem])
def get_supported_capitals() -> list[CapitalOptionItem]:
    try:
        return meroshare_service.get_capitals()
    except MeroShareError:
        # Keep old fallback behaviour if upstream is not available.
        return [
            CapitalOptionItem(code=code, name=f'DP {code}', client_id=client_id)
            for code, client_id in sorted(CAPITAL_BY_CODE.items())
        ]


@app.get(f'{settings.api_prefix}/ipos', response_model=list[IPOItem])
def get_open_ipos() -> list[IPOItem]:
    try:
        return meroshare_service.get_open_ipos()
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post(f'{settings.api_prefix}/ipos/open', response_model=list[IPOItem])
def get_open_ipos_for_account(request: IpoFetchRequest) -> list[IPOItem]:
    try:
        return meroshare_service.get_open_ipos_for_account(
            dp_id=request.dp_id,
            boid_or_username=request.boid,
            password=request.password,
        )
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post(f'{settings.api_prefix}/banks', response_model=list[BankOptionItem])
def get_banks_for_account(request: IpoFetchRequest) -> list[BankOptionItem]:
    try:
        return meroshare_service.get_banks_for_account(
            dp_id=request.dp_id,
            boid_or_username=request.boid,
            password=request.password,
        )
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post(f'{settings.api_prefix}/apply/bulk', response_model=BulkApplyResponse)
def apply_bulk(request: BulkApplyRequest) -> BulkApplyResponse:
    return meroshare_service.apply_bulk(request.company_share_id, request.accounts)


@app.post(f'{settings.api_prefix}/results/check', response_model=ResultCheckResponse)
def check_results(request: ResultCheckRequest) -> ResultCheckResponse:
    return meroshare_service.check_results(request.accounts, request.company_share_id)


@app.post(f'{settings.api_prefix}/results/ipos', response_model=list[ResultIpoItem])
def list_result_ipos(request: IpoFetchRequest) -> list[ResultIpoItem]:
    try:
        return meroshare_service.list_result_ipos(
            dp_id=request.dp_id,
            boid_or_username=request.boid,
            password=request.password,
        )
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(f'{settings.api_prefix}/results/ipos', response_model=list[ResultIpoItem])
def list_result_ipos_default() -> list[ResultIpoItem]:
    try:
        return meroshare_service.list_result_ipos_default()
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(f'{settings.api_prefix}/nepse/chart', response_model=NepseChartResponse)
def get_nepse_chart(
    limit: int = 90,
    mode: str = "daily",
    index_id: int = 58,
) -> NepseChartResponse:
    try:
        return meroshare_service.get_nepse_chart(
            limit=limit,
            mode=mode,
            index_id=index_id,
        )
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(f'{settings.api_prefix}/nepse/news', response_model=list[NepseNewsItem])
def get_nepse_news(limit: int = 100) -> list[NepseNewsItem]:
    return meroshare_service.get_nepse_news(limit=limit)


@app.get(f'{settings.api_prefix}/market/overview', response_model=MarketOverviewResponse)
def get_market_overview() -> MarketOverviewResponse:
    try:
        return meroshare_service.get_market_overview()
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(f'{settings.api_prefix}/market/summary', response_model=MarketSummaryResponse)
def get_market_summary(language: str = "en") -> MarketSummaryResponse:
    try:
        return meroshare_service.get_market_summary(language=language)
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(f'{settings.api_prefix}/market/ipos/news', response_model=list[IpoNewsArticleItem])
def get_ipo_news_articles(limit: int = 5) -> list[IpoNewsArticleItem]:
    try:
        return meroshare_service.get_ipo_news_articles(limit=limit)
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post(f'{settings.api_prefix}/market/analysis/ipo', response_model=MarketSummaryResponse)
def analyze_ipo_article(request: IpoAnalysisRequest) -> MarketSummaryResponse:
    try:
        return meroshare_service.analyze_ipo_article(request)
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(f'{settings.api_prefix}/market/companies', response_model=list[StockCompanyItem])
def get_market_companies() -> list[StockCompanyItem]:
    try:
        return meroshare_service.get_market_companies()
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get(f'{settings.api_prefix}/market/companies/{{symbol}}')
def get_market_company_details(symbol: str) -> dict:
    try:
        return meroshare_service.get_market_company_details(symbol)
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post(f'{settings.api_prefix}/market/analysis/stock', response_model=MarketSummaryResponse)
def analyze_stock_details(request: StockAnalysisRequest) -> MarketSummaryResponse:
    try:
        return meroshare_service.analyze_stock_details(request)
    except MeroShareError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post(f'{settings.api_prefix}/market/chat', response_model=MarketChatResponse)
def market_chat(request: MarketChatRequest, client_request: Request) -> MarketChatResponse:
    client_host = client_request.client.host if client_request.client else "unknown"
    session_key = request.session_id.strip() if request.session_id else client_host
    try:
        return meroshare_service.ask_market_chat(request, client_key=session_key)
    except MeroShareError as exc:
        status_code = 429 if "Rate limit exceeded" in str(exc) else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc

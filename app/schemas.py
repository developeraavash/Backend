from pydantic import AliasChoices, BaseModel, Field
from typing import Any, Optional


class IPOItem(BaseModel):
    company_share_id: int
    company_name: str
    issue_manager: str
    min_quantity: int
    max_quantity: int
    is_applied: bool


class IpoFetchRequest(BaseModel):
    dp_id: str = Field(..., min_length=5, validation_alias=AliasChoices("dp_id", "dp"))
    boid: str = Field(
        ...,
        min_length=8,
        validation_alias=AliasChoices("boid", "username"),
        description="Either BOID/Demat or MeroShare username",
    )
    password: str = Field(..., min_length=1)


class AccountApplyRequest(BaseModel):
    dp_id: str = Field(..., min_length=5, validation_alias=AliasChoices("dp_id", "dp"))
    boid: str = Field(
        ...,
        min_length=8,
        validation_alias=AliasChoices("boid", "username"),
        description="Either BOID/Demat or MeroShare username",
    )
    password: str = Field(..., min_length=1)
    quantity: int = Field(10, gt=0)
    crn_number: str = Field(..., min_length=4, validation_alias=AliasChoices("crn_number", "crn"))
    transaction_pin: str = Field(
        ...,
        min_length=4,
        validation_alias=AliasChoices("transaction_pin", "pin"),
    )
    bank_id: Optional[int] = None


class BulkApplyRequest(BaseModel):
    company_share_id: int = Field(..., validation_alias=AliasChoices("company_share_id", "companyShareId"))
    accounts: list[AccountApplyRequest] = Field(default_factory=list)


class AccountApplyResult(BaseModel):
    boid: str
    success: bool
    code: str
    message: str
    upstream_status_code: Optional[int] = None


class BulkApplyResponse(BaseModel):
    company_share_id: int
    total: int
    success_count: int
    failure_count: int
    results: list[AccountApplyResult]


class ResultCheckAccountRequest(BaseModel):
    dp_id: str = Field(..., min_length=5, validation_alias=AliasChoices("dp_id", "dp"))
    boid: str = Field(
        ...,
        min_length=8,
        validation_alias=AliasChoices("boid", "username"),
        description="Either BOID/Demat or MeroShare username",
    )
    password: str = Field(..., min_length=1)


class ResultCheckRequest(BaseModel):
    accounts: list[ResultCheckAccountRequest] = Field(default_factory=list)
    company_share_id: Optional[int] = Field(
        None,
        validation_alias=AliasChoices("company_share_id", "companyShareId"),
    )


class ResultStatus(BaseModel):
    boid: str
    status: str
    allotted_quantity: int
    message: str
    company_share_id: Optional[int] = None
    company_name: Optional[str] = None


class ResultCheckResponse(BaseModel):
    total: int
    results: list[ResultStatus]


class ResultIpoItem(BaseModel):
    company_share_id: int
    company_name: str
    status: Optional[str] = None


class BankOptionItem(BaseModel):
    bank_id: int
    bank_name: str


class CapitalOptionItem(BaseModel):
    code: str
    name: str
    client_id: int


class NepseChartPoint(BaseModel):
    date: str
    value: float


class NepseChartResponse(BaseModel):
    source: str
    mode: str = "daily"
    trading_date: Optional[str] = None
    points: list[NepseChartPoint]


class NepseNewsItem(BaseModel):
    title: str
    url: str
    published_at: Optional[str] = None
    source: str = "merolagani"


class MarketMoverItem(BaseModel):
    symbol: str
    ltp: Optional[float] = None
    point_change: Optional[float] = None
    percentage_change: Optional[float] = None
    turnover: Optional[float] = None


class MarketIndexItem(BaseModel):
    index: str
    current_value: Optional[float] = None
    change: Optional[float] = None
    per_change: Optional[float] = None


class MarketOverviewResponse(BaseModel):
    status: str
    gainers: list[MarketMoverItem]
    losers: list[MarketMoverItem]
    turnover: list[MarketMoverItem]
    indices: list[MarketIndexItem]


class MarketSummaryResponse(BaseModel):
    summary: str
    language: str = "en"
    provider: str = "fallback"
    used_fallback: bool = True


class IpoNewsArticleItem(BaseModel):
    title: str
    date: str
    link: str
    content: str


class IpoAnalysisRequest(BaseModel):
    title: str = Field(..., min_length=1)
    content: str = Field(..., min_length=1)
    language: str = "en"


class StockCompanyItem(BaseModel):
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None


class StockAnalysisRequest(BaseModel):
    symbol: str = Field(..., min_length=1)
    details: dict[str, Any]
    language: str = "en"


class MarketChatContext(BaseModel):
    type: str
    name: str
    data: str


class MarketChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    language: str = "en"
    session_id: Optional[str] = None
    context: Optional[MarketChatContext] = None


class MarketChatResponse(BaseModel):
    response: str
    language: str = "en"
    provider: str = "fallback"
    used_fallback: bool = True

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Bulk IPO Apply API"
    app_env: str = "development"
    api_prefix: str = "/api/v1"

    default_dp_id: str = ""
    default_boid_or_username: str = ""
    default_password: str = ""

    request_timeout_seconds: int = 30
    gemini_api_key: str = ""
    gemini_api_keys: str = ""
    gemini_api_keys_file: str = "app/api.txt"
    market_hub_chat_messages_per_minute: int = 2
    accounts_csv_path: str = "accounts.csv"
    default_apply_quantity: int = 10
    result_check_max_workers: int = 8

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


settings = Settings()

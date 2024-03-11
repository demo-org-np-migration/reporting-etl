"""Configuración por variables de entorno (las convenciones internas de API §1).

Todo entra por env, nada hardcodeado salvo los defaults documentados acá.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    service_name: str = "reporting-etl"
    env: str = "staging"
    port: int = 8080
    log_level: str = "INFO"

    kafka_bootstrap: str
    kafka_topic_prefix: str = ""
    kafka_group_id: str = "reporting-etl"

    db_host: str
    db_port: int = 5432
    db_name: str = "warehouse"
    db_user: str
    db_password: str

    @property
    def payments_completed_topic(self) -> str:
        return f"{self.kafka_topic_prefix}payments.completed"

    @property
    def dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )


def get_settings() -> Settings:
    return Settings()

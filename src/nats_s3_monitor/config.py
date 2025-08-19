from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping
import boto3
from polars_hist_db.config.helpers import load_yaml, get_nested_key

from pyapi_service_kit.service import validate_guid
from pyapi_service_kit.nats import NatsConfig

from .cache.latest_file_cache import LatestFileCache


@dataclass
class MonitorConfig:
    id: str
    bucket: str
    lookback: str
    lookback_step: str
    search_prefix: str
    update_interval: str
    file_time_extract: str
    output_subject: str
    output_stream: str

    _cache: LatestFileCache = field(default_factory=LatestFileCache)


@dataclass
class MonitorsConfig:
    monitors: List[MonitorConfig]

    @classmethod
    def from_dict(cls, data: List[Dict[str, Any]]) -> "MonitorsConfig":
        return cls(
            monitors=[
                MonitorConfig(**monitor)
                for monitor in data
                if isinstance(monitor, dict)
            ]
        )


@dataclass
class S3Config:
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    default_region: str

    def client(self) -> Any:
        return boto3.client(
            "s3",
            region_name=self.default_region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )


@dataclass
class ServiceConfig:
    instance_id: str
    time_service_subject: str

    def __post_init__(self):
        self.instance_id = validate_guid(self.instance_id)


class Config:
    _borg: Dict[str, Any] = {}

    # for auto-complete
    nats_config: NatsConfig
    s3_config: S3Config
    service_config: ServiceConfig
    monitors_config: MonitorsConfig

    def __init__(self):
        self.__dict__ = self._borg

    @classmethod
    def from_yaml(
        cls,
        filename: str,
        nats_config_path: str = "nats",
        s3_config_path: str = "s3",
        service_config_path: str = "service",
        monitors_config_path: str = "monitors",
    ) -> "Config":
        yaml_dict: Mapping[str, Any] = load_yaml(filename)

        raw_nats_config = get_nested_key(yaml_dict, nats_config_path.split("."))
        nats_config = NatsConfig.from_dict(raw_nats_config)

        raw_s3_config = get_nested_key(yaml_dict, s3_config_path.split("."))
        s3_config = S3Config(**raw_s3_config)

        raw_service_config = get_nested_key(yaml_dict, service_config_path.split("."))
        service_config = ServiceConfig(**raw_service_config)

        raw_monitors_config = get_nested_key(yaml_dict, monitors_config_path.split("."))
        monitors_config = MonitorsConfig.from_dict(raw_monitors_config)

        # Populate the _borg state directly since this is a classmethod
        cls._borg = {
            "nats_config": nats_config,
            "s3_config": s3_config,
            "service_config": service_config,
            "monitors_config": monitors_config,
            "config_filename": filename,
        }

        return cls()

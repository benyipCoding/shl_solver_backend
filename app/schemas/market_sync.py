from pydantic import BaseModel, Field


class SyncQueueAddRequest(BaseModel):
    instrument_ids: list[int] = Field(
        default_factory=list,
        max_length=500,
        description="本地已有品种的 ID。",
    )
    symbols: list[str] = Field(
        default_factory=list,
        max_length=50,
        description="本地尚未落库的 FXCM 品种符号，会先经 sidecar 注册。",
    )


class SyncQueueRemoveRequest(BaseModel):
    instrument_ids: list[int] = Field(
        ...,
        min_length=1,
        max_length=500,
        description="要移出采集队列的品种 ID。",
    )

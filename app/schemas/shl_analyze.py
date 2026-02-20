from pydantic import BaseModel


class ImageData(BaseModel):
    mimeType: str
    data: str


class SHLAnalyzePayload(BaseModel):
    images_data: list[ImageData]
    llmKey: str


class SHLAnalyzeResult(BaseModel):
    summary: str
    key_concepts: list[str]
    constraints: list[str]
    solutions: dict[str, str]
    complexity: dict[str, str]

from pydantic import BaseModel


class ImageData(BaseModel):
    mimeType: str
    data: str


class SHLAnalyzePayload(BaseModel):
    images_data: list[ImageData]
    llmId: int


class SHLAnalyzeResult(BaseModel):
    summary: str
    key_concepts: list[str]
    constraints: list[str]
    solutions: dict[str, str]
    complexity: dict[str, str]


class SHLCodeVerifyPayload(BaseModel):
    image_data: ImageData  # single image for verification
    reference_code: str
    language: str = "python"  # default to python


class CodeVerifyError(BaseModel):
    reference_line: int
    type: str
    expected_segment: str
    found_segment: str
    message: str


class SHLCodeVerifyResult(BaseModel):
    summary: str
    has_errors: bool
    errors: list[CodeVerifyError]

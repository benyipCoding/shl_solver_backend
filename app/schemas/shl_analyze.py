from typing import Optional
from pydantic import BaseModel


class ImageData(BaseModel):
    mimeType: str
    data: str


class SHLAnalyzePayload(BaseModel):
    images_data: list[ImageData]
    llmId: int
    test_case_image: Optional[ImageData] = None


class SHLAnalyzeResult(BaseModel):
    summary: str
    key_concepts: list[str]
    constraints: list[str]
    solutions: dict[str, str]
    complexity: dict[str, str] | str


class SHLCodeVerifyPayload(BaseModel):
    images_data: Optional[list[ImageData]] = (
        None  # support multiple images for verification
    )
    image_data: Optional[ImageData] = None
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


class TaskSubmitResponse(BaseModel):
    task_id: str
    status: str

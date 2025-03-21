
from pydantic import BaseModel


class SignedURLResponse(BaseModel):
    url: str
    fields: list[str] = []

import math
from datetime import date, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class Reading(BaseModel):
    model_config = {"frozen": True}

    id: UUID = Field(default_factory=uuid4)
    city: str
    created: datetime
    value: float

    @field_validator("value")
    @classmethod
    def value_in_range(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("value must be between 0.0 and 1.0")
        return v

    @property
    def day(self) -> date:
        return self.created.date()

    @property
    def value_bucket(self) -> int:
        # 10 buckets: 0→[0.0,0.1), 1→[0.1,0.2), …, 9→[0.9,1.0]
        return min(int(math.floor(self.value * 10)), 9)

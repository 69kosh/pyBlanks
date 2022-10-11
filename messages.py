from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


class BaseMessage(BaseModel):
	id: str
	createdAt: datetime = Field(default_factory=datetime.now)
	repeated: int = 0
	repeatedAt: datetime | None


class StatusMessageType(str, Enum):
	STARTED = 'started'
	SUCCESS = 'success'
	FAILURE = 'failure'

	def __str__(self):
		return str(self.value)


class StatusMessage(BaseMessage):
	type: StatusMessageType = StatusMessageType.STARTED
	progress: float = 0.0
	description: str | None = None
	raw: str | None = None
	
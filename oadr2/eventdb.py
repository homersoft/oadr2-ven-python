from datetime import datetime
from typing import List, Optional, Sequence, Tuple, Dict, Union

from sqlalchemy import (Column, Float, ForeignKey, Integer, String,
                        create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, sessionmaker
from oadr2.schemas import EventSchema

Base = declarative_base()


class Signal(Base):
    __tablename__ = "signals"

    event_id = Column(String, ForeignKey("events.id"), primary_key=True)
    index = Column(Integer, primary_key=True)
    duration = Column(String)
    level = Column(Float)


class Event(Base):
    __tablename__ = "events"

    id = Column(String, primary_key=True, index=True, unique=True)
    mod_number = Column(Integer, nullable=False, default=0)
    _start = Column(String)
    _original_start = Column(String)
    _end = Column(String)
    _signals = relationship("Signal", cascade="all")
    cancellation_offset = Column(String)
    status = Column(String)

    @property
    def start(self) -> datetime:
        return datetime.fromisoformat(self._start)

    @start.setter
    def start(self, value: datetime) -> None:
        self._start = value.isoformat()

    @property
    def original_start(self) -> datetime:
        return datetime.fromisoformat(self._original_start)

    @original_start.setter
    def original_start(self, value: datetime) -> None:
        self._original_start = value.isoformat()

    @property
    def end(self) -> datetime:
        return datetime.fromisoformat(self._end)

    @end.setter
    def end(self, value: datetime) -> None:
        self._end = value.isoformat()

    @property
    def signals(self) -> List[Dict[str, Union[float, int, str]]]:
        return [dict(duration=signal.duration, index=signal.index, level=signal.level) for signal in self._signals]

    @signals.setter
    def signals(self, value: List[Dict[str, Union[float, int, str]]]) -> None:
        self._signals = [Signal(
            event_id=self.id, index=signal["index"], duration=signal["duration"], level=signal["level"]
        ) for signal in value]


class DBHandler:
    def __init__(self, db_path: str):
        engine = create_engine(f"sqlite:///{db_path}")
        self.session: Session = sessionmaker(bind=engine, autocommit=True)()
        Event.metadata.create_all(engine)

    def get_active_events(self) -> List[EventSchema]:
        return [EventSchema.from_orm(evt) for evt in self.session.query(Event).all()]

    def update_event(self, event: EventSchema) -> None:
        accepted_params = {"id", "mod_number", "start", "original_start", "end", "signals",
                           "cancellation_offset", "status"}
        db_item = Event(**event.dict(include=accepted_params))
        self.session.add(db_item)

    def get_event(self, event_id: str) -> Optional[EventSchema]:
        evt = self.session.query(Event).filter_by(id=event_id).first()
        return EventSchema.from_orm(evt) if evt else None

    def remove_events(self, event_ids: Sequence[str]) -> None:
        for event_id in event_ids:
            self.session.query(Event).filter_by(id=event_id).delete()

from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import random
import string
import operator
from functools import reduce
from typing import List, Optional

from lxml import etree


def random_uid() -> str:
    chars = [x for x in string.ascii_lowercase] + [str(x) for x in range(10)]
    series_lengths = [8, 4, 4, 4, 12]

    def random_string(length):
        return ''.join(random.choice(chars) for _ in range(length))

    uid = "-".join(random_string(x) for x in series_lengths)
    return uid


def format_duration(duration: timedelta) -> str:
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"P0Y0M0DT{hours}H{minutes}M{seconds}S"


def format_datetime(time: datetime) -> str:
    return f"{time.isoformat()}Z"


class AdrEventStatus(Enum):
    PENDING = "near"
    ACTIVE = "active"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


@dataclass
class AdrInterval:
    value: float
    duration: timedelta

    def __post_init__(self):
        self.uid = random_uid()
        self.start_time: Optional[datetime] = None

    def to_xml(self):
        return f"""
        <ei:interval>
          <ical:duration>
            <ical:duration>{format_duration(self.duration)}</ical:duration>
          </ical:duration>
          <ical:uid>
            <ical:text>{self.uid}</ical:text>
          </ical:uid>
          <ei:signalPayload>
            <ei:payloadFloat>
              <ei:value>{self.value}</ei:value>
            </ei:payloadFloat>
          </ei:signalPayload>
        </ei:interval>"""


@dataclass
class AdrEvent:
    start_time: datetime
    status: AdrEventStatus
    intervals: List[AdrInterval]

    def __post_init__(self):
        self.event_uid = random_uid()
        self.signal_uid = random_uid()
        self.fill_intervals_start_times()

    def fill_intervals_start_times(self):
        start_time = self.start_time
        for interval in self.intervals:
            interval.start_time = start_time
            start_time += interval.duration

    @property
    def overall_duration(self) -> timedelta:
        intervals_durations = [i.duration for i in self.intervals]
        return reduce(operator.add, intervals_durations, timedelta(seconds=0))

    @property
    def stop_time(self) -> datetime:
        return self.start_time + self.overall_duration

    def interval_start_time(self, n):
        intervals_durations = [i.duration for i in self.intervals[:n]]
        return self.start_time + reduce(operator.add, intervals_durations, timedelta(seconds=0))

    def to_etree(self):
        utf8_parser = etree.XMLParser(encoding="utf-8")
        return etree.fromstring(self.to_xml().encode("utf-8"), parser=utf8_parser)

    def to_xml(self):
        intervals_xml = "".join([i.to_xml() for i in self.intervals])
        return f"""
<ei:eiEvent
  xmlns:ei="http://docs.oasis-open.org/ns/energyinterop/201110"
  xmlns="http://openadr.org/oadr-2.0a/2012/07"
  xmlns:emix="http://docs.oasis-open.org/ns/emix/2011/06"
  xmlns:pyld="http://docs.oasis-open.org/ns/energyinterop/201110/payloads"
  xmlns:strm="urn:ietf:params:xml:ns:icalendar-2.0:stream"
  xmlns:ical="urn:ietf:params:xml:ns:icalendar-2.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ei:eventDescriptor>
    <ei:eventID>{self.event_uid}</ei:eventID>
    <ei:modificationNumber>1</ei:modificationNumber>
    <ei:priority>1</ei:priority>
    <ei:eiMarketContext>
      <emix:marketContext>http://some-url</emix:marketContext>
    </ei:eiMarketContext>
    <ei:createdDateTime>2020-01-01T13:00:00.000Z</ei:createdDateTime>
    <ei:eventStatus>{self.status.value}</ei:eventStatus>
    <ei:testEvent>False</ei:testEvent>
    <ei:vtnComment></ei:vtnComment>
  </ei:eventDescriptor>
  <ei:eiActivePeriod>
    <ical:properties>
      <ical:dtstart>
        <ical:date-time>{format_datetime(self.start_time)}</ical:date-time>
      </ical:dtstart>
      <ical:duration>
        <ical:duration>{format_duration(self.overall_duration)}</ical:duration>
      </ical:duration>
      <ical:tolerance>
        <ical:tolerate>
          <ical:startafter>P0Y0M0DT0H0M0S</ical:startafter>
        </ical:tolerate>
      </ical:tolerance>
      <ei:x-eiNotification>
        <ical:duration>P0Y0M0DT0H0M0S</ical:duration>
      </ei:x-eiNotification>
      <ei:x-eiRampUp>
        <ical:duration>P0Y0M0DT0H0M0S</ical:duration>
      </ei:x-eiRampUp>
      <ei:x-eiRecovery>
        <ical:duration>P0Y0M0DT0H0M0S</ical:duration>
      </ei:x-eiRecovery>
    </ical:properties>
    <ical:components xsi:nil="true"/>
  </ei:eiActivePeriod>
  <ei:eiEventSignals>
    <ei:eiEventSignal>
      <strm:intervals>{intervals_xml}
      </strm:intervals>
      <ei:signalName>simple</ei:signalName>
      <ei:signalType>level</ei:signalType>
      <ei:signalID>{self.signal_uid}</ei:signalID>
      <ei:currentValue>
        <ei:payloadFloat>
          <ei:value>0.0</ei:value>
        </ei:payloadFloat>
      </ei:currentValue>
    </ei:eiEventSignal>
  </ei:eiEventSignals>
  <ei:eiTarget/>
</ei:eiEvent>"""


def generate():
    xml_dir = 'xml_files/signal_level_files'

    event = AdrEvent(
        start_time=datetime(year=2020, month=3, day=18, hour=20),
        status=AdrEventStatus.PENDING,
        intervals=[
            AdrInterval(2.0, timedelta(hours=2)),
            AdrInterval(2.0, timedelta(hours=3)),
        ],
    )

    xml_text = event.to_xml()

    # with open(f"{xml_dir}/event_pending.xml", "w") as f:
    #     f.write(xml_text)

    # print(xml_text)
    print([i.start_time for i in event.intervals])


if __name__ == "__main__":
    generate()

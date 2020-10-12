from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Union

from lxml import etree

from oadr2.schemas import EventSchema, SignalSchema


def format_duration(duration: Union[timedelta, None]) -> str:
    if not duration:
        return "PT0M"
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
    index: int
    level: float
    duration: timedelta

    def to_xml(self):
        return f"""
        <ei:interval>
          <ical:duration>
            <ical:duration>{format_duration(self.duration)}</ical:duration>
          </ical:duration>
          <ical:uid>
            <ical:text>{self.index}</ical:text>
          </ical:uid>
          <ei:signalPayload>
            <ei:payloadFloat>
              <ei:value>{self.level}</ei:value>
            </ei:payloadFloat>
          </ei:signalPayload>
        </ei:interval>"""


class AdrEvent:
    def __init__(
            self,
            id: Union[str, None],
            start: datetime,
            signals: List[Dict[str, Union[float, int, timedelta]]],
            status: AdrEventStatus,
            mod_number: Optional[int] = 0,
            end: Optional[datetime] = None,
            start_before: Optional[timedelta] = None,
            start_after: Optional[timedelta] = None,
            original_start: datetime = None,
            cancellation_offset: timedelta = None,
            group_ids: Optional[List[str]] = None,
            resource_ids: Optional[List[str]] = None,
            party_ids: Optional[List[str]] = None,
            ven_ids: Optional[List[str]] = ['VEN_ID'],
            vtn_id: Optional[str] = "TH_VTN",
            market_context: Optional[str] = "http://market.context",
            test_event: bool = False,
            priority: int = 1,
            response_required: bool = True,
            signal_name: str = "simple"
    ):

        self.id = id
        self.start = start
        self.original_start = original_start or start

        self.cancellation_offset = cancellation_offset
        self.raw_signals = [signal for signal in signals]
        self.signals = signals
        self.intervals = [AdrInterval(**signal) for signal in self.signals]
        self.duration = timedelta()
        for signal in self.signals:
            self.duration += signal["duration"]
        self.end = end or self.start + self.duration

        self.group_ids = group_ids
        self.resource_ids = resource_ids
        self.party_ids = party_ids
        self.ven_ids = ven_ids
        self.mod_number = mod_number
        self.status = status
        self.start_before = start_before
        self.start_after = start_after
        self.vtn_id = vtn_id
        self.market_context = market_context
        self.created_date = datetime(2020, 1, 1, 10, 10)
        self.test_event = test_event
        self.priority = priority
        self.response_required = response_required
        self.signal_name = signal_name

    def to_obj(self):
        _signals = [
            dict(
                index=s["index"],
                level=s["level"],
                duration=format_duration(s["duration"])
            ) for s in self.signals
        ]
        return EventSchema(
            id=self.id,
            vtn_id=self.vtn_id,
            mod_number=self.mod_number,
            start=self.start,
            original_start=self.original_start,
            end=self.end,
            signals=[SignalSchema(**signal) for signal in _signals],
            status=self.status.value,
            cancellation_offset=format_duration(self.cancellation_offset) if self.cancellation_offset else None,
            ven_ids=self.ven_ids,
            market_market_context=self.market_context,
            group_ids=self.group_ids,
            resource_ids=self.resource_ids,
            party_ids=self.party_ids,
            test_event=self.test_event,
            priority=self.priority
        )

    def to_xml(self):
        intervals_xml = "".join([interval.to_xml() for interval in self.intervals])
        start_after = f"""<ical:tolerance>
        <ical:tolerate>
          <ical:startafter>{format_duration(self.start_after)}</ical:startafter>
        </ical:tolerate>
      </ical:tolerance>""" if self.start_after else ""

        ven_xml = f"<ei:venID>{','.join(self.ven_ids)}</ei:venID>" if self.ven_ids else ""
        group_xml = f"<ei:groupID>{','.join(self.group_ids)}</ei:groupID>" if self.group_ids else ""
        resource_xml = f"<ei:resourceID>{','.join(self.resource_ids)}</ei:resourceID>" if self.resource_ids else ""
        party_xml = f"<ei:partyID>{','.join(self.party_ids)}</ei:partyID>" if self.party_ids else ""

        return f"""
<oadrEvent>
  <ei:eiEvent>
    <ei:eventDescriptor>
      <ei:eventID>{self.id}</ei:eventID>
      <ei:modificationNumber>{self.mod_number}</ei:modificationNumber>
      <ei:priority>{self.priority}</ei:priority>
      <ei:eiMarketContext>
        <emix:marketContext>{self.market_context}</emix:marketContext>
      </ei:eiMarketContext>
      <ei:createdDateTime>{format_datetime(self.created_date)}</ei:createdDateTime>
      <ei:eventStatus>{self.status.value}</ei:eventStatus>
      <ei:testEvent>{self.test_event}</ei:testEvent>
      <ei:vtnComment></ei:vtnComment>
    </ei:eventDescriptor>
    <ei:eiActivePeriod>
      <ical:properties>
        <ical:dtstart>
          <ical:date-time>{format_datetime(self.start)}</ical:date-time>
        </ical:dtstart>
        <ical:duration>
          <ical:duration>{format_duration(self.duration)}</ical:duration>
        </ical:duration>
        {start_after}
        <ei:x-eiNotification>
          <ical:duration>P0Y0M0DT0H0M0S</ical:duration>
        </ei:x-eiNotification>
      </ical:properties>
      <ical:components xsi:nil="true"/>
    </ei:eiActivePeriod>
    <ei:eiEventSignals>
      <ei:eiEventSignal>
        <strm:intervals>
          {intervals_xml}
        </strm:intervals>
        <ei:signalName>{self.signal_name}</ei:signalName>
        <ei:signalType>level</ei:signalType>
        <ei:signalID>SignalID</ei:signalID>
        <ei:currentValue>
          <ei:payloadFloat>
            <ei:value>0.0</ei:value>
          </ei:payloadFloat>
        </ei:currentValue>
      </ei:eiEventSignal>
    </ei:eiEventSignals>
    <ei:eiTarget>
      {ven_xml}
      {party_xml}
      {resource_xml}
      {group_xml}
    </ei:eiTarget>
  </ei:eiEvent>
  <oadrResponseRequired>{"always" if self.response_required else "never"}</oadrResponseRequired>
</oadrEvent>
"""


def generate_payload(event_list, vtn_id="TH_VTN"):
    evt_xml = "".join([event.to_xml() for event in event_list])
    template = f"""
<oadrDistributeEvent  
  xmlns="http://openadr.org/oadr-2.0a/2012/07"
  xmlns:ei="http://docs.oasis-open.org/ns/energyinterop/201110"
  xmlns:emix="http://docs.oasis-open.org/ns/emix/2011/06"
  xmlns:pyld="http://docs.oasis-open.org/ns/energyinterop/201110/payloads"
  xmlns:strm="urn:ietf:params:xml:ns:icalendar-2.0:stream"
  xmlns:ical="urn:ietf:params:xml:ns:icalendar-2.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
>
  <eiResponse>
    <responseCode>200</responseCode>
    <pyld:requestID/>
  </eiResponse>
  <pyld:requestID>OadrDisReq092520_152645_178</pyld:requestID>
  <ei:vtnID>{vtn_id}</ei:vtnID>
    {evt_xml}
</oadrDistributeEvent>
"""
    # print(template)
    return etree.fromstring(template)

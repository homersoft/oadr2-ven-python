from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional, Union
from lxml import etree
from oadr2 import schedule


# Stuff for the 2.0a spec of OpenADR
OADR_XMLNS_A = 'http://openadr.org/oadr-2.0a/2012/07'
PYLD_XMLNS_A = 'http://docs.oasis-open.org/ns/energyinterop/201110/payloads'
EI_XMLNS_A = 'http://docs.oasis-open.org/ns/energyinterop/201110'
EMIX_XMLNS_A = 'http://docs.oasis-open.org/ns/emix/2011/06'
XCAL_XMLNS_A = 'urn:ietf:params:xml:ns:icalendar-2.0'
STRM_XMLNS_A = 'urn:ietf:params:xml:ns:icalendar-2.0:stream'
NS_A = {
    'oadr': OADR_XMLNS_A,
    'pyld': PYLD_XMLNS_A,
    'ei': EI_XMLNS_A,
    'emix': EMIX_XMLNS_A,
    'xcal': XCAL_XMLNS_A,
    'strm': STRM_XMLNS_A
}

# Stuff for the 2.0b spec of OpenADR
OADR_XMLNS_B = 'http://openadr.org/oadr-2.0b/2012/07'
DSIG11_XMLNS_B = 'http://www.w3.org/2009/xmldsig11#'
DS_XMLNS_B = 'http://www.w3.org/2000/09/xmldsig#'
CLM5ISO42173A_XMLNS_B = 'urn:un:unece:uncefact:codelist:standard:5:ISO42173A:2010-04-07'
SCALE_XMLNS_B = 'http://docs.oasis-open.org/ns/emix/2011/06/siscale'
POWER_XMLNS_B = 'http://docs.oasis-open.org/ns/emix/2011/06/power'
GB_XMLNS_B = 'http://naesb.org/espi'
ATOM_XMLNS_B = 'http://www.w3.org/2005/Atom'
CCTS_XMLNS_B = 'urn:un:unece:uncefact:documentation:standard:CoreComponentsTechnicalSpecification:2'
GML_XMLNS_B = 'http://www.opengis.net/gml/3.2'
GMLSF_XMLNS_B = 'http://www.opengis.net/gmlsf/2.0'
XSI_XMLNS_B = 'http://www.w3.org/2001/XMLSchema-instance'
NS_B = {  # If you see an 2.0a variable used here, that means that the namespace is the same
    'oadr': OADR_XMLNS_B,
    'pyld': PYLD_XMLNS_A,
    'ei': EI_XMLNS_A,
    'emix': EMIX_XMLNS_A,
    'xcal': XCAL_XMLNS_A,
    'strm': STRM_XMLNS_A,
    'dsig11': DSIG11_XMLNS_B,
    'ds': DS_XMLNS_B,
    'clm': CLM5ISO42173A_XMLNS_B,
    'scale': SCALE_XMLNS_B,
    'power': POWER_XMLNS_B,
    'gb': GB_XMLNS_B,
    'atom': ATOM_XMLNS_B,
    'ccts': CCTS_XMLNS_B,
    'gml': GML_XMLNS_B,
    'gmlsf': GMLSF_XMLNS_B,
    'xsi': XSI_XMLNS_B
}

# Other important constants that we need
VALID_SIGNAL_TYPES = ('level', 'price', 'delta', 'setpoint')
OADR_PROFILE_20A = '2.0a'
OADR_PROFILE_20B = '2.0b'


class SignalSchema(BaseModel):
    index: int
    duration: str
    level: float

    class Config:
        orm_mode = True


class EventSchema(BaseModel):
    id: Union[str, None]
    start: datetime
    original_start: datetime
    end: datetime
    cancellation_offset: Union[str, None]
    signals: List[SignalSchema]
    group_ids: Optional[List[str]]
    resource_ids: Optional[List[str]]
    party_ids: Optional[List[str]]
    ven_ids: Optional[List[str]]
    market_context: Optional[str]
    mod_number: int
    status: str
    test_event: bool

    class Config:
        orm_mode = True

    def get_current_interval(self, now=datetime.utcnow()) -> Union[SignalSchema, None]:
        if self.start > now < self.end:
            return None

        previous_signal_end = self.start
        for signal in self.signals:
            current_signal_end = previous_signal_end + schedule.duration_to_delta(signal.duration)[0]
            if previous_signal_end < now <= current_signal_end:
                return signal
            previous_signal_end = current_signal_end
        # TODO: neverending events

    def cancel(self):
        if self.status == "active":
            self.end = schedule.random_offset(datetime.utcnow(), 0, self.cancellation_offset) if self.cancellation_offset else datetime.utcnow()
        else:
            self.end = datetime.utcnow()
        self.status = "cancelled"

    @staticmethod
    def from_xml(evt_xml: etree.XML):
        # print("###")
        # print(etree.tostring(evt_xml, pretty_print=True).decode("utf-8"))
        event_id = EventSchema.get_event_id(evt_xml)
        event_original_start = EventSchema.get_active_period_start(evt_xml)
        signal_list = EventSchema.get_signals(evt_xml)
        event_signals = (
            [SignalSchema(duration=evt[0], index=int(evt[1]), level=float(evt[2])) for evt in signal_list]
            if signal_list
            else []
        )
        event_group_ids = EventSchema.get_group_ids(evt_xml)
        event_resource_ids = EventSchema.get_resource_ids(evt_xml)
        event_party_ids = EventSchema.get_party_ids(evt_xml)
        event_ven_ids = EventSchema.get_ven_ids(evt_xml)
        event_market_context = EventSchema.get_market_context(evt_xml)
        event_mod_number = EventSchema.get_mod_number(evt_xml)
        event_status = EventSchema.get_status(evt_xml)

        start_offset = EventSchema.get_start_before_after(evt_xml)
        event_start = schedule.random_offset(event_original_start, *start_offset)

        event_duration = EventSchema.get_active_period_duration(evt_xml)[0]
        ending_time = event_duration + event_original_start
        event_test = EventSchema.get_test_event(evt_xml)

        return EventSchema(
            id=event_id,
            signals=event_signals,
            start=event_start,
            end=ending_time,
            cancellation_offset=start_offset[1],
            original_start=event_original_start,
            group_ids=event_group_ids,
            resource_ids=event_resource_ids,
            party_ids=event_party_ids,
            ven_ids=event_ven_ids,
            market_context=event_market_context,
            mod_number=event_mod_number,
            status=event_status,
            test_event=event_test
        )

    @staticmethod
    def get_event_id(evt, ns_map=NS_A):
        return evt.findtext("ei:eventDescriptor/ei:eventID", namespaces=ns_map)

    @staticmethod
    def get_status(evt, ns_map=NS_A):
        return evt.findtext("ei:eventDescriptor/ei:eventStatus", namespaces=ns_map)

    @staticmethod
    def get_test_event(evt, ns_map=NS_A):
        test_event = evt.findtext("ei:eventDescriptor/ei:testEvent", namespaces=ns_map)
        if not test_event or test_event.lower() == "false":
            return False
        else:
            return True

    @staticmethod
    def get_mod_number(evt, ns_map=NS_A):
        return int(evt.findtext(
            "ei:eventDescriptor/ei:modificationNumber",
            namespaces=ns_map))

    @staticmethod
    def get_market_context(evt, ns_map=NS_A):
        return evt.findtext("ei:eventDescriptor/ei:eiMarketContext/emix:marketContext", namespaces=ns_map)

    @staticmethod
    def get_current_signal_value(evt, ns_map=NS_A):
        return evt.findtext(
            'ei:eiEventSignals/ei:eiEventSignal/ei:currentValue/' + \
            'ei:payloadFloat/ei:value', namespaces=ns_map)

    @staticmethod
    def get_signals(evt, ns_map=NS_A):
        simple_signal = None
        signals = []
        for signal in evt.iterfind('ei:eiEventSignals/ei:eiEventSignal', namespaces=ns_map):
            signal_name = signal.findtext('ei:signalName', namespaces=ns_map)
            signal_type = signal.findtext('ei:signalType', namespaces=ns_map)

            if signal_name == 'simple' and signal_type in VALID_SIGNAL_TYPES:
                simple_signal = signal  # This is A profile only conformance rule!

        if simple_signal is None:
            return None

        for interval in simple_signal.iterfind('strm:intervals/ei:interval', namespaces=ns_map):
            duration = interval.findtext('xcal:duration/xcal:duration', namespaces=ns_map)
            uid = interval.findtext('xcal:uid/xcal:text', namespaces=ns_map)
            value = interval.findtext('ei:signalPayload//ei:value', namespaces=ns_map)
            signals.append((duration, uid, value))

        return signals

    @staticmethod
    def get_active_period_start(evt, ns_map=NS_A):
        dttm_str = evt.findtext(
            'ei:eiActivePeriod/xcal:properties/xcal:dtstart/xcal:date-time',
            namespaces=ns_map)
        return schedule.str_to_datetime(dttm_str)

    @staticmethod
    def get_active_period_duration(evt, ns_map=NS_A):
        dttm_str = evt.findtext(
            'ei:eiActivePeriod/xcal:properties/xcal:duration/xcal:duration',
            namespaces=ns_map)
        return schedule.duration_to_delta(dttm_str)

    @staticmethod
    def get_start_before_after(evt, ns_map=NS_A):
        return (evt.findtext(
            'ei:eiActivePeriod/xcal:properties/xcal:tolerance/xcal:tolerate/xcal:startbefore',
            namespaces=ns_map),
                evt.findtext(
                    'ei:eiActivePeriod/xcal:properties/xcal:tolerance/xcal:tolerate/xcal:startafter',
                    namespaces=ns_map))

    @staticmethod
    def get_group_ids(evt, ns_map=NS_A):
        return [e.text for e in evt.iterfind('ei:eiTarget/ei:groupID', namespaces=ns_map)]

    @staticmethod
    def get_resource_ids(evt, ns_map=NS_A):
        return [e.text for e in evt.iterfind('ei:eiTarget/ei:resourceID', namespaces=ns_map)]

    @staticmethod
    def get_party_ids(evt, ns_map=NS_A):
        return [e.text for e in evt.iterfind('ei:eiTarget/ei:partyID', namespaces=ns_map)]

    @staticmethod
    def get_ven_ids(evt, ns_map=NS_A):
        return [e.text for e in evt.iterfind('ei:eiTarget/ei:venID', namespaces=ns_map)]


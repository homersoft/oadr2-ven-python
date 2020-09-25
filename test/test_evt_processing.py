from datetime import datetime, timedelta

import pytest
from lxml import etree

from oadr2 import controller, event, eventdb
from oadr2.schemas import EventSchema, SignalSchema, NS_A

template = """<eiEvent xmlns="http://docs.oasis-open.org/ns/energyinterop/201110" xmlns:ns2="http://docs.oasis-open.org/ns/energyinterop/201110/payloads" xmlns:ns3="http://docs.oasis-open.org/ns/emix/2011/06" xmlns:ns4="urn:ietf:params:xml:ns:icalendar-2.0" xmlns:ns5="urn:ietf:params:xml:ns:icalendar-2.0:stream" xmlns:ns6="http://openadr.org/oadr-2.0a/2012/07">
  <eventDescriptor>
    <eventID>Event092420_135848_673_0</eventID>
    <modificationNumber>1</modificationNumber>
    <eiMarketContext>
      <ns3:marketContext>http://MarketContext1</ns3:marketContext>
    </eiMarketContext>
    <createdDateTime>{createdDateTime}Z</createdDateTime>
    <eventStatus>{eventStatus}</eventStatus>
  </eventDescriptor>
  <eiActivePeriod>
    <ns4:properties>
      <ns4:dtstart>
        <ns4:date-time>{dtstart}Z</ns4:date-time>
      </ns4:dtstart>
      <ns4:duration>
        <ns4:duration>{duration}</ns4:duration>
      </ns4:duration>
      {startafter}
      <x-eiNotification>
        <ns4:duration>PT1M</ns4:duration>
      </x-eiNotification>
      {cancellation_time}
      {ending_time}
    </ns4:properties>
    <ns4:components xmlns:ei="http://docs.oasis-open.org/ns/energyinterop/201110" xmlns:emix="http://docs.oasis-open.org/ns/emix/2011/06" xmlns:oadr="http://openadr.org/oadr-2.0a/2012/07" xmlns:pyld="http://docs.oasis-open.org/ns/energyinterop/201110/payloads" xmlns:strm="urn:ietf:params:xml:ns:icalendar-2.0:stream" xmlns:xcal="urn:ietf:params:xml:ns:icalendar-2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"/>
  </eiActivePeriod>
  <eiEventSignals>
    <eiEventSignal>
      <ns5:intervals>
        <interval>
          <ns4:duration>
            <ns4:duration>PT3M</ns4:duration>
          </ns4:duration>
          <ns4:uid>
            <ns4:text>0</ns4:text>
          </ns4:uid>
          <signalPayload>
            <payloadFloat>
              <value>1.0</value>
            </payloadFloat>
          </signalPayload>
        </interval>
        <interval>
          <ns4:duration>
            <ns4:duration>PT2M</ns4:duration>
          </ns4:duration>
          <ns4:uid>
            <ns4:text>1</ns4:text>
          </ns4:uid>
          <signalPayload>
            <payloadFloat>
              <value>1.0</value>
            </payloadFloat>
          </signalPayload>
        </interval>
      </ns5:intervals>
      <signalName>simple</signalName>
      <signalType>level</signalType>
      <signalID>String</signalID>
      <currentValue>
        <payloadFloat>
          <value>0.0</value>
        </payloadFloat>
      </currentValue>
    </eiEventSignal>
  </eiEventSignals>
  <eiTarget/>
</eiEvent>
"""

distribute_event_template = """
<ns6:oadrDistributeEvent xmlns="http://docs.oasis-open.org/ns/energyinterop/201110" xmlns:ns2="http://docs.oasis-open.org/ns/energyinterop/201110/payloads" xmlns:ns3="http://docs.oasis-open.org/ns/emix/2011/06" xmlns:ns4="urn:ietf:params:xml:ns:icalendar-2.0" xmlns:ns5="urn:ietf:params:xml:ns:icalendar-2.0:stream" xmlns:ns6="http://openadr.org/oadr-2.0a/2012/07">
  <eiResponse>
    <responseCode>200</responseCode>
    <ns2:requestID/>
  </eiResponse>
  <ns2:requestID>OadrDisReq092520_152645_178</ns2:requestID>
  <vtnID>TH_VTN</vtnID>
  <ns6:oadrEvent>
    {event}
    <ns6:oadrResponseRequired>always</ns6:oadrResponseRequired>
  </ns6:oadrEvent>
</ns6:oadrDistributeEvent>"""

TEST_DB_ADDR = "sqlite:///%s/test2.db"


@pytest.fixture
def params():
    return {}


@pytest.fixture
def event_payload_params(params):
    created_date = params.get("created_date", datetime.utcnow().isoformat())
    event_status = params.get("status", "far")
    start_date = params.get("start_date", (datetime.utcnow() + timedelta(seconds=30)).isoformat())
    start_after = f"""<ns4:tolerance>
        <ns4:tolerate>
          <ns4:startafter>{params.get("start_after")}</ns4:startafter>
        </ns4:tolerate>
      </ns4:tolerance>""" if "start_after" in params else ""
    duration = params.get("duration", "PT1M")
    cancellation_time = f"""<ns6:cancellation_time>{params.get("cancellation_time")}Z</ns6:cancellation_time>""" if "cancellation_time" in params else ""
    ending_time = f"""<ns6:ending_time>{params.get("ending_time")}Z</ns6:ending_time>""" if "ending_time" in params else ""

    return dict(
        createdDateTime=created_date,  # datetime iso string
        eventStatus=event_status,
        dtstart=start_date,  # datetime iso string
        duration=duration,  # iCal string
        startafter=start_after,  # iCal string
        ending_time=ending_time,
        cancellation_time=cancellation_time
    )


@pytest.fixture
def payload_object(event_payload_params):
    return etree.fromstring(
        distribute_event_template.format(event=template.format(**event_payload_params))
    )


@pytest.fixture
def event_params(params):
    return dict()


@pytest.fixture
def event_object(event_params):
    return EventSchema(
        id=event_params.get("id", 'Event092420_135848_673_0'),
        vtn_id=event_params.get("vtn_id", "VTN_ID"),
        mod_number=event_params.get("mod_number", 0),
        start=event_params.get("start", datetime.utcnow() + timedelta(seconds=1)),
        original_start=event_params.get("original_start", datetime.utcnow() + timedelta(seconds=1)),
        end=event_params.get("end", datetime.utcnow() + timedelta(seconds=4)),
        signals=event_params.get("signals", [
            SignalSchema(
                index=0, duration="PT3S", level=1.0
            )
        ]),
        status=event_params.get("status", "far"),
        cancellation_offset=event_params.get("cancellation_offset", None)
    )


@pytest.mark.parametrize(
    "event_params, expected",
    [
        pytest.param(
            {},
            (0, None, []),
            id="event not started"
        ),
        pytest.param(
            dict(
                status="active",
                start=(datetime.utcnow() - timedelta(seconds=1)).isoformat(),
                end=(datetime.utcnow() + timedelta(seconds=2)).isoformat(),
            ),
            (1.0, 'Event092420_135848_673_0', []),
            id="event started"
        ),
        pytest.param(
            dict(
                created_date=(datetime.utcnow() - timedelta(hours=1)).isoformat(),
                status="cancelled",
                start_date=(datetime.utcnow() - timedelta(seconds=5)).isoformat(),
                end=(datetime.utcnow() - timedelta(seconds=1)).isoformat(),
            ),
            (0, None, ["Event092420_135848_673_0"]),
            id="event cancelled"
        ),
        pytest.param(
            dict(
                created_date=(datetime.utcnow() - timedelta(hours=1)).isoformat(),
                status="cancelled",
                start=(datetime.utcnow() - timedelta(seconds=5)).isoformat(),
                end=(datetime.utcnow() + timedelta(seconds=10)).isoformat(),
                signals=[
                    SignalSchema(index=0, duration="PT3M", level=1.0)
                ]
            ),
            (1.0, "Event092420_135848_673_0", []),
            id="event cancelled but not deleted"
        ),
    ],
)
def test_calculate_current_event_status(expected, event_object, tmpdir):
    event_handler = event.EventHandler("VEN_ID", db=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([event_object])

    assert (signal_level, evt_id, remove_events) == expected


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(
            {},
            id="event not started"
        ),
    ]
)
def test_handle_payload(params, payload_object, tmpdir):
    event_handler = event.EventHandler("VEN_ID", db=TEST_DB_ADDR % tmpdir)

    reply = event_handler.handle_payload(payload_object)
    assert reply.findtext('pyld:eiCreatedEvent/ei:eiResponse/ei:responseCode', namespaces=NS_A) == "200"
    assert reply.findtext('pyld:eiCreatedEvent/ei:eventResponses/ei:eventResponse/pyld:requestID', namespaces=NS_A) == "OadrDisReq092520_152645_178"

    print(etree.tostring(reply, pretty_print=True).decode("utf-8"))

from datetime import datetime, timedelta
from test.adr_event_generator import AdrEvent, AdrEventStatus, generate_payload
from unittest import mock

import pytest
from freezegun import freeze_time

from oadr2 import controller, event
from oadr2.poll import OpenADR2
from oadr2.schemas import NS_A

TEST_DB_ADDR = "%s/test2.db"

responseCode = 'pyld:eiCreatedEvent/ei:eiResponse/ei:responseCode'
requestID = 'pyld:eiCreatedEvent/ei:eventResponses/ei:eventResponse/pyld:requestID'
optType = 'pyld:eiCreatedEvent/ei:eventResponses/ei:eventResponse/ei:optType'
venID = 'pyld:eiCreatedEvent/ei:venID'
eventResponse = "pyld:eiCreatedEvent/ei:eventResponses/ei:eventResponse"


def test_6_test_event(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    The presence of any string except “false” in the oadrDisributeEvent
    testEvent element is treated as a trigger for a test event.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow()-timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.ACTIVE, test_event=True
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    event_handler.handle_payload(generate_payload([test_event]))

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([test_event.to_obj()])
    assert (signal_level, evt_id, remove_events) == (0, None, [])

    active_event = event_handler.get_active_events()[0]
    expected_event = test_event.to_obj()
    assert active_event == expected_event


@pytest.mark.parametrize(
    "response_required",
    [
        pytest.param(
            False,
            id="response required"
        ),
        pytest.param(
            True,
            id="response not required"
        ),
    ]
)
def test_12_response_required(response_required, tmpdir):
    """
    VEN, EiEvent Service, oadrCreatedEvent Payload
    The VEN must respond to an event in oadrDistributeEvent based upon the
    value in each event’s oadrResponseRequired element as follows:
    Always – The VEN shall respond to the event with an oadrCreatedEvent
    eventResponse . This includes unchanged, new, changed, and cancelled
    events
    Never – The VEN shall not respond to the event with a oadrCreatedEvent
    eventResponse
    Note that oadrCreatedEvent event responses SHOULD be returned in one
    message, but CAN be returned in separate messages.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() - timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.ACTIVE, response_required=response_required
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    reply = event_handler.handle_payload(generate_payload([test_event]))

    assert bool(reply) == response_required


def test_18_overlaping_events(tmpdir):
    """
    VEN/VTN, EiEvent Service
    The VEN/VTN must honor the following rules with regards to overlapping
    active periods...
    DR events with overlapping active periods may be issued, but only if they
    are from different marketContexts and only if the programs have a priority
    associated with them. DR events for programs with higher priorities
    supersede the events of programs with lower priorities. If two programs with
    overlapping events have the same priority then the program whose event
    was activated first takes priority.
    The behavior of a VEN is undefined with respect to the receipt on an
    overlapping event in the same market context. The VTN shall not send
    overlapping events in the same market context, including events that could
    potentially overlap a randomized event cancellation. Nothing in this rule
    should preclude a VEN from opting into overlapping events in different
    market contexts.
    """
    expected_events = [
        AdrEvent(
            id="FooEvent1",
            start=datetime.utcnow() - timedelta(seconds=60),
            signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
            status=AdrEventStatus.ACTIVE, market_context="context1", priority=1
        ),
        AdrEvent(
            id="FooEvent2",
            start=datetime.utcnow() - timedelta(seconds=60),
            signals=[dict(index=0, duration=timedelta(minutes=10), level=2.0)],
            status=AdrEventStatus.ACTIVE, market_context="context2", priority=2
        ),
    ]

    event_handler = event.EventHandler(
        "VEN_ID",
        db_path=TEST_DB_ADDR % tmpdir,
        vtn_ids="TH_VTN",
        market_contexts="context1,context2"
    )
    event_controller = controller.EventController(event_handler)

    event_handler.handle_payload(generate_payload(expected_events))

    active_events = event_handler.get_active_events()

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status(active_events)
    assert (signal_level, evt_id, remove_events) == (2.0, "FooEvent2", [])


def test_19_valid_invalid_events(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    If an oadrDistributeEvent payload has as mix of valid and invalid events,
    the implementation shall only respond to the relevant valid events and not
    reject the entire message.
    """
    expected_events = [
        AdrEvent(
            id="FooEvent",
            start=datetime.utcnow() + timedelta(seconds=60),
            signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
            status=AdrEventStatus.PENDING
        ),
        AdrEvent(
            id="FooFailed",
            start=datetime.utcnow() + timedelta(seconds=160),
            signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
            status=AdrEventStatus.PENDING, ven_ids=["Wrong_Ven"]
        ),
        AdrEvent(
            id="AnotherFooEvent",
            start=datetime.utcnow() + timedelta(seconds=260),
            signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
            status=AdrEventStatus.PENDING
        )
    ]
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler(
        "VEN_ID",
        db_path=TEST_DB_ADDR % tmpdir,
        vtn_ids="TH_VTN"
    )
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload(expected_events))

    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"
    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    for event_reply in reply.iterfind(eventResponse, namespaces=NS_A):
        event_id = event_reply.findtext("ei:qualifiedEventID/ei:eventID", namespaces=NS_A)

        assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
        if event_id == "FooFailed":
            assert event_reply.findtext("ei:responseCode", namespaces=NS_A) == "403"
            assert event_reply.findtext("ei:optType", namespaces=NS_A) == "optOut"
        else:
            assert event_reply.findtext("ei:responseCode", namespaces=NS_A) == "200"
            assert event_reply.findtext("ei:optType", namespaces=NS_A) == "optIn"


def test_21a_ven_id_validation(tmpdir):
    """
    VEN/VTN, EiEvent Service, oadrDistributeEvent Payload
    If venID, vtnID, or EventID is included in payloads, the receiving entity must
    validate the ID values are as expected and generate an error if no ID is
    present or an unexpected value is received.
    Exception: A VEN shall not generate an error upon receipt of a cancelled
    event whose eventID is not previously known.
    """
    expected_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
        status=AdrEventStatus.PENDING, ven_ids=["Wrong_Ven"]
    )
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler(
        "VEN_ID",
        db_path=TEST_DB_ADDR % tmpdir,
        vtn_ids="TH_VTN"
    )
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload([expected_event]))
    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(optType, namespaces=NS_A) == "optOut"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"


def test_21b_vtn_id_validation(tmpdir):
    expected_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
        status=AdrEventStatus.PENDING
    )
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler(
        "VEN_ID",
        db_path=TEST_DB_ADDR % tmpdir,
        vtn_ids="Wrong_Vtn"
    )
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload([expected_event]))
    assert reply.findtext(responseCode, namespaces=NS_A) == "400"
    assert reply.findtext('pyld:eiCreatedEvent/ei:eiResponse/pyld:requestID', namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"


@pytest.mark.parametrize(
    "expected_event",
    [
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, resource_ids=["resource_id"], ven_ids=[]
            ),
            id="resource_id"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, party_ids=["party_id"], ven_ids=[]
            ),
            id="party_id"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, group_ids=["group_id"], ven_ids=[]
            ),
            id="group_id"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING
            ),
            id="ven_id"
        ),
    ]
)
def test_22_target_validation(expected_event, tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    If no sub elements are present in oadrDistributeEvent eiTarget, the
    presumption is that the recipient is the intended target of the event. If
    multiple criteria are present in eiTarget subelements, the values are OR’d
    togther to determine whether the VEN is a target for the event. However,
    the VENs behavior with respect to responding to an event when it matches
    one of the eiTarget criteria is implementation dependent.
    """
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler(
        "VEN_ID",
        db_path=TEST_DB_ADDR % tmpdir,
        resource_id="resource_id",
        party_id="party_id",
        group_id="group_id"
    )
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload([expected_event]))
    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(optType, namespaces=NS_A) == "optIn"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"


@pytest.mark.skip(reason="No need to test")
def test_23_oadrRequestEvent():
    """
    VEN/VTN, EiEvent Service, oadrRequestEvent Payload
    oadrRequestEvent many only be sent in the VEN to VTN direction
    """
    assert False


@pytest.mark.skip(reason="Covered in other tests")
def test_25_error_reporting():
    """
    VEN/VTN, EiEvent Service
    VTN and VEN: The following rules must be followed with respect to
    application level responses with respect to multiple events:
    1)If the Response indicates success, there is no need to examine each
    element in the Responses.
    2)If some elements fail and other succeed, the Response will indicate the
    error, and the recipient should evaluate each element in Responses to
    discover which components of the operation failed.
    Exception: For oadrCreatedEvent, the presence of a failure indication in
    eventResponse:responseCode shall not force a failure indication in
    eiResponse:responseCode. Typical behavior would be for the VEN to report
    a success indication in eiResponse:responseCode and indicate any event
    specific errors in eventResponse:responseCode. The
    """
    assert False


def test_30_start_time_randomization(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    The VEN must randomize the dtstart time of the event if a value is present
    in the startafter element. Event completion times are determined by adding
    the event duration to the randomized dtstart time. Modifications to an event
    should maintain the same random offset, unless the startafter element itself
    is modified.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(minutes=10),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING, start_after=timedelta(minutes=2)
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    event_handler.handle_payload(generate_payload([test_event]))

    active_event = event_handler.get_active_events()[0]
    expected_event = test_event.to_obj()
    assert active_event.start != expected_event.start
    assert (active_event.start - expected_event.start) < timedelta(minutes=2)


@pytest.mark.skip(reason="Covered in other tests")
def test_31_active_period_subelements():
    """
    # VEN, EiEvent Service, oadrDistributeEvent Payload
    # The VEN must recognize and act upon values specified in the subelements
    # of activePeriod including:
    #     dtStart
    #     duration
    #     tolerence
    #     x-eiRampUp (positive and negative)
    #     x-eiRecovery (positive and negative)
    # Note: x-eiRampup and x-eiRecovery are not testable requirements
    """
    assert False


@pytest.mark.skip(reason="Covered in other tests")
def test_32_intervals_subelements():
    """
    VEN/VTN, EiEvent Service, oadrDistributeEvent Payload
    The VEN must recognize and act upon values specified in the subelements
    of intervals including:
        duration
        signalPayload
    """
    assert False


@pytest.mark.skip(reason="Covered in other tests")
def test_31_event_error_indication():
    """
    VEN/VTN
    The implementation must provide an application layer error indication as a
    result of the following conditions:
        Schema does not validate
        Missing expected information
        Payload not of expected type
        ID not as expected
        Illogical request – Old date on new event, durations don’t add up
        correctly, etc.
        Etc.
    """
    assert False


def test_35_response_created_event(tmpdir):
    """
    VEN, EiEvent Service, oadrCreatedEvent Payload
    The eiResponses element in oadrCreatedEvent is mandatory, except when
    an error condition is reported in eiResponse.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(minutes=10),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    reply = event_handler.handle_payload(generate_payload([test_event]))

    assert bool(reply.find("pyld:eiCreatedEvent/ei:eventResponses", namespaces=NS_A))


def test_36_cancellation_acknowledgement(tmpdir):
    """
    VEN, EiEvent Service, oadrCreatedEvent Payload
    An event cancellation received by the VEN must be acknowledged with an
    oadrCreatedEvent with the optType element set as follows, unless the
    oadrResponseRequired is set to ‘never”:
    optIn = Confirm to cancellation
    optOut = Cannot cancel
    Note: Once an event cancellation is acknowledged by the VEN, the event
    shall not be included in subsequent oadrCreatedEvent payloads unless the
    VTN includes this event in a subsequent oadrDistributeEvent payload.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(minutes=10),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.CANCELLED
    )
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload([test_event]))

    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(optType, namespaces=NS_A) == "optIn"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"

    db_mock.assert_not_called()


@pytest.mark.skip(reason="No need to test")
def test_37_push_pull_model():
    """
    VEN
    A VEN Implementation must support pull model and can optionally also
    support push
    """
    assert False


@pytest.mark.skip(reason="Covered in other tests")
def test_41_request_id():
    """
    VEN/VTN, EiEvent Service, oadrDistributeEvent Payload
    The VTN must send a requestID value as part of the oadrDistributeEvent payload.
    Note: The requestID value is not required to be unique, and in fact may be the
    same for all oadrDistributeEvent payloads. That there are two requestID fields
    in oadrDistributeEvent. The feild that must be populated with a requestID is
    located at oadrDistributeEvent:requestID
    """
    assert False


def test_42_request_id(tmpdir):
    """
    VEN, EiEvent Service, oadrCreatedEvent Payload
    A VEN receiving an oadrDistributeEvent eiEvent must use the received requestID
    value in the EiCreatedEvent eventResponse when responding to the event. This
    includes any and all subsequent EiCreatedEvent messages that may be sent to
    change the opt status of the VEN.
    The eiResponse:requestID in oadrCreatedEvent shall be left empty if the
    payload contains eventResponses. The VTN shall
    look inside each
    eventResponse for the relevant requestID
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(minutes=10),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING, start_after=timedelta(minutes=2)
    )
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload([test_event]))
    assert reply.findtext(
        'pyld:eiCreatedEvent/ei:eventResponses/ei:eventResponse/pyld:requestID',
        namespaces=NS_A
    ) == "OadrDisReq092520_152645_178"


@pytest.mark.skip(reason="No need to test")
def test_43_request_id_uniqueness():
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    The VEN must make no assumptions regarding the uniqueness of requestID values
    received from the VTN in the oadrDistributePayload
    """
    assert False


@pytest.mark.skip(reason="No need to test")
def test_44_empty_request_id():
    """
    VEN/VTN
    With the exception of oadrDistributeEvent and oadrCreatedEvent payloads,
    requestID may be an empty element in other payloads and if a requestID value is
    present, it may be ignored
    """
    assert False


@pytest.mark.skip(reason="No need to test")
def test_45_schema_location():
    """
    VEN/VTN
    Messages sent between VENs and VTNs shall
    *not* include a
    schemaLocation attribute
    """
    assert False


@pytest.mark.skip(reason="Covered in other tests")
def test_46_optional_elements():
    """
    VEN/VTN
    Optional elements do not need to be included in outbound payloads, but if
    they are, the VEN or VTN receiving the payload must understand and act
    upon those optional elements
    """
    assert False


def test_47_unending_event(tmpdir):
    """
    VEN/VTN, EiEvent Service, oadrDistributeEvent Payload
    An event with an overall duration of 0 indicates an event with no defined
    end time and will remain active until explicitly cancelled.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=0), level=1.0)],
        status=AdrEventStatus.ACTIVE
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    event_handler.handle_payload(generate_payload([test_event]))

    active_event = event_handler.get_active_events()[0]

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([active_event])
    assert (signal_level, evt_id, remove_events) == (0, None, [])

    with freeze_time(datetime.utcnow() + timedelta(seconds=70)):
        signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([active_event])
        assert (signal_level, evt_id, remove_events) == (1.0, "FooEvent", [])

    with freeze_time(datetime.utcnow() + timedelta(minutes=70)):
        signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([active_event])
        assert (signal_level, evt_id, remove_events) == (1.0, "FooEvent", [])

    with freeze_time(datetime.utcnow() + timedelta(hours=70)):
        signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([active_event])
        assert (signal_level, evt_id, remove_events) == (1.0, "FooEvent", [])

    test_event.status = AdrEventStatus.CANCELLED
    test_event.mod_number += 1

    event_handler.handle_payload(generate_payload([test_event]))
    active_event = event_handler.get_active_events()[0]

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([active_event])
    assert (signal_level, evt_id, remove_events) == (0, None, ["FooEvent"])


@pytest.mark.parametrize(
    "expected_event",
    [
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, market_context="http://bad.context"
            ),
            id="market_context"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, signal_name="bad"
            ),
            id="signal_name"
        ),
    ]
)
def test_48_payload_error_indication(expected_event, tmpdir):
    """
    When a VTN or VEN receives schema compliant oadr payload that has
    logical errors, the receiving device must provide an application layer error
    indication of 4xx. The detailed error message number is informational and
    not a requirement for response to a specific scenario. If the error is in an
    event contained in an oadrDistributeEvent payload, it should be reported in
    the eventResponse element of oadrCreatedEvent. The following logical
    errors must be detected by implementations:
        VEN receives non-matching market context
        VEN receives non-matching eiTarget
        VEN receives unsupported signalName
        VTN receives non-matching eventID in oadrCreatedEvent Response
        VTN receives mismatched modificationNumber in oadrCreatedEvent
    """
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler(
        "VEN_ID",
        market_contexts="http://market.context",
        db_path=TEST_DB_ADDR % tmpdir,
        resource_id="resource_id",
        party_id="party_id",
        group_id="group_id"
    )
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload([expected_event]))
    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(optType, namespaces=NS_A) == "optOut"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"


@pytest.mark.skip(reason="No need to test")
def test_50_distributed_event():
    """
    VEN/VTN, EiEvent Service, oadrDistributeEvent Payload
    In both the push and pull model, oadrDistributeEvent MUST contain all
    existing events which have the eventStatus element set to either FAR,
    NEAR, or ACTIVE. Events with an eventStatus of cancelled MUST be
    included in the payload upon change to the modificationNumber and MAY
    be included in subsequent payloads.
    """
    assert False


@pytest.mark.skip(reason="No need to test")
def test_52_cancellation_acknowledgment():
    """
    VTN, EiEvent Service, oadrDistributeEvent Payload
    If a VTN requests acknowledgment of a cancelled event with
    oadrResponserequired of always, the VTN shall continue to send the
    cancelled event to the VEN until the event is acknowledged, eventStatus
    transitions to the complete state, or some well defined number of retries is
    attempted
    """
    assert False


@pytest.mark.skip(reason="No need to test")
def test_53_http_transport():
    """
    VEN/VTN
    Shall implement the simple http transport. Including support for the
    following mandatory http headers:
        Host
        Content-Length
        Content-Type of application/xml
    """
    assert False


@pytest.mark.skip(reason="No need to test")
def test_54_polling_frequency():
    """
    VEN
    HTTP PULL VEN’s MUST be able to guarantee worst case latencies for the
    delivery of information from the VTN by using deterministic and well defined
    polling frequencies. The VEN SHOULD support the ability for its polling
    frequency to be configured to support varying latency requirements. If the
    VEN intends to poll for information at varying frequencies based upon
    attributes of the information being exchanged (e.g. market context) then the
    VEN MUST support the configuration of polling frequencies on a per
    attribute basis.
    """
    assert False


def test_55_max_polling_frequency():
    """
    VEN
    HTTP PULL VEN’s MUST NOT poll the VTN on average greater than some
    well defined and deterministic frequency. THE VEN SHOULD support the
    ability for the maximum polling frequency to be configured.
    """
    with pytest.raises(AssertionError):
        OpenADR2(
            event_config=dict(
                ven_id="TH_VEN"
            ),
            vtn_base_uri="",
            vtn_poll_interval=9,
            start_thread=False,
        )


def test_56_new_event(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    If the VTN sends an oadrEvent with an eventID that the VEN is not aware
    then it should process the event and add it to its list of known events
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow()+timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    event_handler.handle_payload(generate_payload([test_event]))

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([test_event.to_obj()])
    assert (signal_level, evt_id, remove_events) == (0, None, [])

    active_event = event_handler.get_active_events()[0]
    expected_event = test_event.to_obj()
    assert active_event == expected_event

    with freeze_time(datetime.utcnow()+timedelta(seconds=70)):
        signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([test_event.to_obj()])
        assert (signal_level, evt_id, remove_events) == (1.0, "FooEvent", [])


def test_57_modified_event(tmpdir):
    """
    VEN/VTN, EiEvent Service, oadrDistributeEvent Payload
    If the VTN sends an oadrEvent with an eventID that the VEN is already
    aware of, but with a higher modification number then the VEN should
    replace the previous event with the new one In its list of known events.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    event_handler.handle_payload(generate_payload([test_event]))

    active_event = event_handler.get_active_events()[0]
    expected_event = test_event.to_obj()
    assert active_event == expected_event

    test_event.mod_number = 1
    test_event.status = AdrEventStatus.ACTIVE

    event_handler.handle_payload(generate_payload([test_event]))

    active_event = event_handler.get_active_events()[0]
    expected_event = test_event.to_obj()
    assert active_event == expected_event


def test_58_modified_event_error(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    If the VTN sends an oadrEvent with an eventID that the VEN is already
    aware of, but which has a lower modification number than one in which the
    VEN is already aware then this is an ERROR and the VEN should respond
    with the appropriate error code. Note that this is true regardless of the
    event state including cancelled.
    """
    test_event1 = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING, mod_number=5
    )
    test_event2 = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING, mod_number=3
    )

    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    event_handler.handle_payload(generate_payload([test_event1]))

    active_event = event_handler.get_active_events()[0]
    expected_event = test_event1.to_obj()
    assert active_event == expected_event

    event_handler.handle_payload(generate_payload([test_event2]))

    active_event = event_handler.get_active_events()[0]
    assert active_event == expected_event


def test_59_event_cancellation(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    If the VTN sends an oadrEvent with the eventStatus set to cancelled and
    has an eventID that the VEN is aware of then the VEN should cancel the
    existing event and delete it from its list of known events.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() + timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.PENDING, mod_number=1
    )

    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    event_handler.handle_payload(generate_payload([test_event]))

    active_event = event_handler.get_active_events()[0]
    assert active_event == test_event.to_obj()

    with freeze_time():
        test_event.status = AdrEventStatus.CANCELLED
        test_event.mod_number += 1
        test_event.end = datetime.utcnow()

        event_handler.handle_payload(generate_payload([test_event]))

        active_event = event_handler.get_active_events()[0]
        assert active_event == test_event.to_obj()

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([test_event.to_obj()])
    assert (signal_level, evt_id, remove_events) == (0, None, ["FooEvent"])


def test_60_new_cancelled_event(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent, oadrCreatedEvent Payload
    If the VTN sends an oadrEvent with the eventStatus set to cancelled and
    has an eventID that the VEN is not aware of then the VEN should ignore
    the event since it is not currently in its list of known events, but still must
    respond with the createdEvent if required to do so by oadrResponseRequired
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() - timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.CANCELLED, mod_number=1
    )

    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    reply = event_handler.handle_payload(generate_payload([test_event]))

    assert reply.findtext(
        responseCode,
        namespaces=NS_A
    ) == "200"
    assert reply.findtext(
        optType,
        namespaces=NS_A
    ) == "optIn"

    active_event = event_handler.get_active_events()[0]

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([active_event])
    assert (signal_level, evt_id, remove_events) == (0, None, ["FooEvent"])


@pytest.mark.skip(reason="Covered in other tests")
def test_61_implied_cancellation():
    """
    VEN, EiEvent Service, oadrDistributeEvent Payload
    If the VTN sends the oadrDistributeEvent payload and it does not contain
    an event for which the VEN is aware (i.e. in its list of known events) then
    the VEN must delete it from its list of known event (i.e. implied cancel).
    Exception: A VEN that has an active event that cannot be immediately
    stopped for operational reasons, may leave the event in its data store until
    the event expires or the event can be stopped.
    """
    assert False


@pytest.mark.skip(reason="Covered in other tests")
def test_62_response():
    """
    VEN, EiEvent Service, oadrDistributeEvent, oadrCreatedEvent Payload
    The VEN must process EVERY oadrEvent event message (new, modified,
    cancelled, etc.) that it receives from the VTN in an oadrDistributeEvent
    payload and it MUST reply with a createdEvent message for every EIEvent
    message in which the responseRequired is set to always. Furthermore if
    the responseRequired is set to never, the VEN MUST NOT respond with a
    createdEvent message. It is at the complete discretion of the VTN as to
    whether responses are required from the VEN. Note that this rule is
    universal and applies to all scenarios including the following:
        The event is one in which the VEN is already aware.
        The event is being cancelled and the VEN did not even know it existed
        It does not matter how the EIEvent payloads were delivered, i.e.
        PUSH, PULL or as the result of being delivered in an ALL payload
    """
    assert False


@pytest.mark.skip(reason="Covered in other tests")
def test_64_polling_cycle():
    """
    VEN, EiEvent Service
    A pull VEN shall respond to all received events before initiating another
    polling cycle.
    """
    assert False


def test_65_cancellation_time_randomization(tmpdir):
    """
    VEN, EiEvent Service, oadrDistributeEvent, oadrCreatedEvent Payload
    When an event containing a randomization value in the startafter element is
    cancelled, either explicitly or implicitly, the VEN MUST randomize its
    termination of the event. The randomization window should be between 0
    and a duration equal to the value specified in startafter.
    """
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() - timedelta(minutes=5),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.ACTIVE, start_after=timedelta(minutes=2)
    )
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    event_handler.handle_payload(generate_payload([test_event]))

    with freeze_time():
        test_event.mod_number += 1
        test_event.status = AdrEventStatus.CANCELLED

        event_handler.handle_payload(generate_payload([test_event]))

        active_event = event_handler.get_active_events()[0]

        assert active_event.end != datetime.utcnow()
        assert (active_event.start - datetime.utcnow()) < timedelta(minutes=2)


@pytest.mark.skip(reason="No need to test")
def test_66_cancelled_event_handling():
    """
    VEN/VTN, EiEvent Service, oadrDistributeEvent, Payload
    If a VTN sends an oadrDistributeEvent payload containing an event with a
    startafter element with a value greater than zero, the VTN must continue to
    include the event in oadrDistributeEvent payloads, even if the event is
    complete, until current time is equal to dtStart plus duration plus startafter.
    The receipt of an eventStatus equal to completed shall not cause the VEN
    to change its operational status with respect to executing the event.
    """
    assert False


@pytest.mark.skip(reason="Cant test here")
def test_67_tls_support():
    """
    VEN/VTN
    VTN and VEN shall support TLS 1.0 and may support higher versions of
    TLS provided that they can still interoperate with TLS 1.0 implementations.
    The default cipher suite selection shall be as follows:
        The VEN client shall offer at least at least one of the default cipher
        suites listed below
        The VEN server shall must support at least one of the default cipher
        suites listed below and must select one of the default cipher suites
        regardless of other cipher suites that may be offered by the VTN
        client
        The VTN client must offer both the default cipher suites listed
        below.
        The VTN server must support both of the default cipher suites listed
        below and must select one of listed the default cipher suites
        regardless of other ciphers that may be offered by the VEN client
    Default cipher suites:
        TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA
        TLS_RSA_WITH_AES_128_CBC_SHA
    Note that a VTN or VEN may be configured to support any TLS version and
    cipher suite combination based on the needs of a specific deployment.
    However in the absence of changes to the default configuration of the VTN
    or VEN, the behavior of the devices shall be as noted above.
    """
    assert False


@pytest.mark.skip(reason="Cant test here")
def test_68_cert_support():
    """
    VEN/VTN
    Both VTNs and VENs shall support client and server X.509v3 certificates. A
    VTN must support both an ECC and RSA certificate. A VEN must support
    either an RSA or ECC certificate and may support both. RSA certificates
    must be signed with a minimum key length of 2048 bits. ECC certificates
    must be signed with a minimum key length of 224 bits. ECC Hybrid
    certificates must be signed with a 256 bit key signed with a RSA 2048 bit
    key.
    """
    assert False

from datetime import datetime, timedelta
from unittest import mock

import pytest

from oadr2 import controller, event
from oadr2.schemas import NS_A
from freezegun import freeze_time

from test.adr_event_generator import AdrEvent, AdrEventStatus, generate_payload


TEST_DB_ADDR = "%s/test2.db"

scenario = dict(
    not_started=AdrEvent(
        id="FooEvent",
        start=datetime.utcnow()+timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
        status=AdrEventStatus.PENDING,
    ),
    started=AdrEvent(
        id="FooEvent",
        start=datetime.utcnow()-timedelta(seconds=5),
        signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
        status=AdrEventStatus.ACTIVE,
    ),
    not_started_with_target=AdrEvent(
            id="FooEvent",
            start=datetime.utcnow() + timedelta(seconds=60),
            signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
            status=AdrEventStatus.PENDING, group_ids=["ids"], party_ids=["ids"], resource_ids=["ids"],
        ),
    signal_1of2=AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() - timedelta(seconds=10),
        signals=[
            dict(index=0, duration=timedelta(seconds=15), level=1.0),
            dict(index=1, duration=timedelta(seconds=5), level=2.0),
        ],
        status=AdrEventStatus.ACTIVE,
    ),
    signal_2of2=AdrEvent(
        id="FooEvent",
        start=datetime.utcnow() - timedelta(seconds=10),
        signals=[
            dict(index=0, duration=timedelta(seconds=5), level=1.0),
            dict(index=1, duration=timedelta(seconds=15), level=2.0),
        ],
        status=AdrEventStatus.ACTIVE,
    ),
    events_1of2=[
        AdrEvent(
            id="FooEvent1",
            start=datetime.utcnow() - timedelta(seconds=10),
            signals=[
                dict(index=0, duration=timedelta(seconds=20), level=1.0),
            ],
            status=AdrEventStatus.ACTIVE,
        ),
        AdrEvent(
            id="FooEvent2",
            start=datetime.utcnow() + timedelta(seconds=10),
            signals=[
                dict(index=0, duration=timedelta(seconds=20), level=2.0),
            ],
            status=AdrEventStatus.PENDING,
        ),
    ],
    events_2of2=[
        AdrEvent(
            id="FooEvent1",
            start=datetime.utcnow() - timedelta(seconds=10),
            signals=[
                dict(index=0, duration=timedelta(seconds=5), level=1.0),
            ],
            status=AdrEventStatus.COMPLETED,
        ),
        AdrEvent(
            id="FooEvent2",
            start=datetime.utcnow() - timedelta(seconds=5),
            signals=[
                dict(index=0, duration=timedelta(seconds=20), level=2.0),
            ],
            status=AdrEventStatus.PENDING,
        ),
    ],
    cancelled=AdrEvent(
        id="FooEvent",
        start=datetime.utcnow()-timedelta(seconds=60),
        signals=[
            dict(index=0, duration=timedelta(seconds=120), level=1.0)
        ],
        status=AdrEventStatus.CANCELLED,
        end=datetime.utcnow()-timedelta(seconds=10)
    ),
    cancelled_still_active=AdrEvent(
        id="FooEvent",
        start=datetime.utcnow()-timedelta(seconds=60),
        signals=[
            dict(index=0, duration=timedelta(seconds=120), level=1.0)
        ],
        status=AdrEventStatus.CANCELLED,
        end=datetime.utcnow()+timedelta(seconds=10)
    )

)


@pytest.mark.parametrize(
    "event_list, expected",
    [
        pytest.param(
            [scenario["not_started"]],
            (0, None, []),
            id="event not started"
        ),
        pytest.param(
            [scenario["not_started_with_target"]],
            (0, None, []),
            id="event not started"
        ),
        pytest.param(
            [scenario["started"]],
            (1.0, 'FooEvent', []),
            id="event started"
        ),
        pytest.param(
            [scenario["signal_1of2"]],
            (1.0, 'FooEvent', []),
            id="event started, signal 1 of 2"
        ),
        pytest.param(
            [scenario["signal_2of2"]],
            (2.0, 'FooEvent', []),
            id="event started, signal 2 of 2"
        ),
        pytest.param(
            scenario["events_1of2"],
            (1.0, 'FooEvent1', []),
            id="event 1 of 2"
        ),
        pytest.param(
            scenario["events_2of2"],
            (2.0, 'FooEvent2', ["FooEvent1"]),
            id="event 2 of 2, first deleted"
        ),
        pytest.param(
            [scenario["cancelled"]],
            (0, None, ["FooEvent"]),
            id="event cancelled"
        ),
        pytest.param(
            [scenario["cancelled_still_active"]],
            (1.0, "FooEvent", []),
            id="event cancelled but not deleted"
        ),
    ]
)
def test_calculate_current_event_status(event_list, expected, tmpdir):
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_controller = controller.EventController(event_handler)

    signal_level, evt_id, remove_events = event_controller._calculate_current_event_status([evt.to_obj() for evt in event_list])

    assert (signal_level, evt_id, remove_events) == expected


@pytest.mark.parametrize(
    "event_list, expected_level, expected_removed",
    [
        pytest.param(
            [scenario["not_started"]], 0, [],
            id="event not started"
        ),
        pytest.param(
            [scenario["started"]], 1.0,  [],
            id="event started"
        ),
        pytest.param(
            [scenario["signal_1of2"]], 1.0,  [],
            id="event started, signal 1 of 2"
        ),
        pytest.param(
            [scenario["signal_2of2"]], 2.0, [],
            id="event started, signal 2 of 2"
        ),
        pytest.param(
            scenario["events_1of2"], 1.0, [],
            id="event 1 of 2"
        ),
        pytest.param(
            scenario["events_2of2"], 2.0, ["FooEvent1"],
            id="event 2 of 2, first deleted"
        ),
        pytest.param(
            [scenario["cancelled"]], 0, ["FooEvent"],
            id="event cancelled"
        ),
        pytest.param(
            [scenario["cancelled_still_active"]], 1.0, [],
            id="event cancelled but not deleted"
        ),
    ]
)
def test_calculate_update_control(event_list, expected_level, expected_removed, tmpdir):
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_handler.db.remove_events = db_mock
    event_controller = controller.EventController(event_handler)

    signal_level = event_controller._update_control([evt.to_obj() for evt in event_list])

    assert signal_level == expected_level

    if expected_removed:
        parsed_events = db_mock.call_args[0][0]
        for evt in expected_removed:
            assert evt in parsed_events
            parsed_events.remove(evt)

        assert parsed_events == []
    else:
        db_mock.assert_not_called()


responseCode = 'pyld:eiCreatedEvent/ei:eiResponse/ei:responseCode'
requestID = 'pyld:eiCreatedEvent/ei:eventResponses/ei:eventResponse/pyld:requestID'
optType = 'pyld:eiCreatedEvent/ei:eventResponses/ei:eventResponse/ei:optType'
venID = 'pyld:eiCreatedEvent/ei:venID'


@pytest.mark.parametrize(
    "event_list",
    [
        pytest.param(
            [scenario["not_started"]],
            id="event not started"
        ),
        pytest.param(
            [scenario["started"]],
            id="event started"
        ),
        pytest.param(
            [scenario["signal_1of2"]],
            id="event started, signal 1 of 2"
        ),
        pytest.param(
            [scenario["signal_2of2"]],
            id="event started, signal 2 of 2"
        ),
        pytest.param(
            [scenario["cancelled"]],
            id="event cancelled"
        ),
        pytest.param(
            [scenario["cancelled_still_active"]],
            id="event cancelled but not deleted"
        ),
    ]
)
def test_handle_payload(event_list, tmpdir):
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)
    event_handler.db.add_event = db_mock

    reply = event_handler.handle_payload(generate_payload(event_list))
    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(optType, namespaces=NS_A) == "optIn"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"

    db_mock.assert_called_once()

    for index, evt in enumerate(event_list):
        parsed_event = db_mock.call_args[0][index]
        expected_event = evt.to_obj()

        assert parsed_event.id == expected_event.id
        assert parsed_event.start == expected_event.start
        assert parsed_event.original_start == expected_event.original_start
        assert parsed_event.cancellation_offset == expected_event.cancellation_offset
        assert parsed_event.signals == expected_event.signals
        assert parsed_event.mod_number == expected_event.mod_number
        assert parsed_event.status == expected_event.status
        if expected_event.status != AdrEventStatus.CANCELLED.value:
            assert parsed_event.end == expected_event.end


@pytest.mark.parametrize(
    "expected_event, handler_param",
    [
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, resource_ids=["some_parameter"]
            ),
            dict(resource_id="some_parameter"),
            id="resource_id"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, party_ids=["some_parameter"]
            ),
            dict(party_id="some_parameter"),
            id="party_id"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, group_ids=["some_parameter"]
            ),
            dict(group_id="some_parameter"),
            id="group_id"
        ),
    ]
)
def test_handle_payload_with_target_info(expected_event, handler_param, tmpdir):
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir, **handler_param)
    event_handler.db.add_event = db_mock

    reply = event_handler.handle_payload(generate_payload([expected_event]))
    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(optType, namespaces=NS_A) == "optIn"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"

    db_mock.assert_called_once()
    parsed_event = db_mock.call_args[0][0]

    expected_event = expected_event.to_obj()

    assert parsed_event.id == expected_event.id
    assert parsed_event.start == expected_event.start
    assert parsed_event.original_start == expected_event.original_start
    assert parsed_event.cancellation_offset == expected_event.cancellation_offset
    assert parsed_event.signals == expected_event.signals
    assert parsed_event.mod_number == expected_event.mod_number
    assert parsed_event.status == expected_event.status
    assert parsed_event.end == expected_event.end


@pytest.mark.parametrize(
    "expected_event, handler_param",
    [
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, resource_ids=["some_parameter"], ven_ids=[]
            ),
            dict(resource_id="other_parameter"),
            id="resource_id"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, party_ids=["some_parameter"], ven_ids=[]
            ),
            dict(party_id="other_parameter"),
            id="party_id"
        ),
        pytest.param(
            AdrEvent(
                id="FooEvent",
                start=datetime.utcnow() + timedelta(seconds=60),
                signals=[dict(index=0, duration=timedelta(seconds=10), level=1.0)],
                status=AdrEventStatus.PENDING, group_ids=["some_parameter"], ven_ids=[]
            ),
            dict(group_id="other_parameter"),
            id="group_id"
        ),
    ]
)
def test_handle_payload_with_wrong_target_info(expected_event, handler_param, tmpdir):
    db_mock = mock.MagicMock()
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir, **handler_param)
    event_handler.db.update_event = db_mock

    reply = event_handler.handle_payload(generate_payload([expected_event]))
    assert reply.findtext(responseCode, namespaces=NS_A) == "200"
    assert reply.findtext(requestID, namespaces=NS_A) == "OadrDisReq092520_152645_178"
    assert reply.findtext(optType, namespaces=NS_A) == "optOut"
    assert reply.findtext(venID, namespaces=NS_A) == "VEN_ID"

    db_mock.assert_not_called()


@pytest.mark.parametrize(
    "event_list",
    [
        pytest.param(
            [scenario["not_started"]],
            id="event not started"
        ),
        pytest.param(
            [scenario["started"]],
            id="event started"
        ),
        pytest.param(
            [scenario["signal_1of2"]],
            id="event started, signal 1 of 2"
        ),
        pytest.param(
            [scenario["signal_2of2"]],
            id="event started, signal 2 of 2"
        ),
    ]
)
def test_handle_payload_with_db(event_list, tmpdir):
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    reply = event_handler.handle_payload(generate_payload(event_list))

    active_events = event_handler.get_active_events()
    for evt in event_list:
        assert evt.to_obj() in active_events


@pytest.mark.parametrize(
    "event_list",
    [
        pytest.param(
            [scenario["cancelled"]],
            id="event cancelled"
        ),
        pytest.param(
            [scenario["cancelled_still_active"]],
            id="event cancelled but not deleted"
        ),
    ]
)
def test_handle_cancelled_payload_with_db(event_list, tmpdir):
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    event_handler.handle_payload(generate_payload(event_list))

    active_event = event_handler.get_active_events()[0]
    expected_event = event_list[0].to_obj()

    assert active_event.end != expected_event.end
    active_event.end = expected_event.end = None
    assert active_event == expected_event


@pytest.mark.parametrize(
    "test_event",
    [
        pytest.param(
            scenario["not_started"],
            id="event not started"
        ),
    ]
)
def test_update_with_db(test_event, tmpdir):
    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    event_handler.handle_payload(generate_payload([test_event]))

    active_events = event_handler.get_active_events()
    assert test_event.to_obj() in active_events

    test_event.mod_number += 1
    test_event.status = AdrEventStatus.ACTIVE

    event_handler.handle_payload(generate_payload([test_event]))

    active_events = event_handler.get_active_events()
    assert test_event.to_obj() in active_events


def test_implied_cancellation(tmpdir):
    event1 = AdrEvent(
        id="FooEvent1",
        start=datetime.utcnow()-timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=1.0)],
        status=AdrEventStatus.ACTIVE,
    )
    event2 = AdrEvent(
        id="FooEvent2",
        start=datetime.utcnow()-timedelta(seconds=50),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=2.0)],
        status=AdrEventStatus.ACTIVE,
    )

    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    event_handler.handle_payload(generate_payload([event1]))

    active_events = event_handler.get_active_events()

    assert [event1.to_obj()] == active_events

    with freeze_time():
        event_handler.handle_payload(generate_payload([event2]))
        active_events = event_handler.get_active_events()

        cancelled_evt = event1.to_obj()
        cancelled_evt.cancel()
        assert [cancelled_evt, event2.to_obj()] == active_events


def test_explicite_cancellation(tmpdir):
    test_event = AdrEvent(
        id="FooEvent",
        start=datetime.utcnow()-timedelta(seconds=60),
        signals=[dict(index=0, duration=timedelta(minutes=10), level=2.0)],
        status=AdrEventStatus.ACTIVE,
    )

    event_handler = event.EventHandler("VEN_ID", db_path=TEST_DB_ADDR % tmpdir)

    event_handler.handle_payload(generate_payload([test_event]))

    active_events = event_handler.get_active_events()

    assert [test_event.to_obj()] == active_events

    with freeze_time():
        test_event.mod_number += 1
        test_event.status = AdrEventStatus.CANCELLED
        test_event.end = datetime.utcnow()

        event_handler.handle_payload(generate_payload([test_event]))
        active_events = event_handler.get_active_events()

        assert [test_event.to_obj()] == active_events

        # test second cancellation
        event_handler.handle_payload(generate_payload([test_event]))
        active_events = event_handler.get_active_events()

        assert [test_event.to_obj()] == active_events



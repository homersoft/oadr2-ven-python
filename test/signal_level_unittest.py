import unittest
from datetime import datetime, timedelta

from freezegun import freeze_time
from os import remove

from oadr2.poll import OpenADR2
from test.adr_event_generator import AdrEventStatus, AdrEvent

DB_FILENAME = "test.db"

class SignalLevelTest(unittest.TestCase):
    def setUp(self):
        self.start_thread = False

        # Some configureation variables, by default, this is for the a handler
        config = {'vtn_ids': 'vtn_1,vtn_2,vtn_3,TH_VTN',
                  'ven_id': 'ven_py', 'db_path': DB_FILENAME}

        self.adr_client = OpenADR2(
            event_config=config,
            vtn_base_uri="",
            start_thread=self.start_thread
        )

        # Make things a little nicer for us to see
        print('')
        print((40 * '='))

    def tearDown(self):
        remove(DB_FILENAME)
        # self.adr_client.event_handler.update_all_events({}, '')  # Clear out the database

    def test_active_event_with_single_interval(self):
        print('in test_active_event_with_single_interval()')
        events = [
            AdrEvent(
                id="EventID",
                start=datetime(year=2020, month=3, day=18, hour=8),
                status=AdrEventStatus.ACTIVE,
                signals=[dict(index=0, duration=timedelta(hours=5), level=1.0)],
            ),
        ]
        xml_events = [e.to_obj() for e in events]

        with freeze_time(events[0].start - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].signals[0]["level"]

        with freeze_time(events[0].end + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_active_event_with_multiple_intervals(self):
        print('in test_active_event_with_multiple_intervals()')
        events = [
            AdrEvent(
                id="EventID",
                start=datetime(year=2020, month=3, day=18, hour=10),
                status=AdrEventStatus.ACTIVE,
                signals=[
                    dict(index=0, duration=timedelta(hours=4), level=3.0),
                    dict(index=1, duration=timedelta(hours=4), level=2.0),
                ],
            ),
        ]
        xml_events = [e.to_obj() for e in events]

        with freeze_time(events[0].start - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].signals[0]["level"]

        with freeze_time(events[0].start + timedelta(hours=4, minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].signals[1]["level"]

        with freeze_time(events[0].end + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_pending_event(self):
        print('in test_pending_event()')
        events = [
            AdrEvent(
                id="EventID",
                start=datetime(year=2020, month=3, day=18, hour=20),
                status=AdrEventStatus.PENDING,
                signals=[
                    dict(index=0, duration=timedelta(hours=2), level=2.0),
                ],
            ),
        ]
        xml_events = [e.to_obj() for e in events]

        with freeze_time(events[0].start - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].signals[0]["level"]

        with freeze_time(events[0].end + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_cancelled_event(self):
        print('in test_cancelled_event()')
        events = [
            AdrEvent(
                id="EventID",
                start=datetime(year=2020, month=3, day=18, hour=8),
                end=datetime(year=2020, month=3, day=18, hour=8, second=1),
                status=AdrEventStatus.CANCELLED,
                signals=[
                    dict(index=0, duration=timedelta(hours=10), level=3.0),
                ],
            ),
        ]
        xml_events = [e.to_obj() for e in events]

        with freeze_time(events[0].start - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].end + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_multiple_events(self):
        print('in test_multiple_events()')
        events = [
            AdrEvent(
                id="EventID1",
                start=datetime(year=2020, month=3, day=18, hour=8),
                status=AdrEventStatus.ACTIVE,
                signals=[
                    dict(index=0, duration=timedelta(hours=2), level=1.0),
                ],
            ),
            AdrEvent(
                id="EventID2",
                start=datetime(year=2020, month=3, day=18, hour=8),
                end=datetime(year=2020, month=3, day=18, hour=8, second=1),
                status=AdrEventStatus.CANCELLED,
                signals=[
                    dict(index=0, duration=timedelta(hours=10), level=3.0),
                ],
            ),
            AdrEvent(
                id="EventID3",
                start=datetime(year=2020, month=3, day=18, hour=10),
                status=AdrEventStatus.ACTIVE,
                signals=[
                    dict(index=0, duration=timedelta(hours=4), level=3.0),
                    dict(index=1, duration=timedelta(hours=4), level=2.0),
                ]
            ),
            AdrEvent(
                id="EventID4",
                start=datetime(year=2020, month=3, day=18, hour=20),
                status=AdrEventStatus.PENDING,
                signals=[
                    dict(index=0, duration=timedelta(hours=2), level=2.0),
                ],
            ),
        ]
        xml_events = [e.to_obj() for e in events]

        with freeze_time(events[0].start - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].signals[0]["level"]

        with freeze_time(events[0].start + events[2].raw_signals[0]["duration"] + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[2].signals[0]["level"]

        with freeze_time(events[2].start + events[2].raw_signals[1]["duration"] + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[2].signals[1]["level"]

        with freeze_time(events[2].end + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[3].start + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[3].signals[0]["level"]

        with freeze_time(events[3].end + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0


if __name__ == '__main__':
    unittest.main()

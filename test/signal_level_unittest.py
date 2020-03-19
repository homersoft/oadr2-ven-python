import unittest
from datetime import datetime, timedelta

from freezegun import freeze_time

from oadr2.poll import OpenADR2
from test.adr_event_generator import AdrEventStatus, AdrEvent, AdrInterval


class SignalLevelTest(unittest.TestCase):
    def setUp(self):
        self.start_thread = False

        # Some configureation variables, by default, this is for the a handler
        config = {'vtn_ids': 'vtn_1,vtn_2,vtn_3,TH_VTN',
                  'ven_id': 'ven_py'}

        self.adr_client = OpenADR2(
            event_config=config,
            vtn_base_uri="",
            start_thread=self.start_thread
        )

        # Make things a little nicer for us to see
        print('')
        print((40 * '='))

    def tearDown(self):
        self.adr_client.event_handler.update_all_events({}, '')  # Clear out the database

    def test_active_event_with_single_interval(self):
        print('in test_active_event_with_single_interval()')
        events = [
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=8),
                status=AdrEventStatus.ACTIVE,
                intervals=[
                    AdrInterval(1.0, timedelta(hours=5)),
                ],
            ),
        ]
        xml_events = [e.to_etree() for e in events]

        with freeze_time(events[0].start_time - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].intervals[0].value

        with freeze_time(events[0].stop_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_active_event_with_multiple_intervals(self):
        print('in test_active_event_with_multiple_intervals()')
        events = [
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=10),
                status=AdrEventStatus.ACTIVE,
                intervals=[
                    AdrInterval(3.0, timedelta(hours=4)),
                    AdrInterval(2.0, timedelta(hours=4)),
                ],
            ),
        ]
        xml_events = [e.to_etree() for e in events]

        with freeze_time(events[0].start_time - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].intervals[0].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].intervals[0].value

        with freeze_time(events[0].intervals[1].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].intervals[1].value

        with freeze_time(events[0].stop_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_pending_event(self):
        print('in test_pending_event()')
        events = [
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=20),
                status=AdrEventStatus.PENDING,
                intervals=[
                    AdrInterval(2.0, timedelta(hours=2)),
                ],
            ),
        ]
        xml_events = [e.to_etree() for e in events]

        with freeze_time(events[0].start_time - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].intervals[0].value

        with freeze_time(events[0].stop_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_cancelled_event(self):
        print('in test_cancelled_event()')
        events = [
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=8),
                status=AdrEventStatus.CANCELLED,
                intervals=[
                    AdrInterval(3.0, timedelta(hours=10)),
                ],
            ),
        ]
        xml_events = [e.to_etree() for e in events]

        with freeze_time(events[0].start_time - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].stop_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

    def test_multiple_events(self):
        print('in test_multiple_events()')
        events = [
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=8),
                status=AdrEventStatus.ACTIVE,
                intervals=[
                    AdrInterval(1.0, timedelta(hours=5)),
                ],
            ),
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=8),
                status=AdrEventStatus.CANCELLED,
                intervals=[
                    AdrInterval(3.0, timedelta(hours=10)),
                ],
            ),
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=10),
                status=AdrEventStatus.ACTIVE,
                intervals=[
                    AdrInterval(3.0, timedelta(hours=4)),
                    AdrInterval(2.0, timedelta(hours=4)),
                ]
            ),
            AdrEvent(
                start_time=datetime(year=2020, month=3, day=18, hour=20),
                status=AdrEventStatus.PENDING,
                intervals=[
                    AdrInterval(2.0, timedelta(hours=2)),
                ],
            ),
        ]
        xml_events = [e.to_etree() for e in events]

        with freeze_time(events[0].start_time - timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[0].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[0].intervals[0].value

        with freeze_time(events[2].intervals[0].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[2].intervals[0].value

        with freeze_time(events[2].intervals[1].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[2].intervals[1].value

        with freeze_time(events[2].stop_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0

        with freeze_time(events[3].start_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == events[3].intervals[0].value

        with freeze_time(events[3].stop_time + timedelta(minutes=1)):
            signal_level, *_ = self.adr_client.event_controller._calculate_current_event_status(xml_events)
            assert signal_level == 0


if __name__ == '__main__':
    unittest.main()

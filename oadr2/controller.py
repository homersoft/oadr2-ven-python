import logging
import threading
from datetime import datetime
from typing import List

from oadr2.schemas import EventSchema

CONTROL_LOOP_INTERVAL = 30   # update control state every X second


# Used by poll.OpenADR2 to handle events
class EventController(object):
    '''
    EventController tracks active events and fires a callback when event levels have
    changed.

    Member Variables:
    --------
    event_handler -- The EventHandler instance
    current_signal_level -- current signal level of a realy/point
    control_loop_interval -- How often to run the control loop
    control_thread -- threading.Thread() object w/ name of 'oadr2.control'
    _control_loop_signal -- threading.Event() object
    _exit -- A threading.Thread() object
    '''

    def __init__(
            self,
            event_handler,
            signal_changed_callback=None,
            start_thread=True,
            control_loop_interval=CONTROL_LOOP_INTERVAL
    ):
        '''
        Initialize the Event Controller

        event_handler -- An instance of event.EventHandler
        start_thread -- Start the control thread
        control_loop_interval -- How often to run the control loop
        '''

        self.event_handler = event_handler
        self.current_signal_level = 0

        self.signal_changed_callback = signal_changed_callback \
                if signal_changed_callback is not None \
                else self.default_signal_callback

        # Add an exit thread for the module
        self._exit = threading.Event()
        self._exit.clear()

        self._control_loop_signal = threading.Event()
        self.control_loop_interval = control_loop_interval

        # The control thread
        self.control_thread = None

        if start_thread:
            self.control_thread = threading.Thread(
                name='oadr2.control',
                target=self._control_event_loop
            )
            self.control_thread.daemon = True
            self.control_thread.start()

    def events_updated(self):
        '''
        Call this when some events have updated to cause the control
        loop to refresh
        '''
        self._control_loop_signal.set()

    def get_current_signal_level(self):
        '''
        Return the signal level and event ID of the currently active event.
        If no events are active, this will return (0,None)
        '''

        signal_level, event_id, expired_events = self._calculate_current_event_status(
            self.event_handler.get_active_events()
        )

        return signal_level, event_id

    def _control_event_loop(self):
        '''
        This is the threading loop to perform control based on current oadr events
        Note the current implementation simply loops based on CONTROL_LOOP_INTERVAL
        except when an updated event is received by a VTN.
        '''
        while not self._exit.is_set():
            try:
                logging.debug("Updating control states...")
                events = self.event_handler.get_active_events()

                new_signal_level = self._update_control(events)
                logging.debug("Highest signal level is: %f", new_signal_level)

                changed = self._update_signal_level(new_signal_level)
                if changed:
                    logging.debug("Updated current signal level!")

            except Exception as ex:
                logging.exception("Control loop error: %s", ex)

            self._control_loop_signal.wait(CONTROL_LOOP_INTERVAL)
            self._control_loop_signal.clear() # in case it was triggered by a poll update

        logging.info("Control loop exiting.")

    def _update_control(self, events):
        '''
        Called by `control_event_loop()` to determine the current signal level.
        This also deletes any events from the database that have expired.

        events -- List of lxml.etree.ElementTree objects (with OpenADR 2.0 tags)
        '''
        signal_level, event_id, remove_events = self._calculate_current_event_status(events)

        if remove_events:
            # remove any events that we've detected have ended or been cancelled.
            # TODO callback for expired events??
            logging.debug("Removing completed or cancelled events: %s", remove_events)
            self.event_handler.remove_events(remove_events)

        if event_id:
            self.event_handler.update_active_status(event_id)

        return signal_level

    def _calculate_current_event_status(self, events: List[EventSchema]):
        '''
        returns a 3-tuple of (current_signal_level, current_event_id, remove_events=[])
        '''

        highest_signal_val = 0
        current_event = None
        remove_events = []  # to collect expired events
        now = datetime.utcnow()

        for evt in events:
            try:
                if evt.status is None:
                    logging.debug(f"Ignoring event {evt.id} - no valid status")
                    continue

                if evt.status.lower() == "cancelled" and datetime.utcnow() > evt.end:
                    logging.debug(f"Event {evt.id}({evt.mod_number}) has been cancelled")
                    remove_events.append(evt.id)
                    continue

                if not evt.signals:
                    logging.debug(f"Ignoring event {evt.id} - no valid signals")
                    continue

                current_interval = evt.get_current_interval(now=now)
                if current_interval is None:
                    if evt.end < now:
                        logging.debug(f"Event {evt.id}({evt.mod_number}) has ended")
                        remove_events.append(evt.id)
                        continue

                    elif evt.start > now:
                        logging.debug(f"Event {evt.id}({evt.mod_number}) has not started yet.")
                        continue

                    else:
                        logging.warning(f"Error getting current interval for event {evt.id}({evt.mod_number}):"
                                        f"Signals: {evt.signals}")
                        continue

                if evt.test_event:
                    logging.debug(f"Ignoring event {evt.id} - test event")
                    continue

                logging.debug(
                    f'Control loop: Evt ID: {evt.id}({evt.mod_number}); '
                    f'Interval: {current_interval.index}; Current Signal: {current_interval.level}'
                )

                if current_interval.level > highest_signal_val or not current_event:
                    if not current_event or evt.priority > current_event.priority:
                        highest_signal_val = current_interval.level
                        current_event = evt

            except Exception as ex:
                logging.exception(f"Error parsing event: {evt.id}: {ex}")

        return highest_signal_val, current_event.id if current_event else None, remove_events

    def _update_signal_level(self, signal_level):
        '''
        Called once each control interval with the 'current' signal level.
        If the signal level has changed from `current_signal_level`, this
        calls `self.signal_changed_callback(current_signal_level, new_signal_level)`
        and then sets `self.current_signal_level = new_signal_level`.

        signal_level -- If it is the same as the current signal level, the
                        function will exit.  Else, it will change the
                        signal relay

        returns True if the signal level has changed from the `current_signal_level`
            or False if the signal level has not changed.
        '''

        # check if the current signal level is different from the new signal level
        if signal_level == self.current_signal_level:
            return False

        try:
            self.signal_changed_callback(self.current_signal_level, signal_level)

        except Exception as ex:
            logging.exception("Error from callback! %s", ex)

        self.current_signal_level = signal_level
        return True

    def default_signal_callback(self, old_level, new_level):
        '''
        The default callback just logs a message.
        '''
        logging.debug(f"Signal level changed from {old_level} to {new_level}")

    def exit(self):
        '''
        Shutdown the threads for the module
        '''
        self._exit.set()
        self._control_loop_signal.set()  # interrupt sleep
        self.control_thread.join(2)

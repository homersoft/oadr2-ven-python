# BaseHandler class - Acts a base object for poll.OpenADR2 and xmpp.OpenADR2

__author__ = 'Benjamin N. Summerton <bsummerton@enernoc.com>'

import threading

from oadr2 import controller, event, logger


class BaseHandler(object):
    '''
    This object acts as a base for poll.OpenADR2 and xmpp.OpenADR2.

    Member Variables:
    --------
    event_handler -- The event.EventHandler instance
    event_controller -- A control.EventController object.
    _exit -- A threading object via threading.Event()
    --------
    '''

    def __init__(self, event_config, control_opts={}):
        '''
        base class initializer, creates an `event.EventHandler` as
        `self.event_handler` and a `control.EventController` as
        `self.event_controller

        event_config -- A dictionary containing keyword arugments for the
                        EventHandler
        control_opts -- a dict of opts for `control.EventController` init
        '''

        # Get an EventHandler and an EventController
        self.event_handler = event.EventHandler(**event_config)
        self.event_controller = controller.EventController(self.event_handler, **control_opts)

        # Add an exit thread for the module
        self._exit = threading.Event()
        self._exit.clear()

        logger.info('Created base handler.')


    def exit(self):
        '''
        Shutdown the base handler and its threads.
        '''

        self.event_controller.exit()    # Stop the event controller
        self._exit.set()

        logger.info('Shutdown base handler.')

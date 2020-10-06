import logging
import threading
import urllib.error
import urllib.parse
import urllib.request
from random import uniform

import requests
from lxml import etree

from oadr2 import base

# HTTP parameters:
REQUEST_TIMEOUT = 5  # HTTP request timeout
DEFAULT_VTN_POLL_INTERVAL = 300  # poll the VTN every X seconds
MINIMUM_POLL_INTERVAL = 10
POLLING_JITTER = 0.1  # polling interval +/-
OADR2_URI_PATH = 'OpenADR2/Simple/'  # URI of where the VEN needs to request from


class OpenADR2(base.BaseHandler):
    '''
    poll.OpenADR2 is the class for sending requests and responses for OpenADR
    2.0 events over HTTP.

    Member Variables:
    --------
    (Everything from base.BaseHandler)
    vtn_base_uri
    vtn_poll_interval
    ven_client_cert_key
    ven_client_cert_pem
    vtn_ca_certs
    poll_thread
    '''

    def __init__(self, event_config, vtn_base_uri,
                 control_opts={},
                 username=None,
                 password=None,
                 ven_client_cert_key=None,
                 ven_client_cert_pem=None,
                 vtn_ca_certs=False,
                 vtn_poll_interval=DEFAULT_VTN_POLL_INTERVAL,
                 start_thread=True):
        '''
        Sets up the class and intializes the HTTP client.

        event_config -- A dictionary containing key-word arugments for the
                        EventHandller
        ven_client_cert_key -- Certification Key for the HTTP Client
        ven_client_cert_pem -- PEM file/string for the HTTP Client
        vtn_base_uri -- Base URI of the VTN's location
        vtn_poll_interval -- How often we should poll the VTN
        vtn_ca_certs -- CA Certs for the VTN
        start_thread -- start the thread for the poll loop or not? left as a legacy option
        '''

        # Call the parent's methods
        super(OpenADR2, self).__init__(event_config, control_opts)

        # Get the VTN's base uri set
        self.vtn_base_uri = vtn_base_uri
        if self.vtn_base_uri:  # append path
            join_char = '/' if self.vtn_base_uri[-1] != '/' else ''
            self.vtn_base_uri = join_char.join((self.vtn_base_uri, OADR2_URI_PATH))
        try:
            self.vtn_poll_interval = int(vtn_poll_interval)
            assert self.vtn_poll_interval >= MINIMUM_POLL_INTERVAL
        except ValueError:
            logging.warning('Invalid poll interval: %s', self.vtn_poll_interval)
            self.vtn_poll_interval = DEFAULT_VTN_POLL_INTERVAL

        # Security & Authentication related
        self.ven_certs = (ven_client_cert_pem, ven_client_cert_key)\
            if ven_client_cert_pem and ven_client_cert_key else None
        self.vtn_ca_certs = vtn_ca_certs
        self.__username = username
        self.__password = password

        self.poll_thread = None
        if start_thread:  # this is left for backward compatibility
            self.start()

        logging.info(" +++++++++++++++ OADR2 module started ++++++++++++++")

    def start(self):
        '''
        Initialize the HTTP client.

        start_thread -- To start the polling thread or not.
        '''

        if self.poll_thread and self.poll_thread.is_alive():
            logging.warning("Thread is already running")
            return

        self.poll_thread = threading.Thread(
            name='oadr2.poll',
            target=self.poll_vtn_loop)
        self.poll_thread.daemon = True
        self._exit.clear()

        self.poll_thread.start()
        logging.info("Polling thread started")

    def stop(self):
        '''
        Stops polling without stopping event controller
        :return:
        '''
        if self.poll_thread is not None:
            self.poll_thread.join(2)  # they are daemons.

        self._exit.set()

        logging.info("Polling thread stopped")

    def exit(self):
        '''
        Shutdown the HTTP client, join the running threads and exit.
        '''

        if self.poll_thread is not None:
            self.poll_thread.join(2)  # they are daemons.

        super(OpenADR2, self).exit()

    def poll_vtn_loop(self):
        '''
        The threading loop which polls the VTN on an interval
        '''

        while not self._exit.is_set():
            try:
                self.query_vtn()

            except urllib.error.HTTPError as ex:  # 4xx or 5xx HTTP response:
                logging.warning("HTTP error: %s\n%s", ex, ex.read())

            except urllib.error.URLError as ex:  # network error.
                logging.debug("Network error: %s", ex)

            except Exception as ex:
                logging.exception("Error in OADR2 poll thread: %s", ex)

            self._exit.wait(
                uniform(
                    self.vtn_poll_interval*(1-POLLING_JITTER),
                    self.vtn_poll_interval*(1+POLLING_JITTER)
                )
            )
        logging.info(" +++++++++++++++ OADR2 polling thread has exited.")

    def query_vtn(self):
        '''
        Query the VTN for an event.
        '''

        if not self.vtn_base_uri:
            logging.warning("VTN base URI is invalid: %s", self.vtn_base_uri)
            return

        event_uri = self.vtn_base_uri + 'EiEvent'
        payload = self.event_handler.build_request_payload()

        logging.debug('New request to: %s\n%s\n----', event_uri,
                      etree.tostring(payload, pretty_print=True))

        try:
            resp = requests.post(
                event_uri,
                cert=self.ven_certs,
                verify=self.vtn_ca_certs,
                data=etree.tostring(payload)
            )
        except Exception as ex:
            logging.warning("Connection failed: %s", ex)
            return

        reply = None
        try:
            payload = etree.fromstring(resp.content)
            logging.debug('Got Payload:\n%s\n----', etree.tostring(payload, pretty_print=True))
            reply = self.event_handler.handle_payload(payload)

        except Exception as ex:
            logging.warning("error parsing payload: %s", ex)

        # If we have a generated reply:
        if reply is not None:
            logging.debug('Reply to: %s\n%s\n----',
                          event_uri,
                          etree.tostring(reply, pretty_print=True))

            # tell the control loop that events may have updated
            # (note `self.event_controller` is defined in base.BaseHandler)
            self.event_controller.events_updated()

            self.send_reply(reply, event_uri)  # And send the response

    def send_reply(self, payload, uri):
        '''
        Send a reply back to the VTN.

        payload -- An lxml.etree.ElementTree object containing an OpenADR 2.0
                   payload
        uri -- The URI (of the VTN) where the response should be sent
        '''

        resp = requests.post(
            uri,
            cert=self.ven_certs,
            verify=self.vtn_ca_certs,
            data=etree.tostring(payload),
            timeout=REQUEST_TIMEOUT
        )

        logging.debug("EiEvent response: %s", resp.status_code)

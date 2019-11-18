# poll.OpenADR2 class
# --------
# Requires the python libXML wrapper "lxml"

import http.client
import logging
import os
import socket
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request

from lxml import etree

from oadr2 import base

# HTTP parameters:
CONTENT_TYPE = 'application/xml'
DEFAULT_HEADERS = {
    'user-agent': 'EnerNOC VEN',
    'content-type': CONTENT_TYPE
}
REQUEST_TIMEOUT = 5  # HTTP request timeout
DEFAULT_VTN_POLL_INTERVAL = 300  # poll the VTN every X seconds
OADR2_URI_PATH = 'OpenADR2/Simple/'  # URI of where the VEN needs to request from

# A Cipther list.  To configure properly, see: http://www.openssl.org/docs/apps/ciphers.html#CIPHER_LIST_FORMAT
HTTPS_CIPHERS = 'TLS_RSA_WITH_AES_128_CBC_SHA'


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
                 ven_client_cert_key=None,
                 ven_client_cert_pem=None,
                 vtn_ca_certs=None,
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
        start_thread -- start the thread for the poll loop or not?
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
        except:
            logging.warn('Invalid poll interval: %s', self.vtn_poll_interval)
            self.vtn_poll_interval = DEFAULT_VTN_POLL_INTERVAL

        # Security & Authentication related
        self.ven_client_cert_key = ven_client_cert_key
        self.ven_client_cert_pem = ven_client_cert_pem
        self.vtn_ca_certs = vtn_ca_certs

        self.poll_thread = None
        start_thread = bool(start_thread)
        self._init_client(start_thread)

        logging.info(" +++++++++++++++ OADR2 module started ++++++++++++++ ")

    def _init_client(self, start_thread):
        '''
        Initialize the HTTP client.

        start_thread -- To start the polling thread or not.
        '''
        handlers = []

        if self.ven_client_cert_key:
            logging.debug("Adding HTTPS client cert key: %s, pem: %s",
                          self.ven_client_cert_key, self.ven_client_cert_pem)
            handlers.append(
                HTTPSClientAuthHandler(
                    self.ven_client_cert_key,
                    self.ven_client_cert_pem,
                    self.vtn_ca_certs,
                    ssl_version=ssl.PROTOCOL_TLSv1,
                    ciphers=HTTPS_CIPHERS)
            )

        # This is our HTTP client:
        self.http = urllib.request.build_opener(*handlers)

        self.poll_thread = threading.Thread(
            name='oadr2.poll',
            target=self.poll_vtn_loop)
        self.poll_thread.daemon = True
        if start_thread:
            self.poll_thread.start()

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
                logging.warn("HTTP error: %s\n%s", ex, ex.read())

            except urllib.error.URLError as ex:  # network error.
                logging.debug("Network error: %s", ex)

            except Exception as ex:
                logging.exception("Error in OADR2 poll thread: %s", ex)

            self._exit.wait(self.vtn_poll_interval)
        logging.info(" +++++++++++++++ OADR2 polling thread has exited.")

    def query_vtn(self):
        '''
        Query the VTN for an event.
        '''

        if not self.vtn_base_uri:
            logging.warn("VTN base URI is invalid: %s", self.vtn_base_uri)
            return

        event_uri = self.vtn_base_uri + 'EiEvent'
        payload = self.event_handler.build_request_payload()

        # Make the request
        req = urllib.request.Request(event_uri, etree.tostring(payload), dict(DEFAULT_HEADERS))
        logging.debug('Request to: %s\n%s\n----', req.get_full_url(),
                      etree.tostring(payload, pretty_print=True))

        # Get the response
        resp = self.http.open(req, None, REQUEST_TIMEOUT)
        data = resp.read()
        resp.close()
        #        logging.debug("EiRequestEvent response: %s\n%s", resp.getcode(), data)

        if CONTENT_TYPE not in resp.headers.get("Content-Type", ""):
            logging.warn('Unexpected content type')

        reply = None
        try:
            payload = etree.fromstring(data)
            logging.debug('Got Payload:\n%s\n----', etree.tostring(payload, pretty_print=True))
            reply = self.event_handler.handle_payload(payload)

        except Exception as ex:
            logging.warn("error parsing payload: %s", ex)

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

        request = urllib.request.Request(uri, etree.tostring(payload), dict(DEFAULT_HEADERS))
        resp = self.http.open(request, None, REQUEST_TIMEOUT)
        logging.debug("EiEvent response: %s", resp.getcode())


# http://stackoverflow.com/questions/1875052/using-paired-certificates-with-urllib2
class HTTPSClientAuthHandler(urllib.request.HTTPSHandler):
    '''
    Allows sending a client certificate with the HTTPS connection.
    This version also validates the peer (server) certificate.
    '''

    def __init__(self, key, cert, ca_certs, ssl_version=None, ciphers=None):
        '''
        Intiailize the Client Authentication Handler
        
        key -- An encryption key
        cert -- A Certificate
        ca_certs -- CA Certificates
        ssl_version -- What version of SSL are we using
        ciphers -- What encryption method
        '''

        urllib.request.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert
        self.ca_certs = ca_certs
        self.ssl_version = ssl_version
        self.ciphers = ciphers

    def https_open(self, req):
        '''
        Open a connection.
        Rather than pass in a reference to a connection class, we pass in
        a reference to a function which, for all intents and purposes,
        will behave as a constructor

        req -- A request
        '''

        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=REQUEST_TIMEOUT):
        '''
        Gets a HTTPS connection.

        host -- What host
        timeout -- How long to wait.

        Return HTTPSConnection object
        '''

        return HTTPSConnection(host,
                               key_file=self.key,
                               cert_file=self.cert,
                               timeout=timeout,
                               ciphers=self.ciphers,
                               ca_certs=self.ca_certs,
                               ssl_version=self.ssl_version)


class HTTPSConnection(http.client.HTTPSConnection):
    '''
    Overridden to allow peer certificate validation, configuration
    of SSL/ TLS version and cipher selection.  See:
    http://hg.python.org/cpython/file/c1c45755397b/Lib/httplib.py#l1144
    and `ssl.wrap_socket()`
    '''

    def __init__(self, host, **kwargs):
        '''
        Initilize the HTTPS conneciton

        host -- See httplib.HTTPSConnection's documentation
        kwargs -- See httplib.HTTPSConnection's documentation
        '''

        self.ciphers = kwargs.pop('ciphers', None)
        self.ca_certs = kwargs.pop('ca_certs', None)
        self.ssl_version = kwargs.pop('ssl_version', ssl.PROTOCOL_SSLv23)

        http.client.HTTPSConnection.__init__(self, host, **kwargs)

    def connect(self):
        '''
        Connect to the server.
        '''

        sock = socket.create_connection((self.host, self.port), self.timeout)

        if self._tunnel_host:
            self.sock = sock
            self._tunnel()

        if self.ca_certs and not os.path.isfile(self.ca_certs):
            logging.warn("CA certs file does not exist: %s", self.ca_certs)

        self.sock = ssl.wrap_socket(sock,
                                    self.key_file, self.cert_file,
                                    ca_certs=self.ca_certs,
                                    ciphers=self.ciphers,  # NOTE: This is Python 2.7-only!
                                    cert_reqs=ssl.CERT_REQUIRED if self.ca_certs else ssl.CERT_NONE,
                                    ssl_version=self.ssl_version)

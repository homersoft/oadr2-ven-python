# Classes for sending/receiving OpenADR 2.0 messages via XMPP
# --------
# Requires the python libXML wrapper "lxml" to function properly
#   Please not that the standard python xml library is needed as well

__author__ = 'Thom Nichols <tnichols@enernoc.com>, Benjamin N. Summerton <bsummerton@enernoc.com>'

import threading, logging
from io import StringIO

# NOTE: As stated in header, we are using two different XML libraries.
#       The python standard XML library is needed because of SleekXMPP
#       Yet we try to use the "lxml," module as much as we can.
from lxml import etree as lxml_etree
from xml.etree import cElementTree as std_ElementTree
from xml.etree.cElementTree import XML as std_XML

import sleekxmpp
from sleekxmpp.stanza.iq import Iq
from sleekxmpp.plugins.base import base_plugin
from sleekxmpp.exceptions import XMPPError

from . import base, event



class OpenADR2(base.BaseHandler):
    '''
    xmpp.OpenADR2 is the XMPP equivalent of poll.OpenADR2.  It will wait for an
    oadrDistributeEvent IQ stanza from the XMPP server and then generate a
    response IQ and send it to the server.

    Memeber variables
    --------
    (Everything from base.BaseHandler class)
    xmpp_client - a sleekxmpp.ClientXMPP object, which will intercept the OpenADR2 stuff for us
    user - JID of user for the VEN
    password - Password for accompanying JID
    server_addr - Address of the XMPP Server
    server_port - Port we should connect to
    '''

    def __init__(self, event_config, user, password, server_addr='localhost', server_port=5222):
        '''
        Initilize what will do XMPP magic for us

        **poll_config -- A dictionary of Keyord arguemnts for the base class (poll.OpenADR2)
        user -- JID of whom we want to login to as on the XMPP Server
        password - Password for corresponding JID
        server_addr -- Address of where the XMPP server is located
        server_port -- Port that the XMPP server is listening on
        '''

        base.BaseHandler.__init__(self, event_config)

        # Make sure we set these variables before calling the parent class' constructor
        self.xmpp_client = None
        self.user = user
        self.password = password
        self.server_addr = server_addr
        self.server_port = int(server_port)

        self._init_client(start_thread=True)


    def _init_client(self, start_thread):
        '''
        Setup/Start the client.  The base class has a function of the same name,
        which is also called in its constructor.

        start_thread -- Right now this variable is unused, but is here so it
                        not conflict with its parent's function declaration.
        '''

        # Setup the XMPP Client that we are going to be using
        self.xmpp_client = sleekxmpp.ClientXMPP(self.user, self.password)
        self.xmpp_client.add_event_handler('session_start', self.xmpp_session_start)
        self.xmpp_client.add_event_handler('message', self.xmpp_message)
        self.xmpp_client.register_plugin('xep_0030')
        self.xmpp_client.register_plugin('xep_0199', 
                pconfig={'keepalive': True, 'frequency': 240})
        self.xmpp_client.register_plugin('OpenADR2Plugin', 
                module='oadr2.xmpp',
                pconfig={'callback': self._handle_oadr_payload})

        # Setup system information disco
        self.xmpp_client['xep_0030'].add_identity(
                category='system', 
                itype='version', 
                name='OpenADR2 Python VEN')
       
        # Connect and thread the client
        self.xmpp_client.connect((self.server_addr, self.server_port))
        self.xmpp_client.process(threaded=True)


    def xmpp_session_start(self, event):
        '''
        'session_start' event handler for our XMPP Client.  Will just send our
        presence.

        event -- An empty dictionary.  Parameter is just here because of
                 SleekXMPP requirements.
        '''

        logging.info('XMPP session has started.')
        self.xmpp_client.sendPresence()


    def xmpp_message(self, msg):
        '''
        'message' event handler for our XMPP Client.
        NOTE: OpenADR 2.0 XMPP does not use Message stanzas at all, so we should
        never receive one, but the handler is here to print it out just in case.

        msg -- The Message.
        '''

        logging.info(msg)

        
    def _handle_oadr_payload(self, msg):
        '''
        Handle OpenADR2 payloads

        msg - A type of OADR2Message
        '''

        # Try to generate a response payload and send it back
        try:
            response = self.event_handler.handle_payload(msg.payload)
            logging.debug('Response Payload:\n%s\n----\n',
                    lxml_etree.tostring(response, pretty_print=True))
            self.send_reply( response, msg.from_ )
        except Exception as ex:
            logging.exception("Error processing OADR2 log request: %s", ex)

    
    def send_reply(self, payload, to):
        '''
        Make and OADR2 Message and sends it to someone (if they are online)

        payload - The body of the IQ stanza, i.e. the OpenADR xml stuff 
                  (lxml.etree.Element object)
        to - The JID of whom the messge will go to
        '''

        # And send it if we are connected
        if self.xmpp_client.state.current_state() != 'connected':
            logging.error('Not connected, cannot send response')
            return

        # Build the IQ reply and send it
        iq_reply = Iq(self.xmpp_client, sto=to, stype='set')
        # Change the lxml object to a standard Python XML object
        iq_reply.set_payload(std_XML(lxml_etree.tostring(payload))) 
        self.xmpp_client.send(iq_reply)


    def exit(self):
        '''
        Shutdown the module and client.
        '''

        # Shutdown the xmpp client
        logging.info('Shutting down the XMPP Client...')

        if self.xmpp_client.state.current_state() == 'connected':
            self.xmpp_client.send_presence(pstatus='unavailable')
            self.xmpp_client.disconnect()

        self.xmpp_client.stop.set()
        self.xmpp_client = None
        logging.info('XMPP Client shutdown.')

        base.BaseHandler.exit(self)     # Stop the parent threads



class OADR2Message(object):
    '''
    Message for OADR2 payload.

    Member Variables:
    --------
    payload -- An XML payload
    id -- An ID from the IQ stanza
    from -- Whom it is from
    stanza_type -- What type of stanza was it
    iq_type -- What type of IQ was it (typically 'set' or 'result')
    oadr_profile_level -- What version of OpenADR 2.0 we are using (either 2.0a or 2.0b)
    ns_map -- The namespaces for the corresponding oadr_profile_level
    '''

    def __init__(self, payload=None, 
            id_=None, stanza_type='iq', iq_type='result', 
            from_=None, to=None, 
            oadr_profile_level=event.OADR_PROFILE_20A):
        '''
        Initizlise the message

        payload -- What data we want to send (an lxml object)
        id_ -- ID of the stanza
        stanza_type -- What type of stanza (should be 'iq')
        iq_type -- What type of IQ
        from_ -- JID of who sent it
        to -- Whom it should go to
        oadr_profile_level -- What gersion of OpenADR 2.0 we should be using.
                              Should come from the event module, either
                              event.OADR_PROFILE_20A, or event.OADR_PROFILE_20B
        '''

        self.payload = payload
        self.id = id_
        self.from_ = from_
        self.stanza_type = stanza_type
        self.iq_type = iq_type
        self.oadr_profile_level = oadr_profile_level
        
        # Set the namespace dependant upon the profile level
        if self.oadr_profile_level == event.OADR_PROFILE_20A:
            self.ns_map = event.NS_A
        elif self.oadr_profile_level == event.OADR_PROFILE_20B:
            self.ns_map = event.NS_B
        else:
            self.oadr_profile_level = OADR_PROFILE_20A     # Default/Safety, make it the 2.0a spec 
            self.ns_map = event.NS_A      


    def get_events(self):
        '''
        Get the events from a payload.

        Returns: All of the events as lxml objects
        '''

        return self.payload.findall("%{(oadr)s}oadrEvent/{%(ei)s}eiEvent"%self.ns_map)


    def get_status(self, event):
        '''
        Get the status of an event from the payload.

        event -- event we are looking for.
        
        Returns: The status of the event as an lxml object
        '''

        return event.findtext("{%(ei)s}eventDescriptor/{%(ei)s}eventStatus"%self.ns_map)


    def get_evt_id(self, event):
        '''
        Get's the ID of an event.

        event -- Event we are looking at.

        Returns: An lxml object.
        '''

        return event.findtext("{%(ei)s}eventDescriptor/{%(ei)s}eventID"%self.ns_map)


    def get_mod_num(self, event):
        '''
        Get's the modification number of an event.

        event -- Event we are looking at.

        Returns: An lxml object.
        '''

        return event.findtext("{%(ei)s}eventDescriptor/{%(ei)s}modificationNumber"%self.ns_map)


    def get_current_signal_level(self, event):
        '''
        Get's the current signal levels of an event.

        event -- Event we are looking at.

        REturns: An lxml object.
        '''

        return event.findtext(('{%(ei)s}eiEventSignals/{%(ei)s}eiEventSignal/' + \
                '{%(ei)s}currentValue/{%(ei)s}payloadFloat/{%(ei)s}value')%self.ns_map)

    # Get the message's payload as XML
    # Return: An XML String of the payload.  Does not include IQ tags
    def to_xml(self):
        data = []
        buffer = StringIO()
        if self.payload is not None: 
            buffer.write(lxml_etree.tostring(self.payload))
            data.append(buffer.getvalue())

        return data



class OpenADR2Plugin(base_plugin):
    '''
    OpenADR 2.0 XMPP handler plugin

    Member Variables:
    --------
    All from SleekXMPP's "base_plugin" class
    callback -- What function do we want to handle our messages
    '''


    # Called when initialize the plugin, not the same as __init__
    def plugin_init(self):
        '''
        Initialize the plugin
        '''

        self.xep = 'OADR2'
        self.description = 'OpenADR 2.0 XMPP Plugin'
        self.xmpp.add_handler(
                "<iq type='set'><oadrDistributeEvent xmlns='%s' /></iq>" % event.OADR_XMLNS_A,
                self._handle_iq )
        self.callback = self.config.get('callback')


    def _handle_iq(self, iq):
        '''
        Handle an IQ stanza with a payload containing an "oadrDistributeEvent"
        tag.  This will pass an OADR2Message to 'self.callback'.

        iq -- A SleekXMPP Iq object.
        '''

        logging.debug('OpenADR2 payload [from=%s, to=%s]',
                (iq.get('from'), iq.get('to')))
        try:
            # Convert a "Standard Python Library XML object," to one from lxml
            payload_element = lxml_etree.XML(std_ElementTree.tostring(iq[0]))
            msg = OADR2Message(
                iq_type = iq.get('type'),
                id_ = iq.get('id'), 
                from_ = iq.get('from'),
                payload = payload_element
            )
            
            # And pass it to the message handler
            self.callback(msg)
        except Exception as e:
            logging.exception("OADR2 XMPP parse error: %s", e)
            raise XMPPError(text=e) 


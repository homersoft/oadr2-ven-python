# Event Handler class.
# --------
# Requires the python libXML wrapper "lxml" to function properly
from oadr2 import database, memdb

__author__ = "Thom Nichols <tnichols@enernoc.com>, Ben Summerton <bsummerton@enernoc.com>"

import uuid
import logging
import uuid
from typing import List

from lxml import etree
from lxml.builder import ElementMaker
from datetime import datetime

from oadr2 import schedule

from oadr2 import eventdb
from oadr2.schemas import NS_A, NS_B, OADR_PROFILE_20A, OADR_PROFILE_20B, EventSchema

__author__ = "Thom Nichols <tnichols@enernoc.com>, Ben Summerton <bsummerton@enernoc.com>"


class EventHandler(object):
    '''
    The Event Handler for the project.

    Our member variables:
    --------
    ven_id -- This VEN's id
    vtn_ids -- List of ids of VTNs
    oadr_profile_level -- The profile level we have
    ns_map -- The XML namespace map we are using
    market_contexts -- List of Market Contexts
    group_id -- ID of group that VEN belogns to
    resource_id -- ID of resource in VEN we want to manipulate
    party_id -- ID of the party we are party of
    db_path -- path to db file
    '''

    def __init__(self, ven_id, vtn_ids=None, market_contexts=None,
                 group_id=None, resource_id=None, party_id=None,
                 oadr_profile_level=OADR_PROFILE_20A,
                 event_callback=None, db_path=None):
        '''
        Class constructor

        ven_id -- What is the ID of our unit
        vtn_ids -- CSV string of VTN Ids to accept
        market_contexts -- Another CSV string
        group_id -- Which group we belong to
        resource_id -- What resouce we are
        party_id -- Which party are we party of
        oadr_profile_level -- What version of OpenADR 2.0 we want to use
        event_callback -- a function to call when events are updated and removed.
           The callback should have the signature `cb(updated,removed)` where
           each parameter will be passed a dict in the form `{event_id, event_etree}`
           where `oadr:oadrEvent` is the root element.  You can use functions defined
           in the `event` module to pick out individual values from each event.
        '''

        # 'vtn_ids' is a CSV string of
        self.vtn_ids = vtn_ids
        if self.vtn_ids is not None:
            self.vtn_ids = self.vtn_ids.split(',')

        # 'market_contexts' is also a CSV string
        self.market_contexts = market_contexts
        if self.market_contexts is not None:
            self.market_contexts = self.market_contexts.split(',')

        self.group_id = group_id
        self.resource_id = resource_id
        self.party_id = party_id

        self.ven_id = ven_id

        self.event_callback = event_callback

        # the default profile is '2.0a'; do this to set the ns_map
        self.oadr_profile_level = oadr_profile_level
        if self.oadr_profile_level == OADR_PROFILE_20A:
            self.ns_map = NS_A
        elif self.oadr_profile_level == OADR_PROFILE_20B:
            self.ns_map = NS_B
        else:
            # Default/Safety, make it the 2.0a spec
            self.oadr_profile_level = OADR_PROFILE_20A
            self.ns_map = NS_A

        self.db = eventdb.DBHandler(db_path=db_path)  # TODO: add this back memdb.DBHandler()
        self.optouts = set()

    def handle_payload(self, payload):
        '''
        Handle a payload.  Puts Events into the handler's event list.

        payload -- An lxml.etree.Element object of oadr:oadrDistributeEvent as root node

        Returns: An lxml.etree.Element object; which should be used as a response payload
        '''

        reply_events = []
        all_events = []

        requestID = payload.findtext('pyld:requestID', namespaces=self.ns_map)
        vtnID = payload.findtext('ei:vtnID', namespaces=self.ns_map)

        # If we got a payload from an VTN that is not in our list,
        # send it a 400 message and return
        if self.vtn_ids and (vtnID not in self.vtn_ids):
            logging.warning("Unexpected VTN ID: %s, expected one of %r", vtnID, self.vtn_ids)
            return self.build_error_response(requestID, '400', 'Unknown vtnID: %s' % vtnID)

        # Loop through all of the oadr:oadrEvent 's in the payload
        for evt in payload.iterfind('oadr:oadrEvent', namespaces=self.ns_map):
            response_required = evt.findtext("oadr:oadrResponseRequired", namespaces=self.ns_map)
            evt = evt.find('ei:eiEvent', namespaces=self.ns_map)  # go to nested eiEvent
            new_event = EventSchema.from_xml(evt)
            current_signal_val = get_current_signal_value(evt, self.ns_map)

            logging.debug(
                f'------ EVENT ID: {new_event.id}({new_event.mod_number}); '
                f'Status: {new_event.status}; Current Signal: {current_signal_val}'
            )

            all_events.append(new_event.id)
            old_event = self.db.get_event(new_event.id)

            # For the events we need to reply to, make our "opts," and check the status of the event

            # By default, we optIn and have an "OK," status (200)
            opt = 'optIn'
            status = '200'

            if old_event and (old_event.mod_number > new_event.mod_number):
                logging.warning(
                    f"Got a smaller modification number "
                    f"({new_event.mod_number} < {old_event.mod_number}) for event {new_event.id}"
                )
                status = '403'
                opt = 'optOut'

            if not self.check_target_info(new_event):
                logging.info(f"Opting out of event {new_event.id} - no target match")
                status = '403'
                opt = 'optOut'

            if new_event.id in self.optouts:
                logging.info(f"Opting out of event {new_event.id} - user opted out")
                status = '200'
                opt = 'optOut'

            if not new_event.signals:
                logging.info(f"Opting out of event {new_event.id} - no simple signal")
                opt = 'optOut'
                status = '403'

            if self.market_contexts and (new_event.market_context not in self.market_contexts):
                logging.info(
                    f"Opting out of event {new_event.id} - market context {new_event.market_context} does not match"
                )
                opt = 'optOut'
                status = '405'

            if response_required == 'always':
                reply_events.append((new_event.id, new_event.mod_number, requestID, opt, status))

            # We have a new event or an updated old one
            # if (old_event is None) or (e_mod_num > old_mod_num):
            if opt == "optIn":
                if old_event and (old_event.mod_number < new_event.mod_number):
                    # Add/update the event to our list
                    # updated_events[e_id] = evt
                    if new_event.status == "cancelled" and new_event.status != old_event.status:
                        new_event.cancel()
                    self.db.update_event(new_event)

                if not old_event:
                    if new_event.status == "cancelled":
                        new_event.cancel()
                    self.db.add_event(new_event)

        # Find implicitly cancelled events and get rid of them
        for evt in self.get_active_events():
            if evt.id not in all_events:
                logging.debug(f'Mark event {evt.id} as cancelled')
                evt.cancel()
                self.db.update_event(evt)

        # If we have any in the reply_events list, build some payloads
        logging.debug("Replying for events %r", reply_events)
        reply = None
        if reply_events:
            reply = self.build_created_payload(reply_events)

        return reply

    def build_request_payload(self):
        '''
        Assemble an XML payload to request an event from the VTN.

        Returns: An lxml.etree.Element object
        '''

        oadr = ElementMaker(namespace=self.ns_map['oadr'], nsmap=self.ns_map)
        pyld = ElementMaker(namespace=self.ns_map['pyld'], nsmap=self.ns_map)
        ei = ElementMaker(namespace=self.ns_map['ei'], nsmap=self.ns_map)
        emix = ElementMaker(namespace=self.ns_map['emix'], nsmap=self.ns_map)

        payload = oadr.oadrRequestEvent(
            pyld.eiRequestEvent(
                pyld.requestID(str(uuid.uuid4())),
                #                    emix.marketContext('http://enernoc.com'),
                ei.venID(self.ven_id),
                #                    ei.eventID('asdf'),
                #                    pyld.eventFilter('all'),
                pyld.replyLimit('99')
            )
        )

        return payload

    def build_created_payload(self, events):
        '''
        Assemble an XML payload to send out for events marked "response
        required."

        events -- List of tuples with the following structure:
                    (Event ID, Modification Number, Request ID,
                     Opt, Status)

        Returns: An XML Tree in a string
        '''

        # Setup the element makers
        oadr = ElementMaker(namespace=self.ns_map['oadr'], nsmap=self.ns_map)
        pyld = ElementMaker(namespace=self.ns_map['pyld'], nsmap=self.ns_map)
        ei = ElementMaker(namespace=self.ns_map['ei'], nsmap=self.ns_map)

        def responses(events):
            for e_id, mod_num, requestID, opt, status in events:
                yield ei.eventResponse(
                    ei.responseCode(str(status)),
                    pyld.requestID(requestID),
                    ei.qualifiedEventID(
                        ei.eventID(e_id),
                        ei.modificationNumber(str(mod_num))),
                    ei.optType(opt))

        payload = oadr.oadrCreatedEvent(
            pyld.eiCreatedEvent(
                ei.eiResponse(
                    ei.responseCode('200'),
                    pyld.requestID()),
                ei.eventResponses(*list(responses(events))),
                ei.venID(self.ven_id)))

        logging.debug("Created payload:\n%s",
                      etree.tostring(payload, pretty_print=True))
        return payload

    def build_error_response(self, request_id, code, description=None):
        '''
        Assemble the XML for an error response payload.

        request_id -- Request ID of offending payload
        code -- The HTTP Error Code Status we want to use
        description -- An extra note on what was not acceptable

        Returns: An lxml.etree.Element object containing the payload
        '''

        oadr = ElementMaker(namespace=self.ns_map['oadr'], nsmap=self.ns_map)
        pyld = ElementMaker(namespace=self.ns_map['pyld'], nsmap=self.ns_map)
        ei = ElementMaker(namespace=self.ns_map['ei'], nsmap=self.ns_map)

        payload = oadr.oadrCreatedEvent(
            pyld.eiCreatedEvent(
                ei.eiResponse(
                    ei.responseCode(code),
                    pyld.requestID()),
                ei.venID(self.ven_id)))

        logging.debug("Error payload:\n%s",
                      etree.tostring(payload, pretty_print=True))
        return payload

    def check_target_info(self, evt: EventSchema):
        '''
        Checks to see if we haven been targeted by the event.

        evt -- lxml.etree.ElementTree object w/ an OpenADR Event structure

        Returns: True if we are in the target info, False otherwise.
        '''

        accept = True

        if evt.party_ids or evt.group_ids or evt.resource_ids or evt.ven_ids:
            accept = False

            if evt.party_ids and self.party_id in evt.party_ids:
                accept = True

            if evt.group_ids and self.group_id in evt.group_ids:
                accept = True

            if evt.resource_ids and self.resource_id in evt.resource_ids:
                accept = True

            if evt.ven_ids and self.ven_id in evt.ven_ids:
                accept = True

        return accept

    def fill_event_target_info(self, evt: EventSchema) -> EventSchema:
        evt.group_ids = [self.group_id] if self.group_id else None
        evt.resource_ids = [self.resource_id] if self.resource_id else None
        evt.party_ids = [self.party_id] if self.party_id else None
        evt.ven_ids = [self.ven_id] if self.ven_id else None
        return evt

    def get_active_events(self) -> List[EventSchema]:
        '''
        Get an iterator of all the active events.

        Return: An iterator containing lxml.etree.ElementTree EiEvent objects
        '''
        # Get the events, and convert their XML blobs to lxml objects
        active = self.db.get_active_events()

        for index, evt in enumerate(active):
            evt = self.fill_event_target_info(evt)
            if evt.id in self.optouts:
                active.pop(index)

        return active


    def remove_events(self, evt_id_list):
        '''
        Remove a list of events from our internal member dictionary

        event_id_list - List of Event IDs
        '''
        self.db.remove_events(evt_id_list)
        for evt in evt_id_list:
            self.optouts.discard(evt)

    def optout_event(self, e_id):
        '''
        Opt out of an event by its ID

        :param e_id: ID of the event we want to opt out of
        :return:
        '''

        if e_id not in self.db.get_active_events():
            return  # optout of not existing event

        self.optouts.add(e_id)


def get_current_signal_value(evt, ns_map=NS_A):
    '''
    Gets the signal value of an event

    evt -- lxml.etree.Element object
    ns_map -- Dictionary of namesapces for OpenADR 2.0; default is the 2.0a spec

    Returns: an ei:value value
    '''

    return evt.findtext(
        'ei:eiEventSignals/ei:eiEventSignal/ei:currentValue/' + \
        'ei:payloadFloat/ei:value', namespaces=ns_map)

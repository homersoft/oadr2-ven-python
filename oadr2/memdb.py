from typing import Dict, NamedTuple, Optional, Sequence, Tuple

EventEntry = NamedTuple("EventEntry", (("vtn_id", str), ("mod_num", int), ("raw_xml", str)))


class DBHandler:
    """
    In-memory OADR2 protocol state backing store based on Python dict.
    """

    def __init__(self, db_path=""):
        self.events = {}  # type: Dict[int, EventEntry]

    def init_database(self):
        pass

    def get_active_events(self) -> Dict[int, str]:
        return {_id: e.raw_xml
                for _id, e in self.events.items()}

    def update_all_events(self, records: Sequence[Tuple[str, str, int, str]]) -> None:
        self.events = {event_id: EventEntry(vtn_id, mod_num, raw_xml)
                       for vtn_id, event_id, mod_num, raw_xml in records}

    def update_event(self, e_id: int, mod_num: int, raw_xml: str, vtn_id: str) -> None:
        self.events.update({e_id: EventEntry(vtn_id, mod_num, raw_xml)})

    def get_event(self, event_id: int) -> Optional[str]:
        try:
            return next(e.raw_xml for _id, e in self.events.items() if _id == event_id)
        except StopIteration:
            return None

    def remove_events(self, event_ids: Sequence[int]) -> None:
        for _id in event_ids:
            if _id in self.events:
                del self.events[_id]

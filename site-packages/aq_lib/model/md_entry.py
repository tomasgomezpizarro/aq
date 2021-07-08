from aq_lib.transport.serializable import Serializable


class MDEntry(Serializable):
    def __init__(self, entry_type=None, entry_px=0, entry_size=0, trd_type=None, position=None, mdUpdateAction=None):
        self.entry_type = entry_type
        self.entry_px = entry_px
        self.entry_size = entry_size
        self.trd_type = trd_type
        self.position = position
        self.mdUpdateAction = mdUpdateAction

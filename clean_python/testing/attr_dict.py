# (c) Nelen & Schuurmans


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__dict__ = self

    def dict(self):
        return self

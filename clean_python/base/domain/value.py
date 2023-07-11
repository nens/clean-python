# (c) Nelen & Schuurmans

__all__ = ["Value"]


class Value:
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        return cls(v)  # type: ignore

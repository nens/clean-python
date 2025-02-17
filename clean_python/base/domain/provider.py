__all__ = ["Provider", "SyncProvider"]


class Provider:
    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass


class SyncProvider:
    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

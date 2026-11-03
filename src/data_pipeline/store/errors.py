"""Exceptions raised by the store."""


class StoreError(Exception):
    """Base class of every error the store raises on purpose."""


class CatalogError(StoreError):
    """The catalog is invalid: the message names the entry and the field."""


class KeyRejectedError(StoreError):
    """The source rejected (or lacks) its credential: the rest of its entries are skipped."""


class LockHeldError(StoreError):
    """Another sync holds the lock of this store."""


class NetworkError(StoreError):
    """The source did not answer after every retry. The message is already scrubbed."""


class RateLimitedError(NetworkError):
    """The source answered 429 on every attempt: it is asking for fewer requests, not failing."""


class PeriodError(StoreError):
    """The source's period text does not match the frequency."""


class QuotaExhaustedError(StoreError):
    """The source reported that its quota is used up."""


class UnknownSeriesError(StoreError):
    """No stored series has this key or alias."""

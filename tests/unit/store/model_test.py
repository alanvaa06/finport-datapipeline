from data_pipeline.store.model import CatalogEntry, Frequency, stale_after


def test_key_joins_source_and_native_id():
    assert CatalogEntry(source="fred", source_id="UNRATE").key == "fred:UNRATE"


def test_stale_after_uses_the_frequency_table():
    assert stale_after(Frequency.MONTHLY) == 124
    assert stale_after(Frequency.DAILY) == 10


def test_stale_after_prefers_the_entry_override():
    assert stale_after(Frequency.MONTHLY, 35) == 35


def test_an_entry_has_no_name_or_frequency_unless_declared():
    plain = CatalogEntry(source="fred", source_id="UNRATE")
    assert (plain.name, plain.frequency) == (None, None)
    declared = CatalogEntry(source="banxico", source_id="SP1", name="INPC", frequency=Frequency.MONTHLY)
    assert (declared.name, declared.frequency) == ("INPC", Frequency.MONTHLY)

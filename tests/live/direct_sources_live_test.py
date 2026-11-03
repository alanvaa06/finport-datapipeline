"""One real call to each direct source. Off by default; run with:  pytest -m live tests/live

None of these sources needs a key, only network access. Each test asks for one series of the
bundled macro catalog and checks that its origin still answers in the shape the source reads.
"""

import pytest

from data_pipeline.store.api import Store

SERIES = [
    ("worldbank", "NY.GDP.MKTP.CD/CHL", "A"),
    ("bis", "WS_TC/Q.AR.P.A.M.770.A", "Q"),
    ("ecb", "FM/B.U2.EUR.4F.KR.MRR_FR.LEV", "D"),
    ("eurostat", "une_rt_m/M.SA.TOTAL.PC_ACT.T.AT", "M"),
    ("oecd", "OECD.SDD.STES,DSD_STES@DF_CLI/AUT.M.BCICP.IX._Z.AA.IX._Z.H", "M"),
    ("imf", "IMF.RES,WEO/ARG.GGXCNL_NGDP.A", "A"),
]


@pytest.mark.live
@pytest.mark.parametrize(("source", "identifier", "frequency"), SERIES, ids=[item[0] for item in SERIES])
def test_the_origin_still_answers_in_the_expected_shape(tmp_path, source, identifier, frequency):
    store = Store(tmp_path / "store")
    store.add(source, [identifier])
    report = store.sync()
    assert report.exit_code == 0, report.lines()
    key = f"{source}:{identifier}"
    assert store.info(key).frequency == frequency
    assert len(store.series(key, projections=True)) > 5


@pytest.mark.live
def test_weo_marks_the_years_ahead_as_projections(tmp_path):
    store = Store(tmp_path / "store")
    store.add("imf", ["IMF.RES,WEO/ARG.GGXCNL_NGDP.A"])
    store.sync()
    with_projections = store.series("imf:IMF.RES,WEO/ARG.GGXCNL_NGDP.A", projections=True)
    assert with_projections["projection"].any()
    assert not with_projections["projection"].all()

from factfinder.download import Download
from factfinder.metadata import Metadata

from . import api_key

year = 2019
source = "acs"
geography = 2010

d = Download(api_key, year=year, source=source, geography=geography)
meta = Metadata(year=year, source=source)


def test_geoquery():
    assert type(d.geoqueries["city"]) == list
    assert len(d.geoqueries["city"]) == 1
    assert len(d.geoqueries["borough"]) == 5
    assert len(d.geoqueries["tract"]) == 5
    assert len(d.geoqueries["block"]) == 5


def test_download_e_m():
    pff_variable = "lgarab2"
    v = meta.create_variable(pff_variable)
    E, M, _, _ = v.census_variables
    df = d("tract", pff_variable)
    for e in E:
        assert e in df.columns
    for m in M:
        assert m in df.columns


def test_download_e_m_p_z():
    pff_variable = "f16pl"
    v = meta.create_variable(pff_variable)
    E, M, PE, PM = v.census_variables
    df = d("borough", pff_variable)
    for e in E:
        assert e in df.columns
    for m in M:
        assert m in df.columns
    for pe in PE:
        assert pe in df.columns
    for pm in PM:
        assert pm in df.columns

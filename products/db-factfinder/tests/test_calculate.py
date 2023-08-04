from factfinder.calculate import Calculate

from . import api_key

calculate = Calculate(
    api_key=api_key, year=2019, source="acs", geography="2010_to_2020"
)


def test_calculate_e_m():
    pff_variable = "pop_1"
    geography = ["NTA", "city", "CT20"]
    for g in geography:
        df = calculate.calculate_e_m(pff_variable, g)
        print("\n")
        print(df.head())
    assert True


def test_calculate_e_m_p_z():
    pff_variable = "f16pl"
    geography = ["tract", "city"]
    for g in geography:
        df = calculate.calculate_e_m_p_z(pff_variable, g)
        print("\n")
        print(df.head())
    assert True


def test_calculate_e_m_multiprocessing():
    pff_variables = ["pop_1", "f16pl", "mdpop10t14"]
    df = calculate.calculate_e_m_multiprocessing(pff_variables, geotype="borough")
    assert df.shape[0] == 15


def test_calculate_e_m_median():
    pff_variable = "mdage"
    df = calculate.calculate_e_m_median(pff_variable, geotype="NTA")
    print("\n")
    print(df.head())
    assert True


def test_calculate_e_m_special():
    pff_variable = "mnhhinc"
    df = calculate.calculate_e_m_special(pff_variable, geotype="NTA")
    print("\n")
    print(df.head())
    assert True


def test_calculate_c_e_m_p_z():
    df = calculate.calculate_c_e_m_p_z("mdage", "NTA")
    print("\n")
    print(df.head())
    df = calculate.calculate_c_e_m_p_z("pop_1", "city")
    print("\n")
    print(df.head())
    df = calculate.calculate_c_e_m_p_z("mnhhinc", "city")
    print("\n")
    print(df.head())
    df = calculate.calculate_c_e_m_p_z("mnhhinc", "NTA")
    print("\n")
    print(df.head())


def test_calculate():
    df = calculate("mdage", "NTA")
    print("\n")
    print(df.head())
    df = calculate("pop_1", "city")
    print("\n")
    print(df.head())
    df = calculate("mnhhinc", "city")
    print("\n")
    print(df.head())
    df = calculate("mnhhinc", "NTA")
    print("\n")
    print(df.head())
    df = calculate("asn1rc", "tract")
    print("\n")
    print(df.head())
    df = calculate("asn1rc", "CT20")
    print("\n")
    print(df.head())
    df = calculate("asn1rc", "borough")
    print("\n")
    print(df.head())
    df = calculate("asn1rc", "CDTA")
    print("\n")
    print(df.head())
    df = calculate("asn1rc", "CT20")
    print("\n")
    print(df.head())
    df = calculate("wrkr16pl", "CT20")
    print("\n")
    print(df.head())
    df = calculate("mdemftwrk", "CT20")
    print("\n")
    print(df.head())
    df = calculate("prdtrnsmm", "CT20")
    print("\n")
    print(df.head())

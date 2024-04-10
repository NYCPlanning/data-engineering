"""These are used for counts/fraction and medians. Counts/fractions and medians are handled by different classes that each call these functions"""
industry_mapper = {
    "Agriculture, Forestry, Fishing and Hunting, and Mining": "AgFFHM",
    "Construction": "Cnstn",
    "Manufacturing": "MNfctr",
    "Wholesale Trade": "Whlsl",
    "Retail Trade": "Rtl",
    "Transportation and Warehousing, and Utilities": "TrWHUt",
    "Information": "Info",
    "Finance and Insurance,  and Real Estate and Rental and Leasing": "FIRE",
    "Professional, Scientific, and Management, and  Administrative and Waste Management Services": "PrfSMg",
    "Educational Services, and Health Care and Social Assistance": "EdHlth",
    "Arts, Entertainment, and Recreation, and  Accommodation and Food Services": "ArtEn",
    "Other Services (except Public Administration)": "Oth",
    "Public Administration": "PbAdm",
}
occupation_mapper = {
    "Management, Business, Science, and Arts Occupations": "mbsa",
    "Service Occupations": "srvc",
    "Sales and Office Occupations": "slsoff",
    "Natural Resources, Construction, and Maintenance Occupations": "cstmnt",
    "Production, Transportation, and Material Moving Occupations": "prdtrn",
}


def occupation_assign(person):
    occupation = occupation_mapper.get(person["OCCP"])
    if occupation:
        return f"occupation-{occupation.lower()}"
    return None


def lf_assign(person):
    if (
        person["ESR"] == "N/A (less than 16 years old)"
        or person["ESR"] == "Not in labor force"
    ):
        return None
    return "lf"


def industry_assign(person):
    industry = industry_mapper.get(person["INDP"])
    if industry:
        return f"industry-{industry.lower()}"
    return None

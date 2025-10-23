from __future__ import annotations
from pydantic_xml import BaseXmlModel, element, attr


class ScopeCd(BaseXmlModel, tag="ScopeCd"):
    value: str | None = attr(name="value", default=None)


class MdHrLv(BaseXmlModel, tag="mdHrLv"):
    scope_cd: ScopeCd | None = element(tag="ScopeCd", default=None)


class CharSetCd(BaseXmlModel, tag="CharSetCd"):
    value: str | None = attr(name="value", default=None)


class MdChar(BaseXmlModel, tag="mdChar"):
    charset_cd: CharSetCd | None = element(tag="CharSetCd", default=None)


class PresFormCd(BaseXmlModel, tag="PresFormCd"):
    value: str | None = attr(name="value", default=None)
    sync: str | None = attr(name="Sync", default=None)


class PresForm(BaseXmlModel, tag="presForm"):
    pres_form_cd: PresFormCd | None = element(tag="PresFormCd", default=None)


class IdCitation(BaseXmlModel, tag="idCitation"):
    res_title: str | None = element(tag="resTitle", default=None)
    pres_form: PresForm | None = element(tag="presForm", default=None)


class Consts(BaseXmlModel, tag="Consts"):
    use_limit: str | None = element(tag="useLimit", default=None)


class ResConst(BaseXmlModel, tag="resConst"):
    consts: Consts | None = element(tag="Consts", default=None)


class DataChar(BaseXmlModel, tag="dataChar"):
    charset_cd: CharSetCd | None = element(tag="CharSetCd", default=None)


class LanguageCode(BaseXmlModel, tag="languageCode"):
    value: str | None = attr(name="value", default=None)
    sync: str | None = attr(name="Sync", default=None)


class CountryCode(BaseXmlModel, tag="countryCode"):
    value: str | None = attr(name="value", default=None)
    sync: str | None = attr(name="Sync", default=None)


class DataLang(BaseXmlModel, tag="dataLang", search_mode="unordered"):
    language_code: LanguageCode | None = element(tag="languageCode", default=None)
    country_code: CountryCode | None = element(tag="countryCode", default=None)


class SpatRepTypCd(BaseXmlModel, tag="SpatRepTypCd"):
    value: str | None = attr(name="value", default=None)
    sync: str | None = attr(name="Sync", default=None)


class SpatRpType(BaseXmlModel, tag="spatRpType"):
    spat_rep_type_cd: SpatRepTypCd | None = element(tag="SpatRepTypCd", default=None)


class ExTypeCode(BaseXmlModel, tag="exTypeCode"):
    sync: str | None = attr(name="exTypeCode", default=None)
    code: str | None = element(text=True, default=None)


class WestBL(BaseXmlModel, tag="westBL"):
    sync: str | None = attr(name="westBL", default=None)
    bound: str | None = element(text=True, default=None)


class EastBL(BaseXmlModel, tag="eastBL"):
    sync: str | None = attr(name="eastBL", default=None)
    bound: str | None = element(text=True, default=None)


class NorthBL(BaseXmlModel, tag="northBL"):
    sync: str | None = attr(name="northBL", default=None)
    bound: str | None = element(text=True, default=None)


class SouthBL(BaseXmlModel, tag="southBL"):
    sync: str | None = attr(name="southBL", default=None)
    bound: str | None = element(text=True, default=None)


class GeoBndBox(BaseXmlModel, tag="GeoBndBox", search_mode="unordered"):
    esri_extent_type: str | None = attr(name="esriExtentType", default=None)
    ex_type_code: ExTypeCode | None = element(tag="exTypeCode", default=None)
    west_bl: WestBL | None = element(tag="westBL", default=None)
    east_bl: EastBL | None = element(tag="eastBL", default=None)
    north_bl: NorthBL | None = element(tag="northBL", default=None)
    south_bl: SouthBL | None = element(tag="southBL", default=None)


class GeoEle(BaseXmlModel, tag="geoEle"):
    geo_bnd_box: GeoBndBox | None = element(tag="GeoBndBox", default=None)


class DataExt(BaseXmlModel, tag="dataExt"):
    geo_ele: GeoEle | None = element(tag="geoEle", default=None)


class EnvirDesc(BaseXmlModel, tag="envirDesc"):
    sync: str | None = attr(name="Sync", default="TRUE")
    environment: str | None = (
        None  # element(text=True, default=None, strip_whitespace=True)
    )


class SearchKeys(BaseXmlModel, tag="searchKeys", search_mode="unordered"):
    keyword: list[str] | None = element(tag="keyword", default_factory=list)


class OtherKeys(BaseXmlModel, tag="otherKeys", search_mode="unordered"):
    keyword: list[str] | None = element(tag="keyword", default_factory=list)


class DataIdInfo(BaseXmlModel, tag="dataIdInfo", search_mode="unordered"):
    id_citation: IdCitation | None = element(tag="idCitation", default=None)
    id_abs: str | None = element(tag="idAbs", default=None)
    id_purp: str | None = element(tag="idPurp", default=None)
    id_credit: str | None = element(tag="idCredit", default=None)
    other_keys: OtherKeys | None = element(tag="otherKeys", default=None)
    search_keys: SearchKeys | None = element(tag="searchKeys", default=None)
    res_const: ResConst | None = element(tag="resConst", default=None)
    data_char: DataChar | None = element(tag="dataChar", default=None)
    envir_desc: EnvirDesc | None = element(tag="envirDesc", default=None)
    data_lang: DataLang | None = element(tag="dataLang", default=None)
    spat_rp_type: SpatRpType | None = element(tag="spatRpType", default=None)
    data_ext: DataExt | None = element(tag="dataExt", default=None)


class MdLang(BaseXmlModel, tag="mdLang", search_mode="unordered"):
    language_code: LanguageCode | None = element(tag="languageCode", default=None)
    country_code: CountryCode | None = element(tag="countryCode", default=None)


class DistFormat(BaseXmlModel, tag="distFormat"):
    format_name: str | None = element(tag="formatName", default=None)


class DistTranOps(BaseXmlModel, tag="distTranOps"):
    trans_size: str | None = element(tag="transSize", default=None)


class DistInfo(BaseXmlModel, tag="distInfo", search_mode="unordered"):
    dist_format: DistFormat | None = element(tag="distFormat", default=None)
    dist_tran_ops: DistTranOps | None = element(tag="distTranOps", default=None)


class IdentCode(BaseXmlModel, tag="identCode"):
    code: str | None = attr(name="code", default=None)
    sync: str | None = attr(name="Sync", default=None)


class RefSysID(BaseXmlModel, tag="refSysID", search_mode="unordered"):
    ident_code: IdentCode | None = element(tag="identCode", default=None)
    id_code_space: str | None = element(tag="idCodeSpace", default=None)
    id_version: str | None = element(tag="idVersion", default=None)


class RefSystem(BaseXmlModel, tag="RefSystem"):
    ref_sys_id: RefSysID | None = element(tag="refSysID", default=None)


class RefSysInfo(BaseXmlModel, tag="refSysInfo"):
    ref_system: RefSystem | None = element(tag="RefSystem", default=None)


class GeoObjTypCd(BaseXmlModel, tag="GeoObjTypCd"):
    value: str | None = attr(name="value", default=None)
    sync: str | None = attr(name="Sync", default=None)


class GeoObjTyp(BaseXmlModel, tag="geoObjTyp"):
    geo_obj_type_cd: GeoObjTypCd | None = element(tag="GeoObjTypCd", default=None)


class GeometObjs(BaseXmlModel, tag="geometObjs", search_mode="unordered"):
    name: str | None = attr(name="Name", default=None)
    geo_obj_typ: GeoObjTyp | None = element(tag="geoObjTyp", default=None)
    geo_obj_cnt: str | None = element(tag="geoObjCnt", default=None)


class TopoLevCd(BaseXmlModel, tag="TopoLevCd"):
    value: str | None = attr(name="value", default=None)
    sync: str | None = attr(name="Sync", default=None)
    code: str | None = element(text=True, default=None)


class TopLvl(BaseXmlModel, tag="topLvl"):
    topo_lev_cd: TopoLevCd | None = element(tag="TopoLevCd", default=None)


class VectSpatRep(BaseXmlModel, tag="VectSpatRep", search_mode="unordered"):
    geomet_objs: GeometObjs | None = element(tag="geometObjs", default=None)
    top_lvl: TopLvl | None = element(tag="topLvl", default=None)


class SpatRepInfo(BaseXmlModel, tag="spatRepInfo"):
    vect_spat_rep: VectSpatRep | None = element(tag="VectSpatRep", default=None)


class Esriterm(BaseXmlModel, tag="esriterm", search_mode="unordered"):
    name: str | None = attr(name="Name", default=None)
    efeat_typ: str | None = element(tag="efeatyp", default=None)
    efea_geom: str | None = element(tag="efeageom", default=None)
    esri_topo: str | None = element(tag="esritopo", default=None)
    efea_cnt: str | None = element(tag="efeacnt", default=None)
    sp_index: str | None = element(tag="spindex", default=None)
    lin_refer: str | None = element(tag="linrefer", default=None)


class Ptvctinf(BaseXmlModel, tag="ptvctinf"):
    esri_term: Esriterm | None = element(tag="esriterm", default=None)


class Spdoinfo(BaseXmlModel, tag="spdoinfo"):
    ptvct_inf: Ptvctinf | None = element(tag="ptvctinf", default=None)


class ScaleRange(BaseXmlModel, tag="scaleRange"):
    min_scale: str | None = element(tag="minScale", default=None)
    max_scale: str | None = element(tag="maxScale", default=None)


class MdDateSt(BaseXmlModel, tag="mdDateSt"):
    sync: str | None = attr(name="Sync", default="TRUE")
    date: str | None = None  # element(text=True, default=None)


class NativeExtBox(BaseXmlModel, tag="nativeExtBox"):
    west_bl: str | None = element(tag="westBL", default=None)
    east_bl: str | None = element(tag="eastBL", default=None)
    south_bl: str | None = element(tag="southBL", default=None)
    north_bl: str | None = element(tag="northBL", default=None)
    ex_type_code: str | None = element(tag="exTypeCode", default=None)


class ItemName(BaseXmlModel, tag="itemName"):
    sync: str | None = attr(name="Sync", default=None)
    name: str | None = None  # element(text=True, default=None)


class Linkage(BaseXmlModel, tag="linkage"):
    sync: str | None = attr(name="Sync", default=None)
    path: str | None = None  # element(text=True, default=None)


class Protocol(BaseXmlModel, tag="protocol"):
    sync: str | None = attr(name="Sync", default=None)
    protocol_type: str | None = None  # element(text=True, default=None)


class ItemLocation(BaseXmlModel, tag="itemLocation"):
    linkage: Linkage | None = element(tag="linkage", default=None)
    protocol: Protocol | None = element(tag="protocol", default=None)


class ItemProps(BaseXmlModel, tag="itemProps", search_mode="unordered"):
    item_name: ItemName | None = element(tag="itemName", default=None)
    ims_content_type: str | None = element(tag="imsContentType", default=None)
    native_ext_box: NativeExtBox | None = element(tag="nativeExtBox", default=None)
    item_size: str | None = element(tag="itemSize", default=None)
    item_location: ItemLocation | None = element(tag="itemLocation", default=None)


class CoordRef(BaseXmlModel, tag="coordRef", search_mode="unordered"):
    type: str | None = element(tag="type", default=None)
    geog_csn: str | None = element(tag="geogcsn", default=None)
    cs_units: str | None = element(tag="csUnits", default=None)
    proj_csn: str | None = element(tag="projcsn", default=None)
    pe_xml: str | None = element(tag="peXml", default=None)


class DataProperties(BaseXmlModel, tag="DataProperties", search_mode="unordered"):
    item_props: ItemProps | None = element(tag="itemProps", default=None)
    coord_ref: CoordRef | None = element(tag="coordRef", default=None)


class Data(BaseXmlModel, tag="Data"):
    esri_property_type: str | None = attr(name="EsriPropertyType", default=None)
    code: str | None = element(text=True, default=None)


class Thumbnail(BaseXmlModel, tag="Thumbnail"):
    data: Data | None = element(name="Data", default=None)


class Binary(BaseXmlModel, tag="Binary"):
    thumbnail: Thumbnail | None = element(tag="Thumbnail", default=None)


class Esri(BaseXmlModel, tag="Esri", search_mode="unordered"):
    crea_date: str | None = element(tag="CreaDate", default=None)
    crea_time: str | None = element(tag="CreaTime", default=None)
    arcgis_format: str | None = element(tag="ArcGISFormat", default="1.0")
    sync_once: str | None = element(tag="SyncOnce", default="TRUE")
    data_properties: DataProperties | None = element(tag="DataProperties", default=None)
    sync_date: str | None = element(tag="SyncDate", default=None)
    sync_time: str | None = element(tag="SyncTime", default=None)
    mod_date: str | None = element(tag="ModDate", default=None)
    mod_time: str | None = element(tag="ModTime", default=None)
    scale_range: ScaleRange | None = element(tag="scaleRange", default=None)
    arcgis_profile: str | None = element(tag="ArcGISProfile", default=None)


class Metadata(BaseXmlModel, tag="metadata", search_mode="unordered"):
    """
    Pydantic-XML model for ESRI ArcGIS metadata.

    Note: Element order in XML matters for validation, though we allow
    flexible parsing. The order below reflects the canonical structure.
    """

    lang: str | None = attr(
        name="{http://www.w3.org/XML/1998/namespace}lang", default=None
    )
    esri: Esri | None = element(tag="Esri", default=None)
    md_char: MdChar | None = element(tag="mdChar", default=None)
    md_hr_lv: MdHrLv | None = element(tag="mdHrLv", default=None)
    md_hr_lv_name: str | None = element(tag="mdHrLvName", default=None)
    md_stan_name: str | None = element(tag="mdStanName", default=None)
    md_stan_ver: str | None = element(tag="mdStanVer", default=None)
    data_id_info: DataIdInfo | None = element(tag="dataIdInfo", default=None)
    md_lang: MdLang | None = element(tag="mdLang", default=None)
    dist_info: DistInfo | None = element(tag="distInfo", default=None)
    ref_sys_info: RefSysInfo | None = element(tag="refSysInfo", default=None)
    spat_rep_info: SpatRepInfo | None = element(tag="spatRepInfo", default=None)
    spdo_info: Spdoinfo | None = element(tag="spdoinfo", default=None)
    md_date_st: MdDateSt | None = element(tag="mdDateSt", default=None)
    binary: Binary | None = element(tag="Binary", default=None)


# from __future__ import annotations

# from typing import Optional
# from pydantic_xml import BaseXmlModel, element, attr


# class ScopeCd(BaseXmlModel, tag="ScopeCd"):
#     value: Optional[str] = attr(name="value", default=None)


# class MdHrLv(BaseXmlModel, tag="mdHrLv"):
#     scopecd: Optional[ScopeCd] = element(tag="ScopeCd")


# class ScaleRange(BaseXmlModel, tag="scaleRange", search_mode="unordered"):
#     minScale: Optional[str] = element(tag="minScale", default=None)
#     maxScale: Optional[str] = element(tag="maxScale", default=None)


# class MdDateSt(BaseXmlModel, tag="mdDateSt", search_mode="unordered"):
#     sync: Optional[str] = attr(name="Sync", default="TRUE")
#     date: str | None = element(text=True, default=None)


# class ItemProps(BaseXmlModel, tag="itemProps"): ...


# class CoordRef(BaseXmlModel, tag="coordRef"): ...


# class DataProperties(BaseXmlModel, tag="DataProperties", search_mode="unordered"):
#     itemprops: Optional[ItemProps] = element(tag="itemProps", search_mode="unordered")
#     coordref: Optional[CoordRef] = element(tag="coordRef", search_mode="unordered")


# class Esri(BaseXmlModel, tag="Esri", search_mode="unordered"):
#     creadate: Optional[str] = element(tag="CreaDate")
#     creatime: Optional[str] = element(tag="CreaTime")
#     arcgisformat: Optional[str] = element(tag="ArcGISFormat", default="1.0")
#     synconce: Optional[str] = element(tag="SyncOnce", default="TRUE")
#     dataproperties: Optional[DataProperties] = element(tag="DataProperties")
#     syncdate: Optional[str] = element(tag="SyncDate")
#     synctime: Optional[str] = element(tag="SyncTime")
#     moddate: Optional[str] = element(tag="ModDate")
#     modtime: Optional[str] = element(tag="ModTime")
#     scalerange: Optional[ScaleRange] = element(tag="scaleRange")
#     arcgisprofile: Optional[str] = element(tag="ArcGISProfile", default="")


# class Metadata(BaseXmlModel, tag="metadata", search_mode="unordered"):
#     """Important: element order matters"""

#     lang: Optional[str] = attr(name="{http://www.w3.org/XML/1998/namespace}lang")
#     esri: Optional[Esri] = element(tag="Esri")
#     mdhrlv: Optional[MdHrLv] = element(tag="mdHrLv")
#     mddatest: Optional[MdDateSt] = element(tag="mdDateSt", default=None)
#     # FYI, defaults to allowing extras
#     # model_config = {"extra": "forbid"}

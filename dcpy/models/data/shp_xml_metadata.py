from __future__ import annotations

from typing import Optional
from pydantic_xml import BaseXmlModel, element, attr


class ScopeCd(BaseXmlModel, tag="ScopeCd"):
    value: Optional[str] = attr(name="value", default=None)


class MdHrLv(BaseXmlModel, tag="mdHrLv"):
    scopecd: Optional[ScopeCd] = element(tag="ScopeCd", default=None)


class CharSetCd(BaseXmlModel, tag="CharSetCd"):
    value: Optional[str] = attr(name="value", default=None)


class MdChar(BaseXmlModel, tag="mdChar"):
    charsetcd: Optional[CharSetCd] = element(tag="CharSetCd", default=None)


class PresFormCd(BaseXmlModel, tag="PresFormCd"):
    value: Optional[str] = attr(name="value", default=None)
    sync: Optional[str] = attr(name="Sync", default=None)


class PresForm(BaseXmlModel, tag="presForm"):
    presformcd: Optional[PresFormCd] = element(tag="PresFormCd", default=None)


class IdCitation(BaseXmlModel, tag="idCitation"):
    restitle: Optional[str] = element(tag="resTitle", default=None)
    presform: Optional[PresForm] = element(tag="presForm", default=None)


class Consts(BaseXmlModel, tag="Consts"):
    uselimit: Optional[str] = element(tag="useLimit", default=None)


class ResConst(BaseXmlModel, tag="resConst"):
    consts: Optional[Consts] = element(tag="Consts", default=None)


class DataChar(BaseXmlModel, tag="dataChar"):
    charsetcd: Optional[CharSetCd] = element(tag="CharSetCd", default=None)


class LanguageCode(BaseXmlModel, tag="languageCode"):
    value: Optional[str] = attr(name="value", default=None)
    sync: Optional[str] = attr(name="Sync", default=None)


class CountryCode(BaseXmlModel, tag="countryCode"):
    value: Optional[str] = attr(name="value", default=None)
    sync: Optional[str] = attr(name="Sync", default=None)


class DataLang(BaseXmlModel, tag="dataLang"):
    languagecode: Optional[LanguageCode] = element(tag="languageCode", default=None)
    countrycode: Optional[CountryCode] = element(tag="countryCode", default=None)


class SpatRepTypCd(BaseXmlModel, tag="SpatRepTypCd"):
    value: Optional[str] = attr(name="value", default=None)
    sync: Optional[str] = attr(name="Sync", default=None)


class SpatRpType(BaseXmlModel, tag="spatRpType"):
    spatreptypcd: Optional[SpatRepTypCd] = element(tag="SpatRepTypCd", default=None)


class GeoBndBox(BaseXmlModel, tag="GeoBndBox"):
    esriextenttype: Optional[str] = attr(name="esriExtentType", default=None)
    extypecode: Optional[str] = element(tag="exTypeCode", default=None)
    westbl: Optional[str] = element(tag="westBL", default=None)
    eastbl: Optional[str] = element(tag="eastBL", default=None)
    northbl: Optional[str] = element(tag="northBL", default=None)
    southbl: Optional[str] = element(tag="southBL", default=None)


class GeoEle(BaseXmlModel, tag="geoEle"):
    geobndbox: Optional[GeoBndBox] = element(tag="GeoBndBox", default=None)


class DataExt(BaseXmlModel, tag="dataExt"):
    geoele: Optional[GeoEle] = element(tag="geoEle", default=None)


class DataIdInfo(BaseXmlModel, tag="dataIdInfo"):
    idcitation: Optional[IdCitation] = element(tag="idCitation", default=None)
    idabs: Optional[str] = element(tag="idAbs", default=None)
    idpurp: Optional[str] = element(tag="idPurp", default=None)
    idcredit: Optional[str] = element(tag="idCredit", default=None)
    otherkeys: Optional[OtherKeys] = element(tag="otherKeys", default=None)
    searchkeys: Optional[SearchKeys] = element(tag="searchKeys", default=None)
    resconst: Optional[ResConst] = element(tag="resConst", default=None)
    datachar: Optional[DataChar] = element(tag="dataChar", default=None)
    envirdesc: Optional[str] = element(tag="envirDesc", default=None)
    datalang: Optional[DataLang] = element(tag="dataLang", default=None)
    spatrptype: Optional[SpatRpType] = element(tag="spatRpType", default=None)
    dataext: Optional[DataExt] = element(tag="dataExt", default=None)


class OtherKeys(BaseXmlModel, tag="otherKeys"):
    keyword: list[str] = element(tag="keyword", default_factory=list)


class SearchKeys(BaseXmlModel, tag="searchKeys"):
    keyword: list[str] = element(tag="keyword", default_factory=list)


class MdLang(BaseXmlModel, tag="mdLang"):
    languagecode: Optional[LanguageCode] = element(tag="languageCode", default=None)
    countrycode: Optional[CountryCode] = element(tag="countryCode", default=None)


class DistFormat(BaseXmlModel, tag="distFormat"):
    formatname: Optional[str] = element(tag="formatName", default=None)


class DistTranOps(BaseXmlModel, tag="distTranOps"):
    transsize: Optional[str] = element(tag="transSize", default=None)


class DistInfo(BaseXmlModel, tag="distInfo"):
    distformat: Optional[DistFormat] = element(tag="distFormat", default=None)
    disttranops: Optional[DistTranOps] = element(tag="distTranOps", default=None)


class IdentCode(BaseXmlModel, tag="identCode"):
    code: Optional[str] = attr(name="code", default=None)
    sync: Optional[str] = attr(name="Sync", default=None)


class RefSysID(BaseXmlModel, tag="refSysID"):
    identcode: Optional[IdentCode] = element(tag="identCode")
    idcodespace: Optional[str] = element(tag="idCodeSpace", default=None)
    idversion: Optional[str] = element(tag="idVersion", default=None)


class RefSystem(BaseXmlModel, tag="RefSystem"):
    refsysid: Optional[RefSysID] = element(tag="refSysID", default=None)


class RefSysInfo(BaseXmlModel, tag="refSysInfo"):
    refsystem: Optional[RefSystem] = element(tag="RefSystem", default=None)


class GeoObjTypCd(BaseXmlModel, tag="GeoObjTypCd"):
    value: Optional[str] = attr(name="value", default=None)
    sync: Optional[str] = attr(name="Sync", default=None)


class GeoObjTyp(BaseXmlModel, tag="geoObjTyp"):
    geoobjtypc: Optional[GeoObjTypCd] = element(tag="GeoObjTypCd", default=None)


class GeometObjs(BaseXmlModel, tag="geometObjs"):
    name: Optional[str] = attr(name="Name", default=None)
    geoobjtyp: Optional[GeoObjTyp] = element(tag="geoObjTyp", default=None)
    geoobjcnt: Optional[str] = element(tag="geoObjCnt", default=None)


class TopoLevCd(BaseXmlModel, tag="TopoLevCd"):
    value: Optional[str] = attr(name="value", default=None)
    sync: Optional[str] = attr(name="Sync", default=None)


class TopLvl(BaseXmlModel, tag="topLvl"):
    topolevcd: Optional[TopoLevCd] = element(tag="TopoLevCd", default=None)


class VectSpatRep(BaseXmlModel, tag="VectSpatRep"):
    geometobjs: Optional[GeometObjs] = element(tag="geometObjs", default=None)
    toplvl: Optional[TopLvl] = element(tag="topLvl", default=None)


class SpatRepInfo(BaseXmlModel, tag="spatRepInfo"):
    vectspatrep: Optional[VectSpatRep] = element(tag="VectSpatRep", default=None)


class Esriterm(BaseXmlModel, tag="esriterm"):
    name: Optional[str] = attr(name="Name", default=None)
    efeattyp: Optional[str] = element(tag="efeatyp", default=None)
    efeageom: Optional[str] = element(tag="efeageom", default=None)
    esritopo: Optional[str] = element(tag="esritopo", default=None)
    efeacnt: Optional[str] = element(tag="efeacnt", default=None)
    spindex: Optional[str] = element(tag="spindex", default=None)
    linrefer: Optional[str] = element(tag="linrefer", default=None)


class Ptvctinf(BaseXmlModel, tag="ptvctinf"):
    esriterm: Optional[Esriterm] = element(tag="esriterm", default=None)


class Spdoinfo(BaseXmlModel, tag="spdoinfo"):
    ptvctinf: Optional[Ptvctinf] = element(tag="ptvctinf", default=None)


class ScaleRange(BaseXmlModel, tag="scaleRange", search_mode="unordered"):
    minScale: Optional[str] = element(tag="minScale", default=None)
    maxScale: Optional[str] = element(tag="maxScale", default=None)


class MdDateSt(BaseXmlModel, tag="mdDateSt", search_mode="unordered"):
    sync: Optional[str] = attr(name="Sync", default="TRUE")
    date: Optional[str] = None


class NativeExtBox(BaseXmlModel, tag="nativeExtBox"):
    westbl: Optional[str] = element(tag="westBL", default=None)
    eastbl: Optional[str] = element(tag="eastBL", default=None)
    southbl: Optional[str] = element(tag="southBL", default=None)
    northbl: Optional[str] = element(tag="northBL", default=None)
    extypecode: Optional[str] = element(tag="exTypeCode", default=None)


class ItemName(BaseXmlModel, tag="itemName"):
    sync: Optional[str] = attr(name="Sync", default=None)
    name: Optional[str] = None


class Linkage(BaseXmlModel, tag="linkage"):
    sync: Optional[str] = attr(name="Sync", default=None)
    path: Optional[str] = None


class Protocol(BaseXmlModel, tag="protocol"):
    sync: Optional[str] = attr(name="Sync", default=None)
    protocol_type: Optional[str] = None


class ItemLocation(BaseXmlModel, tag="itemLocation"):
    linkage: Optional[Linkage] = element(tag="linkage", default=None)
    protocol: Optional[Protocol] = element(tag="protocol", default=None)


class ItemProps(BaseXmlModel, tag="itemProps"):
    itemname: Optional[ItemName] = element(tag="itemName", default=None)
    imscontenttype: Optional[str] = element(tag="imsContentType", default=None)
    nativeextbox: Optional[NativeExtBox] = element(tag="nativeExtBox", default=None)
    itemsize: Optional[str] = element(tag="itemSize", default=None)
    itemlocation: Optional[ItemLocation] = element(tag="itemLocation", default=None)


class CoordRef(BaseXmlModel, tag="coordRef"):
    type: Optional[str] = element(tag="type", default=None)
    geogcsn: Optional[str] = element(tag="geogcsn", default=None)
    csunits: Optional[str] = element(tag="csUnits", default=None)
    projcsn: Optional[str] = element(tag="projcsn", default=None)
    pexml: Optional[str] = element(tag="peXml", default=None)


class DataProperties(BaseXmlModel, tag="DataProperties", search_mode="unordered"):
    itemprops: Optional[ItemProps] = element(
        tag="itemProps", search_mode="unordered", default=None
    )
    coordref: Optional[CoordRef] = element(
        tag="coordRef", search_mode="unordered", default=None
    )


class Esri(BaseXmlModel, tag="Esri", search_mode="unordered"):
    creadate: Optional[str] = element(tag="CreaDate", default=None)
    creatime: Optional[str] = element(tag="CreaTime", default=None)
    arcgisformat: Optional[str] = element(tag="ArcGISFormat", default="1.0")
    synconce: Optional[str] = element(tag="SyncOnce", default="TRUE")
    dataproperties: Optional[DataProperties] = element(
        tag="DataProperties", default=None
    )
    syncdate: Optional[str] = element(tag="SyncDate", default=None)
    synctime: Optional[str] = element(tag="SyncTime", default=None)
    moddate: Optional[str] = element(tag="ModDate", default=None)
    modtime: Optional[str] = element(tag="ModTime", default=None)
    scalerange: Optional[ScaleRange] = element(tag="scaleRange", default=None)
    arcgisprofile: Optional[str] = element(tag="ArcGISProfile", default=None)


class Metadata(BaseXmlModel, tag="metadata", search_mode="unordered"):
    """
    Pydantic-XML model for ESRI ArcGIS metadata.

    Note: Element order in XML matters for validation, though search_mode="unordered"
    allows flexible parsing. The order below reflects the canonical structure.
    """

    lang: Optional[str] = attr(name="{http://www.w3.org/XML/1998/namespace}lang")
    esri: Optional[Esri] = element(tag="Esri")
    mdchar: Optional[MdChar] = element(tag="mdChar", default=None)
    mdhrlv: Optional[MdHrLv] = element(tag="mdHrLv", default=None)
    mdhrlvname: Optional[str] = element(tag="mdHrLvName", default=None)
    mdstanname: Optional[str] = element(tag="mdStanName", default=None)
    mdstanver: Optional[str] = element(tag="mdStanVer", default=None)
    dataidinfo: Optional[DataIdInfo] = element(tag="dataIdInfo", default=None)
    mdlang: Optional[MdLang] = element(tag="mdLang", default=None)
    distinfo: Optional[DistInfo] = element(tag="distInfo", default=None)
    refsysinfo: Optional[RefSysInfo] = element(tag="refSysInfo", default=None)
    spatrepinfo: Optional[SpatRepInfo] = element(tag="spatRepInfo", default=None)
    spdoinfo: Optional[Spdoinfo] = element(tag="spdoinfo", default=None)
    mddatest: Optional[MdDateSt] = element(tag="mdDateSt", default=None)


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
#     date: Optional[str] = None


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

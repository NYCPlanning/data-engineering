from __future__ import annotations

from typing import Optional
from pydantic_xml import BaseXmlModel, element, attr


class ScopeCd(BaseXmlModel, tag="ScopeCd"):
    value: Optional[str] = attr(name="value")


class MdHrLv(BaseXmlModel, tag="mdHrLv"):
    scopecd: Optional[ScopeCd] = element(tag="ScopeCd")


class ScaleRange(BaseXmlModel, tag="scaleRange", search_mode="unordered"):
    minScale: Optional[str] = element(tag="minScale")
    maxScale: Optional[str] = element(tag="maxScale")


class Esri(BaseXmlModel, tag="Esri", search_mode="unordered"):
    creadate: Optional[str] = element(tag="CreaDate")
    creatime: Optional[str] = element(tag="CreaTime")
    arcgisformat: Optional[str] = element(tag="ArcGISFormat")
    scalerange: Optional[ScaleRange] = element(tag="scaleRange")
    arcgisprofile: Optional[str] = element(tag="ArcGISProfile", default="")
    synconce: Optional[str] = element(tag="SyncOnce")


class Metadata(BaseXmlModel, tag="metadata", search_mode="unordered"):
    lang: Optional[str] = attr(name="{http://www.w3.org/XML/1998/namespace}lang")
    esri: Optional[Esri] = element(tag="Esri")
    mdhrlv: Optional[MdHrLv] = element(tag="mdHrLv")
    mddatest: Optional[str] = element(tag="mdDateSt")
    # FYI, defaults to allowing extras
    # model_config = {"extra": "forbid"}

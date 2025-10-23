from pathlib import Path

from dcpy.models.data.shp_xml_metadata import Metadata


def test_parse_minimum_shp_metadata():
    p = Path(__file__).parent / "resources" / "shp_metadata_minimum.xml"
    xml = p.read_text(encoding="utf-8")
    md = Metadata.from_xml(xml)
    assert md is not None, "Metadata should not be None"
    assert md.esri is not None, "Esri section should not be None"
    assert md.esri.crea_date == "19261122", (
        f"Creation date expected to be '19261122', but found '{md.esri.crea_date}'"
    )
    assert md.md_hr_lv is not None, "mdHrLv should not be None"
    assert md.md_hr_lv.scope_cd.value == "005", (
        f"Scope code value expected to be '005', but found '{md.md_hr_lv.scope_cd.value}'"
    )
    assert md.md_date_st is not None, "mdDateSt should not be None"
    assert md.md_date_st.sync == "TRUE", (
        f"Sync value expected to be 'TRUE', but found '{md.md_date_st.sync}'"
    )
    assert md.md_date_st.date == "19261122", (
        f"Date expected to be '19261122', but found '{md.md_date_st.date}'"
    )


def test_parse_pluto_shp_metadata():
    p = (
        Path(__file__).parent
        / "resources"
        / "shp_metadata_mappluto_existing_as_iso.xml"
    )
    xml = p.read_text(encoding="utf-8")
    md = Metadata.from_xml(xml)
    assert md is not None, "Metadata should not be None"

    # Metadata root attributes
    assert md.lang == "en", f"Language expected to be 'en', but found '{md.lang}'"

    # Esri section
    assert md.esri is not None, "Esri section should not be None"
    assert md.esri.crea_date == "19700101", (
        f"Creation date expected to be '19700101', but found '{md.esri.crea_date}'"
    )
    assert md.esri.crea_time == "00000000", (
        f"Creation time expected to be '00000000', but found '{md.esri.crea_time}'"
    )
    assert md.esri.arcgis_format == "1.0", (
        f"ArcGIS format expected to be '1.0', but found '{md.esri.arcgis_format}'"
    )
    assert md.esri.sync_once == "FALSE", (
        f"Sync once expected to be 'FALSE', but found '{md.esri.sync_once}'"
    )
    assert md.esri.sync_date == "20251016", (
        f"Sync date expected to be '20251016', but found '{md.esri.sync_date}'"
    )
    assert md.esri.sync_time == "16530800", (
        f"Sync time expected to be '16530800', but found '{md.esri.sync_time}'"
    )
    assert md.esri.mod_date == "20251016", (
        f"Mod date expected to be '20251016', but found '{md.esri.mod_date}'"
    )
    assert md.esri.mod_time == "16530800", (
        f"Mod time expected to be '16530800', but found '{md.esri.mod_time}'"
    )

    # DataProperties
    assert md.esri.data_properties is not None, "Data properties should not be None"
    assert md.esri.data_properties.item_props is not None, (
        "Item properties should not be None"
    )
    assert md.esri.data_properties.item_props.item_name.sync == "TRUE", (
        f"Item name sync expected to be 'TRUE', but found '{md.esri.data_properties.item_props.item_name.sync}'"
    )
    assert md.esri.data_properties.item_props.item_name.name == "MapPLUTO_UNCLIPPED", (
        f"Item name expected to be 'MapPLUTO_UNCLIPPED', but found '{md.esri.data_properties.item_props.item_name.name}'"
    )
    assert md.esri.data_properties.item_props.ims_content_type == "002", (
        f"IMS content type expected to be '002', but found '{md.esri.data_properties.item_props.ims_content_type}'"
    )

    # NativeExtBox
    native_ext = md.esri.data_properties.item_props.native_ext_box
    assert native_ext.west_bl == "913001.169713", (
        f"West bound expected to be '913001.169713', but found '{native_ext.west_bl}'"
    )
    assert native_ext.east_bl == "1067335.951263", (
        f"East bound expected to be '1067335.951263', but found '{native_ext.east_bl}'"
    )
    assert native_ext.south_bl == "119521.548408", (
        f"South bound expected to be '119521.548408', but found '{native_ext.south_bl}'"
    )
    assert native_ext.north_bl == "273128.264171", (
        f"North bound expected to be '273128.264171', but found '{native_ext.north_bl}'"
    )
    assert native_ext.ex_type_code == "1", (
        f"Ex type code expected to be '1', but found '{native_ext.ex_type_code}'"
    )

    assert md.esri.data_properties.item_props.item_size == "132.313", (
        f"Item size expected to be '132.313', but found '{md.esri.data_properties.item_props.item_size}'"
    )

    # CoordRef
    coord_ref = md.esri.data_properties.coord_ref
    assert coord_ref.type == "Projected", (
        f"Coord ref type expected to be 'Projected', but found '{coord_ref.type}'"
    )
    assert coord_ref.geog_csn == "GCS_North_American_1983", (
        f"Geographic coordinate system name expected to be 'GCS_North_American_1983', but found '{coord_ref.geog_csn}'"
    )
    assert coord_ref.cs_units == "Linear Unit: Foot_US (0.304801)", (
        f"Coordinate system units expected to be 'Linear Unit: Foot_US (0.304801)', but found '{coord_ref.cs_units}'"
    )
    assert (
        coord_ref.proj_csn == "NAD_1983_StatePlane_New_York_Long_Island_FIPS_3104_Feet"
    ), (
        f"Projection coordinate system name expected to be 'NAD_1983_StatePlane_New_York_Long_Island_FIPS_3104_Feet', but found '{coord_ref.proj_csn}'"
    )

    # Character set
    assert md.md_char.charset_cd.value == "004", (
        f"Character set code expected to be '004', but found '{md.md_char.charset_cd.value}'"
    )

    # Hierarchy level
    assert md.md_hr_lv.scope_cd.value == "005", (
        f"Scope code value expected to be '005', but found '{md.md_hr_lv.scope_cd.value}'"
    )
    assert md.md_hr_lv_name == "dataset", (
        f"HR level name expected to be 'dataset', but found '{md.md_hr_lv_name}'"
    )
    assert md.md_stan_name == "ArcGIS Metadata", (
        f"Standard name expected to be 'ArcGIS Metadata', but found '{md.md_stan_name}'"
    )
    assert md.md_stan_ver == "1.0", (
        f"Standard version expected to be '1.0', but found '{md.md_stan_ver}'"
    )

    # Data identification info
    assert (
        md.data_id_info.id_citation.res_title == "MapPLUTO 25v2_1 - Water Area Included"
    ), (
        f"Resource title expected to be 'MapPLUTO 25v2_1 - Water Area Included', but found '{md.data_id_info.id_citation.res_title}'"
    )
    assert md.data_id_info.id_citation.pres_form.pres_form_cd.value == "005", (
        f"Presentation form code expected to be '005', but found '{md.data_id_info.id_citation.pres_form.pres_form_cd.value}'"
    )
    assert md.data_id_info.id_abs is not None, "Abstract should not be None"
    assert md.data_id_info.id_purp is not None, "Purpose should not be None"
    assert (
        md.data_id_info.id_credit
        == "NYC Department of City Planning, Information Technology Division"
    ), (
        f"Credit expected to be 'NYC Department of City Planning, Information Technology Division', but found '{md.data_id_info.id_credit}'"
    )

    # Keywords
    expected_keywords = ["Tax Lot", "Parcels", "DTM", "PLUTO", "MapPLUTO", "boundaries"]
    assert md.data_id_info.other_keys.keyword == expected_keywords, (
        f"Other keywords expected to be {expected_keywords}, but found {md.data_id_info.other_keys.keyword}"
    )
    assert md.data_id_info.search_keys.keyword == expected_keywords, (
        f"Search keywords expected to be {expected_keywords}, but found {md.data_id_info.search_keys.keyword}"
    )

    # Data characteristics
    assert md.data_id_info.data_char.charset_cd.value == "004", (
        f"Data character set code expected to be '004', but found '{md.data_id_info.data_char.charset_cd.value}'"
    )
    assert (
        md.data_id_info.envir_desc.environment
        == "Microsoft Windows 10 Version 10.0 (Build 22631) ; Esri ArcGIS 13.1.2.41833"
    ), (
        f"Environment description expected to be 'Microsoft Windows 10 Version 10.0 (Build 22631) ; Esri ArcGIS 13.1.2.41833', but found '{md.data_id_info.envir_desc.environment}'"
    )

    # Language
    assert md.data_id_info.data_lang.language_code.value == "eng", (
        f"Language code expected to be 'eng', but found '{md.data_id_info.data_lang.language_code.value}'"
    )
    assert md.data_id_info.data_lang.country_code.value == "USA", (
        f"Country code expected to be 'USA', but found '{md.data_id_info.data_lang.country_code.value}'"
    )

    # Spatial representation
    assert md.data_id_info.spat_rp_type.spat_rep_type_cd.value == "001", (
        f"Spatial representation type code expected to be '001', but found '{md.data_id_info.spat_rp_type.spat_rep_type_cd.value}'"
    )

    # Geographic bounds
    geo_box = md.data_id_info.data_ext.geo_ele.geo_bnd_box
    assert geo_box.esri_extent_type == "search", (
        f"ESRI extent type expected to be 'search', but found '{geo_box.esri_extent_type}'"
    )
    assert geo_box.west_bl.bound == "-74.257791", (
        f"West bound expected to be '-74.257791', but found '{geo_box.west_bl.bound}'"
    )
    assert geo_box.east_bl.bound == "-73.699380", (
        f"East bound expected to be '-73.699380', but found '{geo_box.east_bl.bound}'"
    )
    assert geo_box.north_bl.bound == "40.916347", (
        f"North bound expected to be '40.916347', but found '{geo_box.north_bl.bound}'"
    )
    assert geo_box.south_bl.bound == "40.494345", (
        f"South bound expected to be '40.494345', but found '{geo_box.south_bl.bound}'"
    )

    # Distribution info
    assert md.dist_info.dist_format.format_name == "Shapefile", (
        f"Format name expected to be 'Shapefile', but found '{md.dist_info.dist_format.format_name}'"
    )
    assert md.dist_info.dist_tran_ops.trans_size == "132.313", (
        f"Transfer size expected to be '132.313', but found '{md.dist_info.dist_tran_ops.trans_size}'"
    )

    # Reference system info
    ref_sys = md.ref_sys_info.ref_system.ref_sys_id
    assert ref_sys.ident_code.code == "2263", (
        f"Identification code expected to be '2263', but found '{ref_sys.ident_code.code}'"
    )
    assert ref_sys.id_code_space == "EPSG", (
        f"Code space expected to be 'EPSG', but found '{ref_sys.id_code_space}'"
    )
    assert ref_sys.id_version == "5.3(9.0.0)", (
        f"Version expected to be '5.3(9.0.0)', but found '{ref_sys.id_version}'"
    )

    # Spatial representation info
    vect_rep = md.spat_rep_info.vect_spat_rep
    assert vect_rep.geomet_objs.name == "MapPLUTO_UNCLIPPED", (
        f"Geometry objects name expected to be 'MapPLUTO_UNCLIPPED', but found '{vect_rep.geomet_objs.name}'"
    )
    assert vect_rep.geomet_objs.geo_obj_typ.geo_obj_type_cd.value == "002", (
        f"Geometry object type code expected to be '002', but found '{vect_rep.geomet_objs.geo_obj_typ.geo_obj_type_cd.value}'"
    )
    assert vect_rep.geomet_objs.geo_obj_cnt == "856912", (
        f"Geometry object count expected to be '856912', but found '{vect_rep.geomet_objs.geo_obj_cnt}'"
    )
    assert vect_rep.top_lvl.topo_lev_cd.value == "001", (
        f"Topology level code expected to be '001', but found '{vect_rep.top_lvl.topo_lev_cd.value}'"
    )

    # Spatial data organization info
    esriterm = md.spdo_info.ptvct_inf.esri_term
    assert esriterm.name == "MapPLUTO_UNCLIPPED", (
        f"ESRI term name expected to be 'MapPLUTO_UNCLIPPED', but found '{esriterm.name}'"
    )
    assert esriterm.efeat_typ == "Simple", (
        f"Feature type expected to be 'Simple', but found '{esriterm.efeat_typ}'"
    )
    assert esriterm.efea_geom == "4", (
        f"Feature geometry expected to be '4', but found '{esriterm.efea_geom}'"
    )
    assert esriterm.esri_topo == "FALSE", (
        f"ESRI topology expected to be 'FALSE', but found '{esriterm.esri_topo}'"
    )
    assert esriterm.efea_cnt == "856912", (
        f"Feature count expected to be '856912', but found '{esriterm.efea_cnt}'"
    )
    assert esriterm.sp_index == "FALSE", (
        f"Spatial index expected to be 'FALSE', but found '{esriterm.sp_index}'"
    )
    assert esriterm.lin_refer == "FALSE", (
        f"Linear reference expected to be 'FALSE', but found '{esriterm.lin_refer}'"
    )

    # Metadata date
    assert md.md_date_st.sync == "TRUE", (
        f"Metadata date sync expected to be 'TRUE', but found '{md.md_date_st.sync}'"
    )
    assert md.md_date_st.date == "19261122", (
        f"Metadata date expected to be '19261122', but found '{md.md_date_st.date}'"
    )

    # Binary thumbnail
    assert md.binary.thumbnail.data.esri_property_type == "PictureX", (
        f"ESRI property type expected to be 'PictureX', but found '{md.binary.thumbnail.data.esri_property_type}'"
    )
    assert md.binary.thumbnail.data.code == "/9j/4AAQSkZJRgNDL/", (
        f"Thumbnail code expected to be '/9j/4AAQSkZJRgNDL/', but found '{md.binary.thumbnail.data.code}'"
    )

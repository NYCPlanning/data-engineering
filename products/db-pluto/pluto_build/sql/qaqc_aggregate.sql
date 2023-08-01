DELETE FROM qaqc_aggregate
WHERE v = :'VERSION'
AND CONDO::boolean = :CONDO
AND MAPPED::boolean = :MAPPED;

INSERT INTO qaqc_aggregate (
SELECT  :'VERSION' as v, 
	    :CONDO as condo,
        :MAPPED as mapped,
        sum(UnitsRes::numeric)::bigint as UnitsRes,
        sum(LotArea::numeric)::bigint as LotArea,
        sum(BldgArea::numeric)::bigint as BldgArea,
        sum(ComArea::numeric)::bigint as ComArea,
        sum(ResArea::numeric)::bigint as ResArea,
        sum(OfficeArea::numeric)::bigint as OfficeArea,
        sum(RetailArea::numeric)::bigint as RetailArea,
        sum(GarageArea::numeric)::bigint as GarageArea,
        sum(StrgeArea::numeric)::bigint as StrgeArea,
        sum(FactryArea::numeric)::bigint as FactryArea,
        sum(OtherArea::numeric)::bigint as OtherArea,
        sum(AssessLand::numeric)::bigint as AssessLand,
        sum(AssessTot::numeric)::bigint as AssessTot,
        sum(ExemptTot::numeric)::bigint as ExemptTot,
        sum(FIRM07_FLAG::numeric)::bigint as FIRM07_FLAG,
        sum(PFIRM15_FLAG::numeric)::bigint as PFIRM15_FLAG
FROM archive_pluto a
:CONDITION);
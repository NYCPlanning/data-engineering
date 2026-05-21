select
    base.uid,
    base.opname,
    base.opabbrev,
    op.optype,
    base.overabbrev,
    ov.overagency,
    ov.overlevel
from {{ source('facdb', 'facdb_base') }} as base
left join {{ ref('lookup_agency') }} as op
    on base.opabbrev = op.agencyabbrev
left join {{ ref('lookup_agency') }} as ov
    on base.overabbrev = ov.agencyabbrev

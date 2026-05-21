select
    base.uid,
    base.factype,
    cls.facsubgrp,
    cls.facgroup,
    cls.facdomain,
    cls.servarea
from {{ source('facdb', 'facdb_base') }} as base
inner join {{ ref('lookup_classification') }} as cls
    on upper(base.facsubgrp) = upper(cls.facsubgrp)

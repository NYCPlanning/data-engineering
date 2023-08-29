CREATE TEMP TABLE tmp (
    bbl text,
    parcelcard text,
    bldgnum text,
    bldgclass text,
    primaryusecode text,
    developmentname text,
    streettype text,
    lottype text,
    residarea text,
    officearea text,
    retailarea text,
    garagearea text,
    storagearea text,
    factoryarea text,
    otherarea text,
    grossarea text,
    ownerarea text,
    grossvolume text,
    commercialarea text,
    proxcode text,
    bsmnt_type text,
    bsmntgradient text,
    bsmntarea text,
    firstfloorarea text,
    secondfloorarea text,
    upperfloorarea text,
    partresfloorarea text,
    unfinishedfloorarea text,
    finishedfloorarea text,
    nonresidfloorarea text,
    residconstrtype text,
    commercialconstrtype text,
    condomainconstrtype text,
    condounitsconstrtype text
);

\COPY tmp FROM PSTDIN WITH NULL AS '' DELIMITER '|' CSV;

DROP TABLE IF EXISTS :TABLE CASCADE;
SELECT * INTO :TABLE FROM tmp;

# Generated largely by datamodel-codegen
# using schema https://proj.org/en/latest/schemas/v0.7/projjson.schema.json
# These classes seem useful, but more from a perspective of reading them.
# For example, the "Id" class is useful to read but seems unwieldy to use in
# our codebase when generally we just want an enum like "EPSG:4326"

# for this reason, leaving named "projjson" instead of more generic "projection"
# it for now is unreferenced by the rest of the codebase, but left as an interesting example

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator
from typing_extensions import Literal
from typing import TypeAlias


def _truthy_xor(a, b):
    return (a and not b) or (b and not a)


class Bbox(BaseModel, extra="forbid"):
    east_longitude: float
    west_longitude: float
    south_latitude: float
    north_latitude: float


class Id(BaseModel, extra="forbid"):
    authority: str
    code: str | int
    version: str | float | None = None
    authority_citation: str | None = None
    uri: str | None = None


class IdIdsMutuallyExclusive(BaseModel, extra="forbid"):
    id: Id | None = None
    ids: list[Id] | None = None

    @model_validator(mode="after")
    def check_id_ids_mutually_exclusive(self):
        if self.id and self.ids:
            raise ValueError(
                "Only one of (and exactly one) 'id' or 'ids' must be provided"
            )
        return self


class OneAndOnlyOneOfDatumOrDatumEnsemble(BaseModel):
    datum: Any = None
    datum_ensemble: Any = None

    @model_validator(mode="after")
    def check_one_and_only_datum_or_datum_ensemble(self):
        if not _truthy_xor(self.datum, self.datum_ensemble):
            raise ValueError(
                "Only one of (and exactly one) 'datum' or 'datum_ensemble' must be provided"
            )
        return self


class TemporalExtent(BaseModel, extra="forbid"):
    start: str
    end: str


class CustomUnit(BaseModel, extra="forbid"):
    type: Literal[
        "LinearUnit", "AngularUnit", "ScaleUnit", "TimeUnit", "ParametricUnit", "Unit"
    ]
    name: str
    conversion_factor: float | None = None
    id: Id | None = None
    ids: list[Id] | None = None


Unit: TypeAlias = Literal["metre", "degree", "unity"] | CustomUnit


class ValueAndUnit(BaseModel, extra="forbid"):
    value: float
    unit: Unit


class VerticalExtent(BaseModel, extra="forbid"):
    minimum: float
    maximum: float
    unit: Unit | None = None


class Member(BaseModel, extra="forbid"):
    name: str
    id: Id | None = None
    ids: list[Id] | None = None


class DeformationModel(BaseModel, extra="forbid"):
    name: str
    id: Id | None = None


class Ellipsoid1(BaseModel):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["Ellipsoid"] = "Ellipsoid"
    name: str
    semi_major_axis: ValueAndUnit | float
    semi_minor_axis: ValueAndUnit | float
    id: Id | None = None
    ids: list[Id] | None = None


class Ellipsoid2(BaseModel):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["Ellipsoid"] = "Ellipsoid"
    name: str
    semi_major_axis: ValueAndUnit | float
    inverse_flattening: float
    id: Id | None = None
    ids: list[Id] | None = None


class Ellipsoid3(BaseModel):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["Ellipsoid"] = "Ellipsoid"
    name: str
    radius: ValueAndUnit | float
    id: Id | None = None
    ids: list[Id] | None = None


Ellipsoid: TypeAlias = Ellipsoid1 | Ellipsoid2 | Ellipsoid3


class Method(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["OperationMethod"] | None = None
    name: str
    id: Id | None = None
    ids: list[Id] | None = None


class Meridian(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["Meridian"] | None = None
    longitude: float | ValueAndUnit
    id: Id | None = None
    ids: list[Id] | None = None


class ObjectUsage1(BaseModel):
    field_schema: str | None = Field(None, alias="$schema")
    scope: str | None = None
    area: str | None = None
    bbox: Bbox | None = None
    vertical_extent: VerticalExtent | None = None
    temporal_extent: TemporalExtent | None = None
    remarks: str | None = None
    id: Id | None = None
    ids: list[Id] | None = None


class ParameterValue(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["ParameterValue"] | None = None
    name: str
    value: str | float
    unit: Unit | None = None
    id: Id | None = None
    ids: list[Id] | None = None


class PrimeMeridian(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["PrimeMeridian"] | None = None
    name: str
    longitude: float | ValueAndUnit | None = None
    id: Id | None = None
    ids: list[Id] | None = None


class Usage(BaseModel, extra="forbid"):
    scope: str | None = None
    area: str | None = None
    bbox: Bbox | None = None
    vertical_extent: VerticalExtent | None = None
    temporal_extent: TemporalExtent | None = None


class Axis(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["Axis"] | None = None
    name: str
    abbreviation: str
    direction: Literal[
        "north",
        "northNorthEast",
        "northEast",
        "eastNorthEast",
        "east",
        "eastSouthEast",
        "southEast",
        "southSouthEast",
        "south",
        "southSouthWest",
        "southWest",
        "westSouthWest",
        "west",
        "westNorthWest",
        "northWest",
        "northNorthWest",
        "up",
        "down",
        "geocentricX",
        "geocentricY",
        "geocentricZ",
        "columnPositive",
        "columnNegative",
        "rowPositive",
        "rowNegative",
        "displayRight",
        "displayLeft",
        "displayUp",
        "displayDown",
        "forward",
        "aft",
        "port",
        "starboard",
        "clockwise",
        "counterClockwise",
        "towards",
        "awayFrom",
        "future",
        "past",
        "unspecified",
    ]
    meridian: Meridian | None = None
    unit: Unit | None = None
    minimum_value: float | None = None
    maximum_value: float | None = None
    range_meaning: Literal["exact", "wraparound"] | None = None
    id: Id | None = None
    ids: list[Id] | None = None


class Conversion(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["Conversion"] | None = None
    name: str
    method: Method
    parameters: list[ParameterValue] | None = None
    id: Id | None = None
    ids: list[Id] | None = None


class CoordinateSystem(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["CoordinateSystem"] | None = None
    name: str | None = None
    subtype: Literal[
        "Cartesian",
        "spherical",
        "ellipsoidal",
        "vertical",
        "ordinal",
        "parametric",
        "affine",
        "TemporalDateTime",
        "TemporalCount",
        "TemporalMeasure",
    ]
    axis: list[Axis]
    id: Id | None = None
    ids: list[Id] | None = None


class DatumEnsemble(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["DatumEnsemble"] | None = None
    name: str
    members: list[Member]
    ellipsoid: Ellipsoid | None = None
    accuracy: str
    id: Id | None = None
    ids: list[Id] | None = None


class ObjectUsage2(BaseModel):
    field_schema: str | None = Field(None, alias="$schema")
    usages: list[Usage] | None = None
    remarks: str | None = None
    id: Id | None = None
    ids: list[Id] | None = None


ObjectUsage: TypeAlias = ObjectUsage1 | ObjectUsage2


class ParametricDatum(BaseModel, extra="forbid"):
    type: Literal["ParametricDatum"] | None = None
    name: str
    anchor: str | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class TemporalDatum(BaseModel, extra="forbid"):
    type: Literal["TemporalDatum"] | None = None
    name: str
    calendar: str
    time_origin: str | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class VerticalReferenceFrame(BaseModel, extra="forbid"):
    type: Literal["VerticalReferenceFrame"] | None = None
    name: str
    anchor: str | None = None
    anchor_epoch: float | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class DynamicGeodeticReferenceFrame(BaseModel, extra="forbid"):
    type: Literal["DynamicGeodeticReferenceFrame"] | None = None
    name: Any
    anchor: Any | None = None
    anchor_epoch: Any | None = None
    ellipsoid: Ellipsoid
    prime_meridian: Any | None = None
    frame_reference_epoch: float
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class DynamicVerticalReferenceFrame(BaseModel, extra="forbid"):
    type: Literal["DynamicVerticalReferenceFrame"] | None = None
    name: Any
    anchor: Any | None = None
    anchor_epoch: Any | None = None
    frame_reference_epoch: float
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class EngineeringDatum(BaseModel, extra="forbid"):
    type: Literal["EngineeringDatum"] | None = None
    name: str
    anchor: str | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class GeodeticReferenceFrame(BaseModel, extra="forbid"):
    type: Literal["GeodeticReferenceFrame"] | None = None
    name: str
    anchor: str | None = None
    anchor_epoch: float | None = None
    ellipsoid: Ellipsoid
    prime_meridian: PrimeMeridian | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class ParametricCrs(BaseModel, extra="forbid"):
    type: Literal["ParametricCRS"] | None = None
    name: str
    datum: ParametricDatum
    coordinate_system: CoordinateSystem | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class TemporalCrs(BaseModel, extra="forbid"):
    type: Literal["TemporalCRS"] | None = None
    name: str
    datum: TemporalDatum
    coordinate_system: CoordinateSystem | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


Datum: TypeAlias = (
    GeodeticReferenceFrame
    | VerticalReferenceFrame
    | DynamicGeodeticReferenceFrame
    | DynamicVerticalReferenceFrame
    | TemporalDatum
    | ParametricDatum
    | EngineeringDatum
)


class DerivedParametricCrs(BaseModel, extra="forbid"):
    type: Literal["DerivedParametricCRS"] | None = None
    name: str
    base_crs: ParametricCrs
    conversion: Conversion
    coordinate_system: CoordinateSystem
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class DerivedTemporalCrs(BaseModel, extra="forbid"):
    type: Literal["DerivedTemporalCRS"] | None = None
    name: str
    base_crs: TemporalCrs
    conversion: Conversion
    coordinate_system: CoordinateSystem
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class EngineeringCrs(BaseModel, extra="forbid"):
    type: Literal["EngineeringCRS"] | None = None
    name: str
    datum: EngineeringDatum
    coordinate_system: CoordinateSystem | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None
    id: Any | None = None
    ids: Any | None = None


class GeodeticCrs(
    IdIdsMutuallyExclusive, OneAndOnlyOneOfDatumOrDatumEnsemble, extra="forbid"
):
    type: Literal["GeodeticCRS", "GeographicCRS"] | None = None
    name: str
    datum: GeodeticReferenceFrame | DynamicGeodeticReferenceFrame | None = None
    datum_ensemble: DatumEnsemble | None = None
    coordinate_system: CoordinateSystem | None = None
    deformation_models: list[DeformationModel] | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class ProjectedCrs(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["ProjectedCRS"] | None = None
    name: str
    base_crs: GeodeticCrs
    conversion: Conversion
    coordinate_system: CoordinateSystem
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class DerivedEngineeringCrs(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["DerivedEngineeringCRS"] | None = None
    name: str
    base_crs: EngineeringCrs
    conversion: Conversion
    coordinate_system: CoordinateSystem
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class DerivedGeodeticCrs(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["DerivedGeodeticCRS", "DerivedGeographicCRS"] | None = None
    name: str
    base_crs: GeodeticCrs
    conversion: Conversion
    coordinate_system: CoordinateSystem
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class DerivedProjectedCrs(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["DerivedProjectedCRS"] | None = None
    name: str
    base_crs: ProjectedCrs
    conversion: Conversion
    coordinate_system: CoordinateSystem
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class AbridgedTransformation(IdIdsMutuallyExclusive, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["AbridgedTransformation"] | None = None
    name: str
    source_crs: Crs | None = None
    method: Method
    parameters: list[ParameterValue]


class BoundCrs(IdIdsMutuallyExclusive, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["BoundCRS"] | None = None
    name: str | None = None
    source_crs: Crs
    target_crs: Crs
    transformation: AbridgedTransformation
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class CompoundCrs(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["CompoundCRS"] | None = None
    name: str
    components: list[Crs]
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class ConcatenatedOperation(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["ConcatenatedOperation"] | None = None
    name: str
    source_crs: Crs
    target_crs: Crs
    steps: list[SingleOperation]
    accuracy: str | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class CoordinateMetadata(BaseModel, extra="forbid"):
    field_schema: str | None = Field(None, alias="$schema")
    type: Literal["CoordinateMetadata"] | None = None
    crs: Crs
    coordinateEpoch: float | None = None


class DerivedVerticalCrs(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["DerivedVerticalCRS"] | None = None
    name: str
    base_crs: VerticalCrs
    conversion: Conversion
    coordinate_system: CoordinateSystem
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class GeoidModel(BaseModel, extra="forbid"):
    name: str
    interpolation_crs: Crs | None = None
    id: Id | None = None


class PointMotionOperation(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["PointMotionOperation"] | None = None
    name: str
    source_crs: Crs
    method: Method
    parameters: list[ParameterValue]
    accuracy: str | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


class VerticalCrs(
    IdIdsMutuallyExclusive, OneAndOnlyOneOfDatumOrDatumEnsemble, extra="forbid"
):
    type: Literal["VerticalCRS"] | None = None
    name: str
    datum: VerticalReferenceFrame | DynamicVerticalReferenceFrame | None = None
    datum_ensemble: DatumEnsemble | None = None
    coordinate_system: CoordinateSystem | None = None
    geoid_model: GeoidModel | None = None
    geoid_models: list[GeoidModel] | None = None
    deformation_models: list[DeformationModel] | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


Crs: TypeAlias = (
    BoundCrs
    | CompoundCrs
    | DerivedEngineeringCrs
    | DerivedGeodeticCrs
    | DerivedParametricCrs
    | DerivedProjectedCrs
    | DerivedTemporalCrs
    | DerivedVerticalCrs
    | EngineeringCrs
    | GeodeticCrs
    | ParametricCrs
    | ProjectedCrs
    | TemporalCrs
    | VerticalCrs
)


class Transformation(IdIdsMutuallyExclusive, extra="forbid"):
    type: Literal["Transformation"] | None = None
    name: str
    source_crs: Crs
    target_crs: Crs
    interpolation_crs: Crs | None = None
    method: Method
    parameters: list[ParameterValue]
    accuracy: str | None = None
    field_schema: str | None = Field(None, alias="$schema")
    scope: Any | None = None
    area: Any | None = None
    bbox: Bbox | None = None
    vertical_extent: Any | None = None
    temporal_extent: Any | None = None
    usages: Any | None = None
    remarks: Any | None = None


SingleOperation: TypeAlias = Conversion | Transformation | PointMotionOperation


Model: TypeAlias = (
    Crs
    | Datum
    | DatumEnsemble
    | Ellipsoid
    | PrimeMeridian
    | SingleOperation
    | ConcatenatedOperation
    | CoordinateMetadata
)

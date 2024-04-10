from typing import Tuple

SENTINEL2_ATTRS = (
    "endingDateTime",
    "platformShortName",
    "beginningDateTime",
    "productType",
    "origin",
    "tileId",
    "cloudCover",
    "orbitNumber",
    "productGroupId",
    "operationalMode",
    "processingLevel",
    "processorVersion",
    "instrumentShortName",
    "relativeOrbitNumber",
    "platformSerialIdentifier",
)

SENTINEL1_ATTRS = (
    "endingDateTime",
    "platformShortName",
    "beginningDateTime",
    "productType",
    "origin",
    "timeliness",
    "orbitNumber",
    "productClass",
    "orbitDirection",
    "operationalMode",
    "processingLevel",
    "swathIdentifier",
    "instrumentShortName",
    "relativeOrbitNumber",
    "polarisationChannels",
    "platformSerialIdentifier",
)

SENTINEL1_RTC_ATTRS = (
    "endingDateTime",
    "platformShortName",
    "beginningDateTime",
    "productType",
    "authority",
    "orbitNumber",
    "orbitDirection",
    "operationalMode",
    "processingLevel",
    "spatialResolution",
    "instrumentShortName",
    "relativeOrbitNumber",
    "polarisationChannels",
    "platformSerialIdentifier",
)

SENTINEL5P_ATTRS = (
    "endingDateTime",
    "platformShortName",
    "beginningDateTime",
    "productType",
)

SENTINEL6_ATTRS = (
    "endingDateTime",
    "platformShortName",
    "beginningDateTime",
    "productType",
    "origin",
    "source",
    "authority",
    "processingDate",
    "processingCenter",
    "processorVersion",
    "platformSerialIdentifier",
)

SENTINEL3_ATTRS = (
    "endingDateTime",
    "platformShortName",
    "beginningDateTime",
    "productType",
    "baselineCollection",
    "instrumentShortName",
)


GLOBALMOSAICS_ATTRS = (
    "processingCenter",
    "platformShortName",
    "productType",
    "beginningDateTime",
    "endingDateTime",
)

SMOS_ATTRS = (
    "authority",
    "orbitNumber",
    "productType",
    "endingDateTime",
    "orbitDirection",
    "acquisitionType",
    "operationalMode",
    "processingLevel",
    "processingCenter",
    "processorVersion",
    "wrsLongitudeGrid",
    "beginningDateTime",
    "platformShortName",
    "spatialResolution",
    "instrumentShortName",
    "nativeProductFormat",
)

ENVISAT_ATTRS = (
    "authority",
    "orbitNumber",
    "phaseNumber",
    "productType",
    "endingDateTime",
    "processingLevel",
    "beginningDateTime",
    "platformShortName",
    "spatialResolution",
    "instrumentShortName",
)
LANDSAT5_ATTRS = (
    "authority",
    "rowNumber",
    "cloudCover",
    "pathNumber",
    "orbitNumber",
    "productType",
    "numberOfBands",
    "endingDateTime",
    "operationalMode",
    "processingLevel",
    "sunAzimuthAngle",
    "beginningDateTime",
    "platformShortName",
    "spatialResolution",
    "sunElevationAngle",
    "instrumentShortName",
)

LANDSAT7_ATTRS = (
    "authority",
    "rowNumber",
    "cloudCover",
    "pathNumber",
    "orbitNumber",
    "productType",
    "numberOfBands",
    "endingDateTime",
    "operationalMode",
    "processingLevel",
    "sunAzimuthAngle",
    "beginningDateTime",
    "platformShortName",
    "spatialResolution",
    "sunElevationAngle",
    "instrumentShortName",
)

LANDSAT8_ATTRS = (
    "authority",
    "rowNumber",
    "cloudCover",
    "pathNumber",
    "productType",
    "numberOfBands",
    "endingDateTime",
    "processingLevel",
    "sunAzimuthAngle",
    "beginningDateTime",
    "platformShortName",
    "spatialResolution",
    "sunElevationAngle",
    "instrumentShortName",
)

TERRAAQUA_ATTRS = (
    "authority",
    "productType",
    "endingDateTime",
    "processingLevel",
    "beginningDateTime",
    "platformShortName",
    "spatialResolution",
    "instrumentShortName",
)

S2GLC_ATTRS = ("productType", "endingDateTime", "processorVersion", "beginningDateTime")

COPDEM_ATTRS = (
    "authority",
    "productType",
    "endingDateTime",
    "beginningDateTime",
    "spatialResolution",
    "polarisationChannels",
)

KNOWN_ATTRS_BY_COLLECTION = {
    "SENTINEL-1": SENTINEL1_ATTRS,
    "SENTINEL-2": SENTINEL2_ATTRS,
    "SENTINEL-3": SENTINEL3_ATTRS,
    "SENTINEL-5P": SENTINEL5P_ATTRS,
    "SENTINEL-6": SENTINEL6_ATTRS,
    "SENTINEL-1-RTC": SENTINEL1_RTC_ATTRS,
    "GLOBAL-MOSAICS": GLOBALMOSAICS_ATTRS,
    "SMOS": SMOS_ATTRS,
    "ENVISAT": ENVISAT_ATTRS,
    "LANDSAT-5": LANDSAT5_ATTRS,
    "LANDSAT-7": LANDSAT7_ATTRS,
    "LANDSAT-8": LANDSAT8_ATTRS,
    "COP-DEM": COPDEM_ATTRS,
    "TERRAAQUA": TERRAAQUA_ATTRS,
    "S2GLC": S2GLC_ATTRS,
}


def get_known_collections() -> Tuple[str, ...]:
    """Get all known collections.

    Returns:
        Tuple[str, ...]: known collections
    """
    return tuple(KNOWN_ATTRS_BY_COLLECTION.keys())


def get_collection_known_attributes(collection: str) -> Tuple[str, ...]:
    """Get all know attributes that can be queried for a collection.

    Args:
        collection (str): collection name

    Raises:
        ValueError: invalid collection

    Returns:
        Tuple[str, ...]: known attributes

    """
    if collection not in get_known_collections():
        raise ValueError(f"Invalid Collection: {collection}")
    return KNOWN_ATTRS_BY_COLLECTION[collection]

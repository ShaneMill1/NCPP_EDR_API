#
# Name: xmlConfig.py
# Purpose: To provide TAC->IWXXM and TAC->USWX decoders & encoders with
# site-specific information and customized settings
#
# Author: Mark Oberfield
# Organization: NOAA/NWS/OSTI/Meteorological Development Laboratory
# Contact Info: Mark.Oberfield@noaa.gov
# Date: 16 January 2020
#
import os
#
_author = 'Mark Oberfield - NOAA/NWS/OSTI/MDL/WIAB'
_email = 'Mark.Oberfield@noaa.gov'
#
# IWXXM versioning
_iwxxm = '3.0'
_release = '3.0'
#
IWXXM_URI = 'http://icao.int/iwxxm/%s' % _iwxxm
IWXXM_URL = 'https://schemas.wmo.int/iwxxm/%s/iwxxm.xsd' % _release
#
# IWXXM-US versioning
_us_iwxxm = '3.0'
_us_iwxxm_release = '3.0'
#
# USWX versioning
_uswx = '1.0'
_uswx_release = '1.0'
#
# The full name and indentifier of entity running this software
#
TranslationCentreName = 'NCEP Central Operations'
TranslationCentreDesignator = 'KWNO'
TRANSLATOR = True
#
# Path to file containing codes obtained from WMO Code Registry in
# RDF/XML format
#
CodesFilePath = os.path.join('/EDR/provider', 'Codes.rdf')
#CodesFilePath = os.path.join('/home/shane.mill/WIAB-NDFD-data-api-edr/data-api/EDR/provider', 'Codes.rdf')

#
# WMO Code Registry Vocabularies contained in 'CodesFilePath' file
PRSRTNDCY_CONTAINER_ID = '0-10-063'
SEACND_CONTAINER_ID = '0-22-061'
RWYDEPST_CONTAINER_ID = '0-20-086'
RWYCNTMS_CONTAINER_ID = '0-20-087'
WEATHER_CONTAINER_ID = '4678'
COLOUR_CODES = 'AviationColourCode'
#
# If prevailing horizontal visibility falls below this value (metres), RVR information should be supplied
RVR_MaximumDistance = 1500
#
# How many elements in the aerodrome's ARP geo-location, either 2 or 3. Shall be set
# to two (2) for the indefinite future.
#
srsDimension = '2'
srsName = 'https://www.opengis.net/def/crs/EPSG/0/4326'
axisLabels = 'Lat Long'
#
# If srsDimensions is equal to 3, then vertical datum must be set correctly for the elevation used
#
# Allowed values are: 'EGM_96', 'AHD', 'NAVD88', or string matching regular expression pattern
# 'OTHER:(\w|_){1,58}'
#
verticalDatum = 'EGM_96'
#
# Elevation value unit of measure (UOM). Either 'FT' or 'M' or string matching regular expression
# pattern OTHER:(\w|_){1,58}
#
elevationUOM = 'M'
#
# URLs to miscellaneous WMO Code Registry tables and entries
#
# NIL reasons
NIL_NOSIGC_URL = 'http://codes.wmo.int/common/nil/noSignificantChange'
NIL_NOOBSV_URL = 'http://codes.wmo.int/common/nil/notObservable'
NIL_NOOPRSIG_URL = 'http://codes.wmo.int/common/nil/nothingOfOperationalSignificance'
NIL_NOAUTODEC_URL = 'http://codes.wmo.int/common/nil/notDetectedByAutoSystem'
NIL_NA_URL = 'http://codes.wmo.int/common/nil/inapplicable'
NIL_MSSG_URL = 'http://codes.wmo.int/common/nil/missing'
NIL_UNKNWN_URL = 'http://codes.wmo.int/commmon/nil/unknown'
NIL_WTHLD_URL = 'http://codes.wmo.int/common/nil/withheld'
NIL_SNOCLO_URL = 'http://codes.wmo.int/bufr4/codeflag/0-20-085/1'
#
CLDCVR_URL = 'http://codes.wmo.int/49-2/CloudAmountReportedAtAerodrome/'
RWYFRCTN_URL = 'http://codes.wmo.int/bufr4/codeflag/0-20-089/'
#
CUMULONIMBUS = 'http://codes.wmo.int/49-2/SigConvectiveCloudType/CB'
TWRNGCUMULUS = 'http://codes.wmo.int/49-2/SigConvectiveCloudType/TCU'
#
PRSRTNDCY = 'http://codes.wmo.int/bufr4/codeflag/%s' % PRSRTNDCY_CONTAINER_ID
ACCUMLTN = 'http://codes.wmo.int/grib2/codeflag/4.10/1'
MAXIMUM = 'http://codes.wmo.int/grib2/codeflag/4.10/2'
MINIMUM = 'http://codes.wmo.int/grib2/codeflag/4.10/3'
AERONAUTICALVIS = 'http://codes.wmo.int/common/quantity-kind/aeronauticalVisibility'
SKYCATALOG = 'http://codes.wmo.int/bufr4/codeflag/0-20-012'
#
# Bit masks
Weather = 1 << 0
CloudAmt = 1 << 1
CloudType = 1 << 2
SeaCondition = 1 << 3
RunwayDeposit = 1 << 4
AffectedRunwayCoverage = 1 << 5
RunwayFriction = 1 << 6
#
# xlink:title attributes are optional in IWXXM XML documents. TITLES determines
# whether they are displayed.
#
# If no xlink:title attributes (with rare exceptions) are wanted in IWXXM XML documents,
# set TITLES to 0 (zero). Otherwise, set bits appropriately.
#
TITLES = 0
# TITLES=(CloudAmt|CloudType|SeaCondition|RunwayDeposit|AffectedRunwayCoverage|RunwayFriction)
#
# If xlink:titles are to appear in the document, set preferred language. English, 'en', is
# the default if the desired language is not found in the WMO Code Registry.
#
PreferredLanguageForTitles = 'en'
#
# The following dictionaries are defined here because the resulting dictionaries constructed
# from the WMO code registry are 'inadequate.'
#
# Therefore..."Thou shalt not change these dictionaries' keys!" However, the plain text in them
# can be changed (i.e. different language, consistent with preferred language above), if desired.
#
RunwayFrictionValues = {'91': 'Braking action poor', '92': 'Braking action medium to poor',
                        '93': 'Braking action medium', '94': 'Braking action medium to good',
                        '95': 'Braking action good', '99': 'Unreliable'}
#
CldCvr = {'CLR': 'Sky clear within limits', 'SKC': 'Sky clear', 'FEW': 'Few',
          'SCT': 'Scattered', 'BKN': 'Broken', 'OVC': 'Overcast'}
#
# US Code Registry for Meteorological Services
OFCM_CODE_REGISTRY_URL = 'https://codes.nws.noaa.gov'
#
# IWXXM-US URI and URLs
IWXXM_US_URI = 'http://www.weather.gov/iwxxm-us/%s' % _us_iwxxm
IWXXM_US_URL = 'https://nws.weather.gov/schemas/iwxxm-us/%s/' % _us_iwxxm_release
IWXXM_US_METAR_URL = 'https://nws.weather.gov/schemas/iwxxm-us/%s/metarSpeci.xsd' % _us_iwxxm_release
#
# USWX_URI and URLs
USWX_URI = 'http://www.weather.gov/uswx/%s' % _uswx
USWX_URL = 'https://nws.weather.gov/schemas/uswx/%s/' % _uswx_release

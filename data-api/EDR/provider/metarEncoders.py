#
# Name: metarEncoders.py
# Purpose: To encode METAR/SPECI information in IWXXM 3.0 XML format.
#
# Author: Mark Oberfield
# Organization: NOAA/NWS/OSTI/Meteorological Development Laboratory
# Contact Info: Mark.Oberfield@noaa.gov
#
import logging
import re
import time
import traceback
import sys
import xml.etree.ElementTree as ET

from EDR.provider import xmlConfig as des
from EDR.provider import xmlUtilities as deu


class Annex3(object):

    def __init__(self, codesFile=des.CodesFilePath, centreName=des.TranslationCentreName,
                 centreDesignator=des.TranslationCentreDesignator):
        #
        self._program_name = 'Annex 3 Encoder'
        self._description = 'To encode METAR/SPECI information in IWXXM %s format.' % des._iwxxm
        self._version = '3.2'  # Software version, not IWXXM XSD version.
        self._annex3_amd = '78'
        self._Logger = logging.getLogger(self._program_name)

        self.centreName = centreName
        self.centreDesignator = centreDesignator
        #
        # Create dictionaries of the following WMO codes
        neededCodes = [des.SEACND_CONTAINER_ID, des.RWYDEPST_CONTAINER_ID,
                       des.RWYCNTMS_CONTAINER_ID, des.WEATHER_CONTAINER_ID]
        try:
            self.codes = deu.parseCodeRegistryTables(codesFile, neededCodes, des.PreferredLanguageForTitles)
        except AssertionError as msg:
            self._Logger.warning(msg)
        #
        # map several encoder tokens to a single function
        setattr(self, 'obv', self.pcp)
        setattr(self, 'vcnty', self.pcp)

        self.observedTokenList = ['temps', 'altimeter', 'wind', 'vsby', 'rvr', 'pcp', 'obv', 'vcnty',
                                  'sky', 'rewx', 'ws', 'seastate', 'rwystate']

        self.trendTokenList = ['wind', 'pcp', 'obv', 'sky']

        self.NameSpaces = {'aixm': 'http://www.aixm.aero/schema/5.1.1',
                           'iwxxm': des.IWXXM_URI,
                           'gml': 'http://www.opengis.net/gml/3.2',
                           'xlink': 'http://www.w3.org/1999/xlink',
                           'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
        #
        self._re_unknwnPcpn = re.compile(r'(?P<mod>[-+]?)(?P<char>(SH|FZ|TS))')
        self._re_ICAO_ID = re.compile(r'^[A-Z]{4}$', re.MULTILINE)
        self._re_Alternate_ID = re.compile(r'^[A-Z0-9]{3,6}$', re.MULTILINE)
        self._re_cloudLyr = re.compile(r'(VV|FEW|SCT|BKN|(0|O)VC|///|CLR|SKC)([/\d]{3})?(CB|TCU|///)?')
        self._TrendForecast = {'TEMPO': 'TEMPORARY_FLUCTUATIONS', 'BECMG': 'BECOMING'}
        self._RunwayDepositDepths = {'92': '100', '93': '150', '94': '200',
                                     '95': '250', '96': '300', '97': '350', '98': '400'}

    def __call__(self, decodedMetar, tacString=None):
        #
        self.decodedMetar = decodedMetar
        self.tacString = tacString.strip()

        self.XMLDocument = None
        self.doReport()

        return self.XMLDocument

    def emit(self):

        self._Logger.info('%s v%s. Decoding METAR/SPECI reports according to Annex 3 Amendment %s' %
                          (self._program_name, self._version, self._annex3_amd))
        self._Logger.debug(self._description)
        self._Logger.debug(des._author)
        self._Logger.debug(des._email)

    def doReport(self):

        self.preamble()
        self.issueTime(self.XMLDocument)
        self.aerodrome(self.XMLDocument, self.decodedMetar['ident'])
        self.observationTime(self.XMLDocument)
        self.observation(self.XMLDocument)
        if self.nilPresent:
            return

        self.forecasts(self.XMLDocument)

    def preamble(self):
        #
        # The root element created here
        self.XMLDocument = ET.Element('iwxxm:%s' % self.decodedMetar['type']['str'])
        #
        # All IWXXM products for data-api is not official
        comment = ET.Comment('********************* NOTICE **********************\n'
                             'This is not an official product. Do not retransmit.\n'
                             '********************* NOTICE **********************')
        self.XMLDocument.append(comment)
        #
        for prefix, uri in self.NameSpaces.items():
            self.XMLDocument.set('xmlns:%s' % prefix, uri)
        #
        self.XMLDocument.set('xsi:schemaLocation', '%s %s' % (des.IWXXM_URI, des.IWXXM_URL))
        #
        # Set its many attributes
        self.XMLDocument.set('reportStatus', 'NORMAL')
        self.XMLDocument.set('automatedStation', 'false')
        self.nilPresent = 'nil' in self.decodedMetar

        if 'cor' in self.decodedMetar:
            self.XMLDocument.set('reportStatus', 'CORRECTION')

        if 'auto' in self.decodedMetar:
            self.XMLDocument.set('automatedStation', 'true')
        #
        # Additional attributes for root element
        self.XMLDocument.set('permissibleUsage', 'OPERATIONAL')
        #
        try:
            self.XMLDocument.set('translatedBulletinID', self.decodedMetar['translatedBulletinID'])
        except KeyError:
            self.XMLDocument.set('translatedBulletinID', 'TTAAiiCCCCYYGGgg')
        #
        try:
            self.XMLDocument.set('translatedBulletinReceptionTime',
                                 self.decodedMetar['translatedBulletinReceptionTime'])
        except KeyError:
            pass
        #
        self.XMLDocument.set('translationCentreDesignator', self.centreDesignator)
        self.XMLDocument.set('translationCentreName', self.centreName)
        #
        try:
            self.XMLDocument.set('translationTime', self.decodedMetar['translationTime'])
        except KeyError:
            self.XMLDocument.set('translationTime', time.strftime('%Y-%m-%dT%H:%M:%SZ'))
        #
        # If TAC translation failed in some way
        if 'err_msg' in self.decodedMetar:

            self.XMLDocument.set('translationFailedTAC', ' '.join(self.tacString.split()))
            self.nilPresent = True
            self.XMLDocument.set('permissibleUsageSupplementary', self.decodedMetar.get('err_msg'))

        self.XMLDocument.set('gml:id', deu.getUUID())

    def issueTime(self, parent):

        indent = ET.SubElement(parent, 'iwxxm:issueTime')
        try:
            self._issueTime = self.decodedMetar['itime']['value']
            indent1 = ET.SubElement(indent, 'gml:TimeInstant')
            indent1.set('gml:id', deu.getUUID())
            indent2 = ET.SubElement(indent1, 'gml:timePosition')
            indent2.text = self._issueTime
            self._issueTimeUUID = '#%s' % indent1.get('gml:id')

        except KeyError:
            indent.set('nilReason', des.NIL_MSSG_URL)

    def aerodrome(self, parent, token):

        indent = ET.SubElement(parent, 'iwxxm:aerodrome')
        indent1 = ET.SubElement(indent, 'aixm:AirportHeliport')
        indent1.set('gml:id', deu.getUUID())

        indent2 = ET.SubElement(indent1, 'aixm:timeSlice')
        indent3 = ET.SubElement(indent2, 'aixm:AirportHeliportTimeSlice')
        indent3.set('gml:id', deu.getUUID())

        indent4 = ET.SubElement(indent3, 'gml:validTime')
        indent4 = ET.SubElement(indent3, 'aixm:interpretation')
        indent4.text = 'SNAPSHOT'

        try:
            try:
                designator = token['alternate'].strip().upper()
            except KeyError:
                designator = token['str'].strip().upper()

            if self._re_Alternate_ID.match(designator) is not None:
                indent4 = ET.SubElement(indent3, 'aixm:designator')
                indent4.text = designator

        except KeyError:
            pass

        try:
            indent4 = ET.Element('aixm:name')
            indent4.text = token['name'].strip().upper()
            if len(indent4.text):
                indent3.append(indent4)

        except KeyError:
            pass
        #
        # ICAO identifier shall only match [A-Z]{4}
        try:
            designator = token['str'].strip().upper()
            if self._re_ICAO_ID.match(designator) is not None:
                indent4 = ET.SubElement(indent3, 'aixm:locationIndicatorICAO')
                indent4.text = designator

        except NameError:
            pass

        try:
            indent4 = ET.Element('aixm:ARP')
            indent5 = ET.SubElement(indent4, 'aixm:ElevatedPoint')
            indent6 = ET.SubElement(indent5, 'gml:pos')
            indent6.text = ' '.join(token['position'].split()[:2])
            indent5.set('srsDimension', des.srsDimension)
            indent5.set('srsName', des.srsName)
            indent5.set('axisLabels', 'Lat Long')
            indent5.set('gml:id', deu.getUUID())
            #
            # If vertical datum information is known, then use it.
            if des.srsDimension == '3':
                try:
                    indent6 = ET.Element('aixm:elevation')
                    indent6.text = token['location'].split()[2]
                    indent6.set('uom', des.elevationUOM)
                    indent5.append(indent6)

                    indent6 = ET.SubElement(indent5, 'aixm:verticalDatum')
                    indent6.text = des.verticalDatum

                except IndexError:
                    pass

            indent3.append(indent4)

        except KeyError:
            pass

    def observationTime(self, parent):

        try:
            indent = ET.SubElement(parent, 'iwxxm:observationTime')
            indent.set('xlink:href', self._issueTimeUUID)

        except AttributeError:
            indent.set('nilReason', des.NIL_MSSG_URL)

    def observation(self, parent):

        indent = ET.SubElement(self.XMLDocument, 'iwxxm:observation')
        if self.nilPresent:
            indent.set('nilReason', des.NIL_MSSG_URL)
            return

        self.result(indent)

    def result(self, parent):

        self.runwayDirectionCache = {}
        indent1 = ET.SubElement(parent, 'iwxxm:MeteorologicalAerodromeObservation')
        indent1.set('gml:id', deu.getUUID())
        indent1.set('cloudAndVisibilityOK', str('cavok' in self.decodedMetar).lower())
        #
        for element in self.observedTokenList:
            function = getattr(self, element)
            try:
                function(indent1, self.decodedMetar[element])
            except KeyError:
                #
                # If this error occurred inside one of the functions, report it
                if len(traceback.extract_tb(sys.exc_info()[2])) > 1:
                    self._Logger.warning('%s\n%s' % (self.tacString, traceback.format_exc()))
                #
                # Mandatory elements shall be reported missing
                elif element in ['temps', 'altimeter', 'wind']:
                    function(indent1, None)
                #
                # If visibility should be reported but isn't...
                elif 'cavok' not in self.decodedMetar and element in ['vsby', 'rvr']:
                    if element == 'vsby':
                        function(indent1, None)
                    elif element == 'rvr':
                        try:
                            token = self.decodedMetar['vsby']
                            if int(deu.checkVisibility(token['value'], token['uom'])) < des.RVR_MaximumDistance:
                                function(indent1, None)

                        except (KeyError, ValueError):
                            pass

    def forecasts(self, parent):
        #
        # If no significant changes, "NOSIG", is forecast
        if 'nosig' in self.decodedMetar:

            indent = ET.SubElement(parent, 'iwxxm:trendForecast')
            indent.set('xsi:nil', 'true')
            indent.set('nilReason', des.NIL_NOSIGC_URL)
            return
        #
        # Or if there are any forecast trends
        try:
            self.doTrendForecasts(self.decodedMetar['trendFcsts'])
        except KeyError:
            pass

    def doTrendForecasts(self, events):

        for event in events:

            indent = ET.SubElement(self.XMLDocument, 'iwxxm:trendForecast')
            indent1 = ET.SubElement(indent, 'iwxxm:MeteorologicalAerodromeTrendForecast')
            indent1.set('gml:id', deu.getUUID())
            indent1.set('changeIndicator', self._TrendForecast[event['type']])
            indent1.set('cloudAndVisibilityOK', str('cavok' in event).lower())
            self.trendPhenomenonTime(indent1, event)
            self.trendForecast(indent1, event)

    def trendPhenomenonTime(self, parent, event):
        #
        indent = ET.Element('iwxxm:phenomenonTime')
        indent1 = ET.Element('gml:TimePeriod')
        indent1.set('gml:id', deu.getUUID())
        begin = ET.SubElement(indent1, 'gml:beginPosition')
        end = ET.SubElement(indent1, 'gml:endPosition')
        #
        # The event may not have a 'ttime' key. In that case, phenomenonTime is unknown
        try:
            trendFcstPeriod = event['ttime']
            try:
                begin.text = trendFcstPeriod['from']
            except KeyError:
                if 'til' in trendFcstPeriod:
                    begin.set('indeterminatePosition', 'before')
                    begin.text = trendFcstPeriod['til']
                else:
                    begin.set('indeterminatePosition', 'unknown')

            try:
                end.text = trendFcstPeriod['til']
            except KeyError:
                if 'from' in trendFcstPeriod:
                    end.set('indeterminatePosition', 'after')
                    end.text = trendFcstPeriod['from']
                else:
                    end.set('indeterminatePosition', 'unknown')

            indent.append(indent1)
        #
        # No time range explictly given
        except KeyError:
            indent.set('nilReason', des.NIL_MSSG_URL)

        parent.append(indent)

    def trendForecast(self, parent, forecast):

        try:
            uom = forecast['vsby']['uom']
            value = deu.checkVisibility(forecast['vsby']['value'], uom)
            #
            # Visibility in trend forecast is handled as a single element type with no children
            indent = ET.Element('iwxxm:prevailingVisibility')
            indent.text = str(value)
            indent.set('uom', forecast['vsby']['uom'])
            parent.append(indent)

        except KeyError:
            pass
        #
        try:
            oper = {'M': 'BELOW', 'P': 'ABOVE'}[forecast['oper']]
            indent = ET.SubElement(parent, 'iwxxm:prevailingVisibilityOperator')
            indent.text = oper

        except KeyError:
            pass
        #
        # The remaining trend forecast elements are handled similarly to the observed ones
        for element in self.trendTokenList:
            function = getattr(self, element)
            try:
                function(parent, forecast[element], True)
            except KeyError:
                #
                # If this error occurred inside one of the functions, report it
                if len(traceback.extract_tb(sys.exc_info()[2])) > 1:
                    self._Logger.warning('%s\n%s' % (self.tacString, traceback.format_exc()))
    #
    def temps(self, parent, token):

        indent = ET.SubElement(parent, 'iwxxm:airTemperature')
        try:
            if deu.is_a_number(token['air']):
                indent.text = token['air']
                indent.set('uom', 'Cel')
            else:
                raise ValueError

        except (TypeError, KeyError, ValueError):

            indent.set('uom', 'N/A')
            indent.set('nilReason', des.NIL_NOOBSV_URL)
            indent.set('xsi:nil', 'true')

        indent = ET.SubElement(parent, 'iwxxm:dewpointTemperature')
        try:
            if deu.is_a_number(token['dewpoint']):
                indent.text = token['dewpoint']
                indent.set('uom', 'Cel')
            else:
                raise ValueError

        except (TypeError, KeyError, ValueError):

            indent.set('uom', 'N/A')
            indent.set('nilReason', des.NIL_NOOBSV_URL)
            indent.set('xsi:nil', 'true')

    def altimeter(self, parent, token):

        indent = ET.SubElement(parent, 'iwxxm:qnh')
        #
        # Always report pressure in hPa
        try:
            if token['uom'] == "[in_i'Hg]":
                indent.text = '%.1f' % (float(token['value']) * 33.8639)
            else:
                indent.text = str(int(token['value']))

            indent.set('uom', 'hPa')

        except (TypeError, KeyError, ValueError):

            indent.set('uom', 'N/A')
            indent.set('nilReason', des.NIL_NOOBSV_URL)
            indent.set('xsi:nil', 'true')

    def wind(self, parent, token, trend=False):

        indent = ET.SubElement(parent, 'iwxxm:surfaceWind')
        if token is None or token['str'].startswith('/////'):
            indent.set('nilReason', des.NIL_NOOBSV_URL)
            indent.set('xsi:nil', 'true')
            return

        if trend:
            indent1 = ET.SubElement(indent, 'iwxxm:AerodromeSurfaceWindTrendForecast')
        else:
            indent1 = ET.SubElement(indent, 'iwxxm:AerodromeSurfaceWind')
            if token['str'].startswith('VRB') or 'ccw' in token:
                indent1.set('variableWindDirection', 'true')
            else:
                indent1.set('variableWindDirection', 'false')

        try:
            indent2 = ET.Element('iwxxm:meanWindDirection')
            indent2.text = str(int(token['dd']))
            indent2.set('uom', 'deg')
            indent1.append(indent2)

        except ValueError:
            if token['dd'] != 'VRB':
                indent2 = ET.Element('iwxxm:meanWindDirection')
                indent2.set('uom', 'N/A')
                indent2.set('nilReason', des.NIL_NOOBSV_URL)
                indent2.set('xsi:nil', 'true')
                indent1.append(indent2)

        indent2 = ET.SubElement(indent1, 'iwxxm:meanWindSpeed')
        try:
            indent2.text = str(int(token['ff']))
            indent2.set('uom', token['uom'])

        except ValueError:
            indent2.set('uom', 'N/A')
            indent2.set('nilReason', des.NIL_NOOBSV_URL)
            indent2.set('xsi:nil', 'true')

        if 'ffplus' in token:

            indent2 = ET.SubElement(indent1, 'iwxxm:meanWindSpeedOperator')
            indent2.text = 'ABOVE'
        #
        # Gusts are optional
        try:
            indent2 = ET.Element('iwxxm:windGustSpeed')
            indent2.text = token['gg']
            indent2.set('uom', token['uom'])
            indent1.append(indent2)

            if 'ggplus' in token:

                indent2 = ET.SubElement(indent1, 'iwxxm:windGustSpeedOperator')
                indent2.text = 'ABOVE'

        except KeyError:
            pass
        #
        # Variable directions are optional
        try:
            indent2 = ET.Element('iwxxm:extremeClockwiseWindDirection')
            indent2.text = str(int(token['cw']))
            indent2.set('uom', 'deg')
            indent1.append(indent2)

            indent2 = ET.Element('iwxxm:extremeCounterClockwiseWindDirection')
            indent2.set('uom', 'deg')
            indent2.text = str(int(token['ccw']))
            indent1.append(indent2)

        except KeyError:
            pass

    def vsby(self, parent, token, trend=False):

        indent = ET.SubElement(parent, 'iwxxm:visibility')
        if token is None or '//' in token['str']:
            indent.set('nilReason', des.NIL_NOOBSV_URL)
            indent.set('xsi:nil', 'true')
            return

        indent1 = ET.SubElement(indent, 'iwxxm:AerodromeHorizontalVisibility')
        indent2 = ET.SubElement(indent1, 'iwxxm:prevailingVisibility')
        #
        # Always report visibility in meters, per Annex 3 Table A3-5
        value = deu.checkVisibility(token['value'], token['uom'])
        uom = 'm'
        indent2.set('uom', uom)

        if int(value) >= 10000:
            indent2.text = '10000'
            token['oper'] = 'P'
        else:
            indent2.text = value

        try:
            oper = {'M': 'BELOW', 'P': 'ABOVE'}[token['oper']]
            indent2 = ET.SubElement(indent1, 'iwxxm:prevailingVisibilityOperator')
            indent2.text = oper

        except KeyError:
            pass

        try:
            indent2 = ET.Element('iwxxm:minimumVisibility')
            indent2.text = deu.checkVisibility(token['min'])
            indent2.set('uom', 'm')
            indent1.append(indent2)

            indent2 = ET.Element('iwxxm:minimumVisibilityDirection')
            indent2.text = token['bearing']
            indent2.set('uom', 'deg')
            indent1.append(indent2)

        except KeyError:
            pass

    def rvr(self, parent, token):

        if token is None:
            indent = ET.SubElement(parent, 'iwxxm:rvr')
            indent.set('nilReason', des.NIL_MSSG_URL)
            indent.set('xsi:nil', 'true')
            return

        for rwy, mean, tend, oper, uom in zip(token['rwy'], token['mean'],
                                              token['tend'], token['oper'],
                                              token['uom']):

            indent = ET.SubElement(parent, 'iwxxm:rvr')
            indent1 = ET.SubElement(indent, 'iwxxm:AerodromeRunwayVisualRange')
            indent1.set('pastTendency', tend)

            indent2 = ET.SubElement(indent1, 'iwxxm:runway')
            self.runwayDirection(indent2, rwy)

            indent2 = ET.SubElement(indent1, 'iwxxm:meanRVR')
            try:
                indent2.text = deu.checkRVR(mean, uom)
                indent2.set('uom', 'm')
                if oper is not None:
                    indent2 = ET.SubElement(indent1, 'iwxxm:meanRVROperator')
                    indent2.text = oper

            except ValueError:
                indent2.set('uom', 'N/A')
                indent2.set('nilReason', des.NIL_NOOBSV_URL)
                indent2.set('xsi:nil', 'true')

    def pcp(self, parent, token, trend=False):

        for ww in token['str']:
            #
            elementName = 'iwxxm:presentWeather'
            if trend:
                elementName = 'iwxxm:weather'

            if ww == '//':
                indent = ET.SubElement(parent, elementName)
                indent.set('nilReason', des.NIL_NOOBSV_URL)
                indent.set('xsi:nil', 'true')
                continue

            if ww == 'NSW':
                indent = ET.SubElement(parent, elementName)
                indent.set('nilReason', des.NIL_NOOPRSIG_URL)
                indent.set('xsi:nil', 'true')
                continue
            #
            # Search WMO Code Registry table
            try:
                uri, title = self.codes[des.WEATHER_CONTAINER_ID][ww]
                indent = ET.SubElement(parent, elementName)
                indent.set('xlink:href', uri)
                if (des.TITLES & des.Weather):
                    indent.set('xlink:title', title)
            #
            # Weather phenomenon token not matched
            except KeyError:

                indent = ET.SubElement(parent, elementName)
                result = self._re_unknwnPcpn.match(ww)
                try:
                    up = '%s%sUP' % (result.group('mod'), result.group('char'))
                    uri, title = self.codes[des.WEATHER_CONTAINER_ID][up.strip()]

                except AttributeError:
                    uri, title = self.codes[des.WEATHER_CONTAINER_ID]['UP']

                indent.set('xlink:href', uri)
                indent.set('xlink:title', '%s: %s' % (title, ww))

    def sky(self, parent, token, trend=False):

        suffix = ''
        if trend:
            suffix = 'Forecast'

        indent = ET.SubElement(parent, 'iwxxm:cloud')
        if token['str'][0] == 'NSC':
            indent.set('nilReason', des.NIL_NOOPRSIG_URL)
            indent.set('xsi:nil', 'true')
            return

        if token['str'][0] == 'NCD':
            indent.set('nilReason', des.NIL_NOAUTODEC_URL)
            indent.set('xsi:nil', 'true')
            self.XMLDocument.set('automatedStation', 'true')
            return

        indent1 = ET.SubElement(indent, 'iwxxm:AerodromeCloud%s' % suffix)
        if trend:
            indent1.set('gml:id', deu.getUUID())

        for lyr in token['str'][:4]:
            if lyr[:3] == '///' and lyr[3:] in ['CB', 'TCU']:
                self.doCloudLayer(indent1, '/', '/', lyr[3:])
            else:
                result = self._re_cloudLyr.match(lyr)
                self.doCloudLayer(indent1, result.group(1), result.group(3), result.group(4))

    def doCloudLayer(self, parent, amount, hgt, typ):
        #
        # Vertical visibility
        if amount == 'VV':
            indent = ET.SubElement(parent, 'iwxxm:verticalVisibility')
            if deu.is_a_number(hgt):
                indent.set('uom', '[ft_i]')
                indent.text = str(int(hgt) * 100)
            else:
                indent.set('uom', 'N/A')
                indent.set('nilReason', des.NIL_NOOBSV_URL)
                indent.set('xsi:nil', 'true')

            return

        indent = ET.SubElement(parent, 'iwxxm:layer')
        if amount == '///' and hgt == '///':
            if self.XMLDocument.get('automatedStation') == 'false':
                indent.set('nilReason', des.NIL_NOOBSV_URL)
            else:
                indent.set('nilReason', des.NIL_NOAUTODEC_URL)

            indent.set('xsi:nil', 'true')
            return

        indent1 = ET.SubElement(indent, 'iwxxm:CloudLayer')
        indent2 = ET.SubElement(indent1, 'iwxxm:amount')
        try:
            title = des.CldCvr[amount]
            indent2.set('xlink:href', '%s%s' % (des.CLDCVR_URL, {'CLR': 'SKC', '0VC': 'OVC'}.get(amount, amount)))
            if (des.TITLES & des.CloudAmt):
                indent2.set('xlink:title', title)

        except KeyError:
            indent2.set('xsi:nil', 'true')
            if self.XMLDocument.get('automatedStation') == 'false':
                indent2.set('nilReason', des.NIL_NOOBSV_URL)
            else:
                indent2.set('nilReason', des.NIL_NOAUTODEC_URL)

        try:
            indent2 = ET.SubElement(indent1, 'iwxxm:base')
            indent2.text = str(int(hgt) * 100)
            indent2.set('uom', '[ft_i]')

        except (TypeError, ValueError):

            if amount == 'CLR':
                indent2.set('nilReason', des.NIL_NA_URL)
            else:
                if self.XMLDocument.get('automatedStation') == 'false':
                    indent2.set('nilReason', des.NIL_NOOBSV_URL)
                else:
                    indent2.set('nilReason', des.NIL_NOAUTODEC_URL)

            indent2.set('xsi:nil', 'true')
            indent2.set('uom', 'N/A')
        #
        # Annex 3 and WMO 306 Manual on Codes specifies only two cloud type in METAR/SPECIs, 'CB' and 'TCU'
        if typ == 'CB':
            indent2 = ET.SubElement(indent1, 'iwxxm:cloudType')
            indent2.set('xlink:href', des.CUMULONIMBUS)
            if (des.TITLES & des.CloudType):
                indent2.set('xlink:title', 'Cumulonimbus')

        if typ == 'TCU':
            indent2 = ET.SubElement(indent1, 'iwxxm:cloudType')
            indent2.set('xlink:href', des.TWRNGCUMULUS)
            if (des.TITLES & des.CloudType):
                indent2.set('xlink:title', 'Towering Cumulus')

        if typ == '///':
            indent2 = ET.SubElement(indent1, 'iwxxm:cloudType')
            indent2.set('nilReason', des.NIL_NOOBSV_URL)
            indent2.set('xsi:nil', 'true')

    def rewx(self, parent, token):

        for ww in token['str']:

            if ww == '//':
                indent = ET.SubElement(parent, 'iwxxm:recentWeather')
                indent.set('nilReason', des.NIL_NOOBSV_URL)
                indent.set('xsi:nil', 'true')
                break

            try:
                uri, title = self.codes[des.WEATHER_CONTAINER_ID][ww]
                indent = ET.SubElement(parent, 'iwxxm:recentWeather')
                indent.set('xlink:href', uri)
                if (des.TITLES & des.Weather):
                    indent.set('xlink:title', title)

            except KeyError:
                try:
                    result = self._re_unknwnPcpn.match(ww)
                    up = '%s%sUP' % (result.group('mod'), result.group('char'))
                except AttributeError:
                    up = 'UP'

                uri, title = self.codes[des.WEATHER_CONTAINER_ID][up.strip()]
                indent = ET.SubElement(parent, 'iwxxm:recentWeather')
                indent.set('xlink:href', uri)
                indent.set('xlink:title', '%s: %s' % (title, ww))

    def ws(self, parent, token):

        indent = ET.SubElement(parent, 'iwxxm:windShear')
        indent1 = ET.SubElement(indent, 'iwxxm:AerodromeWindShear')
        if 'ALL' in token['str']:
            indent1.set('allRunways', 'true')
        else:
            indent2 = ET.SubElement(indent1, 'iwxxm:runway')
            self.runwayDirection(indent2, token['rwy'])

    def seastate(self, parent, token):

        indent = ET.SubElement(parent, 'iwxxm:seaCondition')
        indent1 = ET.SubElement(indent, 'iwxxm:AerodromeSeaCondition')
        indent2 = ET.SubElement(indent1, 'iwxxm:seaSurfaceTemperature')
        try:
            indent2.text = str(int(token['seaSurfaceTemperature']))
            indent2.set('uom', 'Cel')

        except ValueError:
            indent2.set('uom', 'N/A')
            indent2.set('nilReason', des.NIL_NOOBSV_URL)
            indent2.set('xsi:nil', 'true')

        try:
            indent2 = ET.Element('iwxxm:significantWaveHeight')
            indent2.text = str(int(token['significantWaveHeight']) * 0.1)
            indent2.set('uom', 'm')
            indent1.append(indent2)

        except ValueError:
            indent2.set('uom', 'N/A')
            indent2.set('xsi:nil', 'true')
            indent2.set('nilReason', des.NIL_NOOBSV_URL)
            indent1.append(indent2)

        except KeyError:
            pass

        try:
            category = token['seaState']
            indent2 = ET.Element('iwxxm:seaState')
            try:
                uri, title = self.codes[des.SEACND_CONTAINER_ID][category]
                indent2.set('xlink:href', uri)
                if (des.TITLES & des.SeaCondition):
                    indent2.set('xlink:title', title)

            except KeyError:
                indent2.set('xsi:nil', 'true')
                indent2.set('nilReason', des.NIL_NOOBSV_URL)

            indent1.append(indent2)

        except KeyError:
            pass

    def rwystate(self, parent, tokens):

        for token in tokens:

            indent1 = ET.SubElement(parent, 'iwxxm:runwayState')
            if token['state'] == 'SNOCLO':
                indent1.set('nilReason', des.NIL_SNOCLO_URL)
                indent1.set('xsi:nil', 'true')
                continue

            indent2 = ET.SubElement(indent1, 'iwxxm:AerodromeRunwayState')
            indent2.set('allRunways', 'false')
            #
            # Attributes set first
            if len(token['runway']) == 0 or token['runway'] == '88':
                indent2.set('allRunways', 'true')

            if token['runway'] == '99':
                indent2.set('fromPreviousReport', 'true')

            if token['state'][:4] == 'CLRD':
                indent2.set('cleared', 'true')
            #
            # Runway direction
            if indent2.get('allRunways') == 'false':
                indent3 = ET.SubElement(indent2, 'iwxxm:runway')
                if token['runway'] == '99':
                    indent3.set('nilReason', des.NIL_NA_URL)
                else:
                    self.runwayDirection(indent3, token['runway'])
            #
            # Runway deposits
            if token['state'][0].isdigit():
                indent3 = ET.SubElement(indent2, 'iwxxm:depositType')
                uri, title = self.codes[des.RWYDEPST_CONTAINER_ID][token['state'][0]]
                indent3.set('xlink:href', uri)
                if (des.TITLES & des.RunwayDeposit):
                    indent3.set('xlink:title', title)
            #
            # Runway contaminates
            if token['state'][1].isdigit():
                indent3 = ET.SubElement(indent2, 'iwxxm:contamination')
                try:
                    uri, title = self.codes[des.RWYCNTMS_CONTAINER_ID][token['state'][1]]
                except KeyError:
                    uri, title = self.codes[des.RWYCNTMS_CONTAINER_ID]['15']

                indent3.set('xlink:href', uri)
                if (des.TITLES & des.AffectedRunwayCoverage):
                    indent3.set('xlink:title', title)
            #
            # Depth of deposits
            indent3 = ET.Element('iwxxm:depthOfDeposit')
            depth = token['state'][2:4]
            if depth.isdigit():
                if depth != '99':
                    indent3.set('uom', 'mm')
                    indent3.text = self._RunwayDepositDepths.get(depth, depth)
                else:
                    indent3.set('uom', 'N/A')
                    indent3.set('xsi:nil', 'true')
                    indent3.set('nilReason', des.NIL_UNKNWN_URL)

                indent2.append(indent3)

            elif depth == '//':
                indent3.set('uom', 'N/A')
                indent3.set('xsi:nil', 'true')
                indent3.set('nilReason', des.NIL_NOOBSV_URL)
                indent2.append(indent3)
            #
            # Runway friction
            friction = token['state'][4:6]
            if friction.isdigit():
                indent3 = ET.SubElement(indent2, 'iwxxm:estimatedSurfaceFrictionOrBrakingAction')
                indent3.set('xlink:href', '%s%s' % (des.RWYFRCTN_URL, friction))
                if (des.TITLES & des.RunwayFriction):
                    title = des.RunwayFrictionValues.get(friction, 'Friction coefficient: %s' % '%.2f' %
                                                         (int(friction) * 0.01))
                    indent3.set('xlink:title', title)

            elif friction == '//':
                indent3 = ET.SubElement(indent2, 'iwxxm:estimatedSurfaceFrictionOrBrakingAction')
                indent3.set('nilReason', des.NIL_MSSG_URL)

    def runwayDirection(self, parent, rwy):

        uuid = self.runwayDirectionCache.get(rwy, deu.getUUID())
        if uuid[0] == '#':
            parent.set('xlink:href', uuid)
            return

        self.runwayDirectionCache[rwy] = '#%s' % uuid
        indent = ET.SubElement(parent, 'aixm:RunwayDirection')
        indent.set('gml:id', uuid)
        indent1 = ET.SubElement(indent, 'aixm:timeSlice')
        indent2 = ET.SubElement(indent1, 'aixm:RunwayDirectionTimeSlice')
        indent2.set('gml:id', deu.getUUID())
        indent3 = ET.SubElement(indent2, 'gml:validTime')
        indent3 = ET.SubElement(indent2, 'aixm:interpretation')
        indent3.text = 'SNAPSHOT'
        indent3 = ET.SubElement(indent2, 'aixm:designator')
        indent3.text = rwy
#
# ============================ METAR/SPECI XML Encoder based on FMH-1 publication =============================


class FMH1(Annex3):

    def __init__(self, codesFile=des.CodesFilePath, centreName=des.TranslationCentreName,
                 centreDesignator=des.TranslationCentreDesignator):
        #
        # Initialize the base class
        Annex3.__init__(self, codesFile=des.CodesFilePath, centreName=des.TranslationCentreName,
                        centreDesignator=des.TranslationCentreDesignator)
        #
        self._program_name = 'FMH-1 Encoder'
        self._description = 'To encode US METAR/SPECI information in IWXXM %s format with extensions.' % des._iwxxm
        self._version = '4.1'  # Software version, not IWXXM XSD version.
        self._Logger = logging.getLogger(self._program_name)
        #
        # Remove items not found in FMH-1
        for item in ['rewx', 'ws', 'seastate', 'rwystate']:
            self.observedTokenList.remove(item)
        #
        # A lot of extra content found in FMH-1 METAR/SPECI reports can be handled by the same set of functions
        setattr(self, 'iceacc1', self.iceAccretion)
        setattr(self, 'iceacc3', self.iceAccretion)
        setattr(self, 'iceacc6', self.iceAccretion)

        setattr(self, 'pcpn1h', self.precipitationAmount)
        setattr(self, 'pcpnamt', self.precipitationAmount)
        setattr(self, 'pcpn24h', self.precipitationAmount)
        setattr(self, 'lwe', self.precipitationAmount)

        setattr(self, 'maxmin6h', self.maxminTemperatures)
        setattr(self, 'maxmin24h', self.maxminTemperatures)
        #
        # References to US Code Registry for the extra information in the reports that FMH-1 allows
        self._FMH1URL = '%s/FMH-1' % des.OFCM_CODE_REGISTRY_URL
        #
        self._re_pcpnhist = re.compile(r'(?P<PCP>(SH|FZ)?(TS|DZ|RA|SN|SG|IC|PE|GR|GS|UP|PL))(?P<TIME>((B|E)[M\d]{2,4})+)')  # noqa: E501
        self._re_event = re.compile(r'(?P<EVENT>B|E)(?P<TIME>[M\d]{2,4})')
        #
        # US puts a lot of additional measurements into the report besides the minimum Annex 3 requirements.
        self.additionalMeasurements1 = ['ostype', 'additive', 'mslp', 'ptndcy', 'pchgr', 'snodpth', 'snoincr']
        self.additionalMeasurements2 = ['pcpnhist', 'pcpn1h', 'pcpnamt', 'pcpn24h', 'lwe', 'iceacc1',
                                        'iceacc3', 'iceacc6', 'maxmin6h', 'maxmin24h', 'secondLocation',
                                        'contrails', 'aurbo', 'event', 'hail', 'nospeci', 'ssmins', 'maintenance']

        self._OSType = {'AO1': '%s/ObservingSystemType/AO1' % self._FMH1URL,
                        'AO2': '%s/ObservingSystemType/AO2' % self._FMH1URL,
                        'A01': '%s/ObservingSystemType/AO1' % self._FMH1URL,
                        'A02': '%s/ObservingSystemType/AO2' % self._FMH1URL,
                        'AO1A': '%s/ObservingSystemType/AO1A' % self._FMH1URL,
                        'AO2A': '%s/ObservingSystemType/AO2A' % self._FMH1URL,
                        'A01A': '%s/ObservingSystemType/AO1A' % self._FMH1URL,
                        'A02A': '%s/ObservingSystemType/AO2A' % self._FMH1URL}

        self._Distance = {'DSNT': '%s/QualitativeDistance/DISTANT' % self._FMH1URL,
                          'OHD': '%s/QualitativeDistance/OVERHEAD' % self._FMH1URL,
                          'VC': '%s/QualitativeDistance/VICINITY' % self._FMH1URL}

        self._CType = {'CB': '%s/ConvectiveCloudType/CUMULONIMBUS' % self._FMH1URL,
                       'TS': '%s/ConvectiveCloudType/THUNDERSTORM' % self._FMH1URL,
                       'CBMAM': '%s/ConvectiveCloudType/CUMULONIMBUS_WITH_MAMMATUS' % self._FMH1URL}

        self._Frequency = {'OCNL': '%s/LightningFrequency/OCCASIONAL' % self._FMH1URL,
                           'FRQ': '%s/LightningFrequency/FREQUENT' % self._FMH1URL,
                           'CONS': '%s/LightningFrequency/CONTINUOUS' % self._FMH1URL}

        self._SensorStatus = {'RVRNO': '%s/MeteorologicalSensor/RUNWAY_VISUAL_RANGE' % self._FMH1URL,
                              'PWINO': '%s/MeteorologicalSensor/PRESENT_WEATHER' % self._FMH1URL,
                              'PNO': '%s/MeteorologicalSensor/PRECIPITATION' % self._FMH1URL,
                              'FZRANO': '%s/MeteorologicalSensor/FREEZING_PRECIPITATION' % self._FMH1URL,
                              'TSNO': '%s/MeteorologicalSensor/THUNDERSTORM' % self._FMH1URL,
                              'VISNO': '%s/MeteorologicalSensor/VISIBILITY' % self._FMH1URL,
                              'CHINO': '%s/MeteorologicalSensor/CEILING' % self._FMH1URL,
                              'SLPNO': '%s/MeteorologicalSensor/PRESSURE' % self._FMH1URL}

    def __call__(self, decodedMetar, tacString=None):
        #
        # Initialize these for each call to the encoder
        self.vcigNotDone = True
        self.vskyNotDone = True
        #
        # Variable RVR is unusual case, make Annex3 call it explictly.
        pos = -1
        if 'vrbrvr' in decodedMetar:
            pos = self.observedTokenList.index('rvr') + 1
            self.observedTokenList.insert(pos, 'vrbrvr')
        #
        # Annex 3 METAR/SPECI XML tree is constructed first
        annex3_text = Annex3.__call__(self, decodedMetar, tacString)
        #
        # Include iwxxm-us namespace into document
        self.XMLDocument.set('xmlns:iwxxm-us', des.IWXXM_US_URI)
        self.XMLDocument.set('xsi:schemaLocation', '%s %s %s %s' % (des.IWXXM_URI, des.IWXXM_URL, des.IWXXM_US_URI,
                                                                    des.IWXXM_US_METAR_URL))
        #
        # Remove 'vrbrvr' from the list
        if pos > 0:
            self.observedTokenList.pop(pos)

        if self.nilPresent:
            return annex3_text
        #
        # Find position of <MeteorologicalAerodromeObservation> in the Element Tree
        observation = self._findTag(self.XMLDocument, 'iwxxm:observation/iwxxm:MeteorologicalAerodromeObservation')
        if observation is None:
            return annex3_text
        #
        extraMeasurements = ET.Element('MeteorologicalAerodromeObservationExtension')
        extraMeasurements.set('xmlns', des.IWXXM_US_URI)
        #
        # Encode most of the content that's found after RMK
        self.encodeExtraMeasurements(extraMeasurements)
        #
        # Append the extension block to <iwxxm:MeteorologicalAerodromeObservation>
        if len(extraMeasurements):

            extension = ET.SubElement(observation, 'iwxxm:extension')
            pos = self.tacString.find('RMK')
            extension.append(extraMeasurements)
        #
        # Do sensor outages
        try:
            self.ssistatus(self.XMLDocument, self.decodedMetar['ssistatus'])
        except KeyError:
            pass
        #
        # All done.
        return self.XMLDocument
    #
    # Methods for including <extensions> blocks to specific data types within the main body of the
    # report
    #
    def wind(self, parent, token, trend=False):

        super(FMH1, self).wind(parent, token, trend)
        #
        # Extension blocks for AerodromeSurfaceWind complex type
        #
        if 'pkwnd' in self.decodedMetar or 'wshft' in self.decodedMetar:
            AerodromeSurfaceWind = self._findTag(parent, 'iwxxm:surfaceWind/iwxxm:AerodromeSurfaceWind')
            #
            # If surface wind group is missing
            if AerodromeSurfaceWind is None:
                surfaceWind = self._findTag(parent, 'iwxxm:surfaceWind')
                try:
                    del surfaceWind.attrib['nilReason']
                    del surfaceWind.attrib['xsi:nil']
                except (AttributeError, KeyError):
                    return
                #
                # Create it but with missing values for direction and speed
                AerodromeSurfaceWind = ET.SubElement(surfaceWind, 'iwxxm:AerodromeSurfaceWind')
                wind = ET.SubElement(AerodromeSurfaceWind, 'iwxxm:meanWindDirection')
                wind.set('uom', 'N/A')
                wind.set('xsi:nil', 'true')
                wind.set('nilReason', des.NIL_NOOBSV_URL)
                wind = ET.SubElement(AerodromeSurfaceWind, 'iwxxm:meanWindSpeed')
                wind.set('uom', 'N/A')
                wind.set('xsi:nil', 'true')
                wind.set('nilReason', des.NIL_NOOBSV_URL)
        else:
            return
        #
        # Peak wind
        if 'pkwnd' in self.decodedMetar:

            indent = ET.SubElement(AerodromeSurfaceWind, 'iwxxm:extension')
            indent1 = ET.SubElement(indent, 'iwxxm-us:AerodromePeakWind')
            indent2 = ET.SubElement(indent1, 'iwxxm-us:windDirection')
            indent2.text = str(int(self.decodedMetar['pkwnd']['dd']))
            indent2.set('uom', 'deg')
            indent2 = ET.SubElement(indent1, 'iwxxm-us:windSpeed')
            indent2.text = self.decodedMetar['pkwnd']['ff']
            indent2.set('uom', self.decodedMetar['pkwnd']['uom'])
            indent2 = ET.SubElement(indent1, 'iwxxm-us:timeOfOccurrence')
            indent3 = ET.SubElement(indent2, 'gml:TimeInstant')
            indent3.set('gml:id', deu.getUUID())
            indent4 = ET.SubElement(indent3, 'gml:timePosition')
            indent4.text = self.decodedMetar['pkwnd']['itime']
        #
        # Wind shift timing
        if 'wshft' in self.decodedMetar:

            indent = ET.SubElement(AerodromeSurfaceWind, 'iwxxm:extension')
            indent1 = ET.SubElement(indent, 'iwxxm-us:AerodromeWindShift')
            #
            # Maybe associated with frontal passage
            try:
                indent1.set('frontalPassage', str(self.decodedMetar['wshft']['fropa']).lower())
            except KeyError:
                indent1.set('frontalPassage', 'false')

            indent2 = ET.SubElement(indent1, 'iwxxm-us:timeOfWindShift')
            indent3 = ET.SubElement(indent2, 'gml:TimeInstant')
            indent3.set('gml:id', deu.getUUID())
            indent4 = ET.SubElement(indent3, 'gml:timePosition')
            indent4.text = self.decodedMetar['wshft']['itime']
    #
    # Variable RVR is handled differently: the superclass method is not called.
    def vrbrvr(self, parent, token):

        for rwy, lo, hi, oper, uom in zip(token['rwy'], token['lo'],
                                          token['hi'], token['oper'],
                                          token['uom']):

            indent = ET.SubElement(parent, 'iwxxm:rvr')
            indent1 = ET.SubElement(indent, 'iwxxm:AerodromeRunwayVisualRange')
            indent2 = ET.SubElement(indent1, 'iwxxm:runway')
            self.runwayDirection(indent2, rwy)
            #
            # There is no mean value.
            indent2 = ET.SubElement(indent1, 'iwxxm:meanRVR')
            indent2.set('uom', 'N/A')
            indent2.set('nilReason', des.NIL_WTHLD_URL)
            indent2.set('xsi:nil', 'true')
            #
            indent2 = ET.SubElement(indent1, 'iwxxm:extension')
            indent3 = ET.SubElement(indent2, 'iwxxm-us:AerodromeVariableRVR')
            indent4 = ET.SubElement(indent3, 'iwxxm-us:minimumRVR')
            indent4.set('uom', 'm')
            indent4.text = deu.checkRVR(lo, uom)

            indent4 = ET.SubElement(indent3, 'iwxxm-us:maximumRVR')
            indent4.set('uom', 'm')
            indent4.text = deu.checkRVR(hi, uom)

            if oper == 'M':
                indent4 = ET.SubElement(indent3, 'iwxxm-us:variableRVROperator')
                indent4.text = 'BELOW'

            elif oper == 'P':
                indent4 = ET.SubElement(indent3, 'iwxxm-us:variableRVROperator')
                indent4.text = 'ABOVE'
    #
    # Prevailing horizontal visibility
    def vsby(self, parent, token, trend=False):

        super(FMH1, self).vsby(parent, token, trend)
        #
        # Additional information on horizontal visibility
        AerodromeHorizontalVisibility = self._findTag(parent, 'iwxxm:visibility/iwxxm:AerodromeHorizontalVisibility')
        try:
            self.sectorvis(AerodromeHorizontalVisibility, self.decodedMetar['sectorvis'])
        except KeyError:
            pass
        #
        # Variable prevailing visibility (non-directional)
        if 'vvis' in self.decodedMetar:

            indent = ET.SubElement(AerodromeHorizontalVisibility, 'iwxxm:extension')
            indent1 = ET.SubElement(indent, 'iwxxm-us:VariableVisibility')
            indent2 = ET.SubElement(indent1, 'iwxxm-us:minimumVisibility')
            indent2.text = deu.checkVisibility(self.decodedMetar['vvis']['lo'], self.decodedMetar['vvis']['uom'])
            indent2.set('uom', 'm')

            indent2 = ET.SubElement(indent1, 'iwxxm-us:maximumVisibility')
            indent2.text = deu.checkVisibility(self.decodedMetar['vvis']['hi'], self.decodedMetar['vvis']['uom'])
            indent2.set('uom', 'm')
        #
        # Control tower visibility
        if 'twrvsby' in self.decodedMetar:

            indent = ET.SubElement(AerodromeHorizontalVisibility, 'iwxxm:extension')
            indent1 = ET.SubElement(indent, 'iwxxm-us:TowerVisibility')
            indent2 = ET.SubElement(indent1, 'iwxxm-us:towerVisibility')
            indent2.text = deu.checkVisibility(self.decodedMetar['twrvsby']['value'],
                                               self.decodedMetar['twrvsby']['uom'])
            indent2.set('uom', 'm')
    #
    # Qualifiers on cloud layers.
    def doCloudLayer(self, parent, amount, hgt, typ):

        super(FMH1, self).doCloudLayer(parent, amount, hgt, typ)
        #
        # Last layer added, first (only) child, i.e [-1][0]
        try:
            CloudLayer = parent[-1][0]
        except IndexError:
            return

        if 'vcig' in self.decodedMetar and self.vcigNotDone:

            try:
                ihgt = int(hgt)
                ilo = int(self.decodedMetar['vcig']['lo'])
                ihi = int(self.decodedMetar['vcig']['hi'])

            except ValueError:
                return

            if ilo <= ihgt <= ihi and amount in ['OVC', 'BKN']:

                indent = ET.SubElement(CloudLayer, 'iwxxm:extension')
                indent1 = ET.SubElement(indent, 'iwxxm-us:VariableCeilingHeight')
                indent2 = ET.SubElement(indent1, 'iwxxm-us:minimumHeight')
                indent2.set('uom', '[ft_i]')
                indent2.text = str(ilo * 100)
                indent2 = ET.SubElement(indent1, 'iwxxm-us:maximumHeight')
                indent2.set('uom', '[ft_i]')
                indent2.text = str(ihi * 100)
                self.vcigNotDone = False

        if 'vsky' in self.decodedMetar and self.vskyNotDone:

            try:
                ihgt = int(hgt)
                ilyr = int(self.decodedMetar['vsky']['hgt'])

            except ValueError:
                return

            if ihgt == ilyr:

                indent = ET.SubElement(CloudLayer, 'iwxxm:extension')
                indent1 = ET.SubElement(indent, 'iwxxm-us:VariableSkyCondition')

                indent2 = ET.SubElement(indent1, 'iwxxm-us:firstSkyCoverValue')
                amount = self.decodedMetar['vsky']['cvr1']

                if amount == '0VC':
                    amount = 'OVC'
                indent2.set('xlink:href', '%s%s' % (des.CLDCVR_URL, amount))

                indent2 = ET.SubElement(indent1, 'iwxxm-us:secondSkyCoverValue')
                amount = self.decodedMetar['vsky']['cvr2']

                if amount == '0VC':
                    amount = 'OVC'
                indent2.set('xlink:href', '%s%s' % (des.CLDCVR_URL, amount))
                self.vskyNotDone = False
    #
    # FMH-1 includes more data after the RMK of the METAR/SPECI report

    def encodeExtraMeasurements(self, parent):
        #
        # Simple elements are done first
        for element in self.additionalMeasurements1:
            function = getattr(self, element)
            try:
                function(parent, self.decodedMetar[element])
            except KeyError:
                pass
        #
        # A little more complicated ones
        VisuallyObservablePhenomena = ET.Element('VisuallyObservablePhenomena')
        for element in ['lightning', 'tstmvmt', 'skychar', 'obsc']:
            function = getattr(self, element)
            try:
                function(VisuallyObservablePhenomena, self.decodedMetar[element])
            except KeyError:
                pass
        #
        if len(VisuallyObservablePhenomena):
            child = ET.SubElement(parent, 'visuallyObservablePhenomena')
            child.append(VisuallyObservablePhenomena)
        #
        # Now the most complicated types
        for element in self.additionalMeasurements2:
            function = getattr(self, element)
            try:
                function(parent, self.decodedMetar[element])
            except KeyError:
                pass

    def ostype(self, parent, token):

        indent = ET.SubElement(parent, 'observingSystemType')
        try:
            indent.set('xlink:href', self._OSType[self.decodedMetar['ostype']['str']])
        except KeyError:
            indent.set('nilReason', des.NIL_UNKNWN_URL)
    #
    #  FMH-1 elements that occur after the RMK token
    def additive(self, parent, token):

        indent = ET.SubElement(parent, 'humanReadableText')
        indent.text = token['str']

    def mslp(self, parent, token):

        indent = ET.Element('seaLevelPressure')
        try:
            indent.text = token['value']
            indent.set('uom', 'hPa')

        except KeyError:

            indent.set('nilReason', deu.NIL_MSSG_URL)
            indent.set('xsi:nil', 'true')
            indent.set('uom', 'N/A')

        parent.append(indent)

    def pchgr(self, parent, token):

        indent = ET.SubElement(parent, 'pressureChangeIndicator')
        indent.set('xlink:href', '%s/PressureChangingRapidly/%s' %
                   (self._FMH1URL, token['value']))

    def ptndcy(self, parent, token):

        try:
            indent = ET.Element('pressureTendency3hr')
            indent.set('uom', 'hPa')
            indent.text = token['pchg']
            parent.append(indent)
            indent = ET.SubElement(parent, 'pressureTendencyCharacteristic3hr')
            indent.set('xlink:href', '%s/%c' % (des.PRSRTNDCY, token['character']))

        except KeyError:

            indent = ET.SubElement(parent, 'pressureTendency3hr')
            indent.set('uom', 'N/A')
            indent.set('nilReason', des.NIL_MSSG_URL)
            indent.set('xsi:nil', 'true')

    def snodpth(self, parent, token):

        indent = ET.SubElement(parent, 'snowDepth')
        try:
            indent.text = str(int(token['value']))
            indent.set('uom', '[in_i]')

        except ValueError:
            indent.set('uom', 'N/A')
            indent.set('nilReason', des.NIL_MSSG_URL)
            indent.set('xsi:nil', 'true')

    def hail(self, parent, token):

        indent = ET.SubElement(parent, 'hailstoneSize')
        indent1 = ET.SubElement(indent, 'HailstoneSize')
        indent2 = ET.SubElement(indent1, 'maximumDiameter')
        indent2.text = token['value']
        indent2.set('uom', '[in_i]')
        try:
            indent2 = ET.Element('sizeOperator')
            indent2.text = token['oper']
            indent1.append(indent2)

        except KeyError:
            pass

    def ssmins(self, parent, token):

        try:
            sunshineMinutes = int(token['value'])
            indent = ET.SubElement(parent, 'durationOfSunshine')
            indent.text = 'PT%dH%dM0S' % (sunshineMinutes / 60, sunshineMinutes % 60)

        except ValueError:
            pass

    def aurbo(self, parent, token):

        indent = ET.SubElement(parent, 'aurora')
        indent.text = 'true'

    def contrails(self, parent, token):

        indent = ET.SubElement(parent, 'condensationTrail')
        indent.text = 'true'

    def nospeci(self, parent, token):

        indent = ET.SubElement(parent, 'noSpecials')
        indent.text = 'true'

    def event(self, parent, token):

        indent = ET.SubElement(parent, '%sObservation' % token['str'].lower())
        indent.text = 'true'

    def maintenance(self, parent, token):

        indent = ET.SubElement(parent, 'maintenanceIndicator')
        indent.text = 'true'
    #
    # Start of more complex types
    def snoincr(self, parent, token):

        temp = ET.Element('temp')

        child = ET.SubElement(parent, 'snowIncrease')
        indent = ET.SubElement(child, 'SnowIncrease')
        indent1 = ET.SubElement(indent, 'snowDepthIncrease')

        code = '%s/StatisticallyProcessedWeatherElements/SNOWFALLRATE' % self._FMH1URL
        self.processedQuantity(temp, token, code)
        indent1.append(temp[0][0])

        indent1 = ET.SubElement(indent, 'snowDepth')
        try:
            indent1.text = str(int(token['depth']))
            indent1.set('uom', '[in_i]')

        except TypeError:
            indent.set('uom', 'N/A')
            indent.set('nilReason', des.NIL_MSSG_URI)
            indent.set('xsi:nil', 'true')
    #
    # Statistically Processed Properties routines
    def precipitationAmount(self, parent, token):

        code = '%s/StatisticallyProcessedWeatherElements/PRECIPITATION' % self._FMH1URL
        self.processedQuantity(parent, token, code)

    def lwe(self, parent, token):

        code = '%s/StatisticallyProcessedWeatherElements/SNOWWATEREQUIVALENT' % self._FMH1URL
        self.processedQuantity(parent, token, code)

    def iceAccretion(self, parent, token):

        code = '%s/StatisticallyProcessedWeatherElements/ICE' % self._FMH1URL
        self.processedQuantity(parent, token, code)

    def processedQuantity(self, parent, token, code):

        child = ET.SubElement(parent, 'statisticallyProcessedQuantity')
        indent = ET.SubElement(child, 'StatisticallyProcessedProperty')
        indent1 = ET.SubElement(indent, 'processedWeatherElement')
        indent1.set('xlink:href', code)
        indent1 = ET.SubElement(indent, 'valueType')
        indent1.set('xlink:href', des.ACCUMLTN)
        indent1 = ET.SubElement(indent, 'valuePeriod')
        indent1.text = 'PT%dH' % int(token['period'])
        try:
            indent1 = ET.Element('qualifier')
            if token['oper'] == 'M':
                indent1.text = 'BELOW'
            else:
                indent1.text = 'ABOVE'

            indent.append(indent1)

        except KeyError:
            pass

        indent1 = ET.SubElement(indent, 'processedValue')

        if deu.is_a_number(token['value']):
            indent1.text = token['value']
            indent1.set('uom', token['uom'])
        else:
            indent1.set('uom', 'N/A')
            indent1.set('xsi:nil', 'true')
            indent1.set('nilReason', 'missing')

    def maxminTemperatures(self, parent, token):

        child = ET.SubElement(parent, 'maxMinTemperatures')
        indent = ET.SubElement(child, 'MaxMinTemperatures')
        indent1 = ET.SubElement(indent, 'precedingPeriod')
        indent1.text = 'PT%sH' % token['period']
        indent1 = ET.SubElement(indent, 'maxTemperature')
        try:
            if deu.is_a_number(token['max']):
                indent1.text = token['max']
                indent1.set('uom', 'Cel')
            else:
                raise KeyError

        except KeyError:
            indent1.set('uom', 'N/A')
            indent1.set('xsi:nil', 'true')
            indent1.set('nilReason', 'missing')

        indent1 = ET.SubElement(indent, 'minTemperature')
        try:
            if deu.is_a_number(token['min']):
                indent1.text = token['min']
                indent1.set('uom', 'Cel')
            else:
                raise KeyError

        except KeyError:
            indent1.set('uom', 'N/A')
            indent1.set('xsi:nil', 'true')
            indent1.set('nilReason', 'missing')

    def pcpnhist(self, parent, token):

        inputstr = token['str']
        issueTime = time.strptime(self._issueTime, '%Y-%m-%dT%H:%M:%SZ')
        pcphistory = {}

        for match in self._getAllMatches(self._re_pcpnhist, inputstr):

            ww = match.get('PCP')
            timeHistory = match.get('TIME')
            events = []

            for event in self._getAllMatches(self._re_event, timeHistory):

                issueTimeList = list(issueTime)
                e = event.get('EVENT')
                hhmm = event.get('TIME')

                if len(hhmm) == 2:
                    issueTimeList[4] = int(hhmm)
                elif len(hhmm) == 4:
                    issueTimeList[3:5] = int(hhmm[:2]), int(hhmm[2:])
                    deu.fix_date(issueTimeList)
                else:
                    continue

                events.append((e, issueTimeList))

            pcphistory.setdefault(ww, []).extend(events)
        #
        # Not all precipitation events in history are found in WMO codes.  Find closest matches to them
        pKeys = pcphistory.keys()
        for asosWW in pKeys:
            try:
                self.codes[des.WEATHER_CONTAINER_ID][asosWW]
            except KeyError:
                newAsosWW = 'SH%s' % asosWW

                try:
                    self.codes[des.WEATHER_CONTAINER_ID][newAsosWW]
                    pcphistory[newAsosWW] = pcphistory.get(asosWW)
                except KeyError:
                    pass

                del pcphistory[asosWW]
        #
        # Okay have broken down the history into individual weather types and times
        for ww in pcphistory:

            child = ET.Element('recentWeather')
            indent = ET.SubElement(child, 'RecentWeather')
            uri, ignored = self.codes[des.WEATHER_CONTAINER_ID][ww]

            indent1 = ET.SubElement(indent, 'weatherPhenomenon')
            indent1.set('xlink:href', uri)
            begin = end = None

            for event, tms in pcphistory[ww]:
                if event == 'B' and begin is None:
                    begin = ET.Element('gml:beginPosition')
                    begin.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))
                else:
                    end = ET.Element('gml:endPosition')
                    end.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))
                #
                if begin is not None and end is not None:
                    toe = ET.SubElement(indent, 'timeOfEvent')
                    toe1 = ET.SubElement(toe, 'gml:TimePeriod')
                    toe1.set('gml:id', deu.getUUID())
                    toe1.append(begin)
                    toe1.append(end)
                    begin = end = None

                elif begin is None and end is not None:
                    begin = ET.Element('gml:beginPosition')
                    begin.set('indeterminatePosition', 'unknown')

                    toe = ET.SubElement(indent, 'timeOfEvent')
                    toe1 = ET.SubElement(toe, 'gml:TimePeriod')
                    toe1.set('gml:id', deu.getUUID())
                    toe1.append(begin)
                    toe1.append(end)
                    begin = end = None
            #
            # If begin/end timing is incomplete, indicate that,
            if begin is not None and end is None:

                end = ET.Element('gml:endPosition')
                end.set('indeterminatePosition', 'unknown')

                toe = ET.SubElement(indent, 'timeOfEvent')
                toe1 = ET.SubElement(toe, 'gml:TimePeriod')
                toe1.set('gml:id', deu.getUUID())
                toe1.append(begin)
                toe1.append(end)

            parent.append(child)

    def skychar(self, parent, token):
        #
        # That an observer is still looking at the sky and recording what she/he sees makes me happy... :)
        child = ET.SubElement(parent, 'characterOfTheSky')
        indent = ET.SubElement(child, 'CharacterOfTheSky')
        low = ET.SubElement(indent, 'lowCloudCharacter')
        mid = ET.SubElement(indent, 'middleCloudCharacter')
        hi = ET.SubElement(indent, 'highCloudCharacter')
        s = token['str']
        if s[2] != '/':
            low.set('xlink:href', '%s/3%c' % (des.SKYCATALOG, s[2]))
        else:
            low.set('xlink:href', '%s' % des.NIL_NOOBSV_URL)

        if s[3] != '/':
            mid.set('xlink:href', '%s/2%c' % (des.SKYCATALOG, s[3]))
        else:
            mid.set('xlink:href', '%s' % des.NIL_NOOBSV_URL)

        if s[4] != '/':
            hi.set('xlink:href', '%s/1%c' % (des.SKYCATALOG, s[4]))
        else:
            hi.set('xlink:href', '%s' % des.NIL_NOOBSV_URL)

    def lightning(self, parent, token):
        for location, sectorLists in token['locations'].items():
            frequency, types = location.split('_')
            if frequency != 'None':
                frequencyElement = ET.Element('frequency')
                frequencyElement.set('xlink:href', self._Frequency[frequency])
            else:
                frequencyElement = None

            if types != '':
                typesElement = ET.Element('type')
                typesElement.set('xlink:href', '%s/LightningType/%s' % (self._FMH1URL, types))
            else:
                typesElement = None

            for sectorInfoList in sectorLists:
                doOverhead = False
                for distance, sectorList in sectorInfoList.items():
                    if distance == 'ATSTN':
                        distanceElement = None
                    else:
                        distanceElement = ET.Element('qualitativeDistance')
                        distanceElement.set('xlink:href', self._Distance[distance])

                    if sectorList == []:
                        child = ET.SubElement(parent, 'lightning')
                        indent = ET.SubElement(child, 'ObservedLightning')
                        for element in [distanceElement, frequencyElement, typesElement]:
                            if element is not None:
                                indent.append(element)

                    for sectorInfo in sectorList:
                        if 'OHD' in sectorInfo['s']:
                            sectorElement = None
                            doOverhead = True
                            continue
                        else:
                            sectorElement = ET.Element('sector')
                            SectorElement = ET.SubElement(sectorElement, 'Sector')
                            self.bearingAndRange(SectorElement, sectorInfo)

                            child = ET.SubElement(parent, 'lightning')
                            indent = ET.SubElement(child, 'ObservedLightning')
                            for element in [distanceElement, frequencyElement, typesElement, sectorElement]:
                                if element is not None:
                                    indent.append(element)
                if doOverhead:

                    doOverhead = False
                    distanceElement = ET.Element('qualitativeDistance')
                    distanceElement.set('xlink:href', self._Distance['OHD'])

                    child = ET.SubElement(parent, 'lightning')
                    indent = ET.SubElement(child, 'ObservedLightning')
                    for element in [distanceElement, frequencyElement, typesElement]:
                        if element is not None:
                            indent.append(element)

    def tstmvmt(self, parent, token):

        allConvection = token['locations']
        for cctype, sectorLists in allConvection.items():

            cloudTypeElement = ET.Element('convectiveCloudType')
            cloudTypeElement.set('xlink:href', self._CType[cctype])
            doOverhead = False

            for allSectorsDictionary, movement in sectorLists:
                if movement is not None:
                    direction = movement['s']
                    if 'OHD' in direction:
                        movementElement = ET.Element('movingOverhead')
                        movementElement = 'true'
                    else:
                        movementElement = ET.Element('directionOfMotion')
                        movementElement.set('uom', 'deg')
                        movementElement.text = deu.CardinalPtsToDegreesS.get(movement['s'], '0')
                else:
                    movementElement = None

                doOverhead = False
                #
                # Only movement is reported
                if allSectorsDictionary == {}:
                    child = ET.SubElement(parent, 'convection')
                    indent = ET.SubElement(child, 'ConvectiveCloudLocation')
                    for element in [cloudTypeElement, movementElement]:
                        if element is not None:
                            indent.append(element)

                for distance, sectorInfoList in allSectorsDictionary.items():
                    if distance == 'ATSTN':
                        distanceElement = None
                    else:
                        distanceElement = ET.Element('qualitativeDistance')
                        distanceElement.set('xlink:href', self._Distance[distance])

                    for sectorInfo in sectorInfoList:
                        if 'OHD' in sectorInfo['s']:
                            sectorElement = None
                            doOverhead = True
                            continue
                        else:
                            sectorElement = ET.Element('sector')
                            SectorElement = ET.SubElement(sectorElement, 'Sector')
                            self.bearingAndRange(SectorElement, sectorInfo)

                        child = ET.SubElement(parent, 'convection')
                        indent = ET.SubElement(child, 'ConvectiveCloudLocation')
                        for element in [cloudTypeElement, distanceElement, sectorElement, movementElement]:
                            if element is not None:
                                indent.append(element)

                if doOverhead:
                    doOverhead = False

                    cloudTypeElement = ET.Element('convectiveCloudType')
                    cloudTypeElement.set('xlink:href', self._CType[cctype])
                    distanceElement = ET.Element('qualitativeDistance')
                    distanceElement.set('xlink:href', self._Distance['OHD'])

                    child = ET.SubElement(parent, 'convection')
                    indent = ET.SubElement(child, 'ConvectiveCloudLocation')
                    for element in [cloudTypeElement, distanceElement, movementElement]:
                        if element is not None:
                            indent.append(element)

    def bearingAndRange(self, parent, token):

        child = ET.SubElement(parent, 'extremeCCWDirection')
        child.set('uom', 'deg')
        child.text = str(token['ccw'])

        try:
            child = ET.Element('extremeCCWDistance')
            child.set('uom', token['uom'])
            child.text = str(token['distance'][0])
            parent.append(child)

        except (TypeError, KeyError):
            pass

        child = ET.SubElement(parent, 'extremeCWDirection')
        child.set('uom', 'deg')
        child.text = str(token['cw'])

        try:
            child = ET.Element('extremeCWDistance')
            child.set('uom', token['uom'])
            child.text = str(token['distance'][1])
            parent.append(child)

        except (TypeError, KeyError):
            pass

    def obsc(self, parent, token):

        codes, cvr, hgt = self.codes[des.WEATHER_CONTAINER_ID][token['pcp']], token['sky'][:-3], token['sky'][-3:]

        indent = ET.SubElement(parent, 'obscuration')
        indent1 = ET.SubElement(indent, 'Obscurations')
        indent2 = ET.SubElement(indent1, 'heightOfWeatherPhenomenon')
        indent2.set('uom', '[ft_i]')
        indent2.text = str(int(hgt) * 100)

        indent2 = ET.SubElement(indent1, 'obscurationAmount')
        indent2.set('xlink:href', '%s%s' % (des.CLDCVR_URL, cvr))

        indent2 = ET.SubElement(indent1, 'weatherCausingObscuration')
        indent2.set('xlink:href', codes[0])

    def sectorvis(self, parent, token):

        AerodromeHorizontalVisibility = parent[-1][-1]

        pvis = int(AerodromeHorizontalVisibility[0].text)
        svis = int(deu.checkVisibility(token['value'], token['uom']))

        if svis < (pvis / 2):
            #
            # 'oper' only appears with automated observations which indicates visibility
            # below 1/4SM, so to show that without an element/attribute that indicates
            # below sensor minimums, halve the visibility. (This may never actually
            # occur in reality.)
            #
            if 'oper' in token:
                svis = 200

            indent = ET.SubElement(AerodromeHorizontalVisibility, 'iwxxm:minimumVisibility')
            indent.set('uom', 'm')
            indent.text = str(svis)
            indent = ET.SubElement(AerodromeHorizontalVisibility, 'iwxxm:minimumVisibilityDirection')
            indent.set('uom', 'deg')
            indent.text = str(int(token['direction']))

        else:

            indent = ET.SubElement(AerodromeHorizontalVisibility, 'iwxxm:extension')
            indent1 = ET.SubElement(indent, 'iwxxm-us:SectorVisibility')
            indent2 = ET.SubElement(indent1, 'iwxxm-us:visibility')
            indent2.set('uom', 'm')
            indent2.text = str(svis)
            indent2 = ET.SubElement(indent1, 'iwxxm-us:direction')
            indent2.set('uom', 'deg')
            indent2.text = str(int(token['direction']))
            if 'oper' in token:
                indent1.set('belowSensorMinimum', 'true')

    def secondLocation(self, parent, token):

        for location in token:
            child = ET.Element('observedAtSecondLocation')
            indent = ET.SubElement(child, 'ObservedAtSecondLocation')
            try:
                indent1 = ET.Element('ceilingHeight')
                indent1.set('uom', token[location]['cuom'])
                indent1.text = token[location]['ceilhgt']
                indent.append(indent1)

            except (TypeError, KeyError):
                pass

            try:
                indent1 = ET.Element('visibility')
                indent1.set('uom', token[location]['vuom'])
                indent1.text = token[location]['vsby']
                indent.append(indent1)

                if token[location]['oper'] == 'M':
                    indent.set('visibilityBelowSensorMinimum', 'true')

            except (TypeError, KeyError):
                pass

            if len(indent):
                indent1 = ET.SubElement(indent, 'location')
                indent2 = ET.SubElement(indent1, 'SensorLocation')
                indent2 = ET.SubElement(indent2, 'description')
                indent2.text = location
                parent.append(child)

    def ssistatus(self, parent, token):

        indent = ET.SubElement(parent, 'iwxxm:extension')
        indent = ET.SubElement(indent, 'InoperativeSensors')
        indent.set('xmlns', des.IWXXM_US_URI)

        for location, sensors in token['sensors'].items():

            indent1 = ET.SubElement(indent, 'failedSensors')
            indent2 = ET.SubElement(indent1, 'FailedSensors')

            for sensor in sensors:

                indent3 = ET.SubElement(indent2, 'parameter')
                indent3.set('xlink:href', self._SensorStatus[sensor])
            #
            # If sensor location is provided . . .
            if location != 'none':

                indent3 = ET.SubElement(indent2, 'location')
                indent4 = ET.SubElement(indent3, 'SensorLocation')
                indent5 = ET.SubElement(indent4, 'description')
                indent5.text = location

    def _getAllMatches(self, re, inputstr):

        curpos = 0
        matches = []
        while len(inputstr[curpos:]):
            try:
                m = re.search(inputstr[curpos:])
                matches.append(m.groupdict())
                curpos += m.end()
            except AttributeError:
                break

        return matches

    def _findTag(self, parent, tagToMatch):
        path = tagToMatch.split('/')
        while path:
            tag = path.pop(0)
            for child in parent:
                if child.tag == tag:
                    break
            else:
                return None

            if path:
                parent = child

        return child

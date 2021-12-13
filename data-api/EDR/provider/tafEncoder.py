#
# tafEncoder.py
#
# Purpose: Encodes a python dictionary consisting of TAF components into a XML
#          document according to the IWXXM 3.0 schema.
#
# Author: Mark Oberfield - MDL/OSTI/NWS/NOAA
#
import logging
import sys
import re
import time
import xml.etree.ElementTree as ET

from EDR.provider import xmlConfig as des
from EDR.provider import xmlUtilities as deu

__python_version__ = sys.version_info[0]


class Encoder:
    """
    Encodes a python dictionary consisting of TAF components into a XML document
    according to the IWXXM/IWXXM-US 3.0 TAF schemas.
    """
    def __init__(self):
        #
        self.NameSpaces = {'aixm': 'http://www.aixm.aero/schema/5.1.1',
                           'gml': 'http://www.opengis.net/gml/3.2',
                           '': des.IWXXM_URI,
                           'xlink': 'http://www.w3.org/1999/xlink',
                           'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
        #
        # For US extension blocks
        self.usTAFAmendmentParameters = {'None': {'href': '%s/NONE' % des.OFCM_CODE_REGISTRY_URL,
                                                  'title': 'No amendments will be issued'},
                                         'CLD': {'href': '%s/CEILING' % des.OFCM_CODE_REGISTRY_URL,
                                                 'title': 'Amendments based on cloud ceilings will be issued'},
                                         'VIS': {'href': '%s/VISIBILITY' % des.OFCM_CODE_REGISTRY_URL,
                                                 'title': 'Amendments based on horizontal visibility will be issued'},
                                         'WIND': {'href': '%s/WIND' % des.OFCM_CODE_REGISTRY_URL,
                                                  'title': 'Amendments based on wind will be issued'},
                                         'WX': {'href': '%s/WEATHER' % des.OFCM_CODE_REGISTRY_URL,
                                                'title': 'Amendments based on weather phenomenon will be issued'}}
        #
        self._re_cloudLyr = re.compile(r'(?P<AMT>VV|SKC|CLR|FEW|SCT|BKN|OVC)(?P<HGT>\d{3})?')
        self._re_ICAO_ID = re.compile(r'[A-Z]{4}')
        self._re_Alternate_ID = re.compile(r'[A-Z0-9]{3,6}')
        #
        self._changeIndicator = {'BECMG': 'BECOMING', 'TEMPO': 'TEMPORARY_FLUCTUATIONS', 'PROB30': 'PROBABILITY_30',
                                 'PROB40': 'PROBABILITY_40', 'PROB30 TEMPO': 'PROBABILITY_30_TEMPORARY_FLUCTUATIONS',
                                 'PROB40 TEMPO': 'PROBABILITY_40_TEMPORARY_FLUCTUATIONS'}

        self._bbbCodes = {'A': 'AMENDMENT', 'C': 'CORRECTION'}
        #
        # Populate the precipitation/obstruction and other phenomenon dictionary
        #
        # Create dictionaries of the following WMO codes
        neededCodes = [des.WEATHER_CONTAINER_ID]
        codesFile = des.CodesFilePath
        try:
            self.codes = deu.parseCodeRegistryTables(codesFile, neededCodes, des.PreferredLanguageForTitles)
        except AssertionError as msg:
            self._Logger.warning(msg)
        #
        # map several token ids to a single function
        setattr(self, 'obv', self.pcp)
        setattr(self, 'vcnty', self.pcp)

    def __call__(self, decodedTaf, tacString):
        #
        self.tacString = tacString
        #
        self.decodedTaf = decodedTaf
        self.nilPresent = False
        self.canceled = False
        self.decodingFailure = False
        self.iwxxmUSPrefix = False
        self.isNWSTAF = False
        #
        # Root element
        self.XMLDocument = ET.Element('TAF')        
        #
        # All IWXXM products for data-api is not official
        comment = ET.Comment('********************* NOTICE **********************\n'
                             'This is not an official product. Do not retransmit.\n'
                             '********************* NOTICE **********************')
        self.XMLDocument.append(comment)
        #
        # Set up namespace info.
        for prefix, uri in list(self.NameSpaces.items()):
            if prefix == '':
                self.XMLDocument.set('xmlns', uri)
            else:
                self.XMLDocument.set('xmlns:%s' % prefix, uri)
        #
        # Check to see if US TAF
        if decodedTaf['ident']['str'][0] in ['K', 'P'] or decodedTaf['ident']['str'][:2] in ['TJ', 'TI', 'NS']:
            self.isNWSTAF = True
        #
        # Count how many non-Annex 3 elements found.
        if self.isNWSTAF and self.nonAnnexElementsCount() > 0:
            self.iwxxmUSPrefix = True
            self.XMLDocument.set('xmlns:%s' % 'iwxxm-us', des.IWXXM_US_URI)

        if self.iwxxmUSPrefix:
            self.XMLDocument.set('xsi:schemaLocation','%s %s %s %s/taf.xsd' % (des.IWXXM_URI, des.IWXXM_URL, des.IWXXM_US_URI, des.IWXXM_US_URL))
        else:
            self.XMLDocument.set('xsi:schemaLocation','%s %s' % (des.IWXXM_URI, des.IWXXM_URL))
        #
        # NIL'd and Cancelled TAFs are recorded in 'state'
        try:
            state = self.decodedTaf['state']
            if state == 'nil':
                self.nilPresent = True
            elif state == 'canceled':
                self.canceled = True
                self.XMLDocument.set('isCancelReport', 'true')

        except KeyError:
            pass

        self.XMLDocument.set('reportStatus', self._bbbCodes.get(self.decodedTaf['bbb'][0], 'NORMAL'))
        self.XMLDocument.set('permissibleUsage', 'OPERATIONAL')
        #
        # If there was a decoding problem
        if 'err_msg' in self.decodedTaf:

            self.XMLDocument.set('translationFailedTAC', self.tacString)
            self.XMLDocument.set('permissibleUsageSupplementary', self.decodedTaf.get('err_msg'))
            self.decodingFailure = True

        self.doIt()
        #
        # Serialize (ElementTree changes behavior of tostring() but does not
        # change ElementTree.VERSION value! Using python version as proxy) >:(
        #
        if __python_version__ == 3:
            xmlstring = ET.tostring(self.XMLDocument, encoding="unicode", method="xml")
        else:
            xmlstring = ET.tostring(self.XMLDocument, encoding="UTF-8", method="xml")

        return xmlstring.replace(' />', '/>')

    def doIt(self):
        #
        # Issuance time and Aerodrome identifier should always be available
        self.itime(self.XMLDocument, self.decodedTaf['itime'])
        self.aerodrome(self.XMLDocument, self.decodedTaf['ident'])
        #
        try:
            if self.canceled:
                self.vtime(ET.SubElement(
                    self.XMLDocument, 'cancelledReportValidPeriod'), self.decodedTaf['vtime'])
                return
            else:
                self.vtime(ET.SubElement(self.XMLDocument, 'validPeriod'), self.decodedTaf['vtime'])
                if not self.isNWSTAF:
                    self.entireValidTimeID = self.validTimeID

                if self.decodingFailure:
                    return
        #
        # No valid time for NIL TAF
        except KeyError:
            if __python_version__ == 3:
                killChild = self.XMLDocument.find('validPeriod')
                self.XMLDocument.remove(killChild)
            else:
                self.XMLDocument._children.pop()
        #
        # Set the "base" forecast, which is the initial prevailing condition of the TAF
        try:
            base = self.decodedTaf['group'].pop(0)
            try:
                self.baseFcst(self.XMLDocument, base['prev'])
                self.changeGroup(self.XMLDocument, base['ocnl'])
            except KeyError:
                pass
        #
        # There is no initial forecast if TAF NIL'd
        except IndexError:
            indent = ET.SubElement(self.XMLDocument, 'baseForecast')
            indent.set('nilReason', des.NIL_MSSG_URL)
        #
        # Now the rest of the forecast "evolves" from the initial condition
        for group in self.decodedTaf['group']:
            self.changeGroup(self.XMLDocument, group['prev'])
            try:
                self.changeGroup(self.XMLDocument, group['ocnl'])
            except KeyError:
                pass
        #
        # Find limits to amendments, if any
        try:
            extension = ET.Element('extension')
            self.amd(extension, self.decodedTaf['amd'])
            self.XMLDocument.append(extension)

        except KeyError:
            pass

    def itime(self, parent, token):

        value = token['value']
        parent.set('gml:id', deu.getUUID())

        indent1 = ET.SubElement(parent, 'issueTime')
        indent2 = ET.SubElement(indent1, 'gml:TimeInstant')
        indent2.set('gml:id', deu.getUUID())

        indent3 = ET.SubElement(indent2, 'gml:timePosition')
        indent3.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(value))

    def aerodrome(self, parent, token):

        indent = ET.SubElement(parent, 'aerodrome')
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
            indent6.text = ' '.join(token['location'].split()[:2])
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

    def vtime(self, parent, token):

        indent = ET.SubElement(parent, 'gml:TimePeriod')
        indent.set('gml:id', deu.getUUID())

        indent1 = ET.SubElement(indent, 'gml:beginPosition')
        indent1.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(token['from']))
        indent1 = ET.SubElement(indent, 'gml:endPosition')
        indent1.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(token['to']))

        self.validTimeID = '#%s' % indent.get('gml:id')

    def baseFcst(self, parent, token):

        indent = ET.SubElement(parent, 'baseForecast')
        if self.nilPresent:
            indent.set('nilReason', des.NIL_MSSG_URL)
            return
        #
        indent1 = ET.SubElement(indent, 'MeteorologicalAerodromeForecast')
        indent2 = ET.SubElement(indent1, 'phenomenonTime')
        try:
            indent2.set('xlink:href', self.entireValidTimeID)
            del self.entireValidTimeID

        except AttributeError:
            self.vtime(indent2, token['time'])
        #
        # Finally the "base" forecast
        self.result(indent1, token, True)

    def changeGroup(self, parent, fcsts):

        if type(fcsts) == type({}):
            fcsts = [fcsts]

        for token in fcsts:
            indent = ET.SubElement(parent, 'changeForecast')
            indent1 = ET.SubElement(indent, 'MeteorologicalAerodromeForecast')
            self.vtime(ET.SubElement(indent1, 'phenomenonTime'), token['time'])
            self.result(indent1, token)

    def result(self, parent, token, baseFcst=False):
        #
        parent.set('cloudAndVisibilityOK', token['cavok'])
        if token['cavok'] == 'true':
            self.ForecastResults = ['wind', 'temp']
        else:
            self.ForecastResults = ['vsby', 'wind', 'pcp', 'vcnty', 'obv', 'nsw', 'sky', 'llws', 'temp']

        if not baseFcst:
            if token['type'] == 'PROB':
                t = token['time']['str'].split()
                if t[1] == 'TEMPO':
                    changeToken = '%s TEMPO' % t[0]
                else:
                    changeToken = t[0]

                parent.set('changeIndicator', self._changeIndicator.get(changeToken, 'PROBABILITY_30'))
            else:
                parent.set('changeIndicator', self._changeIndicator.get(token['type'], 'FROM'))

        parent.set('gml:id', deu.getUUID())
        #
        for element in self.ForecastResults:
            function = getattr(self, element)
            try:
                function(parent, token[element])
            except KeyError:
                pass

    def wind(self, parent, token):

        indent = ET.SubElement(parent, 'surfaceWind')
        indent1 = ET.Element('AerodromeSurfaceWindForecast')
        if token['str'].startswith('VRB'):
            indent1.set('variableWindDirection', 'true')
        else:
            try:
                indent1.set('variableWindDirection', 'false')
                indent2 = ET.Element('meanWindDirection')
                indent2.text = token['dd']
                indent2.set('uom', 'deg')

            except KeyError:
                pass

            indent1.append(indent2)

        try:
            indent2 = ET.Element('meanWindSpeed')
            indent2.text = token['ff']
            indent2.set('uom', token['uom'])
            indent1.append(indent2)

        except KeyError:
            pass

        if 'ffplus' in token:

            indent2 = ET.SubElement(indent1, 'meanWindSpeedOperator')
            indent2.text = 'ABOVE'

        try:
            indent2 = ET.Element('windGustSpeed')
            indent2.text = token['gg']
            indent2.set('uom', token['uom'])
            indent1.append(indent2)
            if 'ggplus' in token:

                indent2 = ET.SubElement(indent1, 'windGustSpeedOperator')
                indent2.text = 'ABOVE'

        except (KeyError, ValueError):
            pass

        if len(indent1):
            indent.append(indent1)

    def vsby(self, parent, token):

        indent = ET.SubElement(parent, 'prevailingVisibility')
        indent.set('uom', 'm')
        indent.text = deu.checkVisibility(token['value'],token['uom'])
        #
        # Visbility above 6SM (P6SM) or 10KM
        if token['value'] == '7' or token['value'] == '10000':
            indent = ET.SubElement(parent, 'prevailingVisibilityOperator')
            indent.text = 'ABOVE'

    def pcp(self, parent, token):
        for ww in token['str'].split():
            #
            # Search BUFR table
            try:
                codes = self.codes[des.WEATHER_CONTAINER_ID][ww]
                indent = ET.SubElement(parent, 'weather')
                indent.set('xlink:href', codes[0])
                if (des.TITLES & des.Weather):
                    indent.set('xlink:title', codes[1])
            #
            # Initial weather phenomenon token not matched
            except KeyError:
                self.wxrPhenomenonSearch(parent, ww)

    def wxrPhenomenonSearch(self, parent, ww):
        #
        # Split the weather string into two; both pieces must be found
        pos = -2
        ww1 = ww[:pos]
        ww2 = ww[pos:]

        while len(ww1) > 1:
            try:
                codes1 = self.codes[des.WEATHER_CONTAINER_ID][ww1]
                codes2 = self.codes[des.WEATHER_CONTAINER_ID][ww2]

                indent = ET.SubElement(parent, 'weather')
                indent.set('xlink:href', codes1[0])
                if (des.TITLES & des.Weather):
                    indent.set('xlink:title', codes1[1])

                indent = ET.SubElement(parent, 'weather')
                indent.set('xlink:href', codes2[0])
                if (des.TITLES & des.Weather):
                    indent.set('xlink:title', codes2[1])
                break

            except KeyError:

                pos -= 2
                ww1 = ww[:pos]
                ww2 = ww[pos:]

    def nsw(self, parent, ignored):

        indent = ET.SubElement(parent, 'weather')
        indent.set('nilReason', des.NIL_NOOPRSIG_URL)

    def sky(self, parent, token):

        indent = ET.SubElement(parent, 'cloud')
        for numberLyr, layer in enumerate(token['str'].split()):
            if layer[:2] == 'VV':
                try:
                    indent1 = ET.SubElement(indent, 'AerodromeCloudForecast')
                    indent1.set('gml:id', deu.getUUID())

                    height = int(layer[2:]) * 100
                    indent2 = ET.Element('verticalVisibility')
                    indent2.text = str(height)
                    indent2.set('uom', '[ft_i]')
                    indent1.append(indent2)

                except ValueError:
                    parent.remove(indent)
                    pass

            elif layer == 'NSC':
                indent.set('nilReason', des.NIL_NOOPRSIG_URL)

            else:
                if numberLyr == 0:
                    indent1 = ET.SubElement(indent, 'AerodromeCloudForecast')
                    indent1.set('gml:id', deu.getUUID())

                self.doCloudLayer(indent1, layer)

    def doCloudLayer(self, parent, layer):

        indent = ET.SubElement(parent, 'layer')
        indent1 = ET.SubElement(indent, 'CloudLayer')
        desc = self._re_cloudLyr.match(layer)

        try:
            amount = desc.group('AMT')
            indent2 = ET.Element('amount')
            indent2.set('xlink:href', '%s%s' % (des.CLDCVR_URL, amount))
            if (des.TITLES & des.CloudAmt):
                indent2.set('xlink:title', des.CldCvr[amount])
            indent1.append(indent2)

        except TypeError:
            return

        indent2 = ET.SubElement(indent1, 'base')
        indent2.set('uom', '[ft_i]')

        try:
            height = int(desc.group('HGT')) * 100
            indent2.text = str(height)

        except TypeError:
            if amount in ['SKC', 'CLR']:
                indent2.set('uom', 'N/A')
                indent2.set('xsi:nil', 'true')
                indent2.set('nilReason', des.NIL_NA_URL)

        if layer.endswith('CB'):
            indent2 = ET.SubElement(indent1, 'cloudType')
            indent2.set('xlink:href', des.CUMULONIMBUS)

        if layer.endswith('TCU'):
            indent2 = ET.SubElement(indent1, 'cloudType')
            indent2.set('xlink:href', des.TWRNGCUMULUS)
            if (des.TITLES & des.CloudType):
                indent2.set('xlink:title', 'Towering cumulus')

    def temp(self, parent, token):

        indent = ET.SubElement(parent, 'temperature')
        indent1 = ET.SubElement(
            indent, 'AerodromeAirTemperatureForecast')
        try:
            indent2 = ET.Element('maximumAirTemperature')
            maxt = token['max']
            indent2.text = maxt['value']
            indent2.set('uom', 'Cel')
            indent1.append(indent2)

            indent2 = ET.Element('maximumAirTemperatureTime')
            indent3 = ET.SubElement(indent2, 'gml:TimeInstant')
            indent3.set('gml:id', deu.getUUID())
            indent4 = ET.SubElement(indent3, 'gml:timePosition')
            indent4.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(maxt['at']))

            indent1.append(indent2)

        except KeyError:
            pass

        try:
            indent2 = ET.Element('minimumAirTemperature')
            mint = token['min']
            indent2.text = mint['value']
            indent2.set('uom', 'Cel')
            indent1.append(indent2)

            indent2 = ET.Element('minimumAirTemperatureTime')
            indent3 = ET.SubElement(indent2, 'gml:TimeInstant')
            indent3.set('gml:id', deu.getUUID())
            indent4 = ET.SubElement(indent3, 'gml:timePosition')
            indent4.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(mint['at']))
            indent1.append(indent2)

        except KeyError:
            pass

    def llws(self, parent, token):

        child = ET.SubElement(parent, 'extension')
        NonConvectiveLLWS = ET.SubElement(child, 'iwxxm-us:NonConvectiveLowLevelWindShear')
        LLWSDir = ET.SubElement(NonConvectiveLLWS, 'iwxxm-us:windDirection')
        LLWSSpd = ET.SubElement(NonConvectiveLLWS, 'iwxxm-us:windSpeed')
        self.layerAboveAerodrome(NonConvectiveLLWS, str(token['hgt']*100), '0', '[ft_i]')
        LLWSDir.set('uom', 'deg')
        LLWSSpd.set('uom', '[kn_i]')
        LLWSDir.text = str(token['dd'])
        LLWSSpd.text = str(token['ff'])

    def layerAboveAerodrome(self, parent, upper, lower, uom):

        child = ET.SubElement(parent, 'iwxxm-us:layerAboveAerodrome')
        lowerLimit = ET.SubElement(child, 'iwxxm-us:lowerLimit')
        upperLimit = ET.SubElement(child, 'iwxxm-us:upperLimit')

        lowerLimit.set('uom', uom)
        lowerLimit.text = lower
        upperLimit.set('uom', uom)
        upperLimit.text = upper

    def amd(self, parent, limits):
        #
        # Get references to time
        alist = []
        s = limits['str']
        _TimePhrase = '(AFT|TIL)\s+(\d{6})|(\d{4}/\d{4})'
        _AmdPat = re.compile(r'AMD\s+NOT\s+SKED(\s+(%s))?|AMD\s+LTD\s+TO(\s+(CLD|VIS|WX|AND|WIND)){1,5}(\s+(%s))?' % (_TimePhrase, _TimePhrase))
        tms = list(time.gmtime(self.decodedTaf['vtime']['from']))
        
        m = _AmdPat.match(s)
        if m:
            #
            # If reference to time is found in the AMD clause one of these
            # groups will have it.
            #
            timestr = m.group(4) or m.group(5) or m.group(11) or m.group(12)
            #
            # (AFT|TIL) DDHHMM clause
            if m.group(4) or m.group(11):
                    
                tms[2:6] = int(timestr[:2]),int(timestr[2:4]),int(timestr[-2:]), 0
                self.fix_date(tms)

                if (m.group(3) or m.group(10)) == 'TIL':
                    limits['time'] = {'from': self.decodedTaf['itime']['value'],'to': time.mktime(tuple(tms))}
                elif (m.group(3) or m.group(10)) == 'AFT':
                    limits['time'] = {'from': time.mktime(
                        tuple(tms)), 'to': self.decodedTaf['vtime']['to']}
            #
            # The D1H1/D2H2 case
            elif m.group(5) or m.group(12):

                for key, timestr in zip(['from', 'to'], timestr.split('/')):
                    tms[2:6] = int(timestr[0:2]), int(timestr[2:4]), 0, 0
                    self.fix_date(tms)
                    alist.append((key, time.mktime(tuple(tms))))

                limits['time'] = dict(alist)
            #
            # If no reference to time, then its the entire time period of the TAF
            else:
                limits['time'] = self.decodedTaf['vtime'].copy()
                limits['time']['from'] = self.decodedTaf['itime']['value']

        TAFAmendmentLimitations = ET.SubElement(parent, 'iwxxm-us:TAFAmendmentLimitations')
    
        if limits['str'].find('AMD NOT SKED') == 0:
            amdTAFParameter = ET.SubElement(TAFAmendmentLimitations, 'iwxxm-us:amendableTAFParameter')
            amdTAFParameter.set('xlink:href', self.usTAFAmendmentParameters['None']['href'])
        else:
            for parameter in ['CLD', 'VIS', 'WIND', 'WX']:
                if limits['str'].find(parameter) > 0:
                    amdTAFParameter = ET.SubElement(TAFAmendmentLimitations, 'iwxxm-us:amendableTAFParameter')
                    amdTAFParameter.set('xlink:href', self.usTAFAmendmentParameters[parameter]['href'])

        periodOfLimitation = ET.SubElement(TAFAmendmentLimitations, 'iwxxm-us:periodOfLimitation')
        periodOfLimitation.set('gml:id', 'uuid.%s' % uuid.uuid4())
        beginPosition = ET.SubElement(periodOfLimitation, 'gml:beginPosition')
        beginPosition.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(limits['time']['from']))
        endPosition = ET.SubElement(periodOfLimitation, 'gml:endPosition')
        endPosition.text = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(limits['time']['to']))

    def nonAnnexElementsCount(self):

        count = 0
        if 'amd' in self.decodedTaf:
            count = 1

        for g in self.decodedTaf['group']:
            if 'llws' in g['prev']:
                count += 1

        return count

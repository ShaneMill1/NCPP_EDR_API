import sys
import time
import uuid
import xml.etree.ElementTree as ET

# Python 2/3 compatibility/differences
__python_version__ = sys.version_info[0]


class Bulletin:
    """Caches IWXXM documents and constructs bulletins using the WMO collect schema"""
    def __init__(self):
        self.docs = {}

    def cacheDocument(self, iwxxm, ahl):
        """Caches IWXXM document for insertion into a Meteorological Bulletin 'envelope'

        iwxxm is the Element Tree 'root' of the XML document.
        ahl is the WMO AHL T2A1A2ii CCCC YYgg [BBB] string"""

        self.docs.setdefault(ahl, []).append(iwxxm)

    def makeBulletin(self):
        """Creates Meteorological Bulletins according to cache documents. When cache is empty, function returns None."""

        keys = list(self.docs.keys())
        try:
            ahl = keys.pop()

        except IndexError:
            return None

        try:
            ttaa, cccc, yygg, bbb = ahl.split(' ')

        except ValueError:
            ttaa, cccc, yygg = ahl.split(' ')
            bbb = ''
        #
        # The IWXXM product(s) needs to be wrapped up in a Meteorological Bulletin "envelope".
        #
        # Construct the root element
        bulletin = ET.Element('MeteorologicalBulletin')
        bulletin.set('xmlns', 'http://def.wmo.int/collect/2014')
        bulletin.set('xmlns:gml', 'http://www.opengis.net/gml/3.2')
        bulletin.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
        bulletin.set('xsi:schemaLocation',
                     'http://def.wmo.int/collect/2014 http://schemas.wmo.int/collect/1.2/collect.xsd')
        bulletin.set('gml:id', 'uuid.%s' % uuid.uuid4())
        #
        # Get all IWXXM documents with this particular WMO AHL key
        while True:
            try:
                iwxxm = self.docs[ahl].pop()
                child = ET.SubElement(bulletin, 'meteorologicalInformation')
                child.append(iwxxm)

            except IndexError:
                del self.docs[ahl]
                break
        #
        # Construct the full day/time stamp for the product
        child = ET.SubElement(bulletin, 'bulletinIdentifier')
        #
        # Construct the name of the bulletin
        child.text = 'A_L%s%s%s%s_C_%s_%s.xml' % (ttaa, cccc, yygg, bbb, cccc, time.strftime('%Y%m%d%H%M%S'))
        #
        # Serialize (ElementTree changes behavior of tostring() but does not
        # change ElementTree.VERSION value! Using python version as proxy) >:(
        #
        if __python_version__ == 3:
            xmlstring = ET.tostring(bulletin, encoding="unicode", method="xml")
        else:
            xmlstring = ET.tostring(bulletin, encoding="UTF-8", method="xml")

        return xmlstring.replace(' />', '/>')

    def getAllBulletins(self):
        """Empty document cache, construct and return all bulletins"""
        allBulletins = []

        while True:
            bulletin = self.makeBulletin()
            if bulletin is not None:
                allBulletins.append(bulletin)
            else:
                break

        return allBulletins

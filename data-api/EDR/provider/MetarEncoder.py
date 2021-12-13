import random
import re
import time

from ..common import bulletin
from EDR.provider import metarDecoders
from EDR.provider import metarEncoders

from EDR.provider import xmlUtilities as deu


class MetarEncoder:
    """Accepts Traditional Alphanumeric Code form of METAR and SPECI and generates equivalent IWXXM form"""

    def __init__(self):
        self.re_METAR = re.compile(r'^(METAR|SPECI)\s+(?P<id>[A-Z][A-Z0-9]{3})\s+\d{6}Z\s[A-Z0-9/\s+-]+', re.MULTILINE)
        self.re_AHL = re.compile(r'S(?P<ttaa>\w\w\w\d\d)\s+(?P<cccc>\w{4})\s+(?P<yygg>\d{6})(\s+(?P<bbb>[ACR]{2}[A-Z]))?')  # noqa: E501

        self.bulletinMaker = bulletin.Bulletin()
        self.Annex3 = (metarDecoders.Annex3(), metarEncoders.Annex3())
        self.FMH1 = (metarDecoders.FMH1(), metarEncoders.FMH1())

    def encode(self, tac, geolocation, name=None, alternate=None):
        r"""Accepts TAC form of METAR or SPECI containing a single instance of WMO AHL and TAC METAR or SPECI.

        text = A character string containing the METAR/SPECI TAC form with appropriate WMO AHL as the first line.
              Second and subsequent lines contain the TAC observation. First line of METAR shall match the regular
              expression '(METAR|SPECI)\s+[A-Z][A-Z0-9]{3}\s+\d{6}Z'. TAC observation shall be terminated with '='
              character.

        geolocation = A string containing the latitude, longitude and elevation (in meters) of the aerodrome. String
              format shall be '%f %f %d'. Latitudes of aerodromes in Southern Hemisphere shall be negative. Longitudes
              of aerodromes in Western Hemisphere shall be negative.

        name = String containing official name of aerodrome (optional).

        alternate = String containing alternate identifier of aerodrome, e.g. IATA identifier. Must match regular
              expression '[A-Z0-9]{3,6}' (optional)."""
        #
        # Get the WMO AHL line and METAR TAC
        try:
            AHL = self.re_AHL.search(tac)
            ttaa, cccc, yygg, ignored, bbb = AHL.groups('')

            result = self.re_METAR.search(tac)
            icaoID = result.group('id')
            #
            # Handle observations from United States and its terrorities differently from the rest of the world
            if icaoID[0] == 'K' or icaoID[:2] in ['PA', 'PH', 'PG', 'TJ']:
                decoder, encoder = self.FMH1
            else:
                decoder, encoder = self.Annex3

            spos, epos = result.span()
            decodedMETAR = decoder(tac[spos:epos])

        except AttributeError:
            return None

        if name is not None:
            decodedMETAR['ident']['name'] = name
        if alternate is not None:
            decodedMETAR['ident']['alternate'] = alternate

        decodedMETAR['ident']['position'] = geolocation
        decodedMETAR['translatedBulletinID'] = ''.join(['S', ttaa, cccc, yygg, bbb])

        now = list(time.gmtime())
        now[2] = int(yygg[:2])
        now[3] = int(yygg[2:4])
        now[4] = int(yygg[4:6])
        now[5] = random.randint(0, 59)
        deu.fix_date(now)
        decodedMETAR['translatedBulletinReceptionTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(now))
        decodedMETAR['translationTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        ahlkey = ' '.join([ttaa, cccc, yygg, bbb])

        self.bulletinMaker.cacheDocument(encoder(decodedMETAR, tac[spos:epos]), ahlkey)

    def getAllBulletins(self):
        """Return WMO Meteorological Bulletins as a list of text strings."""

        return self.bulletinMaker.getAllBulletins()

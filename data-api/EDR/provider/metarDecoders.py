#
# Name: metarDecoders.py
#
# Purpose: Annex 3: To decode, in its entirety, the METAR/SPECI traditional alphanumeric code
#          as described in the Meteorological Service for International Air Navigation,
#          Annex 3 to the Convention on International Civil Aviation, Eighteenth Edition.
#          Annex3 decoder is meant for non-domestic (non US) METAR/SPECI observations.
#
#          FMH1: To decode, in its entirety, the METAR/SPECI traditional alphanumeric code as
#          described in the US Federal Meteorological Handbook No. 1 (FMH-1)
#
# Author: Mark Oberfield
# Organization: NOAA/NWS/OSTI/MDL/WIAB
#
import calendar
import logging
import re
import time
import traceback

from EDR.provider import tpg
from EDR.provider import xmlUtilities as deu
##############################################################################


class Annex3(tpg.Parser):
    r"""
    set lexer = ContextSensitiveLexer
    set lexer_dotall = True

    separator spaces:    '\s+' ;

    token type:  'METAR|SPECI' ;
    token ident: '[A-Z][A-Z0-9]{3}' ;
    token itime: '\d{6}Z' ;
    token auto:  'AUT(O|0)' ;
    token wind: '(VRB|(\d{3}|///))P?(\d{2,3}|//)(GP?\d{2,3})?(MPS|KT)' ;
    token wind_vrb: '\d{3}V\d{3}' ;
    token vsby1: '((?P<whole>\d{1,3}(?!/))?(?P<fraction>(M|\s+)?\d/\d{1,2})?|/{2,4})SM' ;
    token vsby2: '[PM]?(?P<vsby>[/\d]{4})\s?(NDV)?' ;
    token minvsby: '\d{4}[NEWS]{0,2}'  ;
    token rvr: 'R(?P<rwy>[/\d]{2}[RCL]?)/(?P<oper>[MP])?(?P<mean>[/\d]{4}(FT)?)/?(?P<tend>[UDN]?)' ;
    token nsw: 'NSW' ;
    token drytstm: '[+-]?TS' ;
    token pcp: '[+-]?(TS|SH|FZ)?(DZ|RA|SN|SG|IC|PE|GR|GS|PL|UP){1,3}' ;
    token obv: '[+-]?(MI|PR|BC|DR|BL|FZ)?(BR|FG|FU|VA|DU|SA|HZ|PY|P(O|0)|SQ|FC|SS|DS|SN|//)' ;
    token vcnty: 'VC(FG|P(O|0)|FC|DS|SS|TS|SH|VA|BLSN|BLSA)' ;
    token noclouds: 'NSC|NCD|SKC|CLR' ;
    token sky: '(VV|FEW|SCT|BKN|(0|O)VC|///)(\d{3}|///)?(CB|TCU|///)?' ;
    token temps: '(?P<air>(M|-)?\d{2}|MM|//)/(?P<dewpoint>(M|-)?\d{2}|MM|//)' ;
    token altimeter: '(Q|A)(\d{3,4}|////)' ;

    token rewx: 'RE(FZ|SH|TS)?(DZ|RASN|RA|(BL)?SN|SG|GR|GS|SS|DS|FC|VA|PL|UP|//)|RETS' ;
    token windshear: 'WS\s(R(WY)?(?P<rwy>\d{2}[RLC]?)|ALL\sRWY)' ;
    token seastate: 'W(?P<temp>(M|-)?\d\d|//)/(S|H)(?P<value>[/\d]{1,3})' ;
    token rwystate: 'R(\d{0,2}[LCR]?)/([\d/]{6}|SNOCLO|CLRD[/\d]{0,2})' ;
    token trendtype:'BECMG|TEMPO' ;
    token ftime: '(AT|FM)\d{4}' ;
    token ttime: 'TL\d{4}' ;
    token twind: '((\d{3}|///))P?(\d{2,3}|//)(GP?\d{2,3})?(MPS|KT)' ;

    START/e -> METAR/e $ e=self.finish() $ ;

    METAR -> Type Ident ITime (NIL|Report) ;
    Report -> Cor? Auto? Main Supplement? TrendFcst? ;
    Main -> Wind VrbDir? (CAVOK|((Vsby1|(Vsby2 MinVsby?)) Rvr{0,4} (Pcp|DryTstm|Obv|Vcnty){0,3} (NoClouds|Sky{1,4}))) Temps Altimeter{1,2} ; # noqa: E501
    Supplement -> RecentPcp{0,3} WindShear? SeaState{0,2} RunwayState*;
    TrendFcst -> NOSIG|(TrendType (FTime|TTime){0,2} TWind? CAVOK? (Vsby1|Vsby2)? Nsw? (Pcp|DryTstm|Obv){0,3} (NoClouds|Sky{0,4}))+ ;

    Type -> type/x $ self.obtype(x) $ ;
    Ident -> ident/x $ self.ident(x) $ ;
    ITime -> itime/x $ self.itime(x) $ ;

    NIL -> 'NIL' $ self.nil() $ ;

    Auto -> auto $ self.auto() $ ;
    Cor ->  'COR' $ self.correction() $ ;
    Wind -> wind/x $ self.wind(x) $ ;
    TWind -> twind/x $ self.wind(x) $ ;
    VrbDir -> wind_vrb/x $ self.wind(x) $ ;
    CAVOK -> 'CAV(O|0)K' $ self.cavok() $ ;

    Vsby1 -> vsby1/x $ self.vsby(x,'[mi_i]') $ ;
    Vsby2 -> vsby2/x $ self.vsby(x,'m') $ ;
    MinVsby -> minvsby/x $ self.vsby(x,'m') $ ;
    Rvr -> rvr/x $ self.rvr(x) $ ;
    Pcp -> pcp/x $ self.pcp(x) $ ;
    Nsw -> nsw/x $ self.pcp(x) $ ;
    DryTstm -> drytstm/x $ self.pcp(x) $ ;
    Obv -> obv/x $ self.obv(x) $ ;
    Vcnty -> vcnty/x $ self.vcnty(x) $ ;
    NoClouds -> noclouds/x $ self.sky(x) $ ;
    Sky -> sky/x $ self.sky(x) $ ;
    Temps -> temps/x $ self.temps(x) $ ;
    Altimeter -> altimeter/x $ self.altimeter(x) $ ;

    RecentPcp -> rewx/x $ self.rewx(x) $ ;

    WindShear -> windshear/x $ self.windshear(x) $ ;
    SeaState -> seastate/x $ self.seastate(x) $ ;
    RunwayState -> rwystate/x $ self.rwystate(x) $ ;
    NOSIG -> 'N(O|0)SIG' $ self.nosig() $ ;

    TrendType -> trendtype/x $ self.trendtype(x) $ ;
    FTime -> ftime/x $ self.timeBoundary(x,'from') $ ;
    TTime -> ttime/x $ self.timeBoundary(x,'til') $ ;
    """

    def __init__(self):

        self._tokenInEnglish = {'_tok_1': 'NIL', '_tok_2': 'COR', '_tok_3': 'CAVOK', '_tok_4': 'NOSIG',
                                'type': 'Keyword METAR or SPECI', 'ident': 'ICAO Identifier',
                                'itime': 'issuance time ddHHmmZ', 'auto': 'AUTO', 'wind': 'wind',
                                'wind_vrb': 'variable wind direction', 'vsby1': 'visibility in statute miles',
                                'vsby2': 'visibility in metres', 'minvsby': 'directional minimum visibility',
                                'rvr': 'runway visual range', 'drytstm': 'thunderstorm', 'pcp': 'precipitation',
                                'nsw': 'NSW', 'obv': 'obstruction to vision', 'vcnty': 'precipitation in the vicinity',
                                'noclouds': 'NCD, NSC, CLR, SKC', 'sky': 'cloud layer',
                                'temps': 'air and dew-point temperature', 'altimeter': 'altimeter',
                                'rewx': 'recent weather', 'windshear': 'windshear', 'seastate': 'state of the sea',
                                'rwystate': 'state of the runway', 'trendtype': 'trend qualifier',
                                'ftime': 'start of trend time period', 'ttime': 'end of trend time period',
                                'twind': 'wind (no VRB allowed)'}

        self._program_name = 'Annex 3 METAR/SPECI decoder'
        self._version = '2.0'
        self._annex3_amd = '78'
        self._description = 'To decode, in its entirety, the METAR/SPECI traditional alphanumeric codes as '\
                            'described in the Meteorological Service for International Air Navigation, '\
                            'Annex 3 to the Convention on International Civil Aviation, Eighteenth Edition.'

        self._Logger = logging.getLogger(self._program_name)

        return super(Annex3, self).__init__()

    def __call__(self, raw):

        self._metar = {'translationTime': time.strftime('%Y-%m-%dT%H:%M:%SZ')}
        metar = ' '.join(raw.split())
        if self.__class__.__name__ == 'Annex3':
            #
            # Remove RMK token and everything beyond that. This decoder follows
            # Annex 3 specifications to the letter and ignores content beyond the
            # RMK keyword. Any unidentified content in the report renders it invalid.
            #
            rmk = metar.find(' RMK ')
            if rmk > 0:
                metar = metar[:rmk]
        #
        # Remove the EOT marker
        eot = metar.find('=')
        if eot > 0:
            metar = metar[:eot]

        self._Logger.debug(metar)
        try:
            self._expected = []
            super(Annex3, self).__call__(metar)
            return self._metar

        except tpg.SyntacticError:
            try:
                if 'altimeter' in self._metar:
                    self._expected.remove('altimeter')
            except ValueError:
                pass

            if len(self._expected):
                err_msg = 'Expecting %s group(s) ' % ' or '.join([self._tokenInEnglish.get(x, x)
                                                                  for x in self._expected])
            else:
                err_msg = 'Unidentified group '

            self._Logger.debug('%s^' % (' ' * self.lexer.max_pos))
            self._Logger.debug(err_msg)

            err_msg += 'after position column %d.' % self.lexer.max_pos
            self._metar['err_msg'] = err_msg
            return self._metar

        except:  # noqa: E722
            self._Logger.critical('%s\n%s' % (metar, traceback.format_exc()))
            return self._metar

    def emit(self):

        self._Logger.info('%s v%s. Decoding METAR/SPECI reports according to Annex 3 Amendment %s' %
                          (self._program_name, self._version, self._annex3_amd))
        self._Logger.debug(self._description)

    def index(self):

        ti = self.lexer.cur_token
        return ('%d.%d' % (ti.line, ti.column - 1),
                '%d.%d' % (ti.end_line, ti.end_column - 1))

    def tokenOK(self, pos=0):
        'Checks whether token ends with a blank'
        try:
            return self.lexer.input[self.lexer.token().stop + pos].isspace()
        except IndexError:
            return True

    def eatCSL(self, name):
        'Overrides super definition'
        try:
            value = super(Annex3, self).eatCSL(name)
            self._expected = []
            # self.stats[name]=self.stats.get(name,0)+1
            return value

        except tpg.WrongToken:
            self._expected.append(name)
            raise

    def updateDictionary(self, key, value, root):
        try:
            d = root[key]
            d['index'].append(self.index())
            d['str'].append(value)

        except KeyError:
            root[key] = {'str': [value], 'index': [self.index()]}

    #######################################################################
    # Methods called by the parser
    def obtype(self, s):
        self._metar['type'] = {'str': s, 'index': self.index()}

    def ident(self, s):
        self._metar['ident'] = {'str': s, 'index': self.index()}

    def itime(self, s):
        d = self._metar['itime'] = {'str': s, 'index': self.index()}
        mday, hour, minute = int(s[:2]), int(s[2:4]), int(s[4:6])

        tms = list(time.gmtime())
        tms[2:6] = mday, hour, minute, 0
        deu.fix_date(tms)
        d['intTime'] = calendar.timegm(tuple(tms))
        d['tuple'] = time.gmtime(d['intTime'])
        d['value'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', d['tuple'])

    def auto(self):
        self._metar['auto'] = {'index': self.index()}

    def correction(self):
        self._metar['cor'] = {'index': self.index()}

    def nil(self):
        self._metar['nil'] = {'index': self.index()}

    def wind(self, s):
        #
        # Wind groups can appear later in the trend section of the report
        try:
            root = getattr(self, '_trend')
        except AttributeError:
            root = getattr(self, '_metar')
        #
        # Handle variable wind direction which always comes after the wind group
        try:
            d = root['wind']
            d['index'] = (d['index'][0], self.index()[1])
            d['str'] = "%s %s" % (d['str'], s)
            ccw, cw = s.split('V')
            d.update({'ccw': ccw, 'cw': cw})
            return

        except KeyError:
            pass

        d = root['wind'] = {'str': s, 'index': self.index()}
        if s[:-3] == 'VRB':
            dd = 'VRB'
        else:
            dd = s[:3]

        if s[-3:] == 'MPS':
            uom = 'm/s'
            spd = s[3:-3]
        elif s[-2:] == 'KT':
            uom = '[kn_i]'
            spd = s[3:-2]

        try:
            ff, gg = spd.split('G')
            if ff[0] == 'P':
                d['ffplus'] = True
                ff = ff[1:]

            if gg[0] == 'P':
                d['ggplus'] = True
                gg = gg[1:]

            d.update({'dd': dd, 'ff': ff, 'gg': gg, 'uom': uom})

        except ValueError:
            if spd[0] == 'P':
                d['ffplus'] = True
                ff = spd[1:]
            else:
                ff = spd

            d.update({'dd': dd, 'ff': ff, 'uom': uom})

    def cavok(self):
        try:
            root = getattr(self, '_trend')
        except AttributeError:
            root = getattr(self, '_metar')

        root['cavok'] = {'index': self.index()}

    def vsby(self, s, uom):
        vis = 0.0
        oper = None
        v = self.lexer.tokens[self.lexer.cur_token.name][0].match(s)
        if self.lexer.cur_token.name == 'vsby1':
            try:
                vis += float(v.group('whole'))
            except (AttributeError, TypeError):
                pass

            try:
                num, den = v.group('fraction').split('/', 1)
                if num[0] == 'M':
                    vis += float(num[1:]) / float(den)
                    oper = 'M'
                else:
                    vis += float(num) / float(den)

            except (AttributeError, ValueError, ZeroDivisionError):
                pass

            value = '%.2f' % vis

        elif self.lexer.cur_token.name == 'vsby2':
            oper = s[0]
            value = v.group('vsby')
            if oper in ['M', 'P']:
                value = value[1:]

        try:
            root = getattr(self, '_trend')
        except AttributeError:
            root = getattr(self, '_metar')

        if 'vsby' in root:
            root['vsby'].update({'min': s[0:4], 'bearing': deu.CardinalPtsToDegreesS.get(s[4:], '/')})
            root['vsby']['index'] = (root['vsby']['index'][0], self.index()[1])
        else:
            root['vsby'] = {'str': s, 'index': self.index(), 'value': value, 'uom': uom, 'oper': oper}

    def rvr(self, s):
        result = self.lexer.tokens[self.lexer.cur_token.name][0].match(s)
        uom = 'm'
        oper = {'P': 'ABOVE', 'M': 'BELOW'}.get(result.group('oper'), None)
        tend = {'D': 'DOWNWARD', 'N': 'NO_CHANGE', 'U': 'UPWARD'}.get(result.group('tend'), 'MISSING_VALUE')
        mean = result.group('mean')
        if mean[-2:] == 'FT':
            mean = mean[:-2]
            uom = '[ft_i]'

        try:
            d = self._metar['rvr']
            d['str'].append(s)
            d['index'].append(self.index())
            d['rwy'].append(result.group('rwy'))
            d['mean'].append(mean)
            d['oper'].append(oper)
            d['tend'].append(tend)
            d['uom'].append(uom)

        except KeyError:
            self._metar['rvr'] = {'str': [s], 'index': [self.index()], 'rwy': [result.group('rwy')],
                                  'oper': [oper], 'mean': [mean], 'tend': [tend], 'uom': [uom]}

    def obv(self, s):
        if s == '//' and not self.tokenOK():
            raise tpg.WrongToken

        try:
            root = getattr(self, '_trend')
        except AttributeError:
            root = getattr(self, '_metar')

        self.updateDictionary('obv', s, root)

    def pcp(self, s):
        try:
            root = getattr(self, '_trend')
        except AttributeError:
            root = getattr(self, '_metar')

        self.updateDictionary('pcp', s, root)

    def vcnty(self, s):
        self.updateDictionary('vcnty', s, self._metar)

    def sky(self, s):

        if not self.tokenOK() and s == '///':
            raise tpg.WrongToken

        try:
            root = getattr(self, '_trend')
        except AttributeError:
            root = getattr(self, '_metar')

        self.updateDictionary('sky', s, root)

    def temps(self, s):
        d = self._metar['temps'] = {'str': s, 'index': self.index(), 'uom': 'Cel'}

        rePattern = self.lexer.tokens[self.lexer.cur_token.name][0]
        result = rePattern.match(s)

        d.update(result.groupdict())
        try:
            d['air'] = str(int(result.group('air').replace('M', '-')))
        except (AttributeError, ValueError):
            pass
        try:
            d['dewpoint'] = str(int(result.group('dewpoint').replace('M', '-')))
        except (AttributeError, ValueError):
            pass

    def altimeter(self, s):
        if s[0] == 'Q':
            self._metar['altimeter'] = {'str': s, 'index': self.index(), 'uom': 'hPa', 'value': s[1:]}
        #
        # Add it only if QNH hasn't been found.
        elif 'altimeter' not in self._metar:
            try:
                value = '%.02f' % (int(s[1:]) * 0.01)
            except ValueError:
                value = '////'

            self._metar['altimeter'] = {'str': s, 'index': self.index(), 'uom': "[in_i'Hg]", 'value': value}

    def rewx(self, s):
        self.updateDictionary('rewx', s[2:], self._metar)

    def windshear(self, s):
        rePattern = self.lexer.tokens[self.lexer.cur_token.name][0]
        result = rePattern.match(s)
        self._metar['ws'] = {'str': s, 'index': self.index(), 'rwy': result.group('rwy')}

    def seastate(self, s):
        rePattern = self.lexer.tokens[self.lexer.cur_token.name][0]
        result = rePattern.match(s)

        stateType = {'S': 'seaState', 'H': 'significantWaveHeight'}.get(result.group(3))

        try:
            self._metar['seastate']['str'] = '%s %s' % (self._metar['seastate']['str'][0], s)
            self._metar['seastate']['index'] = (self._metar['seastate']['index'][0], self.index()[1])
            self._metar['seastate'].update({stateType: result.group('value')})

        except KeyError:
            try:
                seatemp = int(result.group('temp').replace('M', '-'))
            except ValueError:
                seatemp = result.group('temp')

            self._metar['seastate'] = {'str': s, 'index': self.index(),
                                       'seaSurfaceTemperature': str(seatemp),
                                       stateType: result.group('value')}

    def rwystate(self, s):
        rePattern = self.lexer.tokens[self.lexer.cur_token.name][0]
        result = rePattern.match(s)
        try:
            self._metar['rwystate'].append({'str': s, 'index': self.index(),
                                            'runway': result.group(1),
                                            'state': result.group(2)})
        except KeyError:
            self._metar['rwystate'] = [{'str': s, 'index': self.index(),
                                        'runway': result.group(1),
                                        'state': result.group(2)}]

    def nosig(self):
        self._metar['nosig'] = {'index': self.index()}

    def trendtype(self, s):
        try:
            self._metar.setdefault('trendFcsts', []).append(getattr(self, '_trend'))
            del self._trend
        except AttributeError:
            pass

        self._trend = {'type': s, 'index': self.index()}

    def timeBoundary(self, s, position):
        hour, minute = int(s[-4:-2]), int(s[-2:])
        tms = list(self._metar['itime']['tuple'])
        tms[3:6] = hour, minute, 0
        if hour == 24:
            tms[3] = 0
            tms[2] += 1

        deu.fix_date(tms)
        #
        # Cases when forecast crosses midnight UTC.
        if calendar.timegm(tms) < self._metar['itime']['intTime']:
            tms[2] += 1
            deu.fix_date(tms)

        try:
            try:
                self._trend['ttime'].update({position: time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))})
            except ValueError:
                self._trend['ttime'].update({position: time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                                                     time.gmtime(calendar.timegm(tuple(tms))))})
        except KeyError:
            try:
                self._trend.update({'ttime': {position: time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))}})
            except ValueError:
                self._trend.update({'ttime': {position: time.strftime('%Y-%m-%dT%H:%M:%SZ',
                                                                      time.gmtime(calendar.timegm(tuple(tms))))}})

    def finish(self):
        #
        # If NIL, no QC checking is required
        if 'nil' in self._metar:
            return
        #
        try:
            self._metar['trendFcsts'].append(self._trend)
            del self._trend
        except (AttributeError, KeyError):
            pass
        #
        # Set boundaries so multiple trend forecasts don't overlap in time
        try:
            for previous, trend in enumerate(self._metar['trendFcsts'][1:]):
                if 'til' not in self._metar['trendFcsts'][previous]['ttime']:
                    self._metar['trendFcsts'][previous]['ttime']['til'] = trend['ttime']['from']
        except KeyError:
            pass

        return


class LocationParser(tpg.Parser):
    r"""
    set lexer = ContextSensitiveLexer
    set lexer_dotall = True
    separator spaces:    '\s+' ;

    token overhead:  'OHD' ;
    token allquads:  'ALQD?S' ;
    token compassPt: '\d{0,3}[NEWS]{1,3}' ;

    START -> Discrete|Span|Point ;

    Point -> (compassPt|overhead|allquads) ;
    Span -> Point('-'Point)+ ;
    Discrete ->  (Span|Point)('AND'(Span|Point))+ ;
    """
    re_compassPt = re.compile(r'(\d{0,3})([NEWS]{1,3})')
    CompassDegrees = {'N': (337.5, 022.5), 'NE': (022.5, 067.5), 'E': (067.5, 112.5), 'SE': (112.5, 157.5),
                      'S': (157.5, 202.5), 'SW': (202.5, 247.5), 'W': (247.5, 292.5), 'NW': (292.5, 337.5)}
    verbose = 0

    def __call__(self, string):
        #
        # Initialize
        self._spans = {}
        self._cnt = -1
        self._newDictionary = True
        self._overhead = False
        self._allquads = False
        try:
            super(LocationParser, self).__call__(string)
        except tpg.SyntacticError:
            pass
        #
        # Combine adjacent compass points
        k = self._spans.keys()
        k.sort()
        delete = []
        for f, s in zip(k, k[1:]):
            if f not in delete and self._spans[f]['cw'] == self._spans[s]['ccw']:
                if self._spans[f]['distance'] == self._spans[s]['distance']:
                    self._spans[f]['cw'] = self._spans[s]['cw']
                    self._spans[f]['s'] += '-%s' % self._spans[s]['s']
                    delete.append(s)
        #
        # Remove those that are combined
        for d in delete:
            del self._spans[d]

        return self._spans.values()

    def eatCSL(self, name):
        """Overrides and enhance base class method"""
        value = super(LocationParser, self).eatCSL(name)
        #
        # If 'overhead' is found, set and return value
        if name == 'overhead':
            if self._overhead:
                return value

            self._overhead = True
            self._cnt += 1
            dKey = '%d' % self._cnt
            self._spans[dKey] = {'ccw': 0, 'cw': 0, 's': value, 'distance': None, 'uom': None}
            self._newDictionary = True

            return value
        #
        if name == 'allquads':
            if self._allquads:
                return value

            self._allquads = True
            self._cnt += 1
            dKey = '%d' % self._cnt
            self._spans[dKey] = {'ccw': 0, 'cw': 360, 's': value, 'distance': None, 'uom': None}
            return value
        #
        # Call token_info() regardless of verbose value.
        stackInfo = self.token_info(self.lexer.token(), "==", name)
        #
        # Determine the context
        frames = stackInfo.split('.')
        #
        # Pop off the first frame since its not useful here
        frames.pop(0)
        aztype = ''.join([f[0] for f in frames])

        if aztype not in ['DSP', 'DS', 'D']:
            return value

        if aztype == 'D':
            self._newDictionary = True
            return value

        if aztype == 'DS':
            dKey = '%d' % self._cnt
            if self._spans[dKey]['s'][-1] != '-' or self._spans[dKey]['s'] != 'OHD':
                self._spans[dKey]['s'] += value
            return value
        #
        # 'value' is a key as it contains the compass point
        try:
            #
            # Separate distance and compass direction
            result = self.re_compassPt.match(value)
            distance, key = result.groups()
            try:
                distance = int(result.group(1))
            except ValueError:
                distance = 0
            #
            dKey = '%d' % self._cnt
            if self._newDictionary:

                self._newDictionary = False
                self._cnt += 1
                dKey = '%d' % self._cnt
                self._spans[dKey] = {'ccw': 0, 'cw': 0, 'distance': [], 's': '', 'uom': '[mi_i]'}

            d = self._spans[dKey]
            if distance != 0:
                d['distance'].append(distance)

            try:
                d['cw'] = self.CompassDegrees[key][1]
            except KeyError:
                d['cw'] = self.CompassDegrees[key[0]][1]

            d['s'] = '%s%s' % (d['s'], key)
            if d['ccw'] == 0:
                try:
                    d['ccw'] = self.CompassDegrees[key][0]
                except KeyError:
                    d['ccw'] = self.CompassDegrees[key[0]][0]

        except ValueError:
            pass

        return value


class FMH1(Annex3):
    r"""
    set lexer = ContextSensitiveLexer
    set lexer_dotall = True

    separator spaces:    '\s+' ;

    token type:  'METAR|SPECI' ;
    token ident: '[A-Z][A-Z0-9]{3}' ;
    token itime: '\d{6}Z' ;
    token auto:  'AUT(O|0)' ;
    token wind: '(VRB|(\d{3}|///))P?(\d{2,3}|//)(GP?\d{2,3})?KT' ;
    token wind_vrb: '\d{3}V\d{3}' ;
    token vsby1: '(?P<whole>\d{1,3}(?!/))?(?P<fraction>(M|\s+)?\d/\d{1,2})?SM' ;
    token vsby2: '[PM]?(?P<vsby>\d{4})' ;
    token rvr: 'R(?P<rwy>[/\d]{2}[RCL]?)/(?P<oper>[MP])?(?P<mean>[/\d]{4}(FT)?)/?(?P<tend>[UDN])?' ;
    token vrbrvr: 'R(?P<rwy>\d{2}[RCL]?)/(?P<lo>M?\d{4})V(?P<hi>P?\d{4})(FT)?' ;
    token drytstm: '[+-]?TS' ;
    token pcp: '[+-]?(TS|SH|FZ)?(DZ|RA|SN|SG|IC|PE|GR|GS|PL|UP){1,3}' ;
    token obv: '[+-]?(MI|PR|BC|DR|BL|FZ)?(BR|FG|FU|VA|DU|SA|HZ|PY|P(O|0)|SQ|FC|SS|DS|SN|//)' ;
    token vcnty: 'VC(FG|P(O|0)|FC|DS|SS|TS|SH|VA|BLSN|BLSA)' ;
    token noclouds: 'SKC|CLR' ;
    token sky: '(VV|FEW|SCT|BKN|(0|O)VC|///)(\d{3}|///)?(CB|TCU|///)?' ;
    token temps: '(?P<air>(M|-)?\d{2}|MM|//)/(?P<dewpoint>(M|-)?\d{2}|MM|//)?' ;
    token altimeter: 'A(\d{4}|////)' ;

    token ostype: 'A(0|O)(1|2)A?' ;
    token pkwnd: 'PK\s+WND\s+\d{5,6}/\d{2,4}' ;
    token wshft: 'WSHFT\s+\d{2,4}' ;
    token fropa: 'FROPA' ;
    token sfcvis1: 'SFC\s+VIS\s+(?P<whole>\d{1,2}(?!/))?(?P<fraction>(M|\s+)?\d/\d{1,2})?' ;
    token sfcvis2: 'SFC\s+VIS\s+([MP]?\d{4})' ;
    token twrvis1: 'TWR\s+VIS\s+(?P<whole>\d{1,2}(?!/))?(?P<fraction>(M|\s+)?\d/\d{1,2})?' ;
    token twrvis2: 'TWR\s+VIS\s+([MP]?\d{4})' ;
    token vvis1: '(VSBY|VIS)\s+(?P<vintlo>\d{1,2}(?!/))?(?P<vfraclo>(M|\s+)?\d/\d{1,2})?V(?P<vinthi>\d{1,2}(?!/))?(?P<vfrachi>(M|\s+)?\d/\d{1,2})?' ;  # noqa: E501
    token vvis2: '(VSBY|VIS)\s+(?P<vlo>M?\d{4})V(?P<vhi>P?\d{4})' ;
    token sctrvis1: 'VIS\s+[NEWS]{1,2}\s+(?P<whole>\d{1,2}(?!/))?(?P<fraction>(M|\s+)?\d/\d{1,2})?' ;
    token sctrvis2: 'VIS\s+[NEWS]{1,2}\s+([PM]?\d{4})' ;
    token vis2loc1: 'VIS\s+(?P<whole>\d{1,2}(?!/))?(?P<fraction>(M|\s+)?\d/\d{1,2})?\s+(?P<loc>(R(WY)?\s*\d{2}[RCL]?(/\d{2}[RCL]?)?|\S+))' ;
    token vis2loc2: 'VIS\s+([PM]?\d{4})\s+(?P<loc>(R(WY)?\s*\d{2}[RCL]?(/\d{2}[RCL]?)?|\S+))' ;
    token ltg: '((OCNL|FRQ|CONS)\s+)?LTG(CG|IC|CC|CA){0,4}(OHD|VC|DSNT|ALQD?S|\d{0,2}[NEWS]{1,3}((?=[-\s])|$)|AND|[-\s])+' ;
    token tstmvmt: '(CBMAM|CB|TS)\s+(OHD|VC|DSNT|ALQD?S|\d{0,2}[NEWS]{1,3}(?=[-\s])|AND|[-\s])*(?P<mov>MOV(D|G)?\s+([NEWS]{1,3}|OHD))?' ;
    token pcpnhist: '((SH|FZ)?(TS|DZ|RA|SN|SG|IC|PE|GR|GS|UP|PL)((B|E)\d{2,4})+)+' ;
    token hail: 'GR(\s+(LESS|GREATER)\s+THAN)?\s+(?P<whole>\d{1,2}(?!/))?(?P<fraction>(M|\s+)?\d/\d{1,2})?' ;
    token vcig: 'CIG\s+(\d{3})V(\d{3})' ;
    token obsc: '(FG|FU|DU|VA|HZ)\s+(FEW|SCT|BKN|(0|O)VC|///)\d{3}' ;
    token vsky: '(FEW|SCT|BKN|(0|O)VC)(\d{3})?\s+V\s+(SCT|BKN|(0|O)VC)' ;
    token cig2loc: 'CIG\s+\d{3}\s+(?P<loc>(R(WY)?\s*\d{2}[RCL]?(/\d{2}[RCL]?)?|\S+))' ;
    token pchgr: 'PRES(R|F)R' ;
    token mslp: 'SLP(\d{3}|///)' ;
    token nospeci: 'NOSPECI' ;
    token aurbo: 'AURBO' ;
    token contrails: 'CONTRAILS' ;
    token snoincr: 'SNINCR\s+\d/[\d/]{1,3}' ;
    token other: '(FIRST|LAST)' ;
    token pcpn1h: 'P(\d{3,4}|/{3,4})' ;
    token pcpn6h: '6(\d{4}|////)' ;
    token pcpn24h: '7(\d{4}|////)' ;
    token iceacc: 'I[1,3,6](\d{3}|///)' ;
    token snodpth: '4/(\d{3}|///)' ;
    token lwe: '933(\d{3}|///)' ;
    token sunshine: '98(\d{3}|///)' ;
    token skychar: '8/[/\d]{3}' ;
    token tempdec: 'T[01]\d{3}[01]\d{3}' ;
    token maxt6h: '1(\d{4}|////)((?=\s)|$)' ;
    token mint6h: '2(\d{4}|////)((?=\s)|$)' ;
    token xtrmet: '4[\d/]{8}' ;
    token ptndcy3h: '5(\d{4}|////)' ;
    token ssindc: '(RVR|PWI|P|FZRA|TS|SLP)NO|(VISNO|CHINO)(\s(R(WY)?\s*\d\d[LCR]?|(N|NE|E|SE|S|SW|W|NW)\s))?' ;
    token maintenance: '\$' ;
    token any: '\S+' ;

    START/e -> METAR/e $ e=self.finish() $ ;

    METAR -> Type Ident ITime Report ;
    Report -> Cor? Auto? Main Remarks? ;
    Main -> Wind? VrbDir? (Vsby1|Vsby2)? (VrbRvr|Rvr){0,4} (Pcp|DryTstm|Obv|Vcnty){0,3} (NoClouds|Sky{1,4})? Temps? Altimeter? ;
    Remarks -> 'RMK' (Ostype|TempDec|Slp|Pcpn1h|Ptndcy3h|Ssindc|Maintenance|MaxT6h|MinT6h|PcpnHist|PkWnd|Ltg|Pcpn6h|XtrmeT|VCig|Pchgr|VVis1|Wshft|Pcpn24h|Cig2Loc|Iceacc|VSky|SfcVis1|TwrVis1|Tstmvmt|Vis2Loc1|Other|Snodpth|VVis2|Obsc|Nospeci|SctrVis1|Snoincr|Contrails|Lwe|Fropa|Vis2Loc2|Hail|SfcVis2|TwrVis2|VVis2|SctrVis2|SkyChar|Aurbo|Sunshine|any)* ;

    Type -> type/x $ self.obtype(x) $ ;
    Ident -> ident/x $ self.ident(x) $ ;
    ITime -> itime/x $ self.itime(x) $ ;

    Auto -> auto $ self.auto() $ ;
    Cor ->  'COR' $ self.correction() $ ;
    Wind -> wind/x $ self.wind(x) $ ;
    VrbDir -> wind_vrb/x $ self.wind(x) $ ;

    Vsby1 -> vsby1/x $ self.vsby(x,'[mi_i]') $ ;
    Vsby2 -> vsby2/x $ self.vsby(x,'m') $ ;
    Rvr -> rvr/x $ self.rvr(x) $ ;
    VrbRvr -> vrbrvr/x $ self.vrbrvr(x) $ ;
    Pcp -> pcp/x $ self.pcp(x) $ ;
    DryTstm -> drytstm/x $ self.pcp(x) $ ;
    Obv -> obv/x $ self.obv(x) $ ;
    Vcnty -> vcnty/x $ self.vcnty(x) $ ;
    NoClouds -> noclouds/x $ self.sky(x) $ ;
    Sky -> sky/x $ self.sky(x) $ ;
    Temps -> temps/x $ self.temps(x) $ ;
    Altimeter -> altimeter/x $ self.altimeter(x) $ ;

    Ostype -> ostype/x $ self.ostype(x) $ ;
    PkWnd -> pkwnd/x $ self.pkwnd(x) $ ;
    Wshft -> wshft/x $ self.wshft(x) $ ;
    Fropa -> fropa/x $ self.fropa(x) $ ;
    SfcVis1 -> sfcvis1/x $ self.sfcvsby(x,'[mi_i]') $ ;
    TwrVis1 -> twrvis1/x $ self.twrvsby(x,'[mi_i]') $ ;
    VVis1 -> vvis1/x $ self.vvis(x,'[mi_i]') $ ;
    SctrVis1 -> sctrvis1/x $ self.sctrvis(x,'[mi_i]') $ ;
    Vis2Loc1 -> vis2loc1/x $ self.vis2loc(x,'[mi_i]') $ ;

    SfcVis2 -> sfcvis2/x $ self.sfcvsby(x,'m') $ ;
    TwrVis2 -> twrvis2/x $ self.twrvsby(x,'m') $ ;
    VVis2 -> vvis2/x $ self.vvis(x,'m') $ ;
    SkyChar -> skychar/x $ self.skychar(x) $ ;
    SctrVis2 -> sctrvis2/x $ self.sctrvis(x,'m') $ ;
    Vis2Loc2 -> vis2loc2/x $ self.vis2loc(x,'m') $ ;

    Ltg -> ltg/x $ self.ltg(x) $ ;
    PcpnHist -> pcpnhist/x $ self.pcpnhist(x) $ ;
    Tstmvmt -> tstmvmt/x $ self.tstmvmt(x) $ ;
    Hail -> hail/x $ self.hail(x) $ ;
    VCig -> vcig/x $ self.vcig(x) $ ;
    Obsc -> obsc/x $ self.obsc(x) $ ;
    VSky -> vsky/x $ self.vsky(x) $ ;
    Cig2Loc -> cig2loc/x $ self.cig2loc(x) $ ;
    Pchgr -> pchgr/x $ self.pressureChgRapidly(x) $ ;
    Slp -> mslp/x $ self.mslp(x) $ ;
    Nospeci -> nospeci/x $ self.nospeci(x) $ ;
    Aurbo -> aurbo/x $ self.aurbo(x) $;
    Contrails -> contrails/x $ self.contrails(x) $;
    Snoincr -> snoincr/x $ self.snoincr(x) $ ;
    Other -> other/x $ self.other(x) $ ;
    Pcpn1h -> pcpn1h/x $ self.pcpn1h(x) $ ;
    Pcpn6h -> pcpn6h/x $ self.pcpn6h(x) $ ;
    Pcpn24h -> pcpn24h/x $ self.pcpn24h(x) $ ;
    Iceacc -> iceacc/x $ self.iceacc(x) $ ;
    Snodpth -> snodpth/x $ self.snodpth(x) $ ;
    Lwe -> lwe/x $ self.lwe(x) $ ;
    Sunshine -> sunshine/x $ self.sunshine(x) $ ;
    TempDec -> tempdec/x $ self.tempdec(x) $ ;
    MaxT6h -> maxt6h/x $ self.maxt6h(x) $ ;
    MinT6h -> mint6h/x $ self.mint6h(x) $ ;
    XtrmeT -> xtrmet/x $ self.xtrmet(x) $ ;
    Ptndcy3h -> ptndcy3h/x $ self.prestendency(x) $ ;
    Ssindc -> ssindc/x $ self.ssindc(x) $ ;
    Maintenance -> maintenance/x $ self.maintenance(x) $ ;
    """
    verbose = 0

    def __init__(self):

        super(FMH1, self).__init__()

        self._tokenInEnglish = {'_tok_1': 'COR', 'type': 'Keyword METAR or SPECI', 'ident': 'ICAO Identifier',
                                'itime': 'issuance time ddHHmmZ', 'auto': 'AUTO', 'wind': 'wind',
                                'wind_vrb': 'variable wind direction', 'vsby1': 'visibility in statute miles',
                                'vsby2': 'visibility in meters', 'rvr': 'runway visual range in feet',
                                'rvr2': 'runway visual range in meters', 'drytstm': 'thunderstorm',
                                'pcp': 'precipitation', 'obv': 'obstruction to vision',
                                'vcnty': 'precipitation in the vicinity', 'noclouds': 'CLR, SKC', 'sky': 'cloud layer',
                                'temps': 'air and dew-point temperature', 'altimeter': 'altimeter'}

        self._CompassDegrees = {'N': (337.5, 022.5), 'NE': (022.5, 067.5), 'E': (067.5, 112.5), 'SE': (112.5, 157.5),
                                'S': (157.5, 202.5), 'SW': (202.5, 247.5), 'W': (247.5, 292.5), 'NW': (292.5, 337.5)}
        #
        # For FMH-1 thunderstorm and lightning location and bearing information
        self._locationParser = LocationParser()
        self._re_VC = re.compile(r'VC\s*(ALQD?S|[NEWS]{1,3}|AND|-|VC|\s)*')
        self._re_DSNT = re.compile(r'DSNT\s*(ALQD?S|[NEWS]{1,3}|AND|-|DSNT|\s)*')
        self._re_sectr = re.compile(r'(\d{0,4})([NEWS]{1,3})')
        self._re_rwy = re.compile(r'R(WY)?\s*(\d\d[LCR]?)')

        self._program_name = 'FMH-1 METAR/SPECI decoder'
        self._version = '3.0'
        self._description = 'To decode, in its entirety, the METAR/SPECI traditional alphanumeric codes as '\
                            'described the FMH-1 publications.'

        self._Logger = logging.getLogger(self._program_name)

    def __call__(self, raw):

        try:
            self._expected = []
            return super(FMH1, self).__call__(raw)

        except tpg.SyntacticError:

            if len(self._expected):
                err_msg = 'Expecting %s group(s) ' % ' or '.join([self._tokenInEnglish.get(x, x)
                                                                  for x in self._expected])
            else:
                err_msg = 'Unidentified group '

            self._Logger.debug('%s^' % (' ' * self.lexer.max_pos))
            self._Logger.debug(err_msg)

            err_msg += 'after position column %d.' % self.lexer.max_pos
            self._metar['err_msg'] = err_msg
            return self._metar

        except:  # noqa: E722
            self._Logger.critical('%s\n%s' % (self.lexer.input, traceback.format_exc()))
            raise

    def emit(self):

        self._Logger.info('%s v%s. Decoding FMH-1/AF Manual 15-111 METAR/SPECI reports' %
                          (self._program_name, self._version))
        self._Logger.debug(self._description)

    def vrbrvr(self, s):

        result = self.lexer.tokens[self.lexer.cur_token.name][0].match(s)

        oper = None
        uom = 'm'
        if s[-2:] == 'FT':
            uom = '[ft_i]'
        lo = result.group('lo')
        hi = result.group('hi')
        if 'M' in lo:
            oper = 'M'
            lo = lo.replace('M', '')
        elif 'P' in hi:
            oper = 'P'
            hi = hi.replace('P', '')

        try:
            d = self._metar['vrbrvr']
            d['str'].append(s)
            d['index'].append(self.index())
            d['rwy'].append(result.group('rwy'))
            d['lo'].append(lo)
            d['hi'].append(hi)
            d['oper'].append(oper)
            d['uom'].append(uom)
        except KeyError:
            self._metar['vrbrvr'] = {'str': [s], 'index': [self.index()], 'rwy': [result.group('rwy')],
                                     'oper': [oper], 'lo': [lo], 'hi': [hi], 'uom': [uom]}

    def ostype(self, s):

        self._metar['ostype'] = {'str': s, 'index': self.index()}

    def pkwnd(self, s):

        d = self._metar['pkwnd'] = {'str': s, 'index': self.index()}
        wind, hhmm = s.split(' ')[-1].split('/')

        d['dd'] = wind[:3]
        d['ff'] = wind[3:]
        try:
            d['uom'] = self._metar['wind']['uom']
        except KeyError:
            d['uom'] = '[kn_i]'

        tms = list(self._metar['itime']['tuple'])
        if len(hhmm) == 2:
            tms[4] = int(hhmm)
            d['itime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))

        elif len(hhmm) == 4:
            tms[3:5] = int(hhmm[:2]), int(hhmm[2:])
            deu.fix_date(tms)
            if calendar.timegm(tms) > self._metar['itime']['intTime']:
                tms[2] -= 1
                deu.fix_date(tms)
            try:
                d['itime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))
            except ValueError:
                d['itime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(calendar.timegm(tuple(tms))))

    def wshft(self, s):

        d = self._metar['wshft'] = {'str': s, 'index': self.index()}
        tokens = s.split()
        hhmm = tokens[1]
        tms = list(self._metar['itime']['tuple'])
        if len(hhmm) == 2:
            tms[4] = int(hhmm)
            d['itime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))

        elif len(hhmm) == 4:
            tms[3:5] = int(hhmm[:2]), int(hhmm[2:])
            deu.fix_date(tms)
            if calendar.timegm(tms) > self._metar['itime']['intTime']:
                tms[2] -= 1
                deu.fix_date(tms)

            try:
                d['itime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', tuple(tms))
            except ValueError:
                d['itime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(calendar.timegm(tuple(tms))))

    def fropa(self, s):

        try:
            self._metar['wshft']['fropa'] = True
        except KeyError:
            del self._metar['fropa']
            raise tpg.WrongToken

    def sfcvsby(self, s, uom):

        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        vis = 0.0
        if self.lexer.cur_token.name == 'sfcvis1':
            try:
                vis += float(v.group('whole'))
            except (AttributeError, TypeError):
                pass

            try:
                num, den = v.group('fraction').split('/', 1)
                if num[0] == 'M':
                    if num != 'M1':
                        vis += float(num[1:]) / float(den)
                    oper = 'M'
                else:
                    vis += float(num) / float(den)

            except (AttributeError, ValueError, ZeroDivisionError):
                pass

            vis = '%.2f' % vis

        else:
            vis = v.group(1)
            oper = vis[0]
            if oper in ['M', 'P']:
                vis = vis[1:]
        #
        # What is in the prevailing group is tower visibility
        self._metar['twrvsby'] = self._metar['vsby'].copy()
        self._metar['vsby'] = {'str': s,
                               'index': self.index(),
                               'value': vis,
                               'uom': uom}
        try:
            self._metar['vsby']['oper'] = oper
        except NameError:
            pass

    def twrvsby(self, s, uom):

        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        vis = 0.0
        if self.lexer.cur_token.name == 'twrvis1':
            try:
                vis += float(v.group('whole'))
            except (AttributeError, TypeError):
                pass

            try:
                num, den = v.group('fraction').split('/', 1)
                if num[0] == 'M':
                    if num != 'M1':
                        vis += float(num[1:]) / float(den)
                    oper = 'M'
                else:
                    vis += float(num) / float(den)

            except (AttributeError, ValueError, ZeroDivisionError):
                pass
        else:
            vis = v.group(1)
            oper = vis[0]
            if oper in ['M', 'P']:
                vis = vis[1:]
            vis = float(vis)

        d = self._metar['twrvsby'] = {'str': s,
                                      'index': self.index(),
                                      'value': '%.2f' % vis,
                                      'uom': uom}
        try:
            d['oper'] = oper
        except NameError:
            pass

    def vvis(self, s, uom):

        d = self._metar['vvis'] = {'str': s, 'index': self.index(), 'uom': uom}
        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        vis = 0.0
        if self.lexer.cur_token.name == 'vvis1':
            try:
                vis += float(v.group('vintlo'))
            except (AttributeError, TypeError):
                pass

            try:
                num, den = v.group('vfraclo').split('/', 1)
                if num[0] == 'M':
                    if num != 'M1':
                        vis += float(num[1:]) / float(den)
                    d['oper'] = 'M'

                else:
                    vis += float(num) / float(den)

            except (AttributeError, TypeError, ZeroDivisionError):
                pass
        else:
            vis = v.group('vlo')
            oper = vis[0]
            if oper == 'M':
                vis = vis[1:]
            vis = int(vis)

        d['lo'] = vis

        vis = 0.0
        if self.lexer.cur_token.name == 'vvis1':
            try:
                vis += float(v.group('vinthi'))
            except (AttributeError, TypeError):
                pass

            try:
                num, den = v.group('vfrachi').split('/', 1)
                vis += float(num) / float(den)

            except (AttributeError, TypeError, ZeroDivisionError):
                pass
        else:
            vis = v.group('vhi')
            oper = vis[0]
            if oper == 'P':
                vis = vis[1:]
            vis = int(vis)

        d['hi'] = vis
        d['uom'] = uom
        #
        # Bad token processed
        if d['hi'] < d['lo']:
            raise tpg.WrongToken

        if uom == '[mi_i]':
            d['hi'] = '%.2f' % d['hi']
            d['lo'] = '%.2f' % d['lo']
        else:
            d['hi'] = str(d['hi'])
            d['lo'] = str(d['lo'])

    def skychar(self, s):

        self._metar['skychar'] = {'str': s, 'index': self.index()}

    def sctrvis(self, s, uom):

        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        vis = 0.0
        if self.lexer.cur_token.name == 'sctrvis1':
            try:
                vis += float(v.group('whole'))
            except (AttributeError, TypeError):
                pass

            try:
                num, den = v.group('fraction').split('/', 1)
                if num[0] == 'M':
                    vis = float(num[1:]) / float(den)
                    oper = 'M'
                else:
                    vis += float(num) / float(den)

            except (AttributeError, TypeError, ZeroDivisionError):
                pass

            vis = '%.2f' % vis
        else:
            vis = v.group(1)
            oper = vis[0]
            if oper in ['M', 'P']:
                vis = vis[1:]

        compassPt = s.split()[1]
        self._metar['sectorvis'] = {'str': s,
                                    'index': self.index(),
                                    'value': vis,
                                    'direction': deu.CardinalPtsToDegreesS.get(compassPt, '360'),
                                    'uom': uom}
        try:
            self._metar['sectorvis']['oper'] = oper
        except (UnboundLocalError, NameError):
            pass

    def vis2loc(self, s, uom):

        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        vis = 0.0
        oper = None
        if self.lexer.cur_token.name == 'vis2loc1':
            try:
                vis += float(v.group('whole'))
            except (AttributeError, TypeError):
                pass

            try:
                num, den = v.group('fraction').split('/', 1)
                if num[0] == 'M':
                    vis += float(num[1:]) / float(den)
                    oper = 'M'
                else:
                    vis += float(num) / float(den)

            except (AttributeError, TypeError, ZeroDivisionError):
                pass

            vis = '%.2f' % vis

        else:
            vis = v.group(1)
            oper = vis[0]
            if oper in ['M', 'P']:
                vis = vis[1:]

        location = v.group('loc')

        try:
            d = self._metar['secondLocation']
            d['str'] = '%s %s' % (d['str'], s)
            d['index'].append(self.index())
            try:
                d[location].update({'vsby': vis, 'vuom': uom, 'oper': oper})
            except KeyError:
                d[location] = {'vsby': vis, 'vuom': uom, 'oper': oper}

        except KeyError:
            self._metar['secondLocation'] = {location: {'vsby': vis, 'vuom': uom, 'oper': oper},
                                             'index': [self.index()],
                                             'str': s}

    def ltg(self, s):

        if 'lightning' in self._metar:

            self._metar['lightning']['str'] = '%s %s' % (self._metar['lightning']['str'], s)
            try:
                self._metar['lightning']['index'].append(self.index())
            except AttributeError:
                t = self._metar['lightning']['index']
                self._metar['lightning']['index'] = [t, self.index()]
        else:
            self._metar['lightning'] = {'str': s, 'index': self.index(), 'locations': {}}

        lxr = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        bpos = s.find('LTG') + 3
        epos = lxr.end(3)
        ss = s.replace('LTG', '   ')
        frequency = lxr.group(2)
        if frequency is not None:
            ss = ss.replace(frequency, ' ' * len(frequency))
        #
        # Sorted lightning characteristics, if any
        sortedTypes = ''
        if epos > 0:

            ltgtypes = ss[bpos:epos]
            sortedTypes = [ltgtypes[n:n + 2] for n in range(0, len(ltgtypes), 2)]
            sortedTypes.sort()
            ss = ss.replace(ltgtypes, ' ' * len(ltgtypes))
        #
        stypes = ''.join(sortedTypes)
        key = '%s_%s' % (frequency, stypes)
        #
        # Location/Distance are present
        locationString = ss[epos + 1:].strip()
        locations = self.processLocationString(locationString)
        for results in locations.values():
            for sector in results:
                del sector['distance']
                del sector['uom']
        #
        self._metar['lightning']['locations'].setdefault(key, []).append(locations)

    def pcpnhist(self, s):

        try:
            d = self._metar['pcpnhist']
            d['str'] = '%s%s' % (d['str'], s)
            d['index'] = (d['index'][0], self.index()[1])

        except KeyError:
            self._metar['pcpnhist'] = {'str': s, 'index': self.index()}

    def tstmvmt(self, s):

        lxr = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        if lxr.group(5) not in [None, '']:
            #
            # Remove cruft
            movement = self._locationParser(lxr.group(5))[0]
            del movement['distance']
            del movement['uom']
        else:
            movement = None

        bpos, epos = lxr.end(1), lxr.start(3)
        results = self.processLocationString(s[bpos:epos])
        if results == {} and movement is None:
            raise tpg.WrongToken

        if 'tstmvmt' in self._metar:

            self._metar['tstmvmt']['str'] = '%s %s' % (self._metar['tstmvmt']['str'], s)
            try:
                self._metar['tstmvmt']['index'].append(self.index())
            except AttributeError:
                t = self._metar['tstmvmt']['index']
                self._metar['tstmvmt']['index'] = [t, self.index()]
        else:
            self._metar['tstmvmt'] = {'str': s, 'index': self.index(), 'locations': {}}
        #
        # Some post-processing
        for key, result in results.items():
            #
            # Fix up 'distance'
            for sectorDictionary in result:
                lRange = sectorDictionary['distance']
                if type(lRange) == list:
                    if len(lRange) == 0:
                        del sectorDictionary['distance']
                        del sectorDictionary['uom']
                    elif len(lRange) == 1:
                        lRange.append(lRange[0])
                    else:
                        while len(lRange) > 2:
                            lRange.pop(1)

        self._metar['tstmvmt']['locations'].setdefault(lxr.group(1), []).append((results, movement))
        return

    def processLocationString(self, locationString):
        #
        locations = {}
        #
        # Parse out language "in the vicinity" (VC); shouldn't be mixed up with "distant" (DSNT)
        vcLocation = self._re_VC.search(locationString)
        if vcLocation:
            bpos, epos = vcLocation.span()
            vcString = locationString[bpos:epos]
            locationString = locationString.replace(vcString, '')
            locations['VC'] = self._locationParser(vcString.replace('VC', ''))

        dsntLocation = self._re_DSNT.search(locationString)
        if dsntLocation:
            bpos, epos = dsntLocation.span()
            dsntString = locationString[bpos:epos]
            locationString = locationString.replace(dsntString, '')
            locations['DSNT'] = self._locationParser(dsntString.replace('DSNT', ''))
        #
        # locationString now has what is left over....
        if locationString.strip():
            locations['ATSTN'] = self._locationParser(locationString)

        return locations

    def hail(self, s):

        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        siz = 0.0
        try:
            siz += float(v.group('whole'))
        except (AttributeError, TypeError):
            pass

        try:
            num, den = v.group('fraction').split('/', 1)
            if num[0] == 'M':
                siz += float(num[1:]) / float(den)
            else:
                siz += float(num) / float(den)

        except (AttributeError, ValueError, ZeroDivisionError):
            pass

        self._metar['hail'] = d = {'str': s, 'value': '%.2f' % siz, 'index': self.index(), 'uom': '[in_i]'}

        if 'LESS' in s:
            d.update({'oper': 'BELOW'})
        elif 'GREATER' in s:
            d.update({'oper': 'ABOVE'})

    def vcig(self, s):

        d = self._metar['vcig'] = {'str': s, 'index': self.index(), 'uom': '[ft_i]'}
        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        d['lo'] = str(int(v.group(1)))
        d['hi'] = str(int(v.group(2)))

    def obsc(self, s):

        pcp, sky = s.split()
        self._metar['obsc'] = {'str': s,
                               'index': self.index(),
                               'pcp': pcp,
                               'sky': sky}

    def vsky(self, s):

        d = self._metar['vsky'] = {'str': s, 'index': self.index(), 'uom': '[ft_i]'}
        v = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)
        try:
            d['hgt'] = str(int(v.group(3)))

        except (TypeError, ValueError):

            vsky_str = ' '.join(self._metar['sky']['str'])
            pos = vsky_str.find(v.group(1))
            try:
                d['hgt'] = str(int(vsky_str[pos + 3:pos + 6]))

            except ValueError:
                try:
                    pos = vsky_str.find(v.group(4))
                    d['hgt'] = str(int(vsky_str[pos + 3:pos + 6]))

                except ValueError:
                    try:
                        d['hgt'] = str(int(vsky_str[-3:]))
                    except ValueError:
                        d['hgt'] = '12000'

        d['cvr1'] = v.group(1)
        d['cvr2'] = v.group(4)

    def cig2loc(self, s):

        c = self.lexer.tokens[self.lexer.cur_token.name][0].search(s)

        location = c.group('loc')
        value = str(int(s.split()[1]) * 100)
        try:
            d = self._metar['secondLocation']
            d['str'] = '%s %s' % (d['str'], s)
            d['index'].append(self.index())
            try:
                d[location].update({'ceilhgt': value, 'cuom': '[ft_i]'})
            except KeyError:
                d[location] = {'ceilhgt': value, 'cuom': '[ft_i]'}

        except KeyError:
            self._metar['secondLocation'] = {location: {'ceilhgt': value, 'cuom': '[ft_i]'},
                                             'index': [self.index()],
                                             'str': s}

    def pressureChgRapidly(self, s):

        self._metar['pchgr'] = {'str': s, 'index': self.index(),
                                'value': {'R': 'RISING', 'F': 'FALLING'}.get(s[-2])}

    def mslp(self, s):

        try:
            p = float(s[3:]) / 10.0

        except ValueError:
            self._metar['mslp'] = {'str': s, 'index': self.index()}
            return

        if p >= 60.0:
            p += 900.0
        else:
            p += 1000.0
        #
        # If record high MSLP is mistaken for low pressure, it usually occurs
        # with extreme cold events. US record SLP: 1078.6 hPa
        try:
            if 960.0 <= p < 980.0 and (float(self._metar['temps']['air']) < -25.0 and int(self._metar['wind']['ff']) < 10):  # noqa: E501
                p += 100.0
            #
            # like-wise extreme low pressure can be mistaken for high pressure. US record SLP 924 hPa
            elif 1060.0 > p > 1020.0 and int(self._metar['wind']['ff']) > 20:
                p -= 100.0

        except KeyError:
            pass

        self._metar['mslp'] = {'str': s, 'index': self.index(), 'uom': 'hPa', 'value': str(p)}

    def nospeci(self, s):

        self._metar['nospeci'] = {'str': s, 'index': self.index()}

    def aurbo(self, s):

        self._metar['aurbo'] = {'str': s, 'index': self.index()}

    def contrails(self, s):

        self._metar['contrails'] = {'str': s, 'index': self.index()}

    def snoincr(self, s):

        d = self._metar['snoincr'] = {'str': s, 'index': self.index(), 'period': '1', 'uom': '[in_i]'}
        try:
            d['value'], d['depth'] = s.split(' ')[1].split('/')
        except ValueError:
            pass

    def other(self, s):

        self._metar['event'] = {'str': s, 'index': self.index()}

    def pcpn1h(self, s):

        try:
            self._metar['pcpn1h'] = {'str': s, 'index': self.index(), 'uom': '[in_i]',
                                     'value': '%.2f' % (float(s[1:]) * 0.01), 'period': '1'}
            if s[1:] == '0000':
                self._metar['pcpn1h']['value'] = '0.01'
                self._metar['pcpn1h']['oper'] = 'M'

        except ValueError:
            if s[1] == '/':
                self._metar['pcpn1h'] = {'str': s, 'index': self.index(), 'uom': 'N/A',
                                         'value': 'unknown'}

    def pcpn6h(self, s):

        try:
            self._metar['pcpnamt'] = {'str': s, 'index': self.index(), 'uom': '[in_i]',
                                      'value': '%.2f' % (float(s[1:]) * 0.01)}
            if s[1:] == '0000':
                self._metar['pcpnamt']['value'] = '0.01'
                self._metar['pcpnamt']['oper'] = 'M'

        except ValueError:
            if s[1] == '/':
                self._metar['pcpnamt'] = {'str': s, 'index': self.index(), 'uom': 'N/A',
                                          'value': 'unknown'}
            else:
                self._metar['pcpnamt'] = {'str': s, 'index': self.index()}

        if self._metar['type']['str'] == 'METAR':

            hm = self._metar['itime']['str'][2:5]
            if hm in ['025', '085', '145', '205']:
                self._metar['pcpnamt']['period'] = '3'
            elif hm in ['055', '115', '175', '235']:
                self._metar['pcpnamt']['period'] = '6'

    def pcpn24h(self, s):

        try:
            self._metar['pcpn24h'] = {'str': s, 'index': self.index(), 'uom': '[in_i]',
                                      'value': '%.2f' % (float(s[1:]) * 0.01),
                                      'period': '24'}
            if s[1:] == '0000':
                self._metar['pcpn24h']['value'] = '0.01'
                self._metar['pcpn24h']['oper'] = 'M'

        except ValueError:
            if s[1] == '/':
                self._metar['pcpn24h'] = {'str': s, 'index': self.index(), 'uom': 'N/A',
                                          'value': 'unknown', 'period': '24'}
            else:
                self._metar['pcpn24h'] = {'str': s, 'index': self.index()}

    def iceacc(self, s):

        try:
            d = self._metar['iceacc%c' % s[1]] = {'str': s, 'index': self.index(), 'uom': '[in_i]',
                                                  'value': '%.2f' % (float(s[2:]) * 0.01),
                                                  'period': s[1]}
            if s[2:] == '0000':
                d['value'] = '0.01'
                d['oper'] = 'M'

        except ValueError:
            self._metar['iceacc%c' % s[1]] = {'str': s, 'index': self.index()}

    def snodpth(self, s):

        self._metar['snodpth'] = {'str': s, 'index': self.index(), 'uom': '[in_i]',
                                  'period': '1',
                                  'value': s[2:]}

    def lwe(self, s):

        try:
            self._metar['lwe'] = {'str': s, 'index': self.index(), 'uom': '[in_i]',
                                  'value': '%.1f' % (float(s[3:]) * 0.1)}
        except ValueError:
            self._metar['lwe'] = {'str': s, 'index': self.index(), 'uom': '[in_i]',
                                  'value': s[3:]}

    def sunshine(self, s):

        self._metar['ssmins'] = {'str': s, 'index': self.index(),
                                 'value': s[2:]}

    def tempdec(self, s):

        d = self._metar['tempdec'] = {'str': s, 'index': self.index()}
        tt = float(s[2:5]) / 10.0
        if s[1] == '1':
            tt = -tt

        td = float(s[6:9]) / 10.0
        if s[5] == '1':
            td = -td

        d.update({'air': str(tt), 'dewpoint': str(td)})
        try:
            self._metar['temps']['air'] = str(tt)
            self._metar['temps']['dewpoint'] = str(td)

        except KeyError:
            self._metar['temps'] = d

    def maxt6h(self, s):

        factor = 1.0
        if s[1] == '1':
            factor = -1.0
        if 'maxmin6h' not in self._metar:
            d = self._metar['maxmin6h'] = {'str': s, 'index': self.index(), 'period': '6'}
        else:
            d = self._metar['maxmin6h']
            d['str'] += ' %s' % s
            d['index'] = (d['index'][0], self.index()[1])

        try:
            d['max'] = '%.1f' % (0.1 * float(s[2:]) * factor)
        except ValueError:
            d['max'] = s[2:]

    def mint6h(self, s):

        factor = 1.0
        if s[1] == '1':
            factor = -1.0
        if 'maxmin6h' not in self._metar:
            d = self._metar['maxmin6h'] = {'str': s, 'index': self.index(), 'period': '6'}
        else:
            d = self._metar['maxmin6h']
            d['str'] += ' %s' % s
            d['index'] = (d['index'][0], self.index()[1])

        try:
            d['min'] = '%.1f' % (0.1 * float(s[2:]) * factor)
        except ValueError:
            d['min'] = s[2:]

    def xtrmet(self, s):

        maxfactor = 1.0
        if s[1] == '1':
            maxfactor = -1.0
        minfactor = 1.0
        if s[5] == '1':
            minfactor = -1.0

        d = self._metar['maxmin24h'] = {'str': s, 'index': self.index(), 'period': '24'}
        try:
            d['max'] = '%.1f' % (0.1 * float(s[2:5]) * maxfactor)
        except ValueError:
            d['max'] = s[2:]

        try:
            d['min'] = '%.1f' % (0.1 * float(s[6:]) * minfactor)
        except ValueError:
            d['min'] = s[6:]

    def prestendency(self, s):

        try:
            self._metar['ptndcy'] = {'str': s, 'index': self.index(),
                                     'character': s[1],
                                     'pchg': '%.1f' % (int(s[2:]) * 0.1)}
        except ValueError:
            self._metar['ptndcy'] = {'str': s, 'index': self.index()}

    def ssindc(self, s):

        try:
            d = self._metar['ssistatus']
            d['str'] = '%s %s' % (d['str'], s)
            d['index'] = (d['index'][0], self.index()[1])

        except KeyError:
            d = self._metar['ssistatus'] = {'str': s, 'index': self.index(), 'sensors': {}}
        #
        # Identify failed sensor and possibly location
        location = 'none'
        tokens = s.split(' ')
        sensor = tokens.pop(0)

        if len(tokens) > 0:

            location = ' '.join(tokens)
            result = self._re_rwy.match(s)
            if result is not None:
                location = 'R%s' % result.group(2)

        d['sensors'].setdefault(location, []).append(sensor)

    def maintenance(self, s):

        self._metar['maintenance'] = {'index': self.index()}

    def finish(self):

        Annex3.finish(self)
        return self.unparsed()

    def unparsed(self):

        self.unparsedText = [list(x) for x in self.lexer.input.split('\n')]
        self.unparsedText.insert(0, [])
        #
        # Remove all tokens from input string that were successfully parsed.
        for key in self._metar:
            try:
                if type(self._metar[key]['index']) == tuple:
                    self.whiteOut(self._metar[key]['index'])
                elif type(self._metar[key]['index']) == list:
                    for index in self._metar[key]['index']:
                        self.whiteOut(index)
            except TypeError:
                pass
        #
        # Before the RMK token, if there is one, should be considered an error
        # After the RMK token, it is considered text added by the observer
        #
        remainder = ''.join([''.join(x) for x in self.unparsedText])
        rmk_pos = remainder.find('RMK')

        text = remainder[:rmk_pos].strip()
        if len(text):
            self._metar['unparsed'] = {'str': text}

        if rmk_pos != -1:
            text = remainder[rmk_pos + 3:].strip()
            if len(text):
                self._metar['additive'] = {'str': ' '.join(text.split())}

        return self._metar

    def whiteOut(self, index):
        #
        # Starting, ending line and character positions
        try:
            slpos, scpos = [int(x) for x in index[0].split('.')]
            elpos, ecpos = [int(x) for x in index[1].split('.')]
        except AttributeError:
            slpos, scpos = [int(x) for x in index[0][0].split('.')]
            elpos, ecpos = [int(x) for x in index[-1][1].split('.')]

        if slpos == elpos:
            self.unparsedText[slpos][scpos:ecpos] = ' ' * (ecpos - scpos)
        else:
            self.unparsedText[slpos][scpos:] = ' ' * len(self.unparsedText[slpos][scpos:])
            self.unparsedText[elpos][:ecpos + 1] = ' ' * (ecpos + 1)

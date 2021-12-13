#
# Name: TAFDecoder.py
# Purpose: This program converts TAC TAF messages into python dictionaries
#          for encoding in XML.
#
import time
import traceback

from EDR.provider import tpg

from EDR.provider import xmlUtilities as deu

##############################################################################


class Decoder(tpg.Parser):
    r"""
    set lexer = ContextSensitiveLexer
    set lexer_dotall = True

    separator spaces:    '\s+' ;
    token prefix: 'TAF(\s+(AMD|COR|CC[A-Z]|RTD))?' ;
    token ident: '[A-Z][A-Z0-9]{3}' ;
    token itime: '\d{6}Z' ;
    token nil: 'NIL' ;
    token vtime: '\d{4}/\d{4}|\d{6}' ;
    token cnl: 'CNL' ;
    token ftime: 'FM\d{6}' ;
    token btime: 'BECMG\s+\d{4}/\d{4}' ;
    token ttime: 'TEMPO\s+\d{4}/\d{4}' ;
    token ptime: 'PROB\d{2}\s+\d{4}/\d{4}|PROB\d{2}(\s+\S+)\s+\d{4}/\d{4}' ;
    token wind: '(VRB|\d{3})P?\d{2,3}(GP?\d{2,3})?(KT|MPS)' ;
    token cavok: 'CAVOK' ;
    token vsby: '\d{4}|(((1\s*)?[13]/[24]|\d|P6)SM)' ;
    token pcp: '[+-]?(SH|TS|FZ)?(DZ|RA|SN|SG|IC|PE|GR|GS|UP|PL)+|TS(\s+[+-]?(SH|TS|FZ)?(DZ|RA|SN|SG|IC|PE|GR|GS|UP|PL)+)?' ; # noqa: E501
    token obv: '(MI|PR|BC|DR|BL|FZ)?(BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|\+?(FC|SS|DS)|SN)(\s+(MI|PR|BC|DR|BL|FZ)?(BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|\+?(FC|SS|DS)|SN))*' ; # noqa: E501
    token vcnty: 'VC\w+' ;
    token nsw: 'NSW' ;
    token sky: 'SKC|CLR|NSC|(((FEW|SCT|BKN|[0O]VC|VV)\d{3}(CB|TCU)?)(\s+(FEW|SCT|BKN|[0O]VC|VV)\d{3}(CB|TCU)?)*)' ;
    token llws: 'WS\d{3}/\d{5,6}KT' ;
    token temp: '(T[NX]([M-]?\d{2})\s*/\s*\d{4}Z)' ;
    token amd: 'AMD\s+(NOT\s+|LTD\s+)[^=]+' ;
    token any: '\S+' ;

    START/e -> TAF/e $ e=self.taf() $ ;
    TAF -> Main (BGroup|TGroup|PGroup)? (FGroup|BGroup|TGroup|PGroup)* any* ;
    Main -> Ident ITime? ( Nil | (VTime ( Cnl | OWeather ))) $ self.add_group('FM') $ ;
    FGroup -> FTime OWeather $ self.add_group('FM') $ ;
    BGroup -> BTime OWeather $ self.add_group('BECMG') $ ;
    TGroup -> TTime OWeather $ self.add_group('TEMPO') $ ;
    PGroup -> PTime OWeather $ self.add_group('PROB') $ ;

    OWeather -> (Wind|Cavok|Vsby|Pcp|Obv|Vcnty|Nsw|Sky|Temp|LLWS|Amd)+ ;

    Prefix -> prefix/x $ self.prefix(x) $ ;
    Ident -> ident/x $ self.ident(x) $ ;
    ITime -> itime/x $ self.itime(x) $ ;
    VTime -> vtime/x $ self.vtime(x) $ ;
    FTime -> ftime/x $ self.ftime(x) $ ;
    BTime -> btime/x $ self.ttime(x) $ ;
    TTime -> ttime/x $ self.ttime(x) $ ;
    PTime -> ptime/x $ self.ptime(x) $ ;
    Nil -> nil $ self._nil = True $ ;
    Cnl -> cnl $ self._canceled = True $ ;
    Wind -> wind/x $ self.wind(x) $ ;
    Cavok -> cavok/x $ self.cavok(x) $ ;
    Vsby -> vsby/x $ self.vsby(x) $ ;
    Pcp -> pcp/x $ self.pcp(x) $ ;
    Obv -> obv/x $ self.obv(x) $ ;
    Nsw -> nsw/x $ self.nsw(x) $ ;
    Vcnty -> vcnty/x $ self.vcnty(x) $ ;
    Sky -> sky/x $ self.sky(x) $ ;
    Temp -> temp/x $ self.temp(x) $ ;
    LLWS -> llws/x $ self.llws(x) $ ;
    Amd -> amd/x $ self.amd(x) $ ;
    """

    def __init__(self):

        super(Decoder, self).__init__()

    def __call__(self, taf):

        self._taf = {'group': []}
        self._group = {'cavok': 'false'}
        self._nil = False
        self._canceled = False

        i = taf.find('RMK')
        if i > 0:
            taf = taf[:i]

        self._text = taf.strip().replace('=', '')
        try:
            return super(Decoder, self).__call__(self._text)

        except tpg.SyntacticError:
            print('%s\n%s' % (self._text, traceback.format_exc()))
            return self._taf

        except Exception:
            print('%s\n%s' % (self._text, traceback.format_exc()))
            raise

    def __index(self, pos, token):

        tmp = self.lexer.input[:pos]
        line = tmp.count('\n')
        row = pos - tmp.rfind('\n') - 1
        return ('%d.%d' % (line, row), '%d.%d' % (line, row + len(token)))

    def index(self):

        token = self.lexer.token()
        return self.__index(token.start, token.text)

    def taf(self):
        """Called by the parser at the end of work"""

        try:
            self._taf['itime']
        except KeyError:
            self._taf['itime'] = {'value': self._taf['vtime']['from']}

        if self._nil:
            self._taf['state'] = 'nil'

        elif self._canceled:
            self._taf['state'] = 'canceled'
            self._taf['group'] = []
            self._taf['prevtime'] = self._taf['vtime'].copy()
            self._taf['vtime']['from'] = self._taf['itime']['value']

        else:
            p = self._taf['group'][-1]
            if p['prev']['type'] == 'FM':
                p['prev']['time']['to'] = self._taf['vtime']['to']

        return self._taf

    def tokenOK(self, pos=0):
        """Checks whether token ends with a blank"""
        try:
            return self._text[self.lexer.token().stop + pos] in ['\n', ' ']
        except IndexError:
            return True

    def add_group(self, ctype):

        self._group['type'] = ctype
        if ctype in ['FM', 'BECMG']:
            if ctype == 'FM' and self._taf['group']:
                p = self._taf['group'][-1]
                p['prev']['time']['to'] = self._group['time']['from']

            self._taf['group'].append({'prev': self._group})

        else:
            period = self._taf['group'][-1]
            period.setdefault('ocnl', []).append(self._group)

        self._group = {'cavok': 'false'}

    ###################################################################
    # Element checks
    def prefix(self, s):

        self._taf['type'] = {'str': s, 'index': self.index()}
        try:
            self._taf['bbb'] = s.split()[1]
        except IndexError:
            self._taf['bbb'] = ' '

    def ident(self, s):

        self._taf['ident'] = {'str': s, 'index': self.index()}

    def itime(self, s):

        self._group['type'] = 'FM'
        d = self._taf['itime'] = {'str': s, 'index': self.index()}
        mday, hour, minute = int(s[:2]), int(s[2:4]), int(s[4:6])
        tms = list(time.gmtime())
        tms[2:6] = mday, hour, minute, 0
        deu.fix_date(tms)
        d['value'] = time.mktime(tuple(tms))

    def vtime(self, s):

        d = self._group['time'] = {'str': s, 'index': self.index()}

        tms = list(time.gmtime())
        tms[2:6] = int(s[0:2]), int(s[2:4]), 0, 0
        deu.fix_date(tms)

        if len(s) == 6:
            mday, shour, ehour = int(s[:2]), int(s[2:4]), int(s[4:6])
        else:
            mday, shour, eday, ehour = int(s[:2]), int(
                s[2:4]), int(s[5:7]), int(s[7:9])

        tms[2:6] = mday, shour, 0, 0
        deu.fix_date(tms)
        d['from'] = time.mktime(tuple(tms))

        if len(s) == 6:
            period = ehour - shour
            if period <= 0:
                period += 24
            d['to'] = d['from'] + 3600 * period
        else:
            tms[2:6] = eday, ehour, 0, 0
            deu.fix_date(tms)
            d['to'] = time.mktime(tuple(tms))

        self._taf['vtime'] = self._group['time'].copy()
        try:
            d['from'] = min(self._taf['vtime']['from'],
                            self._taf['itime']['value'])
        except KeyError:
            d['from'] = self._taf['vtime']['from']

    def ftime(self, s):

        d = self._group['time'] = {'str': s, 'index': self.index()}

        mday, hour, minute = int(s[2:4]), int(s[4:6]), int(s[6:8])
        try:
            tms = list(time.gmtime(self._taf['vtime']['from']))
            tms[2:5] = mday, hour, minute

            t = time.mktime(tuple(tms))
            if t <= self._taf['vtime']['from'] - 1800:
                deu.fix_date(tms)
                t = time.mktime(tuple(tms))

            d.update({'from': t, 'to': self._taf['vtime']['to']})

        except KeyError:
            pass

    def ttime(self, s):

        d = self._group['time'] = {'str': s, 'index': self.index()}
        tmp = s.split()[1]
        sday, shour, eday, ehour = int(tmp[:2]), int(tmp[2:4]),\
            int(tmp[5:7]), int(tmp[7:9])

        tms = list(time.gmtime(self._taf['vtime']['from']))
        tms[2:4] = sday, shour
        t = time.mktime(tuple(tms))
        if t < self._taf['vtime']['from']:
            deu.fix_date(tms)

        t = time.mktime(tuple(tms))

        tms[2:4] = eday, ehour
        if eday < sday:
            tms[1] += 1

        d.update({'from': t, 'to': time.mktime(tuple(tms))})

    def ptime(self, s):

        d = self._group['time'] = {'str': s, 'index': self.index()}
        tokens = s.split()
        if len(tokens) == 3:
            #
            # Only PROB%% TEMPO is allowed in ICAO Annex 3
            if tokens[1] != 'TEMPO':
                d['str'] = '%s %s' % (tokens[0], tokens[2])

        tmp = tokens[-1]
        sday, shour, eday, ehour = int(tmp[:2]), int(tmp[2:4]),\
            int(tmp[5:7]), int(tmp[7:9])

        tms = list(time.gmtime(self._taf['vtime']['from']))
        tms[2:4] = sday, shour
        t = time.mktime(tuple(tms))
        if t < self._taf['vtime']['from']:
            deu.fix_date(tms)

        t = time.mktime(tuple(tms))
        tms[2:4] = eday, ehour
        if eday < sday:
            tms[1] += 1

        d.update({'from': t, 'to': time.mktime(tuple(tms))})

    def cavok(self, s):

        self._group['cavok'] = 'true'
        return

    def wind(self, s):

        d = self._group['wind'] = {'str': s, 'index': self.index(), 'uom': 'm/s'}
        uompos = -3

        if s.startswith('VRB'):
            d['dd'] = 'VRB'
        elif s[:3] != '///':
            d['dd'] = s[:3]

        if s.endswith('KT'):
            d['uom'] = '[kn_i]'
            uompos = -2

        tok = s[3:uompos].split('G', 1)
        try:
            d['ff'] = tok[0]
            if d['ff'][0] == 'P':
                d['ffplus'] = True
                d['ff'] = tok[0][1:]
        except ValueError:
            pass

        if len(tok) > 1:
            try:
                d['gg'] = tok[1]
                if d['gg'][0] == 'P':
                    d['ggplus'] = True
                    d['gg'] = tok[1][1:]
            except ValueError:
                pass

    def vsby(self, s):
        #
        # Parser may confuse elevated layers with visibility
        if not self.tokenOK():
            raise tpg.WrongToken
        
        d = self._group['vsby'] = {'str': s, 'index': self.index()}

        if s.endswith('SM'):
            d['uom'] = '[mi_i]'
            d['value'] = {'0': '0', 'M1/4': '0.125', '1/4': '0.25', '1/2': '0.5', '3/4': '0.75', '1': '1.0',
                          '11/4': '2000', '1 1/4': '2000', '11/2': '2400', '1 1/2': '2400', '2': '2.0',
                          '3': '3.0', '4': '4.0', '5': '5.0', '6': '6.0', 'P6': '7.0'}.get(s, '7')
        else:
            d['uom'] = 'm'
            try:
                d['value'] = s
                if d['value'] == '9999':
                    d['value'] = '10000'

            except ValueError:
                pass

    def pcp(self, s):

        if not self.tokenOK():
            raise tpg.WrongToken

        self._group['pcp'] = {'str': s, 'index': self.index()}

    def obv(self, s):

        if not self.tokenOK():
            raise tpg.WrongToken

        self._group['obv'] = {'str': s, 'index': self.index()}

    def nsw(self, s):

        self._group['nsw'] = {'str': s, 'index': self.index()}

    def vcnty(self, s):

        self._group['vcnty'] = {'str': s, 'index': self.index()}

    def sky(self, s):

        self._group['sky'] = {'str': s, 'index': self.index()}

    def temp(self, s):

        try:
            d = self._group['temp']
        except KeyError:
            d = self._group['temp'] = {'str': s, 'index': self.index(), 'uom': 'Cel'}

        temp, tstamp = map(str.strip, s.split('/'))
        sday, shour = int(tstamp[:2]), int(tstamp[2:4])

        tms = list(time.gmtime(self._taf['vtime']['from']))
        tms[2:4] = sday, shour
        t = time.mktime(tuple(tms))
        if t < self._taf['vtime']['from']:
            deu.fix_date(tms)

        temp = temp.replace('M', '-')

        if s[1] == 'X':
            d.update({'max': {'value': temp[2:], 'at': time.mktime(tuple(tms))}})
        else:
            d.update({'min': {'value': temp[2:], 'at': time.mktime(tuple(tms))}})

    def llws(self, s):
        d = self._group['llws'] = {'str': s, 'index': self.index()}
        h = int(s[2:5])
        dd = int(s[6:9])
        ff = int(s[9:-2])
        d.update({'hgt': h, 'dd': dd, 'ff': ff})

    def amd(self, s):
        self._taf['amd'] = {'str': s, 'index': self.index()}

if __name__ == '__main__':
    decoder = Decoder()
    text = """KDCA 121745Z 1218/1324 VRB04KT P6SM FEW040 SCT250
  FM121900 16005KT P6SM -SHRA VCTS BKN040CB
  FM130100 12004KT P6SM BKN040
  FM131100 06004KT 5SM -SHRA VCTS OVC050CB
  FM131500 06004KT P6SM -SHRA SCT025 OVC045"""
    print(decoder(text))

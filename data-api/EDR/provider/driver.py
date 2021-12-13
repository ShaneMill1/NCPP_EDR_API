import tafDecoder as TD
import tafEncoder as TE

decoder = TD.Decoder()
encoder = TE.Encoder()

tac = """KBWI 101507Z 1015/1118 21011G19KT P6SM OVC090 WS020/21050KT
  FM102300 23012KT P6SM BKN050
  FM110300 28009KT P6SM BKN060
  FM110500 31008KT P6SM SCT090
  FM111100 34005KT P6SM FEW250"""

decoded = decoder(tac)
decoded['bbb'] = ' '
xml = encoder(decoded, tac)
print(xml)

from eccodes import GribFile, GribMessage

def get_vdvt(INPUT):
   with GribFile(INPUT) as grib:
      msg=GribMessage(grib)
      validityDate=msg['validityDate']
      validityTime=msg['validityTime']
   return validityDate,validityTime

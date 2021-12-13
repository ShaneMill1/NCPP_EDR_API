template = """"{pName}": {
      "type" : "Parameter",
      "description": {
      	"en": "{pDescription}"
      },
      "unit" : {
        "label": {
          "en": "{unitLabel}"
        },
        "symbol": {
          "value": "1",
          "type": "http://www.opengis.net/def/uom/UCUM/"
        }
      },
      "observedProperty" : {
        "id" : "{uri}",
        "label" : {
          "en": "{property_description}"
        }
      }
    }"""



def getDef(pName):
    result = {
        "type": "Parameter",
        "description": {
            "en": pName
        },
        "unit": {
            "label": {
                "en": "u"
            },
            "symbol": {
                "value": "1",
                "type": "u"
            }
        },
        "observedProperty": {
            "id": "http://codes.wmo.int/",
            "label": {
                "en": "No match found"
            }
        }
    }

    return result

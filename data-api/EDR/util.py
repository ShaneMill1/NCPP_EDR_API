# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2018 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import copy
from osgeo import osr

WGS84 = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"


LOGGER = logging.getLogger(__name__)

headTemplate = """<head>
    <style>
        #header {
            background: #2a2a2a;
            color: #fff;
        }
    table {
      font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
      border-collapse: collapse;
      width: 100%;
    }

    table td,
    table th {
      border: 1px solid #ddd;
      padding: 8px;
    }

    table tr:nth-child(4n+2) {
      background-color: #cfe1f4;
    }

    table tr:hover {
      background-color: #ddd;
    }

    table th {
      padding-top: 12px;
      padding-bottom: 12px;
      text-align: left;
      background-color: #4CAF50;
    }

        #headerContent {

            position: relative;
            padding: 2px 5px 0;

        }

        .mainLogo {
            background: url(/) no-repeat -1.4em .1em;
                background-size: 7em 5em;
            font-size: 2.2em;
            height: 69px;
            width: 150px;
            display: block;
            overflow: hidden;
            text-indent: 101%;
            white-space: nowrap
        }
    </style>
</head><body>
<div id="header">
    <div class="contentWidth">
        <div id="headerContent">
            <div id="toplogo"><a href="/" title="Home" class="logo" accesskey="1">
                    <span class="mainLogo"></span></a></div>
        </div>
    </div>
</div>
<div id="content">"""

def style_html(in_html):
    start = 0
    output = copy.deepcopy(in_html)
 #   replaced = []
    while (start > -1) and (start < len(output)):
        start = output.find("http",start)
        if (start > -1):
            end = output.find("</td>",start)
            url = output[start:end]
            if url.find("</a>") == -1: 
                link_url = '<a href=\"'+url+'\">'+url+'</a>'

                output = output.replace(url,link_url)
#                replaced.append(url)
            start = output.find("</tr>",end)

    output = "<html>" + headTemplate + output + "</div></body></html>"
    return output



def get_url(scheme, host, port, basepath):
    """
    Provides URL of instance

    :returns: string of complete baseurl
    """

    url = '{}://{}'.format(scheme, host)

    if port not in [80, 443]:
        url = '{}:{}'.format(url, port)

    url = '{}{}'.format(url, basepath)

    return url

def isFloat(in_str):
    try:
        float(in_str)
        return True
    except ValueError:                   
        return False

def isInteger(in_str):
    try:
        int(in_str)
        return True
    except ValueError:                    
        return False

def wkt2proj(wkt):
    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt)
    return srs.ExportToProj4()    
    

def proj2wkt(proj4):
    srs = osr.SpatialReference()
    srs.ImportFromProj4(proj4)
    return srs.ExportToWkt() 

def horizontaldef(names,coords,values):
    result = {}
    result["name"] = names
    result["coordinates"] = coords
    result["geographic"] = "BBOX[" + ",".join(map(str, values)) + "]"
    return result

def coorddef(names,coords,values):
    result = {}
    result["name"] = names
    result["coordinates"] = coords
    result["range"] = values
    return result

def geographictoextent(geographic):
    values = geographic.replace("BBOX[","").replace("]","").split(",")
    return values

def createquerylinks(href,rel,qtype,title):
    qlink = {}
    qlink["href"] = href
    qlink["rel"] = rel
    qlink["type"] = qtype
    qlink["title"] = title
    return qlink


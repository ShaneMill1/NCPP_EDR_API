<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:atom="http://www.w3.org/2005/Atom"
  xmlns:cap="urn:oasis:names:tc:emergency:cap:1.1"
  xmlns:ha="http://www.alerting.net/namespace/index_1.0" version="1.0">

  <xsl:import href="dst_check.xsl"/>
  
  <xsl:output method="html"
   doctype-system="http://www.w3.org/TR/html4/strict.dtd" 
   doctype-public="-//W3C//DTD HTML 4.01//EN" indent="yes" />


  <!-- Feed header -->
  <xsl:template match="atom:feed">

  <xsl:variable name="state">
   <xsl:call-template name="globalReplace">
    <xsl:with-param name="outputString" select="substring(normalize-space(./atom:title/text()),46)"/>
    <xsl:with-param name="target" select="'Issued by the National Weather Service'"/>
    <xsl:with-param name="replacement" select="''"/>
   </xsl:call-template>
  </xsl:variable>

  <xsl:variable name="lcletters">abcdefghijklmnopqrstuvwxyz</xsl:variable>
  <xsl:variable name="ucletters">ABCDEFGHIJKLMNOPQRSTUVWXYZ</xsl:variable>

  <xsl:variable name="lcst">
   <xsl:choose>
    <xsl:when test="contains(atom:id/text(),'atom')">
     <xsl:value-of select="translate(substring-after(substring-before(./atom:id/text(),'.atom'),'cap/'),$ucletters,$lcletters)"/>
    </xsl:when>
    <xsl:otherwise>
     <xsl:value-of select="translate(substring(normalize-space(substring-after(./atom:id/text(),'x=')),0,3),$ucletters,$lcletters)"/>
    </xsl:otherwise>
   </xsl:choose>
  </xsl:variable>

  <html>
    <head>
     <title>
      <xsl:value-of select="./atom:title/text()"/>
     </title>
     <link rel="stylesheet" type="text/css" href="../css/main.css" />
     <link rel="alternate" type="application/rss+xml" title="ATOM" href="{atom:id}" /> 
     </head>
    <style type="text/css">
      body { background: #FFF url(../images/background1.gif); }
      .label { font-weight: bold; vertical-align: text-top; text-align: right;}
      .xsllocation { font-size: 18px; color: white; font-weight: bold; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
      .entry { background-color: #ffffff; margin-top: 5px; border: 1px solid black; }
      .headline {  font-weight: bold; }
      .stateheadline { text-align: center; font-weight: bold; font-size: 1.5em; }
      .detail,.light,.area { font-weight: normal; }
    </style>
    <body>
    <!-- start banner -->
    <div style="height: 120px; padding: 0; margin: 0;">
    <!-- first div is the top line that contains the skip graphic -->
     <div style="position: relative; top: 0; left: 0; height: 19px; width: 100%; background-image: url(../images/topbanner.jpg); background-repeat: no-repeat;">
      <div style="float: right; border: 0"><a href="#contents"><img src="../images/skipgraphic.gif" alt="Skip Navigation Links" width="1" height="1" border="0" /></a> <a href="/"><span class="nwslink">weather.gov</span></a>&#160;</div>
     </div>

    <!-- second div is the main part of the banner with the noaa and nws logos as well as the page name whether it be a WFO or national page -->
     <div style="clear: right; position: relative; top: -1px; left: 0; height: 78px; width: 100%; background-image: url(../images/wfo_bkgrnd.jpg); background-repeat: repeat;">
      <div style="float: right; width: 85px; height: 78px;"><a href="https://www.weather.gov"><img src="../images/nwsright.jpg" alt="NWS logo-Select to go to the NWS homepage" width="85" height="78" border="0" /></a></div>
      <div style="position: absolute; padding: 0; margin: 0; border: 0;"><a href="https://www.noaa.gov"><img src="../images/noaaleft.jpg" alt="NOAA logo-Select to go to the NOAA homepage" width="85" height="78" border="0" /></a></div>
      <div style="position: absolute; padding: 0; margin: 0 0 0 85px; background-image: url(../images/blank_title.jpg); width: 500px; height: 20px; border: 0; text-align: center;"><div class="source">Watches, Warnings or Advisories for</div></div>
      <div style="position: absolute; padding: 0; margin: 20px 0 0 85px; background-image: url(../images/blank_name.jpg); width: 500px; height: 58px; border: 0; text-align: center;"><div class="location" style="font-size:14px;"><xsl:copy-of select="$state"/></div></div>
     </div>

    <!-- third div is the horizontal navigation that contains a link to the Site Map, News, Organization and search box -->
     <div style="clear: right; position: relative; top: -1px; left: 0; height: 23px; width: 100%; background-image: url(../images/navbkgrnd.gif); background-repeat: repeat;">
      <div style="position: absolute; margin: 0 24px 1px 150px; width: 75%; white-space: nowrap;">
       <ul style="padding: 0; margin: 0 auto;" id="menuitem">
        <li style="display: inline; list-style-type: none; padding-right: 15%;" class="nav"><a href="/sitemap.php">Site Map</a></li>
        <li style="display: inline; list-style-type: none; padding-right: 10%;" class="nav"><a href="/pa/">News</a></li>
        <li style="display: inline; list-style-type: none; padding-right: 15%;" class="nav"><a href="/organization.php">Organization</a></li>
        <li style="display: inline; list-style-type: none;" class="nav">
         <form method="get" action="https://search.usa.gov/search" style="display: inline; white-space: nowrap;">
           <input type="hidden" name="v:project" value="firstgov" />
           <label for="query" class="yellow">Search&#160;&#160;</label>
           <input type="text" name="query" id="query" size="10"/>
           <input type="radio" name="affiliate" checked="checked" value="nws.noaa.gov" id="nws" />
           <label for="nws" class="yellow">NWS</label>
           <input type="radio" name="affiliate" value="noaa.gov" id="noaa" />
           <label for="noaa" class="yellow">All NOAA</label>
           <input type="submit" value="Go" />
         </form>
        </li>
       </ul>
      </div>
      <div style="float: right; border: 0; background-image: url(../images/navbarendcap.jpg); background-repeat: no-repeat; width: 24px; height: 23px;"></div>
      <div style="border: 0; background-image: url(../images/navbarleft.jpg); background-repeat: no-repeat; width: 94px; height: 23px;"></div>
     </div>
    </div>
   <!-- end banner -->
      <table width="700" border="0" cellspacing="0">
        <tr>
          <td width="119" valign="top">
            <!-- start leftmenu -->
            <xsl:variable name="leftmenu" select="document('../includes/leftmenu.php')"/>
            <xsl:copy-of select="$leftmenu"/>
            <!-- end leftmenu -->
          </td>
          <td width="525" valign="top">
            <table cellspacing="2" cellpadding="0" border="0">
              <tr valign="top">
                <td>&#160;&#160;&#160;&#160;&#160;&#160;&#160;</td>
                <td width="100%"><p><a name="contents" id="contents"></a><a href="https://www.weather.gov">Home</a> &gt; <a href="/">Alerts</a> &gt; <xsl:copy-of select="$state"/></p>
                  <h2 style="text-align: center;">Watches, Warnings or Advisories for<br /><xsl:copy-of select="$state"/></h2>
		    <p> 
                    This page shows alerts <i>currently</i> in effect for <xsl:copy-of select="$state"/> and is normally updated every two-three minutes. 
                    Please see <a href="../">here for other state and listing by county</a>. 
<!--                    <a border="0">
                     <xsl:attribute name="href">
                      <xsl:value-of select="concat($lcst,'.php?x=0')"/>
                     </xsl:attribute>
                     <img src='../images/xml.gif' alt='XML' border='0' align='right'/></a> -->
                    </p> 
                    <table>
                      <tr>
                       <td colspan="2" class="detail">
                        <xsl:if test="cap:status/text()!='Actual'">
                          <xsl:value-of select="cap:status/text()"/>
                        </xsl:if>
                        <xsl:text>
                        </xsl:text>
                        <xsl:value-of select="cap:msgType/text()"/>
                        <span class="light">Last updated: </span>
                        <xsl:value-of select="substring(normalize-space(atom:updated/text()), 12, 5)"/>
                        <xsl:call-template name="timeZoneName">
                          <xsl:with-param name="offset" select="substring(normalize-space(atom:updated/text()), 20, 6)"/>
                          <xsl:with-param name="year" select="substring(normalize-space(atom:updated/text()), 1, 4)"/>
                          <xsl:with-param name="month" select="substring(normalize-space(atom:updated/text()), 6, 2)"/>
                          <xsl:with-param name="day" select="substring(normalize-space(atom:updated/text()), 9, 2)"/>
                          <xsl:with-param name="st" select="$lcst"/>
                        </xsl:call-template>
                        <xsl:text> on </xsl:text>
                        <xsl:value-of select="substring(normalize-space(atom:updated/text()), 6, 2)"/>-<xsl:value-of select="substring(normalize-space(atom:updated/text()), 9, 2)"/>-<xsl:value-of select="substring(normalize-space(atom:updated/text()), 1, 4)"/>
                       </td>
                      </tr>
                  </table>

                  <table align="center">
                  <!-- Individual Entries -->
                  <!-- if no entry tags, nothing in effect -->
                  <xsl:if test="count(//atom:entry)=0">
                   <br></br><span class="detail">There are no active alerts at this time</span>
                  </xsl:if>
                  <xsl:for-each select="atom:entry">
                   <!--<xsl:sort select="atom:updated" data-type="text"/>-->
                   <tr>
                    <xsl:choose>
                     <xsl:when test="contains(atom:title,'There are no active watches, warnings')">
                     <td><br></br><span class="headline"><xsl:value-of select="atom:title"/></span></td>
                     </xsl:when>
                     <xsl:when test="contains(atom:title,'Current Watches')">
                      <td class="stateheadline"><br/><br/><xsl:value-of select="substring-before(substring-after(atom:title/text(),'Advisories for'),'Issued')"/></td>
                     </xsl:when>
                     <xsl:otherwise>
                     <td>
                     <table width="100%" class="entry">
                      <tr>
                       <td align="left" valign="top" class="headline"> <a border="0">
                        <xsl:attribute name="href">
                         <xsl:apply-templates select="atom:link"/>
                        </xsl:attribute>
                        <xsl:value-of select="substring-before(atom:title/text(),'issued')"/><br />

                        <xsl:choose>
                         <xsl:when test="contains(atom:title,'until')">
                          <xsl:value-of select="concat('Issued: ',substring-after(substring-before(atom:title/text(),'until'),'issued'))"/><br />
                          <xsl:choose>
                           <xsl:when test="contains(atom:title,'further')">
                            <xsl:value-of select="concat('Expiring: until ',substring-after(substring-before(atom:title/text(),'by NWS'),'until'))"/>
                           </xsl:when>
                           <xsl:otherwise>
                            <xsl:value-of select="concat('Expiring: ',substring-after(substring-before(atom:title/text(),'by NWS'),'until'))"/>
                           </xsl:otherwise>
                          </xsl:choose>
                         </xsl:when>
                         <xsl:otherwise>
                          <xsl:value-of select="concat('Issued: ',substring-after(substring-before(atom:title/text(),'by NWS'),'issued'))"/>
                         </xsl:otherwise>
                        </xsl:choose>
                        </a>

                        <xsl:choose>
                         <xsl:when test="contains(atom:title,'http:')">
                          <br />Issued by <a border="0">
                          <xsl:attribute name="href">
                           <xsl:value-of select="concat('http',substring-after(atom:title/text(),'http'))"/>
                          </xsl:attribute>
                          <xsl:value-of select="concat('NWS',substring-before(substring-after(atom:title/text(),'by NWS'),'http'))"/> </a>
                         </xsl:when>
                         <xsl:otherwise>
                          <xsl:if test="contains(atom:title,'Issuing Weather')">
                           <br />Issuing Weather Forecast Office Homepage: 
                           <xsl:value-of select="substring-after(atom:title/text(),'Issuing Weather Forecast Office Homepage:')"/> 
                          </xsl:if>
                         </xsl:otherwise>
                        </xsl:choose> 
                       </td>
                       <td align="right" valign="top" class="detail">
                        <span class="label">Urgency: </span>
                        <xsl:choose>
                         <xsl:when test="contains(cap:urgency/text(),'Immediate')">
                           <span style="font-size: 1.2em; background-color: red;"><xsl:value-of select="cap:urgency/text()"/></span>
                         </xsl:when>
                         <xsl:otherwise>
                           <xsl:value-of select="cap:urgency/text()"/>
                         </xsl:otherwise>
                        </xsl:choose>
                        <br /><span class="label">Status: </span><xsl:value-of select="cap:status/text()"/>
                       </td>
                      </tr>
                      <tr>
                       <td colspan="2" class="area">
                        <xsl:if test="cap:areaDesc[.!='']">
                         <span class="label">Areas affected:</span> 
                        </xsl:if>
                        <xsl:value-of select="cap:areaDesc/text()"/>
                       </td>
                      </tr>
                     </table>
                     </td>
                     </xsl:otherwise>
                    </xsl:choose>
                   </tr>
                  </xsl:for-each>
                  </table>
                 </td>
                </tr>
               </table>
               <xsl:variable name="footer" select="document('../includes/footer.php')"/> 
               <xsl:copy-of select="$footer"/>
           </td>
          </tr>
         </table>
      </body>
    </html>
  </xsl:template>

  <xsl:template match="atom:link">
    <xsl:call-template name="filename">
      <xsl:with-param name="path" select="@href"/>
    </xsl:call-template>
  </xsl:template>

  <xsl:template name="filename">
    <xsl:param name="path" />
    <xsl:choose>
      <xsl:when test="contains($path,'/')">
        <xsl:call-template name="filename">
          <xsl:with-param name="path" select="substring-after($path,'/')" />
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$path" />
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>


  <!-- Time Zone function -->
  <xsl:template name="timeZoneName">
   <xsl:param name="offset"/>
   <xsl:param name="year"/>
   <xsl:param name="month"/>
   <xsl:param name="day"/>
   <xsl:param name="st"/>

   <!-- determine if its DST or not, based on run time -->
   <xsl:variable name="dst_start">
    <xsl:call-template name="day-light-savings-start">
     <xsl:with-param name="year" select="$year"/>
    </xsl:call-template>
   </xsl:variable>

   <xsl:variable name="dst_end">
    <xsl:call-template name="day-light-savings-end">
     <xsl:with-param name="year" select="$year"/>
    </xsl:call-template>
   </xsl:variable>

   <xsl:variable name="tod_jul">
    <xsl:call-template name="date-to-absolute-day">
     <xsl:with-param name="year" select="$year"/>
     <xsl:with-param name="month" select="$month"/>
     <xsl:with-param name="day" select="$day"/>
    </xsl:call-template>
   </xsl:variable>

   <xsl:variable name="dst_flag">
    <xsl:choose>
     <xsl:when test="$dst_start &lt;= $tod_jul and $dst_end &gt;= $tod_jul">dst</xsl:when>
     <xsl:otherwise>nodst</xsl:otherwise>
    </xsl:choose>
   </xsl:variable>

   <xsl:if test="$offset = '+10:00'"> <xsl:text> ChST</xsl:text> </xsl:if>
   <xsl:if test="$offset = '-11:00'"> <xsl:text> SST</xsl:text> </xsl:if>
   <xsl:if test="$offset = '-10:00'"> <xsl:text> HST</xsl:text> </xsl:if>
   <xsl:if test="$offset = '-00:00'"> <xsl:text> GMT</xsl:text> </xsl:if>
   <xsl:if test="$offset = '+00:00'"> <xsl:text> GMT</xsl:text> </xsl:if>

   <xsl:choose>
    <xsl:when test="$dst_flag = 'nodst'">
     <xsl:if test="$offset = '-09:00'"> <xsl:text> AKST</xsl:text> </xsl:if>
     <xsl:if test="$offset = '-08:00'"> <xsl:text> PST</xsl:text> </xsl:if>
     <xsl:if test="$offset = '-07:00'"> <xsl:text> MST</xsl:text> </xsl:if>
     <xsl:if test="$offset = '-06:00'"> <xsl:text> CST</xsl:text> </xsl:if>
     <xsl:if test="$offset = '-05:00'"> <xsl:text> EST</xsl:text> </xsl:if>
     <xsl:if test="$offset = '-04:00'"> <xsl:text> AST</xsl:text> </xsl:if>
    </xsl:when>
    <xsl:otherwise>
     <xsl:choose>
      <xsl:when test="$st = 'az'">
       <xsl:if test="$offset = '-08:00'"> <xsl:text> PST</xsl:text> </xsl:if>
       <xsl:if test="$offset = '-07:00'"> <xsl:text> MST</xsl:text> </xsl:if>
       <xsl:if test="$offset = '-06:00'"> <xsl:text> CST</xsl:text> </xsl:if>
      </xsl:when>
      <xsl:otherwise>
       <xsl:if test="$offset = '-08:00'"> <xsl:text> AKDT</xsl:text> </xsl:if>
       <xsl:if test="$offset = '-07:00'"> <xsl:text> PDT</xsl:text> </xsl:if>
       <xsl:if test="$offset = '-06:00'"> <xsl:text> MDT</xsl:text> </xsl:if>
       <xsl:if test="$offset = '-05:00'"> <xsl:text> CDT</xsl:text> </xsl:if>
       <xsl:if test="$offset = '-04:00'"> <xsl:text> EDT</xsl:text> </xsl:if>
       <xsl:if test="$offset = '-03:00'"> <xsl:text> ADT</xsl:text> </xsl:if>
      </xsl:otherwise>
     </xsl:choose>
    </xsl:otherwise>
   </xsl:choose>

  </xsl:template>

  <xsl:template name="globalReplace">
   <xsl:param name="outputString"/>
   <xsl:param name="target"/>
   <xsl:param name="replacement"/>
   <xsl:choose>
     <xsl:when test="contains($outputString,$target)">
    
       <xsl:value-of select=
         "concat(substring-before($outputString,$target),
                $replacement)"/>
       <xsl:call-template name="globalReplace">
         <xsl:with-param name="outputString" 
              select="substring-after($outputString,$target)"/>
         <xsl:with-param name="target" select="$target"/>
         <xsl:with-param name="replacement" 
              select="$replacement"/>
       </xsl:call-template>
     </xsl:when>
     <xsl:otherwise>
       <xsl:value-of select="$outputString"/>
     </xsl:otherwise>
   </xsl:choose>
  </xsl:template>

  <!-- Replace new lines with html <br> tags -->
  <xsl:template name="br-replace">
    <xsl:param name="text"/>
    <xsl:variable name="cr" select="'&#xa;'"/>
    <xsl:choose>
      <!-- If the value of the $text parameter contains carriage ret -->
      <xsl:when test="contains($text,$cr)">
        <!-- Return the substring of $text before the carriage return -->
        <xsl:value-of select="substring-before($text,$cr)"/>
        <!-- And construct a <br/> element -->
        <br/>
        <!--
         | Then invoke this same br-replace template again, passing the
         | substring *after* the carriage return as the new "$text" to
         | consider for replacement
         +-->
        <xsl:call-template name="br-replace">
          <xsl:with-param name="text" select="substring-after($text,$cr)"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$text"/>
      </xsl:otherwise>
   </xsl:choose>
  </xsl:template>

  <xsl:template match="atom:title">

   <xsl:variable name="state">
    <xsl:call-template name="globalReplace">
     <xsl:with-param name="outputString" select="substring(.,47)"/>
     <xsl:with-param name="target" select="'Issued by Weather Forecast Office'"/>
     <xsl:with-param name="replacement" select="''"/>
    </xsl:call-template>
   </xsl:variable>
 
   <xsl:variable name="firstchar" select="substring(.,1,1)"/>

   <xsl:choose>
    <xsl:when test = "$firstchar = '&#xa;'">
     <xsl:choose>
      <xsl:when test="contains(.,'Issued by')">
       <xsl:variable name="group" select="substring(.,2)"/>
       <xsl:variable name="type" select="substring-before($group,'issued ')"/>
       <xsl:variable name="issuedat" select="substring-before(substring-after($group,'issued '),'until ')"/>
       <xsl:variable name="expireat" select="substring-before(substring-after($group,'until '),'Issued by')"/>

       <xsl:copy-of select="$type"/> 
       <br />Issued At: 
         <xsl:copy-of select="$issuedat"/>
       <br />Expires At: 
         <xsl:copy-of select="$expireat"/>
      </xsl:when>
      <xsl:when test="contains(.,'Current Watches')">
       <xsl:copy-of select="$state"/>
      </xsl:when>
      <xsl:otherwise>
       <xsl:call-template name="br-replace">
        <xsl:with-param name="text" select="substring(.,2)"/>
       </xsl:call-template>
      </xsl:otherwise>
     </xsl:choose>
    </xsl:when>
    <xsl:otherwise>
     <xsl:choose>
      <xsl:when test="contains(.,'Issued by')">
       <xsl:variable name="group" select="."/>
       <xsl:variable name="type" select="substring-before($group,'issued ')"/>
       <xsl:variable name="issuedat" select="substring-before(substring-after($group,'issued '),'until ')"/>
       <xsl:variable name="expireat" select="substring-before(substring-after($group,'until '),'Issued by')"/>


       <xsl:copy-of select="$type"/> 
       <br />Issued At: <xsl:copy-of select="$issuedat"/> 
       <br />Expires At: <xsl:copy-of select="$expireat"/>
       <br />
      </xsl:when>
      <xsl:when test="contains(.,'Current Watches')">
       <xsl:copy-of select="$state"/>
      </xsl:when>
      <xsl:otherwise>
       <xsl:call-template name="br-replace">
        <xsl:with-param name="text" select="."/>
       </xsl:call-template>
      </xsl:otherwise>
     </xsl:choose>
    </xsl:otherwise>
    </xsl:choose>

  </xsl:template>

  <!-- Ignore anything else -->
  <xsl:template match="*"></xsl:template>
  
</xsl:stylesheet>

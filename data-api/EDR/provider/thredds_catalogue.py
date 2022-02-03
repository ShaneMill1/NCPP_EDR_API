from siphon.catalog import TDSCatalog
from siphon.http_util import session_manager
from datetime import datetime, timedelta
import copy
from EDR import util


class ThreddsCatalogueProvider():

    def query_catalogue(self, mp_, server, collection):
        cat = TDSCatalog(
            'http://thredds.ucar.edu/thredds/catalog.xml')
        results = []
        for item in cat.catalog_refs:
            detail = TDSCatalog(cat.catalog_refs[item].href)
            for sub_detail in detail.catalog_refs:
                if sub_detail == 'GFS Quarter Degree Forecast':
                    gfs025 = TDSCatalog(
                        detail.catalog_refs[sub_detail].href)
                    index = -1
                    for ds in gfs025.datasets:
                        index += 1
                        if ds.find('Latest') > -1:

                            best_ds = gfs025.datasets[index]
                            ncss = best_ds.subset()
                            results = self.build_description(
                                server, collection, ncss.metadata.gridsets, ncss.metadata.axes, mp_)

        return results

    def get_parameter_details(self, grid, mp_, extent):

        parameters = {}
        for p in grid:
            parameters[p] = (mp_.set_grib_detail("grib2", "codeflag", "4.2-"+str(grid[p]["attributes"]["Grib2_Parameter"][0])+"-"+str(grid[p]["attributes"]["Grib2_Parameter"][1])+"-"+str(
                grid[p]["attributes"]["Grib2_Parameter"][2]), p, grid[p]["attributes"]["units"]))
            parameters[p]['extent'] = copy.deepcopy(extent)

            if ('coordinates' in parameters[p]['extent']['vertical']):
                del parameters[p]['extent']['vertical']['units']
                del parameters[p]['extent']['vertical']['unit_desc']
                if (parameters[p]['extent']['vertical']['name'][0] == ''):
                    del parameters[p]['extent']['vertical']
        
        return parameters

    def get_extent_details(self, gridset, coll, axes):

        extent = {}

        extent["horizontal"] = copy.deepcopy(util.horizontaldef(["longitude", "latutude"], ["x", "y"], [
                                             gridset["projectionBox"]["maxx"], gridset["projectionBox"]["maxy"], gridset["projectionBox"]["minx"],  gridset["projectionBox"]["miny"]]))

        vname = ""
        vert_values = []
        units = ""
        unit_desc = ""
        if len(axes) > 3:
            vname = axes[1]
            vert_values, units, unit_desc = self.get_vert_values(coll[axes[1]]["attributes"])

        extent["vertical"] = copy.deepcopy(
            util.coorddef([vname], ["z"], vert_values))
        extent["vertical"]['units'] = units    
        extent["vertical"]['unit_desc'] = unit_desc    
        time_values = self.get_time_values(coll[axes[0]]["attributes"])
        extent["temporal"] = copy.deepcopy(
            util.coorddef(["time"], ["time"], time_values))

        return extent

    def get_description_header(self, server, collection, col, gridset, coll):

        description = {}

        description["id"] = collection['name'] + "_" + col.replace(" ", "-")
        description["title"] = collection['title'] + " " + col
        description["description"] = collection['description'] + " " + col
        description["extent"] = self.get_extent_details(
            gridset, coll, col.split(" "))
        description["links"] = []
        description["links"].append(util.createquerylinks(server + '/collections/' + collection['name'] + "_" + col.replace(
            " ", "-") + '/latest/position', 'self', 'position', 'Position query for latest ' + collection['name'] + "_" + col.replace(" ", "-")))
        description["links"].append(util.createquerylinks(server + '/collections/' + collection['name'] + "_" + col.replace(
            " ", "-") + '/latest/position_query_selector', 'self', 'position_query_selector', 'Position query for latest ' + collection['name'] + "_" + col.replace(" ", "-")))
        description["links"].append(util.createquerylinks(server + '/collections/' + collection['name'] + "_" + col.replace(
            " ", "-") + '/latest/area', 'self', 'area', 'Area query for latest ' + collection['name'] + "_" + col.replace(" ", "-")))
        description["links"].append(util.createquerylinks(server + '/collections/' + collection['name'] + "_" + col.replace(
            " ", "-") + '/latest/area_query_selector', 'self', 'area_query_selector', 'Area query for latest ' + collection['name'] + "_" + col.replace(" ", "-")))

        return description

    def get_time_values(self, attributes):

        time_values = []
        time_def = attributes[0]["units"].split(
            " ")
        init_time = datetime.strptime(
            time_def[2], '%Y-%m-%dT%H:%M:%SZ')

        for ta in attributes:
            if "values" in ta:
                for t in ta["values"]:
                    t = float(t)
                    if time_def[0].find("Hour") > -1:
                        time_values.append(
                            (init_time + timedelta(hours=t)).isoformat() + 'Z')
                    elif time_def[0].find("Day") > -1:
                        time_values.append(
                            (init_time + timedelta(days=t)).isoformat() + 'Z')
                    elif time_def[0].find("Minute") > -1:
                        time_values.append(
                            (init_time + timedelta(minutes=t)).isoformat() + 'Z')
                    elif time_def[0].find("Second") > -1:
                        time_values.append(
                            (init_time + timedelta(seconds=t)).isoformat() + 'Z')
        return time_values

    def get_vert_values(self, attributes):

        vert_values = []
        units = ""
        unit_desc = ""
        grib_level_type = -1
        multi = 1
        for ai in attributes:
            if "positive" in ai:
                if ai["positive"] == "down":
                    multi = -1
            elif "values" in ai:
                if units == "m":
                    vert_values = []
                    for val in ai["values"]: 
                        vert_values.append(round(float(copy.deepcopy(val)) * multi,3))  
                else:
                    vert_values = copy.deepcopy(ai["values"])
            elif "units" in ai:
                units = copy.deepcopy(ai["units"])
            elif "long_name" in ai:
                unit_desc = copy.deepcopy(ai["long_name"])
            elif "Grib_level_type" in ai:
                grib_level_type = copy.deepcopy(ai["Grib_level_type"][0])


        return vert_values, units, unit_desc

    def build_description(self, server, collection, gridsets, axes, mp_):
        results = []
        for col in gridsets:

            description = self.get_description_header(
                server, collection, col, gridsets[col], axes)

            description['parameters'] = self.get_parameter_details(
                gridsets[col]["grid"], mp_,description['extent'])

            description["crs"] = [
                {"id": "EPSG:4326", "wkt": util.proj2wkt(util.WGS84)}]
            description["polygon-query-options"] = {}
            description["polygon-query-options"]["interpolation-x"] = ["nearest_neighbour"]
            description["polygon-query-options"]["interpolation-y"] = ["nearest_neighbour"]
            description["point-query-options"] = {}
            description["point-query-options"]["interpolation"] = ["nearest_neighbour"]            
            description["f"] = ["CoverageJSON"]
            results.append(description)
        return results

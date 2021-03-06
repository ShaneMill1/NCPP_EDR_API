import sqlite3
import logging
import os
import json
from EDR.provider.base import BaseProvider, ProviderConnectionError
from EDR.provider import InvalidProviderError

LOGGER = logging.getLogger(__name__)


class SQLiteProvider(BaseProvider):
    """Generic provider for SQLITE using sqlite3 module.
    This module requires install of libsqlite3-mod-spatialite
    TODO: DELETE, UPDATE, CREATE
    """

    def __init__(self, provider_def):
        """
        SQLiteProvider Class constructor

        :param provider_def: provider definitions from yml config.
                             data,id_field, name set in parent class

        :returns: EDR.providers.base.SQLiteProvider
        """
        BaseProvider.__init__(self, provider_def)

        self.table = provider_def['table']

        self.dataDB = None

        LOGGER.debug('Setting Sqlite propreties:')
        LOGGER.debug('Data source:{}'.format(self.data))
        LOGGER.debug('Name:{}'.format(self.name))
        LOGGER.debug('ID_field:{}'.format(self.id_field))
        LOGGER.debug('Table:{}'.format(self.table))

    def __response_feature_collection(self):
        """Assembles GeoJSON output from DB query

        :returns: GeoJSON FeaturesCollection
        """

        feature_list = list()
        for row_data in self.dataDB:
            row_data = dict(row_data)  # sqlite3.Row is doesnt support pop
            feature = {
                'type': 'Feature'
            }
            feature["geometry"] = json.loads(
                row_data.pop('AsGeoJSON(geometry)')
                )
            feature['properties'] = row_data
            feature['id'] = feature['properties'].pop(self.id_field)
            feature_list.append(feature)

        feature_collection = {
            'type': 'FeatureCollection',
            'features': feature_list
        }

        return feature_collection

    def __response_feature_hits(self, hits):
        """Assembles GeoJSON/Feature number

        :returns: GeoJSON FeaturesCollection
        """

        feature_collection = {"features": [],
                              "type": "FeatureCollection"}
        feature_collection['numberMatched'] = hits

        return feature_collection

    def __load(self):
        """
        Private method for loading spatiallite,
        get the table structure and dump geometry

        :returns: sqlite3.Cursor
        """

        if (os.path.exists(self.data)):
            conn = sqlite3.connect(self.data)
        else:
            raise InvalidProviderError

        try:
            conn.enable_load_extension(True)
        except AttributeError as err:
            LOGGER.error('Extension loading not enabled: {}'.format(err))
            raise ProviderConnectionError()

        conn.row_factory = sqlite3.Row
        conn.enable_load_extension(True)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT load_extension('mod_spatialite.so')")
            cursor.execute("PRAGMA table_info({})".format(self.table))
        except sqlite3.OperationalError as err:
            LOGGER.error('Extension loading error: {}'.format(err))
            raise ProviderConnectionError()
        result = cursor.fetchall()
        try:
            # TODO: Better exceptions declaring
            # InvalidProviderError as Parent class
            assert len(result), "Table not found"
            assert len([item for item in result
                        if item['pk'] == 1]), "Primary key not found"
            assert len([item for item in result
                        if self.id_field in item]), "id_field not present"
            assert len([item for item in result
                        if 'GEOMETRY' in item]), "GEOMETRY column not found"

        except InvalidProviderError:
            raise

        self.columns = [item[1] for item in result if item[1] != 'GEOMETRY']
        self.columns = ",".join(self.columns)+",AsGeoJSON(geometry)"

        return cursor

    def query(self, startindex=0, limit=10, resulttype='results',
              bbox=[], time=None, properties=[], sortby=[]):
        """
        Query Sqlite for all the content.
        e,g: http://localhost:5000/collections/countries/items?
        limit=1&resulttype=results

        :param startindex: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param time: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)

        :returns: GeoJSON FeaturesCollection
        """
        LOGGER.debug('Querying Sqlite')

        cursor = self.__load()

        LOGGER.debug('Got cursor from DB')

        if resulttype == 'hits':
            res = cursor.execute("select count(*) as hits from {};".format(
                self.table))

            hits = res.fetchone()["hits"]
            return self.__response_feature_hits(hits)

        end_index = startindex + limit
        # Not working
        # http://localhost:5000/collections/countries/items/?startindex=10
        sql_query = "select {} from {} where rowid >= ? \
        and rowid <= ?;".format(self.columns, self.table)

        LOGGER.debug('SQL Query:{}'.format(sql_query))
        LOGGER.debug('Start Index:{}'.format(startindex))
        LOGGER.debug('End Index{}'.format(end_index))

        self.dataDB = cursor.execute(sql_query, (startindex, end_index, ))

        feature_collection = self.__response_feature_collection()
        return feature_collection

    def get(self, identifier):
        """
        Query the provider for a specific
        feature id e.g: /collections/countries/items/1

        :param identifier: feature id

        :returns: GeoJSON FeaturesCollection
        """

        LOGGER.debug('Get item from Sqlite')

        cursor = self.__load()

        LOGGER.debug('Got cursor from DB')

        sql_query = "select {} from {} where {}==?;".format(self.columns,
                                                            self.table,
                                                            self.id_field)

        LOGGER.debug('SQL Query:{}'.format(sql_query))
        LOGGER.debug('Identifier:{}'.format(identifier))

        self.dataDB = cursor.execute(sql_query, (identifier, ))

        feature_collection = self.__response_feature_collection()
        return feature_collection

    def __repr__(self):
        return '<SQLiteProvider> {},{}'.format(self.data, self.table)

import collections
from flask import Flask
from flask import make_response
from flask import request, Response
from flask import render_template, send_from_directory, redirect, url_for
import glob
from os import path, sep
import os
from EDR.search_engine.ogc_csw.pycsw.server import Csw
from six.moves.configparser import SafeConfigParser
import yaml
import click
from EDR.provider.base import ProviderQueryError
from EDR.api import API
from EDR.provider.metadata import MetadataProvider
from EDR.formatters.format_output import FormatOutput
from EDR.util import style_html
from flask_cors import CORS
from jinja2 import Environment, FileSystemLoader
from dask.distributed import Client, LocalCluster


app = Flask(__name__, static_url_path='/static')
CORS(app)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 300


#This is where we turn on/off dask cluster
#cluster is local, docker is set up in start.sh so only need to connect client to 0.0.0.0:5500
cluster = LocalCluster(dashboard_address=':5610',scheduler_port=5600)
client = Client(cluster)
#client = Client('0.0.0.0:5500')

with open(os.environ.get('EDR_CONFIG')) as fh:
   CONFIG = yaml.safe_load(fh)
base_url=CONFIG['server']['url']

api_ = API(CONFIG)
registry_ = MetadataProvider(CONFIG)

TEMPLATES = '{}{}templates'.format(os.path.dirname(
    os.path.realpath(__file__)), os.sep)

@app.route('/')
def root():
    headers, status_code, content = api_.root(
        request.headers, request.args)
    response = make_response(content, status_code)

    if headers:
        response.headers = headers
    return response

@app.route('/schemas/<path:path>')
def schemas(path):
    return send_from_directory('static/schemas',path)

@app.route('/parameters/<path:path>')
def parameters(path):
    return send_from_directory('static/parameters',path)


@app.route('/layers/<path:path>')
def layers(path):
    return send_from_directory('static/layers',path)


@app.route('/', methods=['POST','GET'])
def search():
    keyword = request.form['text']
    if ' ' in keyword:
       keyword=keyword.replace(' ','+')
    url=base_url+'/csw?\
mode=opensearch\
&service=CSW\
&version=2.0.2\
&request=GetRecords\
&elementsetname=full\
&typenames=csw:Record\
&resulttype=results\
&q='+keyword+'&outputFormat=application/json'
    return redirect(url)



@app.route('/csw')
def csw_wrapper():
    """CSW wrapper"""
    rtconfig='/EDR/search_engine/ogc_csw/default.cfg'
    cswconfig = SafeConfigParser()
    if isinstance(rtconfig, dict):  # dictionary
       for section, options in rtconfig.items():
          cswconfig.add_section(section)
          for k, v in options.items():
             cswconfig.set(section, k, v)
    else:  # configuration file
       import codecs
       with codecs.open(rtconfig, encoding='utf-8') as scp:
          cswconfig.readfp(scp)
    # version: defaults to '3.0.0'
    csw = Csw(cswconfig, request.environ, version='2.0.2')
    # dispatch the request
    http_status_code, response = csw.dispatch_wsgi()
    return response, http_status_code, {'Content-type': csw.contenttype}


@app.route('/groups', strict_slashes=False)
@app.route('/groups/<path:subpath>', strict_slashes=False)
def groups(subpath=None):
    headers, status_code, content = api_.describe_group(
        request.headers, request.args, subpath)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response





@app.route('/collections/<collection>/<identifier>/position')
def get_data(collection,identifier):
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
   
@app.route('/collections/<collection>/<identifier>/position_query_selector')
def get_query_selector(collection,identifier):
        return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/collections/<collection>/<identifier>/area')
def get_polygon_data(collection, identifier):

        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
   
@app.route('/collections/<collection>/<identifier>/area_query_selector')
def get_polygon_query_selector(collection, identifier):
    return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/api', strict_slashes=False)
def api():

    headers, status_code, content = api_.api(request.headers, request.args)
    response = make_response(content, status_code)
    if headers:
        response.headers = headers

    return response

@app.route('/collections/<collection>/instances/<identifier>', strict_slashes=False)
def get_automated_collection_details(collection, identifier):
    collection_type=collection.split('_')
    #try:
    if len(collection_type) > 3:
       headers, status_code, content = api_.describe_automated_collections(request.headers, request.args, collection, identifier)
       response = make_response(content, status_code)
    else:
       headers, status_code, content = api_.describe_collection(request.headers, request.args, collection, identifier)
       response = make_response(content, status_code)
    #except:
    #   headers, status_code, content = api_.describe_collection(request.headers, request.args, collection, identifier)
    #   response = make_response(content, status_code)


    if headers:
        response.headers = headers

    return response

@app.route('/collections/<collection>/<identifier>', strict_slashes=False)
def get_collection_details(collection, identifier,cid=None):

    headers, status_code, content = api_.describe_collection(
        request.headers, request.args, collection, identifier)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers
    return response


@app.route('/collections/<collection>/instances/<identifier>/position', strict_slashes=False)
def get_data_automated(collection,identifier):
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection,identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
   
@app.route('/collections/<collection>/instances/<identifier>/position_query_selector', strict_slashes=False)
def get_data_automated_query_selector(collection,identifier):
        return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/collections/<collection>/instances/<identifier>/area')
def get_polygon_data_automated(collection, identifier):
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
   
@app.route('/collections/<collection>/instances/<identifier>/area_query_selector')
def get_polygon_data_automated_query_selector(collection, identifier):
        return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/collections/<collection>/instances/<identifier>/cube')
def get_cube_data_automated(collection, identifier):

        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
   
@app.route('/collections/<collection>/instances/<identifier>/cube_query_selector')
def get_cube_data_automated_query_selector(collection, identifier):
        return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/collections/<collection>/instances/<identifier>/radius')
def get_radius_data_automated(collection, identifier):
   headers, status_code, content = api_.get_feature(request.headers, request.args, collection, identifier)
   response = make_response(content, status_code)
   if headers:
      response.headers = headers
   return response

@app.route('/collections/<collection>/instances/<identifier>/radius_query_selector')
def get_radius_data_automated_query_selector(collection, identifier):
   return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/collections/<collection>/instances/<identifier>/trajectory')
def get_trajectory_data_automated(collection, identifier):

        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response

   
@app.route('/collections/<collection>/instances/<identifier>/trajectory_query_selector')
def get_trajectory_data_automated_query_selector(collection, identifier):
        return _render_j2_template(CONFIG, "index_p.html", None)

   
@app.route('/collections/<collection>/instances/<identifier>/corridor')
def get_corridor_data_automated(collection, identifier):

        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
   
@app.route('/collections/<collection>/instances/<identifier>/corridor_query_selector')
def get_corridor_data_automated_query_selector(collection, identifier):
        return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/collections/<collection>/instances', strict_slashes=False)
def get_identifer_details(collection):


    headers, status_code, content = api_.list_identifers(
        request.headers, request.args, collection)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@app.route('/collections/<collection>', strict_slashes=False)
def get_identifier_details(collection):
    if collection=='wwa_active':
       headers, status_code, content = api_.describe_collection(request.headers, request.args, collection)
       response = make_response(content, status_code)

    if collection =="tca" or collection=="metar_tgftp" or collection=='wifs_png' or 'wwa_active_' in collection:
       headers, status_code, content = api_.item_label(
           request.headers, request.args, collection)
       response = make_response(content, status_code)
    else:
       if collection=='wwa_active':
          headers, status_code, content = api_.describe_collection(request.headers, request.args, collection)
          response = make_response(content, status_code) 
       else:
          headers, status_code, content = api_.instance_label(request.headers, request.args, collection)
          response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@app.route('/collections/<collection>/items', strict_slashes=False)
def get_items_details(collection):


    headers, status_code, content = api_.list_items(
        request.headers, request.args, collection)

    response = make_response(content, status_code)

    if headers:
        response.headers = headers

    return response


@app.route('/collections/<collection>/items/<item>', strict_slashes=False)
def get_item_details(collection, item):
    headers, status_code, content = api_.describe_item(request.headers, request.args, collection, item)
    response = make_response(content, status_code)
    if headers:
        response.headers = headers

    return response


@app.route('/collections/<collection>/locations/<item>', strict_slashes=False)
def get_location_query(collection, item):
    if item=='capatom.xsl' or item=='dst_check.xsl':
       return send_from_directory('static/ext_schemas',item)
    else:
       headers, status_code, content = api_.get_feature(request.headers, request.args, collection, item)
       response = make_response(content, status_code)
       if headers:
          response.headers = headers

       return response



@app.route('/collections', strict_slashes=False)
def collection():


    headers, status_code, content = api_.describe_collections(
        request.headers, request.environ, request.args)

    response = make_response(content, status_code)
    if headers:
        response.headers = headers
    return response

@app.route('/metadata/', strict_slashes=False)
@app.route('/metadata/<register>', strict_slashes=False)
@app.route('/metadata/<register>/<table>', strict_slashes=False)
@app.route('/metadata/<register>/<table>/<codeid>', strict_slashes=False)
def metadata(register=None, table=None, codeid=None):

    headers, status_code, content = registry_.query(request.headers, request.args,  register, table, codeid, True)
    response = make_response(content, status_code)
    if headers:
        response.headers = headers

    return response

@app.route('/conformance', strict_slashes=False)
def conformance():
    headers, status_code, content = api_.api_conformance(request.headers,
                                                         request.args)

    response = make_response(content, status_code)
    if headers:
        response.headers = headers

    return response


@app.route('/palettes')
def palettes():
    palette_list=list()
    for palette_name in glob.glob('/EDR/static/color_palettes/*.txt'):
       palette_name=palette_name.split('/')[-1]
       palette_list.append(palette_name)
    palettes={'palettes':palette_list}
    return palettes


@app.route('/metrics')
def metrics():
   return redirect('http://localhost:8089', code=301)

@click.command()
@click.pass_context
@click.option('--debug', '-d', default=False, is_flag=True, help='debug')
def serve(ctx, debug=False):
    """Serve weather on the web via Flask"""

    if not api_.config['server']['pretty_print']:
        app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

    if 'cors' in api_.config['server'] and api_.config['server']['cors']:
        from flask_cors import CORS
        CORS(app)

    app.run(debug=debug, host=api_.config['server']['bind']['host'],
            port=api_.config['server']['bind']['port'])

def _render_j2_template(config, template, data):
    """
    render Jinja2 template

    :param config: dict of configuration
    :param template: template (relative path)
    :param data: dict of data

    :returns: string of rendered template
    """

    env = Environment(loader=FileSystemLoader(TEMPLATES))

    template = env.get_template(template)
    return template.render(config=config, data=data, version='0.0.1')



#!/home/MDL/smill/miniconda3/envs/search_engine/bin/python3
import collections
from flask import Flask
from flask import make_response
from flask import request, Response
from flask import render_template, send_from_directory, redirect,url_for
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
from flask import request as rq

real_path=os.getcwd()
app = Flask(__name__, static_url_path='/static', static_folder=real_path+'/EDR/static')
CORS(app)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

real_path=os.getcwd()
WoW_CONFIG=real_path+'/EDR/config/config_local.yml'

with open(WoW_CONFIG) as fh:
    CONFIG = yaml.load(fh)
api_ = API(CONFIG)
registry_ = MetadataProvider(CONFIG)

#TEMPLATES = '{}{}templates'.format(os.path.dirname(
#    os.path.realpath(__file__)), os.sep)

TEMPLATES = real_path+'/EDR/templates'
class AttrDict(dict):
    """ Dictionary subclass whose entries can be accessed by attributes
        (as well as normally).
    """
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    @staticmethod
    def from_nested_dict(data):
        """ Construct nested AttrDicts from nested dictionaries. """
        if not isinstance(data, dict):
            return data
        else:
            return AttrDict({key: AttrDict.from_nested_dict(data[key])
                                for key in data})


@app.route('/')
def root():
    headers, status_code, content = api_.root(
        request.headers, request.args)
  

    response = make_response(content, status_code)
    if headers:
        response.headers = headers

    return response


@app.route('/', methods=['POST','GET'])
def search():
    keyword = request.form['text']
    url='http://localhost:5400/csw?\
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
    rtconfig='EDR/search_engine/ogc_csw/default.cfg'
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

    # initialize pycsw
    # pycsw_config: either a ConfigParser object or a dict of
    # the pycsw configuration
    #
    # env: dict of (HTTP) environment (defaults to os.environ)
    #
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

    try:
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
    except ProviderQueryError:
        return _render_j2_template(CONFIG, "index_p.html", None)

@app.route('/collections/<collection>/<identifier>/area')
def get_polygon_data(collection, identifier):

    try:
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection,identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
    except ProviderQueryError:
        return _render_j2_template(CONFIG, "index_p.html", None)

@app.route('/collections/<collection>/instances/<identifier>/cube')
def get_cube_data_automated(collection, identifier):

    try:
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
    except ProviderQueryError:
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
    if len(collection_type) > 3:   
       headers, status_code, content = api_.describe_automated_collections(request.headers, request.args, collection, identifier)
       response = make_response(content, status_code)
    else:
       headers, status_code, content = api_.describe_collection(request.headers, request.args, collection, identifier)
       response = make_response(content, status_code)

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

    try:
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
    except ProviderQueryError:
        return _render_j2_template(CONFIG, "index_p.html", None)


@app.route('/collections/<collection>/instances/<identifier>/area')
def get_polygon_data_automated(collection, identifier):

    try:
        headers, status_code, content = api_.get_feature(
            request.headers, request.args, collection, identifier)

        response = make_response(content, status_code)

        if headers:
            response.headers = headers

        return response
    except ProviderQueryError:
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


    headers, status_code, content = api_.instance_label(
        request.headers, request.args, collection)

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

@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


if __name__ == '__main__':
   app.run(host='0.0.0.0',port='5400',debug = True)

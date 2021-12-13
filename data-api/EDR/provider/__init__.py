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

import importlib
import logging

LOGGER = logging.getLogger(__name__)

PROVIDERS = {
    'metar': 'EDR.provider.metars.MetarProvider',
    'osmhighways': 'EDR.provider.osm.OSMProvider',    
    'dem': 'EDR.provider.dem.DEMProvider',    
    'ndfd': 'EDR.provider.ndfd.NDFDSurfaceProvider',
    'thredds': 'EDR.provider.thredds.ThreddsProvider',
    'taf': 'EDR.provider.taf.TafProvider',
    'tca': 'EDR.provider.tca.TcaProvider',
    'vaa': 'EDR.provider.vaa.VaaProvider',
    'automated_nam': 'EDR.provider.automated_nam.AutomatedCollectionProvider',
    'automated_arpege': 'EDR.provider.automated_arpege.AutomatedCollectionProvider',
    'automated_gem': 'EDR.provider.automated_gem.AutomatedCollectionProvider',
    'automated_nasa': 'EDR.provider.automated_nasa.AutomatedCollectionProvider',
    'automated_nbm': 'EDR.provider.automated_nbm.AutomatedCollectionProvider',
    'metar_tgftp': 'EDR.provider.metar_tgftp.MetarTgftpProvider',
    'ndfd_xml': 'EDR.provider.ndfd_xml.NDFDXmlProvider',
    'rtma_xml': 'EDR.provider.rtma_xml.RTMAXmlProvider',
    'himawari': 'EDR.provider.himawari.HimawariProvider',
    'goes': 'EDR.provider.goes.GoesProvider',
    'automated_provider': 'EDR.provider.automated_provider.AutomatedCollectionProvider',
    'wifs_png': 'EDR.provider.wifs_png.WIFSPNGProvider',
    's3_zarr': 'EDR.provider.s3_zarr.s3_zarr_Provider',
    'wwa_active': 'EDR.provider.wwa_active.WWAActiveProvider'
}


def load_provider(dataset, config):
    """
    loads provider by name

    :param provider_def: provider definition

    :returns: provider object
    """

    LOGGER.debug('Providers: {}'.format(PROVIDERS))

    if dataset in config['datasets']:
        pname = config['datasets'][dataset]['provider']['name']
    else:
        try:
           pname = config['datasets'][dataset.split("_")[0]+'_'+dataset.split("_")[1]]['provider']['name']
        except: 
           pname = config['datasets'][dataset.split("_")[0]]['provider']['name']

    if '.' not in pname and pname not in PROVIDERS.keys():
        msg = 'Provider {} not found'.format(pname)
        LOGGER.exception(msg)
        raise InvalidProviderError(msg)

    if '.' in pname:  # dotted path
        packagename, classname = pname.rsplit('.', 1)
    else:  # core provider
        packagename, classname = PROVIDERS[pname].rsplit('.', 1)

    LOGGER.debug('package name: {}'.format(packagename))
    LOGGER.debug('class name: {}'.format(classname))

    module = importlib.import_module(packagename)
    class_ = getattr(module, classname)
    provider = class_(dataset, config)
    return provider


class InvalidProviderError(Exception):
    """invalid provider"""
    pass

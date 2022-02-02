# This file is part of the MapProxy project.
# Copyright (C) 2010 Omniscale <http://omniscale.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Demo service handler
"""
from __future__ import division

import os
import pkg_resources
import mimetypes
from collections import defaultdict

from mapproxy.config.config import base_config
from mapproxy.compat import PY2
from mapproxy.exception import RequestError
from mapproxy.service.base import Server
from mapproxy.response import Response
from mapproxy.srs import SRS, get_epsg_num
from mapproxy.layer import SRSConditional, CacheMapLayer, ResolutionConditional
from mapproxy.source.wms import WMSSource

if PY2:
    import urllib2
else:
    from urllib import request as urllib2

from mapproxy.template import template_loader, bunch
env = {'bunch': bunch}
get_template = template_loader(__name__, 'templates', namespace=env)


def static_filename(name):
    if base_config().template_dir:
        return os.path.join(base_config().template_dir, name)
    else:
        return pkg_resources.resource_filename(__name__, os.path.join('templates', name))

class DemoServer(Server):
    names = ('demo',)
    def __init__(self, layers, md, request_parser=None, tile_layers=None,
                 srs=None, image_formats=None, services=None, restful_template=None):
        Server.__init__(self)
        self.layers = layers
        self.tile_layers = tile_layers or {}
        self.md = md
        self.image_formats = image_formats
        filter_image_format = []
        for format in self.image_formats:
            if 'image/jpeg' == format or 'image/png' == format:
                filter_image_format.append(format)
        self.image_formats = filter_image_format
        self.srs = srs
        self.services = services or []
        self.restful_template = restful_template

    def handle(self, req):
        if req.path.startswith('/demo/static/'):
            if '..' in req.path:
                return Response('file not found', content_type='text/plain', status=404)
            filename = req.path.lstrip('/')
            filename = static_filename(filename)
            if not os.path.isfile(filename):
                return Response('file not found', content_type='text/plain', status=404)
            type, encoding = mimetypes.guess_type(filename)
            return Response(open(filename, 'rb'), content_type=type)

        # we don't authorize the static files (css, js)
        # since they are not confidential
        try:
            authorized = self.authorized_demo(req.environ)
        except RequestError as ex:
            return ex.render()
        if not authorized:
            return Response('forbidden', content_type='text/plain', status=403)

        if 'wms_layer' in req.args:
            demo = self._render_wms_template('demo/wms_demo.html', req)
        elif 'tms_layer' in req.args:
            demo = self._render_tms_template('demo/tms_demo.html', req)
        elif 'wmts_layer' in req.args:
            demo = self._render_wmts_template('demo/wmts_demo.html', req)
        elif 'hips_layer' in req.args:
            demo = self._render_hips_template('demo/hips_demo.html', req)
        elif 'wms_capabilities' in req.args:
            internal_url = '%s/service?REQUEST=GetCapabilities'%(req.server_script_url)
            url = internal_url.replace(req.server_script_url, req.script_url)
            capabilities = urllib2.urlopen(internal_url)
            demo = self._render_capabilities_template('demo/capabilities_demo.html', capabilities, 'WMS', url)
        elif 'wmsc_capabilities' in req.args:
            internal_url = '%s/service?REQUEST=GetCapabilities&tiled=true'%(req.server_script_url)
            url = internal_url.replace(req.server_script_url, req.script_url)
            capabilities = urllib2.urlopen(internal_url)
            demo = self._render_capabilities_template('demo/capabilities_demo.html', capabilities, 'WMS-C', url)
        elif 'wmts_capabilities_kvp' in req.args:
            internal_url = '%s/service?REQUEST=GetCapabilities&SERVICE=WMTS' % (req.server_script_url)
            url = internal_url.replace(req.server_script_url, req.script_url)
            capabilities = urllib2.urlopen(internal_url)
            demo = self._render_capabilities_template('demo/capabilities_demo.html', capabilities, 'WMTS', url)
        elif 'wmts_capabilities' in req.args:
            internal_url = '%s/wmts/1.0.0/WMTSCapabilities.xml' % (req.server_script_url)
            url = internal_url.replace(req.server_script_url, req.script_url)
            capabilities = urllib2.urlopen(internal_url)
            demo = self._render_capabilities_template('demo/capabilities_demo.html', capabilities, 'WMTS', url)
        elif 'tms_capabilities' in req.args:
            if 'layer' in req.args and 'srs' in req.args:
                # prevent dir traversal (seems it's not possible with urllib2, but better safe then sorry)
                layer = req.args['layer'].replace('..', '')
                srs = req.args['srs'].replace('..', '')
                internal_url = '%s/tms/1.0.0/%s/%s'%(req.server_script_url, layer, srs)
            else:
                internal_url = '%s/tms/1.0.0/'%(req.server_script_url)
            capabilities = urllib2.urlopen(internal_url)
            url = internal_url.replace(req.server_script_url, req.script_url)
            demo = self._render_capabilities_template('demo/capabilities_demo.html', capabilities, 'TMS', url)
        elif req.path == '/demo/':
            demo = self._render_template(req, 'demo/demo.html')
        else:
            resp = Response('', status=301)
            resp.headers['Location'] = req.script_url.rstrip('/') + '/demo/'
            return resp
        return Response(demo, content_type='text/html')

    def layer_srs(self, layer):
        """
        Return a list tuples with title and name of all SRS for the layer.
        The title of SRS that are native to the layer are suffixed with a '*'.
        """
        cached_srs = []
        for map_layer in layer.map_layers:
            # TODO unify map_layers interface
            if isinstance(map_layer, SRSConditional):
                for srs_key in map_layer.srs_map.keys():
                    cached_srs.append(srs_key.srs_code)
            elif isinstance(map_layer, CacheMapLayer):
                cached_srs.append(map_layer.grid.srs.srs_code)
            elif isinstance(map_layer, ResolutionConditional):
                cached_srs.append(map_layer.srs.srs_code)
            elif isinstance(map_layer, WMSSource):
                if map_layer.supported_srs:
                    for supported_srs in map_layer.supported_srs:
                        cached_srs.append(supported_srs.srs_code)

        uncached_srs = []

        for srs_code in self.srs:
            if srs_code not in cached_srs:
                uncached_srs.append(srs_code)

        sorted_cached_srs = sorted(cached_srs, key=lambda srs: get_epsg_num(srs))
        sorted_uncached_srs = sorted(uncached_srs, key=lambda srs: get_epsg_num(srs))
        sorted_cached_srs = [(s + '*', s) for s in sorted_cached_srs]
        sorted_uncached_srs = [(s, s) for s in sorted_uncached_srs]
        return sorted_cached_srs + sorted_uncached_srs

    def _render_template(self, req, template):
        template = get_template(template, default_inherit="demo/static.html")
        tms_tile_layers = defaultdict(list)
        for layer in self.tile_layers:
            name = self.tile_layers[layer].md.get('name')
            tms_tile_layers[name].append(self.tile_layers[layer])
        wmts_layers = tms_tile_layers.copy()

        hips_layer_names = []
        if 'hips' in self.services:
            for layer_name, layer in self.layers.items():
                enabled = layer.md.get('hips', {}).get('enabled', True)
                if enabled:
                    _, _, _, allsky_available, _ = self._hips_info(req, layer_name)
                    hips_layer_names.append([layer_name, allsky_available])

        return template.substitute(layers=self.layers,
                                   formats=self.image_formats,
                                   srs=self.srs,
                                   layer_srs=self.layer_srs,
                                   tms_layers=tms_tile_layers,
                                   wmts_layers=wmts_layers,
                                   hips_layer_names=hips_layer_names,
                                   services=self.services)

    def _render_wms_template(self, template, req):
        template = get_template(template, default_inherit="demo/static.html")
        layer = self.layers[req.args['wms_layer']]
        srs = escape(req.args['srs'])
        bbox = layer.extent.bbox_for(SRS(srs))
        width = bbox[2] - bbox[0]
        height = bbox[3] - bbox[1]
        min_res = max(width/256, height/256)
        return template.substitute(layer=layer,
                                   image_formats=self.image_formats,
                                   format=escape(req.args['format']),
                                   srs=srs,
                                   layer_srs=self.layer_srs,
                                   bbox=bbox,
                                   res=min_res)

    def _render_tms_template(self, template, req):
        template = get_template(template, default_inherit="demo/static.html")
        for layer in self.tile_layers.values():
            if (layer.name == req.args['tms_layer'] and
                    layer.grid.srs.srs_code == req.args['srs']):
                tile_layer = layer
                break

        resolutions = tile_layer.grid.tile_sets
        res = []
        for level, resolution in resolutions:
            res.append(resolution)

        if tile_layer.grid.srs.is_latlong:
            units = 'degree'
        else:
            units = 'm'

        if tile_layer.grid.profile == 'local':
            add_res_to_options = True
        else:
            add_res_to_options = False
        return template.substitute(layer=tile_layer,
                                   srs=escape(req.args['srs']),
                                   format=escape(req.args['format']),
                                   resolutions=res,
                                   units=units,
                                   add_res_to_options=add_res_to_options,
                                   all_tile_layers=self.tile_layers)

    def _render_wmts_template(self, template, req):
        template = get_template(template, default_inherit="demo/static.html")
        for layer in self.tile_layers.values():
            if (layer.name == req.args['wmts_layer'] and
                    layer.grid.srs.srs_code == req.args['srs']):
                wmts_layer = layer
                break

        restful_url = self.restful_template.replace('{Layer}', wmts_layer.name, 1)
        if '{Format}' in restful_url:
            restful_url = restful_url.replace('{Format}', wmts_layer.format)

        if wmts_layer.grid.srs.is_latlong:
            units = 'degree'
        else:
            units = 'm'
        return template.substitute(layer=wmts_layer,
                                   matrix_set=wmts_layer.grid.name,
                                   format=escape(req.args['format']),
                                   srs=escape(req.args['srs']),
                                   resolutions=wmts_layer.grid.resolutions,
                                   units=units,
                                   all_tile_layers=self.tile_layers,
                                   restful_url=restful_url)

    def _hips_info(self, req, layer_name):
        hips_internal_url = req.server_script_url + '/hips/' + layer_name

        hips_order_max = 5
        hips_tile_format = 'png'
        hips_frame = 'planet'

        # Download the /properties document to get a few metadata we
        # need: hips_order_max, hips_tile_format and hips_frame
        from mapproxy.client.http import HTTPClient, HTTPClientError
        resp = HTTPClient(hips_internal_url).open(hips_internal_url + "/properties")

        from mapproxy.util.hips import parse_properties

        properties = parse_properties(resp.read().decode('utf-8'))
        if 'hips_order' in properties: # Mandatory element
            hips_order_max = int(properties['hips_order'])

        if 'hips_tile_format' in properties: # Mandatory element
            tile_format = None
            value = properties['hips_tile_format']
            for x in value.split(' '):
                if x in ('jpeg', 'png'):
                    tile_format = x
                    break
            if tile_format is None:
                return Exception(f'hips_tile_format = {value} does not contain jpeg or png')
            hips_tile_format = tile_format

        if 'hips_frame' in properties:
            hips_frame = properties['hips_frame']

        # Check if the /Norder3/Allsky.png/.jpg file is already pregenerated.
        try:
            allsky_ext = 'png' if hips_tile_format == 'png' else 'jpg'
            allsky_file = hips_internal_url + "/Norder3/Allsky." + allsky_ext
            HTTPClient(hips_internal_url).open(allsky_file, method='HEAD')
            allsky_available = True
        except HTTPClientError:
            allsky_available = False

        return hips_order_max, hips_tile_format, hips_frame, allsky_available, allsky_file

    def _render_hips_template(self, template, req):
        hips_layer = req.args['hips_layer']
        hips_url = req.script_url + '/hips/' + hips_layer

        hips_order_max, hips_tile_format, hips_frame, allsky_available, allsky_file = self._hips_info(req, hips_layer)

        allsky_msg = ''
        if not allsky_available:
            allsky_msg = f'<p><b>WARNING</b>: {allsky_file} does not exist. It should be pregenerated with "mapproxy-util hips-allsky -f path_to_mapproxy.yaml -l {hips_layer} -o 3 -c $(nproc)"</p>'

        template = get_template(template, default_inherit="demo/static.html")
        return template.substitute(hips_url=hips_url,
                                   hips_layer=hips_layer,
                                   hips_frame=hips_frame,
                                   hips_order_max=hips_order_max,
                                   hips_tile_format=hips_tile_format,
                                   allsky_msg=allsky_msg)

    def _render_capabilities_template(self, template, xmlfile, service, url):
        template = get_template(template, default_inherit="demo/static.html")
        return template.substitute(capabilities = xmlfile,
                                   service = service,
                                   url = url)

    def authorized_demo(self, environ):
        if 'mapproxy.authorize' in environ:
            result = environ['mapproxy.authorize']('demo', [], environ=environ)
            if result['authorized'] == 'unauthenticated':
                raise RequestError('unauthorized', status=401)
            if result['authorized'] == 'full':
                return True
            return False
        return True


def escape(data):
    """
    Escape user-provided input data for safe inclusion in HTML _and_ JS to prevent XSS.
    """
    data = data.replace('&', '&amp;')
    data = data.replace('>', '&gt;')
    data = data.replace('<', '&lt;')
    data = data.replace("'", '')
    data = data.replace('"', '')
    return data

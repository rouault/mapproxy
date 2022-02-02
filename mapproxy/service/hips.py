# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

from functools import lru_cache
from PIL import Image

from mapproxy.cache.base import TileLocker
from mapproxy.cache.file import FileCache
from mapproxy.cache.tile import Tile
from mapproxy.grid import tile_grid
from mapproxy.layer import MapQuery
from mapproxy.request.wms import WMSMapRequest, WMSMapRequestParams
from mapproxy.response import Response
from mapproxy.service.base import Server
from mapproxy.service.wms import LayerRenderer
from mapproxy.srs import SRS
from mapproxy.image import ImageSource, img_to_buf
from mapproxy.image.merge import LayerMerger
from mapproxy.image.opts import ImageOptions
from mapproxy.util.hips import hp_subpixel_to_axis_coord, hp_boundaries_lonlat, healpix_resolution_degree
from mapproxy.util.resampling import bilinear_resample, bicubic_resample, has_numba
import healpy as hp
import numpy as np
import math
import logging
import os

log_hips = logging.getLogger('mapproxy.hips')

if has_numba:
    from numba import jit
else:
    class jit(object):
        def __init__(self, nopython = True, nogil = True):
            pass

        def __call__(self, f):
            return f

# Cache the result to speed-up consecutive requests
@lru_cache()
def subpixel_to_axis_coord_array(hips_shift, tile_size):
    """ Return the array of (x,y) pixel coordinates for HIPS pixels 0...tile_size^2-1 at order hips_shift """
    return np.array([hp_subpixel_to_axis_coord(hips_shift, i) for i in range (tile_size*tile_size)])

@lru_cache()
def get_hipsserver(mapproxy_conf):
    """ Utility function for _allsky_task() """
    from mapproxy.config.loader import load_configuration
    from mapproxy.config import local_base_config
    proxy_configuration = load_configuration(mapproxy_conf)
    with local_base_config(proxy_configuration.base_config):
        for service in proxy_configuration.configured_services():
            if isinstance(service, HIPSServer):
                return service

def _allsky_task(arg):
    """ Worker function for multiprocessing generation of allsky file """
    mapproxy_conf, layer_name, norder, shift, tile_size, width, npix = arg
    service = get_hipsserver(mapproxy_conf)
    num_tiles_per_row = width // tile_size
    y = npix // num_tiles_per_row
    x = npix % num_tiles_per_row
    return (y, x, service._generate_hips_tile(layer_name, norder, npix, shift))


def _seed_task(arg):
    """ Worker function for multiprocessing generation of tiles """
    mapproxy_conf, layer_name, norder, shift, npix = arg
    service = get_hipsserver(mapproxy_conf)

    cache_dir = os.path.join(service.cache_dir, layer_name, str(norder))

    tile_formats = service._get_hips_tile_format(layer_name)
    has_png = 'png' in tile_formats
    has_jpeg = 'jpeg' in tile_formats

    # Check if the requested tile is already cached
    is_cached_png = False
    is_cached_jpg = False

    if has_png:
        cache_png = FileCache(cache_dir, 'png')
        locker_png = TileLocker(service.lock_dir, service.lock_timeout, cache_png.lock_cache_id)
        tile_png = Tile([norder, npix, 0])
        with locker_png.lock(tile_png):
            if cache_png.is_cached(tile_png):
                is_cached_png = True
        img_opts_png = ImageOptions(format = 'png')

    if has_jpeg:
        cache_jpg = FileCache(cache_dir, 'jpg')
        locker_jpg = TileLocker(service.lock_dir, service.lock_timeout, cache_jpg.lock_cache_id)
        tile_jpg = Tile([norder, npix, 0])
        with locker_jpg.lock(tile_jpg):
            if cache_jpg.is_cached(tile_jpg):
                is_cached_jpg = True
        img_opts_jpg = ImageOptions(format = 'jpeg')

    if (not has_png or is_cached_png) and (not has_jpeg or is_cached_jpg):
        return

    # print(f'Generating {npix}')
    hips_tile_ar = service._generate_hips_tile(layer_name, norder, npix, shift)
    num_channels = hips_tile_ar.shape[2]
    img = Image.fromarray(hips_tile_ar, mode= 'RGBA' if num_channels == 4 else 'RGB')

    if has_png:
        with locker_png.lock(tile_png):
            result_buf = img_to_buf(img, img_opts_png)
            tile_png.source = ImageSource(result_buf)
            cache_png.store_tile(tile_png)

    if has_jpeg:
        with locker_jpg.lock(tile_jpg):
            result_buf = img_to_buf(img, img_opts_jpg)
            tile_jpg.source = ImageSource(result_buf)
            cache_jpg.store_tile(tile_jpg)


class HIPSServer(Server):
    """ Implements a server that handles requests for HIPS tiles, Allsky preview
        files and properties endpoint.
    """
    names = ('hips',)

    def __init__(self, cache_dir, lock_dir, lock_timeout, populate_cache, wms_root_layer, tile_layers, resampling_method):
        Server.__init__(self)
        self.cache_dir = cache_dir
        self.lock_dir = lock_dir
        self.lock_timeout = lock_timeout
        self.populate_cache = populate_cache
        self.layers = wms_root_layer.child_layers()
        self.hips_shift = 9
        self.tile_layers = tile_layers
        self.resampling_method = resampling_method
        self.resample_func = None
        if self.resampling_method == 'nearest_neighbour':
            pass
        elif self.resampling_method == 'bilinear':
            self.resample_func = bilinear_resample
        elif self.resampling_method == 'bicubic':
            self.resample_func = bicubic_resample
        else:
            assert False, self.resampling_method


    def _get_hips_shift(self, layer_name):
        """ Return the HIPS shift (log2(tile_width)) for the requested layer """

        hips_md = self.layers[layer_name].md.get('hips', {})
        hips_tile_width = int(hips_md.get('hips_tile_width', 1 << self.hips_shift))
        hips_shift = math.log2(hips_tile_width)
        if hips_shift != int(hips_shift):
            raise Exception(f'Invalid hips_tile_width={hips_tile_width} for layer {layer_name}')
        return int(hips_shift)


    def _get_hips_tile_format(self, layer_name):
        """ Return the value of the hips_tile_format property of the /properties file """

        hips_md = self.layers[layer_name].md.get('hips', {})
        return hips_md.get('hips_tile_format', 'png jpeg')


    def handleProperties(self, layer_name):
        """ Handle a request for the /properties file """

        from datetime import datetime

        hips_md = self.layers[layer_name].md.get('hips', {})

        # Required properties
        properties = {
            'creator_did': hips_md.get('creator_did', 'ivo://example.com/unknown_resource_FIXME'),
            'obs_title': hips_md.get('obs_title', self.layers[layer_name].title),
            'dataproduct_type': 'image',
            'hips_version': '1.4',
            'hips_release_date': hips_md.get('hips_release_date', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')),
            'hips_status': hips_md.get('hips_status', 'public master clonableOnce'),
            'hips_tile_format' : self._get_hips_tile_format(layer_name),
            'hips_order': hips_md.get('hips_order', '5'),
            'hips_tile_width': str(1 << self._get_hips_shift(layer_name)),
            'hips_frame': hips_md.get('hips_frame', 'planet'),
            'dataproduct_subtype': 'color', # required for Aladin Desktop to display in colors
        }

        # Add other keys from metadata
        for key in hips_md:
            if key not in properties:
                properties[key] = hips_md[key]

        # Format response as key=value pair lines
        s = ''
        for key in properties:
            s += key + '=' + str(properties[key]) + '\n'

        return Response(s, content_type='text/plain', status=200)


    def handleAllSky(self, req, layer_name, norder_arg, allskyFilename):
        """ Handle a request for a Allsky preview file """

        cache_dir = os.path.join(self.cache_dir, layer_name, norder_arg)
        cache_filename = os.path.join(cache_dir, allskyFilename)
        ext = allskyFilename.split('.')[1]
        if os.path.exists(cache_filename):
            img_opts = ImageOptions(format = 'png' if ext == 'png' else 'jpeg')
            content_type = img_opts.format.mime_type

            if req.environ['REQUEST_METHOD'] == 'HEAD':
                return Response(None, status=200, content_type=content_type)
            resp = Response(open(cache_filename, 'rb'), content_type=content_type)
            return resp

        return Response('Allsky requests should be pre-generated with mapproxy-util hips-allsky', content_type='text/plain', status=404)


    def _checkLayer(self, layer_name):
        """ Check if layer_name is a layer allowed for HIPS service """

        if layer_name not in self.layers:
            return Response(f'Unhandled layer name {layer_name}', content_type='text/plain', status=404)

        enabled = self.layers[layer_name].md.get('hips', {}).get('enabled', True)
        if not enabled:
            return Response(f'HIPS not enabled for layer {layer_name}', content_type='text/plain', status=404)

        return None


    def handle(self, req):
        """ Entry point for a request """

        log_hips.info('Handle request ' + req.path)

        # Analyze the request path to get layer name, HIPS order and tile number
        # and do a few sanity checks
        path_components = req.path.split('/')
        if len(path_components) < 4:
            return Response('Bath path for /hips. Should be /hips/layer/...', content_type='text/plain', status=404)
        assert path_components[0] == ''
        assert path_components[1] == 'hips'
        layer_name = path_components[2]

        layer_check_response = self._checkLayer(layer_name)
        if layer_check_response:
            return layer_check_response

        if len(path_components) == 4 and path_components[3] == 'properties':
            return self.handleProperties(layer_name)

        norder_arg = path_components[3]
        if not norder_arg.startswith('Norder'):
            return Response(f'Bath path for /hips. Component {norder_arg} should start with Norder', content_type='text/plain', status=404)

        if len(path_components) == 5 and path_components[4] in ('Allsky.png', 'Allsky.jpg'):
            norder_arg = path_components[3]
            return self.handleAllSky(req, layer_name, norder_arg, path_components[4])

        if len(path_components) < 6:
            return Response('Bath path for /hips. Should be /hips/layer/NorderK/DirD/NpixN.ext', content_type='text/plain', status=404)
        dir_arg = path_components[4]
        npix_arg = path_components[5]

        norder = int(norder_arg[len('Norder'):])

        if norder < 0 or norder > 30:
            return Response(f'Bath path for /hips. Invalid norder={norder}', content_type='text/plain', status=404)

        if not dir_arg.startswith('Dir'):
            return Response(f'Bath path for /hips. Component {dir_arg} should start with Dir', content_type='text/plain', status=404)
        dir_num = int(dir_arg[len('Dir'):])

        if not npix_arg.startswith('Npix'):
            return Response(f'Bath path for /hips. Component {npix_arg} should start with Npix', content_type='text/plain', status=404)
        npix_with_ext = npix_arg[len('Npix'):]
        pos_dot = npix_with_ext.find('.')
        ext = 'png'
        if pos_dot < 0:
            npix = int(npix_with_ext)
        else:
            npix = int(npix_with_ext[0:pos_dot])
            ext = npix_with_ext[pos_dot+1:]

        if ext not in ('png', 'jpg'):
            return Response(f'Bath path for /hips. Unhandled extension={ext}', content_type='text/plain', status=404)

        nside = 1 << norder
        if npix < 0 or npix >= 12 * nside * nside:
            return Response(f'Bath path for /hips. Invalid npix={npix}', content_type='text/plain', status=404)

        if dir_num != (npix // 10000) * 10000:
            return Response(f'Bath path for /hips. Inconsistent Dir and Npix', content_type='text/plain', status=404)

        img_opts = ImageOptions(format = 'png' if ext == 'png' else 'jpeg')

        # Check if the requested tile is already cached
        cache_dir = os.path.join(self.cache_dir, layer_name, norder_arg)
        cache = FileCache(cache_dir, ext)
        locker = TileLocker(self.lock_dir, self.lock_timeout, cache.lock_cache_id)
        tile = Tile([norder, npix, 0])
        with locker.lock(tile):
            if cache.is_cached(tile):
                if cache.load_tile(tile):
                    img = tile.source_image()
                    if img:
                        resp = Response(img_to_buf(img, img_opts), content_type=img_opts.format.mime_type)
                        return resp


        # If not, generate it
        hips_shift = self._get_hips_shift(layer_name)
        hips_tile_ar = self._generate_hips_tile(layer_name, norder, npix, hips_shift)
        num_channels = hips_tile_ar.shape[2]
        img = Image.fromarray(hips_tile_ar, mode= 'RGBA' if num_channels == 4 else 'RGB')

        result_buf = img_to_buf(img, img_opts)

        # And cache it if that's allowed in the service configuration
        if self.populate_cache:
            with locker.lock(tile):
                tile.source = ImageSource(result_buf)
                cache.store_tile(tile)

        resp = Response(result_buf, content_type=img_opts.format.mime_type)
        return resp


    def generate_allsky_file(self, mapproxy_conf, layer_name, norder, concurrency):
        """ Generate a Allsky.png/jpg preview file.
            See paragraph 4.3.2 of https://ivoa.net/documents/HiPS/20170519/REC-HIPS-1.0-20170519.pdf
            This is a single image file that contains previews of HIPS tiles.
            This method is used by the mapproxy-util hips-allsky utility.

            :param mapproxy_conf: Configuration file name.
            :param layer_name: Name of the layer to generate.
            :param norder: Norder to generate, generally in the [0-3] range.
            :param concurrency: Number of concurrent processes to use for the generation.
        """

        # Basic HIPS parameters
        nside = 1 << norder
        ntiles = 12 * nside * nside

        # Requirement: The width of this array must be the square root of the number of the tiles of the order
        width = int(ntiles ** 0.5)
        # Hence the height
        height = (ntiles + width - 1) // width

        # Compute the size in pixels of a tile inside the preview file, such
        # that the size of the preview file doesn't exceed 2048x2048 pixels.
        # Start with the standard HIPS tile size (generally 512 pixels)
        shift = self.hips_shift
        width *= 1 << shift
        height *= 1 << shift
        while height > 2048:
            shift -= 1
            width = width // 2
            height = height // 2

        tile_size = 1 << shift
        log_hips.info('Generating %dx%d tiles of size %dx%d', width // tile_size, height // tile_size, tile_size, tile_size)
        ar = np.zeros((height, width, 4), dtype=np.uint8)
        ar[:][:][3] = 255

        def update_allsky_array(y, x, hips_tile_ar):
            """ Update the content of the preview file (ar) with the passed HIPS previw tile """
            ar[y*tile_size:(y+1)*tile_size, x*tile_size:(x+1)*tile_size,0:hips_tile_ar.shape[2]] = hips_tile_ar

        if concurrency > 1:
            from multiprocessing import Pool
            with Pool(processes=concurrency) as pool:
                res = pool.map(_allsky_task, [(mapproxy_conf, layer_name, norder, shift, tile_size, width, npix) for npix in range(ntiles)])

            for y, x, hips_tile_ar in res:
                update_allsky_array(y, x, hips_tile_ar)
        else:
            num_tiles_per_row = width // tile_size
            for npix in range(ntiles):
                y = npix // num_tiles_per_row
                x = npix % num_tiles_per_row
                hips_tile_ar = self._generate_hips_tile(layer_name, norder, npix, shift)
                update_allsky_array(y, x, hips_tile_ar)

        cache_dir = os.path.join(self.cache_dir, layer_name, "Norder%d" % norder)
        os.makedirs(cache_dir, exist_ok=True)

        img_opts = ImageOptions(format = 'png')
        result_buf = img_to_buf(Image.fromarray(ar, mode='RGBA'), img_opts)
        open(os.path.join(cache_dir, "Allsky.png"), "wb").write(result_buf.read())

        img_opts = ImageOptions(format = 'jpeg')
        result_buf = img_to_buf(Image.fromarray(ar, mode='RGBA'), img_opts)
        open(os.path.join(cache_dir, "Allsky.jpg"), "wb").write(result_buf.read())


    def seed(self, mapproxy_conf, layer_name, norder, concurrency):
        """ Generate all HIPS tiles of a give Norder.
            This method is used by the mapproxy-util hips-seed utility.

            :param mapproxy_conf: Configuration file name.
            :param layer_name: Name of the layer to generate.
            :param norder: Norder to generate, generally in the [0-3] range.
            :param concurrency: Number of concurrent processes to use for the generation.
        """

        # Basic HIPS parameters
        nside = 1 << norder
        ntiles = 12 * nside * nside
        shift = self.hips_shift

        if concurrency > 1:
            from multiprocessing import Pool
            with Pool(processes=concurrency) as pool:
                it = pool.imap(_seed_task, [(mapproxy_conf, layer_name, norder, shift, npix) for npix in range(ntiles)])
                i = 0
                for _ in it:
                    i += 1
                    print('Seeding completed at %.2f %%' % (100.0 * i / ntiles))

        else:
            for npix in range(ntiles):
                _seed_task((mapproxy_conf, layer_name, norder, shift, npix))
                print('Seeding completed at %.2f %%' % (100.0 * (npix+1) / ntiles))


    def _generate_hips_tile(self, layer_name, norder, npix, hips_shift):

        # Unused for now
        grid = tile_grid(srs = 'EPSG:4326', bbox = [-180,-90,180,90])
        for layer, layer_obj_iter in self.tile_layers.iteritems():
            if layer_obj_iter.name == layer_name:
                grid = layer_obj_iter.grid
                break
        grid #noqa

        tile_size = 1 << hips_shift

        # Get the coordinates of the 4 corners of our HealPIX pixel of interest
        lon_bounds, lat_bounds = hp_boundaries_lonlat(norder, npix)
        lon_bounds = [ x if x <= 180 else x - 360 for x in lon_bounds ]
        # print(lon_bounds, lat_bounds)

        # Do not take into account longitudes at poles
        min_lon = float('inf')
        max_lon = -float('inf')
        found_lon_180 = False
        for i in range(len(lon_bounds)):
            if abs(lat_bounds[i]) != 90:
                min_lon = min(min_lon, lon_bounds[i])
                if lon_bounds[i] == 180:
                    found_lon_180 = True
                else:
                    max_lon = max(max_lon, lon_bounds[i])

        #for norder=0, tile=2, lon_bounds = [0.0, 180.0, -135.00000000000003, -90.0]
        if found_lon_180:
            if max_lon < 0:
                min_lon = -180
            else:
                max_lon = 180

        min_lat = min(lat_bounds)
        max_lat = max(lat_bounds)

        # Compute the angular resolution of a HealPIX pixel
        healpix_resolution = healpix_resolution_degree(norder, tile_size)

        # Compute the oversampling ratio to go from hips tile resolution
        # to geodetic tile resolution.
        oversampling_ratio_lat = (max_lat - min_lat) / (tile_size * healpix_resolution)
        EPSILON = 1e-5
        if max_lon - min_lon <= 90 + EPSILON:
            oversampling_ratio_lon = (max_lon - min_lon) / (tile_size * healpix_resolution)
        else:
            oversampling_ratio_lon = (lon_bounds[3] + 360 - lon_bounds[1]) / (tile_size * healpix_resolution)

        log_hips.debug(f'oversampling_ratio_lon = {oversampling_ratio_lon}')
        log_hips.debug(f'oversampling_ratio_lat = {oversampling_ratio_lat}')

        if self.resample_func is None:
            # Nearest resampling: no need to oversample excessively.
            oversampling_ratio = min(oversampling_ratio_lat, oversampling_ratio_lon)
        else:
            # Non-nearest resampling: take the max of the two ratios,
            # so that the source-to-target ratios in the 2 directions are >= 1
            # (if we don't need to saturate for performance reasons)
            # At equator, oversampling_ratio_lon ~= 1.54 and oversampling_ratio_lat ~= 1.30
            oversampling_ratio_unsaturated = max(oversampling_ratio_lat, oversampling_ratio_lon)

            # Limit our amount of oversampling. This is triggered when computing
            # pixels whose latitude range is close to the poles, where excessive
            # oversampling in longitude could happen otherwise. 2.0 is an arbitary
            # small value.
            oversampling_ratio = min(oversampling_ratio_unsaturated, 2.0)

        if max_lon - min_lon <= 90 + EPSILON:
            wrap_long_to_m180_180 = True
            # Compute source image spatial extent and size in pixels
            src_width = int(tile_size * oversampling_ratio)
            src_height = int(tile_size * oversampling_ratio)
            src_bbox = [min_lon, min_lat, max_lon, max_lat]
            source_image = self._get_source_image(layer_name, 'EPSG:4326', src_bbox, src_width, src_height)

            """
            img_opts = ImageOptions(format = 'png')
            num_channels = source_image.shape[2]
            result_buf = img_to_buf(Image.fromarray(source_image, mode= 'RGBA' if num_channels == 4 else 'RGB'), img_opts)
            open('/tmp/out.png', 'wb').write(result_buf.read())
            """

        else:
            # In healpix tiles crossing the antimeridian are crossing it in the middle
            # Issue two requests to get the source image of both sides of the
            # antimeridian and unit them together
            # (putting the part in [-180, -180+x] range to [180,180+x])
            wrap_long_to_m180_180 = False
            assert abs(lon_bounds[0] - 180) < 1e-10
            assert abs(lon_bounds[2] - 180) < 1e-10
            assert lon_bounds[1] > 0
            assert lon_bounds[3] < 0
            left_lon = lon_bounds[1]
            right_lon = lon_bounds[3]
            left_extent_lon = 180 - left_lon
            right_extent_lon = right_lon - -180
            extent_lon = left_extent_lon + right_extent_lon

            src_width_left = int(tile_size * left_extent_lon / extent_lon * oversampling_ratio)
            src_width_right = int(tile_size * right_extent_lon / extent_lon * oversampling_ratio)
            src_height = int(tile_size * oversampling_ratio)

            src_bbox = [left_lon, min_lat, 180, max_lat]
            source_image_left = self._get_source_image(layer_name, 'EPSG:4326', src_bbox, src_width_left, src_height)

            src_bbox = [-180, min_lat, right_lon, max_lat]
            source_image_right = self._get_source_image(layer_name, 'EPSG:4326', src_bbox, src_width_right, src_height)

            source_image = np.concatenate((source_image_left, source_image_right), axis=1)

            src_width = src_width_left + src_width_right
            min_lon = left_lon
            max_lon = right_lon + 360

        src_image_top = max_lat
        src_image_left = min_lon
        src_image_xres = (max_lon - min_lon) / src_width
        src_image_yres = (max_lat - min_lat) / src_height

        nside = 1 << (hips_shift + norder)
        hips_tile_ar = np.zeros((tile_size, tile_size, source_image.shape[2]), dtype=source_image.dtype)
        healpix_pix_offset = npix * tile_size * tile_size
        pixels = [healpix_pix_offset + i for i in range (tile_size*tile_size)]
        lon, lat = hp.pix2ang(nside, pixels, nest=True, lonlat=True)
        num_channels = hips_tile_ar.shape[2]

        """
        from mapproxy.util.hips import axis_coord_to_hp_subpixel

        def get_area(x_ar, y_ar):
            return 0.5 * abs(sum(x_ar[i] * (y_ar[(i+1) % len(x_ar)] - y_ar[i-1]) for i in range(len(x_ar))))

        def get_area_in_src_pixel_coordinates(pixel):
            lon_ar, lat_ar = hp_boundaries_lonlat(norder + hips_shift, npix * tile_size * tile_size + pixel)
            print(lon_ar, lat_ar)
            assert(max(lon_ar) - min(lon_ar) <= 90 + 1e-5)
            x = [(lon - src_image_left) / src_image_xres for lon in lon_ar]
            x.append(x[0])
            y = [(lat - src_image_top) / -src_image_yres for lat in lat_ar]
            y.append(y[0])
            return get_area(x, y)

        # Compute the area of a HealPIX sub-pixel, in the middle in the y direction,
        # of the image in terms of the source image
        area_in_src_pixel_coords = get_area_in_src_pixel_coordinates(axis_coord_to_hp_subpixel(hips_shift, (0, tile_size-1)))
        """

        # Compute the scaling factors to apply when resampling.
        # We could perhaps compute those values for each target pixel (by latitude)
        # by using the shape of the target pixel expressed in source pixels
        # but that seems overkill.
        src_to_tgt_scaling_x = 1.0
        src_to_tgt_scaling_y = 1.0
        if self.resample_func is not None:
            if oversampling_ratio_lon > oversampling_ratio_lat:
                src_to_tgt_scaling_x = oversampling_ratio / oversampling_ratio_lat
                src_to_tgt_scaling_y = oversampling_ratio / oversampling_ratio_unsaturated
            else:
                src_to_tgt_scaling_x = oversampling_ratio / oversampling_ratio_unsaturated
                src_to_tgt_scaling_y = oversampling_ratio / oversampling_ratio_lon

        log_hips.debug(f'src_to_tgt_scaling_x = {src_to_tgt_scaling_x}')
        log_hips.debug(f'src_to_tgt_scaling_y = {src_to_tgt_scaling_y}')

        coord_array = subpixel_to_axis_coord_array(hips_shift, tile_size)

        @jit(nopython=True)
        def create_image(tile_size, source_image, hips_tile_ar, resample_func):
            for i in range (tile_size*tile_size):
                x, y = coord_array[i]
                # The axis of the image are swapped compared to the HealPIX ones
                y, x = x, y
                lon_i = lon[i]
                if wrap_long_to_m180_180:
                    lon_i = lon_i if lon_i <= 180 else lon_i - 360

                src_x_float = (lon_i - src_image_left) / src_image_xres
                src_y_float = (lat[i] - src_image_top) / -src_image_yres
                if resample_func is None:
                    src_x = int(src_x_float)
                    src_y = int(src_y_float)
                    if src_x >= 0 and src_x < src_width and \
                       src_y >= 0 and src_y < src_height:
                        hips_tile_ar[y, x] = source_image[src_y, src_x]
                else:
                    # -0.5 to go from center-of-pixel to array indices,
                    # as pix2ang computed coordinates of center of pixel
                    src_x_float -= 0.5
                    src_y_float -= 0.5
                    if src_x_float >= 0 and src_x_float < src_width and \
                       src_y_float >= 0 and src_y_float < src_height:
                        if has_numba:
                            # Faster to use the resample_func on scalars than numpy
                            # arrays when using numba jit'ed functions
                            for k in range(num_channels):
                                hips_tile_ar[y,x,k] = max(0,min(255,int(resample_func(source_image[:,:,k], src_x_float, src_y_float, src_to_tgt_scaling_x, src_to_tgt_scaling_y) + 0.5)))
                        else:
                            hips_tile_ar[y,x] = np.clip(np.round_(resample_func(source_image, src_x_float, src_y_float, src_to_tgt_scaling_x, src_to_tgt_scaling_y)),0,255)

        create_image(tile_size, source_image, hips_tile_ar, self.resample_func)
        return hips_tile_ar


    def _get_source_image(self, layer_name, srs, bbox, width, height):
        """ Return a numpy array whose content corresponds to the content of
            layer layer_name, with the passed in bbox and srs, and with the
            dimensions width x height in pixels.
        """
        params = WMSMapRequestParams({'layers': layer_name,
                                      'version': '1.1.1',
                                      'srs': srs,
                                      'bbox': ','.join([str(x) for x in bbox]),
                                      'width': str(width),
                                      'height': str(height),
                                      'format': 'image/png'})
        query = MapQuery(params.bbox, params.size, SRS(params.srs), params.format)
        wmsrequest = WMSMapRequest(params)

        layer = self.layers[layer_name]
        render_layers = []
        if layer.renders_query(query):
            for _, map_layers in layer.map_layers_for_query(query):
                render_layers.extend(map_layers)

        img_opts = ImageOptions(format = params.format)
        renderer = LayerRenderer(render_layers, query, wmsrequest)
        merger = LayerMerger()
        renderer.render(merger)
        result = merger.merge(size=query.size, image_opts=img_opts,
            bbox=query.bbox, bbox_srs=params.srs)

        return np.array(result.as_image())

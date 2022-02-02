# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

from PIL import Image
from mapproxy.client.http import HTTPClientError
from mapproxy.image import ImageSource
from mapproxy.layer import DefaultMapExtent, MapLayer
from mapproxy.srs import SRS
from mapproxy.util import hips
from mapproxy.util.resampling import bilinear_resample, bicubic_resample, has_numba
import logging
import math
import numpy as np

log_hips = logging.getLogger('mapproxy.hips')
log_config = logging.getLogger('mapproxy.config')

if has_numba:
    from numba import jit
else:
    class jit(object):
        def __init__(self, nopython = True, nogil = True):
            pass

        def __call__(self, f):
            return f

class HIPSSource(MapLayer):
    def __init__(self, http_client, url, resampling_method, coverage=None, image_opts=None):
        MapLayer.__init__(self, image_opts=image_opts)
        self.http_client = http_client
        self.url = url
        self.coverage = coverage
        if self.coverage:
            self.extent = coverage.extent
        else:
            self.extent = DefaultMapExtent()
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
        self.locker = None
        self.cache = None
        # Actual value of the below properties will only be known after
        # _load_properties() execution
        self.hips_shift = None
        self.hips_tile_format = None
        self.hips_order_max = None


    def _load_properties(self):
        """ This method fetches the /properties document from the HIPS source
            to get the tile format, maximum HIPS order and the size in pixels
            of a HIPS tile """

        # Do it only once
        if self.hips_shift is not None:
            return

        # The two properties should normally be overriden
        self.hips_tile_format = 'jpeg'
        self.hips_order_max = 5

        # Download the /properties document to get a few metadata we
        # need: size of tiles, maximum HIPS order and tile format.
        resp = self.http_client.open(self.url + "/properties")

        properties = hips.parse_properties(resp.read().decode('utf-8'))

        if 'hips_order' in properties: # Mandatory element
            self.hips_order_max = int(properties['hips_order'])

        if 'hips_tile_format' in properties: # Mandatory element
            tile_format = None
            value = properties['hips_tile_format']
            for x in value.split(' '):
                if x in ('jpeg', 'png'):
                    tile_format = x
                    break
            if tile_format is None:
                return Exception(f'hips_tile_format = {value} does not contain jpeg or png')
            self.hips_tile_format = tile_format

        hips_tile_width = 512
        if 'hips_tile_width' in properties:
            hips_tile_width = int(properties['hips_tile_width'])

        self.hips_shift = math.log2(hips_tile_width)
        assert self.hips_shift == int(self.hips_shift)
        self.hips_shift = int(self.hips_shift)


    def get_map(self, query):
        # print("get_map()", query.bbox, query.size, query.srs)

        self._load_properties()

        resx = (query.bbox[2] - query.bbox[0]) / query.size[0]
        resy = (query.bbox[3] - query.bbox[1]) / query.size[1]

        srs_4326 = SRS(4326)
        if srs_4326 != query.srs:

            def get_area(xy_ar):
                return 0.5 * abs(sum(xy_ar[i][0] * (xy_ar[(i+1) % len(xy_ar)][1] - xy_ar[i-1][1]) for i in range(len(xy_ar))))
            def res_at(I,J):
                def point(i,j):
                    return (query.bbox[0] + resx * (I + i),
                            query.bbox[1] + resy * (J + j))
                local_lonlat = query.srs.transform_to(srs_4326,
                    [point(0,0),point(0,1),point(1,1),point(1,0)])
                local_lonlat = [xy for xy in local_lonlat]
                local_lon = [xy[0] for xy in local_lonlat]
                if max(local_lon) - min(local_lon) > 180:
                    return None
                # Multiply longitudes by the cosinus of the latitude, to get
                # the equivalent of the area at the equator, and be able to
                # compare that to a HIPS resolution.
                return get_area([(xy[0] * math.cos(xy[1] / 180. * math.pi),xy[1]) for xy in local_lonlat])**0.5

            # Comput average resolution in degree/pixel by sampling at a few
            # points in the target image
            sum_res = 0
            num_res = 0
            for (x,y) in ((0.25,0.25),(0.25,0.75),(0.75,0.25),(0.75,0.75)):
                res = res_at(x*query.size[0], y*query.size[1])
                if res is not None:
                    sum_res += res
                    num_res += 1
            res = sum_res / num_res

            lonlat = query.srs.transform_to(srs_4326,
                [(query.bbox[0] + (i + 0.5) * resx,
                  query.bbox[3] - (j + 0.5) * resy) for j in range(query.size[1]) for i in range(query.size[0])])
            lon = []
            lat = []
            for x in lonlat:
                lon.append(x[0])
                lat.append(x[1])
        else:
            lon = [ query.bbox[0] + (i + 0.5) * resx for j in range(query.size[1]) for i in range(query.size[0]) ]
            lat = [ query.bbox[3] - (j + 0.5) * resy for j in range(query.size[1]) for i in range(query.size[0]) ]
            res = resx

        hips_tile_size = 1 << self.hips_shift
        hips_tile_order = min(max(math.ceil(hips.hips_order_for_resolution(res, hips_tile_size)),0), self.hips_order_max)
        hips_tile_res = hips.healpix_resolution_degree(hips_tile_order, hips_tile_size)
        src_to_tgt_scaling = hips_tile_res / res

        # For geodetic tile at zoom level 0, part of the latitudes are outside of [-90,90]
        # so clamp them, otherwise lonlat_to_hp_pixel() will emit an exception
        lat_clamped = [ max(-90, min(90, x)) for x in lat ]

        # Compute HealPIX pixel coordinates for the center of each target pixel
        # Request also the fractional part of the HealPIX coordinate to be
        # able to do interpolation.
        pixels_filtered, dx_ar, dy_ar = hips.lonlat_to_hp_pixel(hips_tile_order + self.hips_shift, lon, lat_clamped, return_offsets=True)

        # Set -1 as pixel number for invalid latitudes
        pixels = np.array([ pixels_filtered[x] if lat[x] == lat_clamped[x] else -1 for x in range(len(pixels_filtered)) ], dtype=np.int64)

        hips_image_ext = 'jpg' if self.hips_tile_format == 'jpeg' else 'png'

        if has_numba:
            # Numba cannot take a untyped dict for an input parameter, so
            # we have to specify the key and value types
            from numba.core import types
            from numba.typed import Dict
            map_tiles = Dict.empty(
                key_type=types.int64,
                value_type=types.uint8[:,:,:],
            )
            map_tile_coord_shift = Dict.empty(
                key_type=types.int64,
                value_type=types.int32[:],
            )
        else:
            map_tiles = dict()
            map_tile_coord_shift = dict()


        # Collect all HIPS tiles that intersect the request
        hips_tiles = set(pixels_filtered >> (2 * self.hips_shift))
        tiles_of_expected_dimension = True
        for hips_tile in hips_tiles:

            map_tile_coord_shift[hips_tile] = np.array([0, 0], np.int32)
            cached_tile = False
            if self.cache:
                from mapproxy.cache.tile import Tile
                tile = Tile([hips_tile_order, hips_tile, 0])
                with self.locker.lock(tile):
                    if self.cache.is_cached(tile):
                        if self.cache.load_tile(tile):
                            img = tile.source_image()
                            if img:
                                cached_tile = True
                                map_tiles[hips_tile] = np.array(img)

            if not cached_tile:
                req_dir = hips_tile // 10000 * 10000
                url = self.url + f"/Norder{hips_tile_order}/Dir{req_dir}/Npix{hips_tile}.{hips_image_ext}"
                try:
                    img = self.http_client.open_image(url)
                    map_tiles[hips_tile] = np.array(img.as_image())
                    if self.cache:
                        with self.locker.lock(tile):
                            tile.source = img
                            self.cache.store_tile(tile)
                except HTTPClientError as e:
                    log_hips.warning('could not retrieve tile: %s', e)

            if hips_tile in map_tiles:
                tile = map_tiles[hips_tile]
                if tile.shape[0] != 1 << self.hips_shift or tile.shape[1] != 1 << self.hips_shift:
                    tiles_of_expected_dimension = False
                    log_hips.warning('tile %d,%d has not expected shape', hips_tile_order, hips_tile)


        # Potential optimization for quality of images if the source HIPS tiles
        # belong to the same of one of the base 12 pixels
        set_tiles_level_zero = set([hips_tile >> (2 * hips_tile_order) for hips_tile in hips_tiles])
        if tiles_of_expected_dimension and len(set_tiles_level_zero) == 1:

            # Get subpixel coordinates of source HIPS tiles, relative to their
            # base pixel and at order hips_tile_order
            max_x = -1
            max_y = -1
            min_x = 1 << hips_tile_order
            min_y = 1 << hips_tile_order
            for hips_tile in hips_tiles:
                x, y = hips.hp_subpixel_to_axis_coord(hips_tile_order, hips_tile % (1 << (2 * hips_tile_order)))
                # The axis of the image are swapped compared to the HealPIX ones
                y, x = x, y
                min_x = min(min_x, x)
                min_y = min(min_y, y)
                max_x = max(max_x, x)
                max_y = max(max_y, y)

            # If the source tiles are grouped together, we can build a single
            # source array and composite them together
            if max_y - min_y <= 1 and max_x - min_x <= 1:
                source_ar = np.zeros(((max_y - min_y + 1) << self.hips_shift,
                                      (max_x - min_x + 1) << self.hips_shift,
                                      4), np.uint8)
                tile_size = 1 << self.hips_shift
                for hips_tile in hips_tiles:
                    if hips_tile in map_tiles:
                        x, y = hips.hp_subpixel_to_axis_coord(hips_tile_order, hips_tile % (1 << (2 * hips_tile_order)))
                        y, x = x, y
                        tile_ar = map_tiles[hips_tile]
                        y_shift = (y - min_y) << self.hips_shift
                        x_shift = (x - min_x) << self.hips_shift
                        source_ar[y_shift:y_shift + tile_size,
                                  x_shift:x_shift + tile_size,
                                  0:tile_ar.shape[2]] = tile_ar
                        if tile_ar.shape[2] == 3:
                            source_ar[y_shift:y_shift + tile_size,
                                      x_shift:x_shift + tile_size,
                                      3] = 255
                        map_tiles[hips_tile] = source_ar

                        # Offset to add to go from tile coordinate to global source array coordinate
                        map_tile_coord_shift[hips_tile] = np.array([y_shift, x_shift], np.int32)


        @jit(nopython=True)
        def create_image(height, width,
                         hips_shift, pixels, dx_ar, dy_ar, map_tiles,
                         map_tile_coord_shift,
                         result_ar, resample_func, src_to_tgt_scaling):
            for j in range(height):
                for i in range(width):
                    idx = j * width + i
                    pixel = pixels[idx]
                    if pixel < 0:
                        continue
                    hips_tile = pixel >> (2 * hips_shift)
                    if hips_tile in map_tiles:
                        subpixel = pixel - (hips_tile << (2 * hips_shift))
                        x, y = hips.hp_subpixel_to_axis_coord(hips_shift, subpixel)
                        # The axis of the image are swapped compared to the HealPIX ones
                        y, x = x, y

                        source_ar = map_tiles[hips_tile]

                        # Offset to add to go from tile coordinate to global source array coordinate
                        shift = map_tile_coord_shift[hips_tile]
                        y += shift[0]
                        x += shift[1]

                        if resample_func is None:
                            result_ar[j,i,0:source_ar.shape[2]] = source_ar[y,x]
                        else:
                            dy, dx = dx_ar[idx], dy_ar[idx]
                            num_channels = source_ar.shape[2]
                            for k in range(num_channels):
                                resampled_val = resample_func(source_ar[:,:,k],
                                                              x + dx,
                                                              y + dy,
                                                              src_to_tgt_scaling,
                                                              src_to_tgt_scaling)
                                result_ar[j,i,k] = max(0,min(255,int(resampled_val + 0.5)))
                        result_ar[j,i,3] = 255


        result_ar = np.zeros((query.size[1],query.size[0],4), dtype=np.uint8)

        import time
        start = time.time()
        create_image(query.size[1], query.size[0],
                     self.hips_shift, pixels, dx_ar, dy_ar,
                     map_tiles, map_tile_coord_shift,
                     result_ar,
                     self.resample_func, src_to_tgt_scaling)
        log_hips.info('Processing time: %.02f s', time.time() - start)

        return ImageSource(Image.fromarray(result_ar, mode='RGBA'))

# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

import healpy as hp
import numpy as np
import math

try:
    from numba import jit
except:
    class jit(object):
        def __init__(self, nopython = True, nogil = True):
            pass

        def __call__(self, f):
            return f


def parse_properties(content_utf8):
    """ Decode the content of the /properties HIPS document and return it
        as a dict """
    properties = content_utf8.split('\n')
    d = {}
    for line in properties:
        equal_pos = line.find('=')
        if line and line[0] != '#' and equal_pos != -1:
            key = line[0:equal_pos]
            value = line[equal_pos+1:]
            key = key.strip()
            value = value.strip()
            d[key] = value
    return d


@jit(nopython=True)
def hp_subpixel_to_axis_coord(order, subpixel):
    """ Convert HealPIX subpixel coordinates to axis coordinates.
        The x axis goes from the bottom point of the diamond to the north-east direction
        The y axis goes from the bottom point of the diamont to the north-west direction
        At order 1, there are 4 subpixels : 0, 1, 2, 3
              +
             / \
            + 3 +
           / \ / \
          + 2 + 1 +
           \ / \ /
            + 0 +
             \ /
              +
        Their corresponding axis coordinates are:
            0 -> x,y=0,0
            1 -> x,y=1,0
            2 -> x,y=0,1
            3 -> x,y=1,1
       """
    x = 0
    y = 0
    assert subpixel >= 0 and subpixel < (1 << (2 * order))
    for l in range(0, order):
        tmp = (subpixel >> (2*l)) & 3
        x = x | ((tmp & 1) << l)
        y = y | ((tmp >> 1) << l)

    return (x, y)


def axis_coord_to_hp_subpixel(order, xy_tuple):
    x, y = xy_tuple
    assert x >= 0 and x < (1 << order)
    assert y >= 0 and y < (1 << order)
    subpixel = 0
    for l in range(0, order):
        subpixel = subpixel | ((((x >> l) & 1) | (((y >> l) & 1) << 1)) << (2*l))
    return subpixel


def hp_boundaries_lonlat(order, pixel):
    nside = 1 << order
    vec = hp.boundaries(nside, pixel, nest=True)
    vec = np.transpose(vec)
    return hp.vec2ang(vec, lonlat = True)


def hp_boundaries_lonlat_with_astropy_healpix(order, pixel):
    from astropy_healpix import HEALPix
    from astropy import units as u
    hp_instance = HEALPix(nside=1 << order, order='nested')
    b = hp_instance.boundaries_lonlat(pixel, step=1)
    b = (b[0].to(u.deg).value)[0], (b[1].to(u.deg).value)[0]
    return b


def lonlat_to_hp_pixel(order, lon, lat, return_offsets=False):
    if not return_offsets:
        nside = 1 << order
        return hp.ang2pix(nside, lon, lat, nest=True, lonlat=True)

    # healpy can't handle order > 29
    extra_order = 29 - order
    nside = 1 << (order + extra_order)
    pixel = hp.ang2pix(nside, lon, lat, nest=True, lonlat=True)
    if isinstance(pixel, list) or isinstance(pixel, np.int64):
        x, y = hp_subpixel_to_axis_coord(extra_order, pixel % (1 << (2 * extra_order)))
        return pixel >> (2 * extra_order), x / float(1 << extra_order), y / float(1 << extra_order)
    else:
        xy = np.array([hp_subpixel_to_axis_coord(extra_order, p % (1 << (2 * extra_order))) for p in pixel]).reshape((len(pixel), 2)).transpose()
        return pixel >> (2 * extra_order), xy[0] / float(1 << extra_order), xy[1] / float(1 << extra_order)


def lonlat_to_hp_pixel_with_astropy_healpix(order, lon, lat, return_offsets=False):
    from astropy_healpix import HEALPix
    from astropy import units as u
    hp_instance = HEALPix(nside=1 << order, order='nested')
    return hp_instance.lonlat_to_healpix(lon * u.deg, lat*u.deg, return_offsets=return_offsets)


def healpix_resolution_degree(order, hips_tile_size):
    # Compute the angular resolution of a HealPIX pixel
    # Formula from https://arxiv.org/abs/astro-ph/0409513v1, page 7
    healpix_resolution = math.sqrt(4 * math.pi / (12 * (hips_tile_size * (1 << order))**2))
    # Convert into degree
    healpix_resolution = healpix_resolution / math.pi * 180.0
    return healpix_resolution


def hips_order_for_resolution(resolution_deg_by_pixel, hips_tile_size):
    """ For a given resolution in degree/pixel and a HIPS tile size in pixels (typically 512)
        return the (floating-point) HIPS tile order.
        Must generally be rounded to the upper value.
        This is the inverse of :func:healpix_resolution_degree()
    """
    res = resolution_deg_by_pixel / 180.0 * math.pi
    order = math.log2(math.sqrt(4 * math.pi / res**2 / 12) / hips_tile_size)
    return order

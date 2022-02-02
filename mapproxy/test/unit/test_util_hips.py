# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

from mapproxy.util.hips import parse_properties, \
                               hp_subpixel_to_axis_coord, \
                               axis_coord_to_hp_subpixel, \
                               hp_boundaries_lonlat, \
                               lonlat_to_hp_pixel, \
                               healpix_resolution_degree, \
                               hips_order_for_resolution
import numpy as np
import pytest

def test_parse_properties():
    assert parse_properties("foo = bar\n#comment line\nbar = baz") == {'bar': 'baz', 'foo': 'bar'}


def test_hp_subpixel_to_axis_coord():
    # order 0
    assert hp_subpixel_to_axis_coord(0, 0) == (0, 0)

    # order 1
    assert hp_subpixel_to_axis_coord(1, 0) == (0, 0)
    assert hp_subpixel_to_axis_coord(1, 1) == (1, 0)
    assert hp_subpixel_to_axis_coord(1, 2) == (0, 1)
    assert hp_subpixel_to_axis_coord(1, 3) == (1, 1)

    # order 2
    assert hp_subpixel_to_axis_coord(2, 5) == (3, 0)
    assert hp_subpixel_to_axis_coord(2, 10) == (0, 3)
    assert hp_subpixel_to_axis_coord(2, 15) == (3, 3)


def test_axis_coord_to_hp_subpixel():

    assert axis_coord_to_hp_subpixel(0, (0,0)) == 0

    assert axis_coord_to_hp_subpixel(2, (3, 0)) == 5
    assert axis_coord_to_hp_subpixel(2, (0, 3)) == 10


def test_hp_boundaries_lonlat():

    order = 2
    pixel = 10
    res = hp_boundaries_lonlat(order, pixel)
    assert res[0] == pytest.approx(np.array([ 0.,  0., 11.25, 22.5]), rel=1e-7)
    assert res[1] == pytest.approx(np.array([54.3409123, 41.8103149, 30., 41.8103149]), rel=1e-7)


def test_lonlat_to_hp_pixel():

    order = 2
    lon = 2.0
    lat = 49.0
    assert lonlat_to_hp_pixel(order, lon, lat) == 10
    assert lonlat_to_hp_pixel(order, [lon, lon], [lat, lat]) == pytest.approx(np.array([10, 10]))
    assert lonlat_to_hp_pixel(order, lon, lat, return_offsets=True) == (10, 0.6449339464306831, 0.9237484931945801)
    res = lonlat_to_hp_pixel(order, [lon, lon], [lat, lat], return_offsets=True)
    assert res[0] ==  pytest.approx(np.array([10,10]))
    assert res[1] ==  pytest.approx(np.array([0.6449339464306831,0.6449339464306831]))
    assert res[2] ==  pytest.approx(np.array([0.9237484931945801,0.9237484931945801]))


def test_healpix_resolution_degree():

    assert healpix_resolution_degree(0, 512) == 0.11451621372724687
    assert healpix_resolution_degree(1, 512) == 0.05725810686362343


def test_hips_order_for_resolution():

    assert hips_order_for_resolution(0.11451621372724687, 512) == pytest.approx(0)
    assert hips_order_for_resolution(0.05725810686362343, 512) == pytest.approx(1)

# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

from mapproxy.util.resampling import bilinear_weight, \
                                     cubic_weight, \
                                     bilinear_resample, \
                                     bicubic_resample
import numpy
import pytest

def test_bilinear_weight():
    assert bilinear_weight(-2) == 0
    assert bilinear_weight(-1) == 0
    assert bilinear_weight(-0.5) == 0.5
    assert bilinear_weight(0) == 1
    assert bilinear_weight(0.5) == 0.5
    assert bilinear_weight(1) == 0
    assert bilinear_weight(2) == 0

def test_cubic_weight():
    assert cubic_weight(-3) == 0
    assert cubic_weight(-2.5) == 0
    assert cubic_weight(-2) == 0
    assert cubic_weight(-1.5) == -0.0625
    assert cubic_weight(-1) == 0
    assert cubic_weight(-0.5) == 0.5625
    assert cubic_weight(0) == 1
    assert cubic_weight(0.5) == 0.5625
    assert cubic_weight(1) == 0
    assert cubic_weight(1.5) == -0.0625
    assert cubic_weight(2) == 0
    assert cubic_weight(2.5) == 0
    assert cubic_weight(3) == 0


def test_bilinear_resample():
    src_height = 8
    src_width = 12
    ar = numpy.arange(src_height*src_width).reshape(src_height,src_width).astype(numpy.float32)

    # No scaling and evaluating at grid nodes: values should be preserved
    for j_dst in range(ar.shape[0]):
        for i_dst in range(ar.shape[1]):
            assert bilinear_resample(ar, i_dst, j_dst, 1, 1) == ar[j_dst][i_dst]


def test_bicubic_resample():
    src_height = 8
    src_width = 12
    ar = numpy.arange(src_height*src_width).reshape(src_height,src_width).astype(numpy.float32)

    # No scaling and evaluating at grid nodes: values should be preserved
    for j_dst in range(ar.shape[0]):
        for i_dst in range(ar.shape[1]):
            assert bicubic_resample(ar, i_dst, j_dst, 1, 1) == ar[j_dst][i_dst]

@pytest.mark.parametrize("method", ["bicubic", "bilinear"])
def test_resample_against_gdal(method):

    try:
        # Check against GDAL implementation
        from osgeo import gdal, gdal_array
    except:
        pytest.skip('GDAL missing')

    src_height = 8
    src_width = 12
    ar = numpy.arange(src_height*src_width).reshape(src_height,src_width).astype(numpy.float32)

    ds = gdal_array.OpenArray(ar)
    dst_height = 8
    dst_width = 5

    if method == 'bicubic':
        gdal_method = gdal.GRIORA_Cubic
        fct = bicubic_resample
    elif method == 'bilinear':
        gdal_method = gdal.GRIORA_Bilinear
        fct = bilinear_resample
    else:
        assert False

    resampled_ar_gdal = ds.ReadAsArray(buf_xsize=dst_width,
                                       buf_ysize=dst_height,
                                       resample_alg=gdal_method)
    x_scale = resampled_ar_gdal.shape[1] / ar.shape[1]
    y_scale = resampled_ar_gdal.shape[0] / ar.shape[0]

    for j_dst in range(resampled_ar_gdal.shape[0]):
        for i_dst in range(resampled_ar_gdal.shape[1]):
            i_src = (i_dst + 0.5) / x_scale - 0.5
            j_src = (j_dst + 0.5) / y_scale - 0.5
            our_val = fct(ar, i_src, j_src, x_scale, y_scale)
            gdal_val = resampled_ar_gdal[j_dst][i_dst]
            assert abs(our_val - gdal_val) <= 1e-7 * abs(our_val), (our_val, gdal_val)

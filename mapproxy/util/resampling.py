# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

import math

try:
    from numba import jit
    has_numba = True
except:
    import logging
    log_system = logging.getLogger('mapproxy.system')
    log_system.warning('cannot load numba. Resampling will be slow')

    has_numba = False
    class jit(object):
        def __init__(self, nopython = True, nogil = True):
            pass

        def __call__(self, f):
            return f


# Ported from GDAL's warper core GWKCubic(), GWKBilinear() and GWKResample() methods
# of https://github.com/OSGeo/gdal/blob/master/gdal/alg/gdalwarpkernel.cpp

@jit(nopython=True, nogil=True)
def bilinear_weight(x):
    absX = abs(x)
    return 1 - absX if absX <= 1.0 else 0.0


@jit(nopython=True, nogil=True)
def cubic_weight(x):
    # http://en.wikipedia.org/wiki/Bicubic_interpolation#Bicubic_convolution_algorithm
    # W(x) formula with a = -0.5 (cubic hermite spline )
    # or https://www.cs.utexas.edu/~fussell/courses/cs384g-fall2013/lectures/mitchell/Mitchell.pdf
    # k(x) (formula 8) with (B,C)=(0,0.5) the Catmull-Rom spline
    absX = abs(x)
    if absX <= 1.0:
        return (x ** 2) * (1.5 * absX - 2.5) + 1
    if absX <= 2.0:
        return (x ** 2) * (-0.5 * absX + 2.5) - 4 * absX + 2
    return 0.0


@jit(nopython=True, nogil=True)
def _convolution_resample(array, x, y, x_scale, y_scale, filter_radius, weight_function):

    x_scale = x_scale if x_scale < 1 else 1
    y_scale = y_scale if y_scale < 1 else 1

    x_radius = int(math.ceil(filter_radius / x_scale))
    y_radius = int(math.ceil(filter_radius / y_scale))

    iX = int(x)
    iY = int(y)
    deltaX = x - iX
    deltaY = y - iY

    # Compute pixel indices that affect our result, and
    # skip sampling over edge of image.
    # Filter window offset depends on the parity of the kernel radius.
    jMin = ((filter_radius + 1) % 2) - y_radius
    jMax = y_radius
    if iY + jMin < 0:
        jMin = -iY
    if iY + jMax >= array.shape[0]:
        jMax = array.shape[0] - iY - 1

    iMin = ((filter_radius + 1) % 2) - x_radius
    iMax = x_radius
    if iX + iMin < 0:
        iMin = -iX
    if iX + iMax >= array.shape[1]:
        iMax = array.shape[1] - iX - 1

    # Precompute horizontal weights
    tab_weightX = [weight_function((i - deltaX) * x_scale) for i in range(iMin, iMax+1)]

    # Loop over pixels that affect our result
    acc = 0
    accWeight = 0
    for j in range(jMin, jMax+1):
        subar = array[iY+j]
        accLocal = subar[iX+iMin] * tab_weightX[0]
        for i in range(iMin+1, iMax+1):
            accLocal += subar[iX+i] * tab_weightX[i - iMin]

        # Take into account the Y weight.
        weightY = weight_function((j - deltaY) * y_scale)
        acc += accLocal * weightY
        accWeight += weightY

    accWeight *= sum(tab_weightX)

    return acc / accWeight


@jit(nopython=True, nogil=True)
def bilinear_resample(array, x, y, x_scale, y_scale):
    """ Given a 2D array, return value of point at coordinates (x,y), where
        x and y are floating point, with a source-to-target scaling of
        (x_scale,y_scale), by applying bilinear interpolation.

        x should be between 0 and array.shape[1] - 1
        y should be between 0 and array.shape[0] - 1

        For example if computing a resampled array whose size is
        (array.shape[0] * x_scale, array.shape[1] * y_scale), this function
        should be evaluated at points with:
            x = (x_dst + 0.5) / x_scale - 0.5
            y = (y_dst + 0.5) / y_scale - 0.5
    """
    filter_radius = 1
    return _convolution_resample(array, x, y, x_scale, y_scale, filter_radius, bilinear_weight)


@jit(nopython=True, nogil=True)
def bicubic_resample(array, x, y, x_scale, y_scale):
    """ Given a 2D array, return value of point at coordinates (x,y), where
        x and y are floating point, with a source-to-target scaling of
        (x_scale,y_scale), by applying bicubic interpolation.

        x should be between 0 and array.shape[1] - 1
        y should be between 0 and array.shape[0] - 1

        For example if computing a resampled array whose size is
        (array.shape[0] * x_scale, array.shape[1] * y_scale), this function
        should be evaluated at points with:
            x = (x_dst + 0.5) / x_scale - 0.5
            y = (y_dst + 0.5) / y_scale - 0.5
    """
    filter_radius = 2
    return _convolution_resample(array, x, y, x_scale, y_scale, filter_radius, cubic_weight)

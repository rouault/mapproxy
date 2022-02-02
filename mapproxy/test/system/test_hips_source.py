# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

import hashlib

from io import BytesIO

from mapproxy.compat.image import Image
from mapproxy.test.http import mock_httpd
from mapproxy.test.image import tmp_image
from mapproxy.test.system import SysTest

import pytest

class TestHIPSSource(SysTest):

    @pytest.fixture(scope="class")
    def config_file(self):
        return "hips_source.yaml"


    def test_0_0_0(self, app):
        with tmp_image((512, 512), format="jpeg", color=(255, 0, 0)) as img:
            expected_reqs = [
                (
                    {"path": r"/hips_source/properties"},
                    {"body": b"hips_tile_format=jpeg\nhips_order=5\nhips_tile_width=512",
                     "headers": {"content-type": "text/plain"}},
                ),
                (
                    {"path": r"/hips_source/Norder0/Dir0/Npix2.jpg"},
                    {"body": img.read(), "headers": {"content-type": "image/jpeg"}},
                )
            ]
            with mock_httpd(("localhost", 42423), expected_reqs):
                resp = app.get("/tms/1.0.0/hips/geodetic/0/0/0.png")
                assert resp.content_type == "image/png"
                img = Image.open(BytesIO(resp.body))
                # Like a red inverted 'house' in the upper-left corner
                #open('/tmp/tmp.png', 'wb').write(resp.body)
                assert img.width == 256
                assert img.height == 256
                assert hashlib.md5(resp.body).hexdigest() in ('6d74a8e25ae071d39151a783c7132474', 'b57fe370becf5892b08e79ef2b1d2acb')


    def test_1_0_0(self, app):
        with tmp_image((512, 512), format="jpeg", color=(255, 0, 0)) as img:
            img_data = img.read()
            expected_reqs = [
                (
                    {"path": r"/hips_source/Norder0/Dir0/Npix10.jpg"},
                    {"body": img_data, "headers": {"content-type": "image/jpeg"}},
                ),
                (
                    {"path": r"/hips_source/Norder0/Dir0/Npix6.jpg"},
                    {"body": img_data, "headers": {"content-type": "image/jpeg"}},
                )
            ]
            with mock_httpd(("localhost", 42423), expected_reqs):
                resp = app.get("/tms/1.0.0/hips/geodetic/1/0/0.png")
                assert resp.content_type == "image/png"
                img = Image.open(BytesIO(resp.body))
                # Most image is red, except on upper-right corner
                # open('/tmp/tmp.png', 'wb').write(resp.body)
                assert img.width == 256
                assert img.height == 256
                assert hashlib.md5(resp.body).hexdigest() in ('bd3dff3ccb40f9cb3edabf60c23bc86f', 'b57fe370becf5892b08e79ef2b1d2acb')

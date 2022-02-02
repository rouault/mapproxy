# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

import hashlib

from io import BytesIO

from mapproxy.compat.image import Image
from mapproxy.test.http import mock_httpd
from mapproxy.test.image import tmp_image
from mapproxy.test.system import SysTest

import os
import os.path
import pytest

class TestHIPSService(SysTest):

    @pytest.fixture(scope="class")
    def config_file(self):
        return "hips_service.yaml"

    def test_bad_path(self, app):
        resp = app.get("/hips", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Should be /hips/layer/...'


    def test_bad_path2(self, app):
        resp = app.get("/hips/direct", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Should be /hips/layer/...'


    def test_properties(self, app):
        resp = app.get("/hips/direct/properties")
        assert resp.content_type == "text/plain"

        # Remove hips_release_date= whose value is varying
        pos = resp.text.find('hips_release_date=')
        assert pos > 0
        pos2 = resp.text.find('\n', pos)
        assert pos2 == pos + len('hips_release_date=YYYY-MM-DDTHH:MM:SSZ')
        text = resp.text[0:pos] + resp.text[pos2+1:]

        assert text == 'creator_did=ivo://example.com/unknown_resource_FIXME\nobs_title=Direct Layer\ndataproduct_type=image\nhips_version=1.4\nhips_status=public master clonableOnce\nhips_tile_format=png jpeg\nhips_order=5\nhips_tile_width=512\nhips_frame=planet\ndataproduct_subtype=color\n'


    def test_bad_layer(self, app):
        resp = app.get("/hips/bad_layer/properties", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Unhandled layer name bad_layer'


    def test_properties_disabled_layer(self, app):
        resp = app.get("/hips/disabled/properties", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'HIPS not enabled for layer disabled'


    def test_bad_path3(self, app):
        resp = app.get("/hips/direct/unexpected", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Component unexpected should start with Norder'


    def test_allsky_not_existing(self, app):
        resp = app.get("/hips/direct/Norder3/Allsky.png", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Allsky requests should be pre-generated with mapproxy-util hips-allsky'


    def test_allsky_head_not_existing(self, app):
        resp = app.head("/hips/direct/Norder3/Allsky.png", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Allsky requests should be pre-generated with mapproxy-util hips-allsky'


    def test_allsky_existing(self, app, cache_dir):
        cache_dir_norder = os.path.join(cache_dir, 'direct', 'Norder3')
        os.makedirs(cache_dir_norder)
        open(os.path.join(cache_dir_norder, 'Allsky.png'), 'wb').write(b'should be some png content')
        open(os.path.join(cache_dir_norder, 'Allsky.jpg'), 'wb').write(b'should be some jpeg content')

        resp = app.head("/hips/direct/Norder3/Allsky.png")
        assert resp.content_type == "image/png"

        resp = app.head("/hips/direct/Norder3/Allsky.jpg")
        assert resp.content_type == "image/jpeg"

        resp = app.get("/hips/direct/Norder3/Allsky.png")
        assert resp.content_type == "image/png"
        assert resp.body == b'should be some png content'

        resp = app.get("/hips/direct/Norder3/Allsky.jpg")
        assert resp.content_type == "image/jpeg"
        assert resp.body == b'should be some jpeg content'


    def test_bad_path4(self, app):
        resp = app.get("/hips/direct/Norder3", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Should be /hips/layer/NorderK/DirD/NpixN.ext'


    def test_bad_path5(self, app):
        resp = app.get("/hips/direct/Norder500/Dir0/Npix0.png", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Invalid norder=500'


    def test_bad_path6(self, app):
        resp = app.get("/hips/direct/Norder3/invalid/Npix0.png", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Component invalid should start with Dir'


    def test_bad_path7(self, app):
        resp = app.get("/hips/direct/Norder3/Dir1/Npix0.png", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Inconsistent Dir and Npix'


    def test_bad_path8(self, app):
        resp = app.get("/hips/direct/Norder3/Dir0/invalid.png", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Component invalid.png should start with Npix'


    def test_bad_path9(self, app):
        resp = app.get("/hips/direct/Norder0/Dir0/Npix12.png", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Invalid npix=12'


    def test_bad_path10(self, app):
        resp = app.get("/hips/direct/Norder0/Dir0/Npix0.invalid", status=404)
        assert resp.content_type == "text/plain"
        assert resp.text == 'Bath path for /hips. Unhandled extension=invalid'


    def test_valid_norder0_npix0(self, app):
        with tmp_image((785, 785), format="png", color=(255, 0, 0)) as img:
            expected_req = (
                {"path": r"/service?layers=bar&bbox=0.0,0.0,90.0,90.0&width=785&height=785&srs=EPSG%3A4326&format=image%2Fpng&request=GetMap&version=1.1.1&service=WMS&styles="},
                {"body": img.read(), "headers": {"content-type": "image/png"}},
            )
            with mock_httpd(("localhost", 42423), [expected_req]):
                resp = app.get("/hips/direct/Norder0/Dir0/Npix0.png")
                assert resp.content_type == "image/png"
                # Red image
                # open('/tmp/tmp.png', 'wb').write(resp.body)
                img = Image.open(BytesIO(resp.body))
                assert img.width == 512
                assert img.height == 512
                assert hashlib.md5(resp.body).hexdigest() in ('e5893f926c84fb46ff1ef0fddf37b19d', 'e5893f926c84fb46ff1ef0fddf37b19d', '83db47dc57b237f47d1b3a81bb910e00')


    def test_valid_norder1_npix16(self, app):
        with tmp_image((730, 730), format="png", color=(255, 0, 0)) as img:
            expected_req = (
                {"path": r"/service?layers=bar&bbox=-22.5,-41.81031489577862,22.5,0.0&width=730&height=730&srs=EPSG%3A4326&format=image%2Fpng&request=GetMap&version=1.1.1&service=WMS&styles="},
                {"body": img.read(), "headers": {"content-type": "image/png"}},
            )
            with mock_httpd(("localhost", 42423), [expected_req]):
                resp = app.get("/hips/direct/Norder1/Dir0/Npix16.png")
                assert resp.content_type == "image/png"
                # Red image
                # open('/tmp/tmp.png', 'wb').write(resp.body)
                img = Image.open(BytesIO(resp.body))
                assert img.width == 512
                assert img.height == 512
                assert hashlib.md5(resp.body).hexdigest() in ('e5893f926c84fb46ff1ef0fddf37b19d', 'e5893f926c84fb46ff1ef0fddf37b19d', '83db47dc57b237f47d1b3a81bb910e00')


class TestHIPSServiceResamplingBilinear(SysTest):

    @pytest.fixture(scope="class")
    def config_file(self):
        return "hips_service_resampling_bilinear.yaml"


    def test_valid_norder0_npix0(self, app):
        with tmp_image((785, 785), format="png", color=(255, 0, 0)) as img:
            expected_req = (
                {"path": r"/service?layers=bar&bbox=0.0,0.0,90.0,90.0&width=785&height=785&srs=EPSG%3A4326&format=image%2Fpng&request=GetMap&version=1.1.1&service=WMS&styles="},
                {"body": img.read(), "headers": {"content-type": "image/png"}},
            )
            with mock_httpd(("localhost", 42423), [expected_req]):
                resp = app.get("/hips/direct/Norder0/Dir0/Npix0.png")
                assert resp.content_type == "image/png"
                # Red image
                # open('/tmp/tmp.png', 'wb').write(resp.body)
                img = Image.open(BytesIO(resp.body))
                assert img.width == 512
                assert img.height == 512
                assert hashlib.md5(resp.body).hexdigest() in ('e5893f926c84fb46ff1ef0fddf37b19d', 'e5893f926c84fb46ff1ef0fddf37b19d', '83db47dc57b237f47d1b3a81bb910e00')


class TestHIPSServiceResamplingBicubic(SysTest):

    @pytest.fixture(scope="class")
    def config_file(self):
        return "hips_service_resampling_bicubic.yaml"


    def test_valid_norder0_npix0(self, app):
        with tmp_image((785, 785), format="png", color=(255, 0, 0)) as img:
            expected_req = (
                {"path": r"/service?layers=bar&bbox=0.0,0.0,90.0,90.0&width=785&height=785&srs=EPSG%3A4326&format=image%2Fpng&request=GetMap&version=1.1.1&service=WMS&styles="},
                {"body": img.read(), "headers": {"content-type": "image/png"}},
            )
            with mock_httpd(("localhost", 42423), [expected_req]):
                resp = app.get("/hips/direct/Norder0/Dir0/Npix0.png")
                assert resp.content_type == "image/png"
                # Red image
                # open('/tmp/tmp.png', 'wb').write(resp.body)
                img = Image.open(BytesIO(resp.body))
                assert img.width == 512
                assert img.height == 512
                assert hashlib.md5(resp.body).hexdigest() in ('e5893f926c84fb46ff1ef0fddf37b19d', 'e5893f926c84fb46ff1ef0fddf37b19d', '83db47dc57b237f47d1b3a81bb910e00')


class TestHIPSServiceCustomProperties(SysTest):

    @pytest.fixture(scope="class")
    def config_file(self):
        return "hips_service_custom_properties.yaml"

    def test_properties(self, app):
        resp = app.get("/hips/direct/properties")
        assert resp.content_type == "text/plain"
        assert resp.text == 'creator_did=my_creator_did\nobs_title=my_obs_title\ndataproduct_type=image\nhips_version=1.4\nhips_release_date=2021-12-31T12:34:56Z\nhips_status=my_hips_status\nhips_tile_format=jpeg\nhips_order=6\nhips_tile_width=512\nhips_frame=mars\ndataproduct_subtype=color\nfoo=bar\n'

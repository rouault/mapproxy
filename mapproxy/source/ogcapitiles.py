import copy
import json
import sys
from threading import Lock

from mapproxy.client.http import HTTPClientError
from mapproxy.config.loader import CacheConfiguration
from mapproxy.grid.tile_grid import tile_grid_from_ogc_tile_matrix_set
from mapproxy.image.opts import ImageOptions
from mapproxy.layer import BlankImage, MapExtent, DefaultMapExtent, MapLayer, CacheMapLayer
from mapproxy.source import SourceError
from mapproxy.srs import ogc_crs_url_to_auth_code
from mapproxy.util.py import reraise_exception

import logging

log = logging.getLogger("mapproxy.source.ogcapitiles")


def _find_href_in_links(links, rel, preferred_media_type):
    href = None
    for link in links:
        if link["rel"] == rel:
            if "type" in link and link["type"] == preferred_media_type:
                href = link["href"]
                break
            elif "type" not in link:
                if href is None:
                    href = link["href"]
    return href


def _normalize_srs_code(srs_code):
    return "EPSG:4326" if srs_code == "OGC:CRS84" else srs_code


class OGCAPITilesSource(MapLayer):
    def __init__(
        self,
        configuration_context,
        landingpage_url,
        collection,
        http_client,
        coverage=None,
        image_opts=None,
        error_handler=None,
        res_range=None,
    ):
        MapLayer.__init__(self, image_opts=image_opts)
        self.configuration_context = copy.copy(configuration_context)
        self.configuration_context.grids = {}
        self.landingpage_url = landingpage_url.rstrip("/")
        self.collection = collection
        self.http_client = http_client
        self.image_opts = image_opts or ImageOptions()
        self.coverage = coverage
        if self.coverage:
            self.extent = MapExtent(self.coverage.bbox, self.coverage.srs)
        else:
            self.extent = DefaultMapExtent()
        self.res_range = res_range
        self.error_handler = error_handler

        self.init_done = False
        self.map_crs_to_tilesets_list = {}
        self.map_srs_to_grid_and_template_url = {}
        self.lock = Lock()

    def _build_url(self, href):
        if href.startswith("/"):
            schema, landingpage_url = self.landingpage_url.split("://", 1)
            if "/" in landingpage_url:
                host = landingpage_url.split("/", 1)[0]
            else:
                host = landingpage_url
            return schema + "://" + host + href

        return href

    def _initialize(self):
        with self.lock:
            if self.init_done:
                return
            self.init_done = True

            headers = {"Accept": "application/json"}

            url = self.landingpage_url
            if self.collection:
                url += "/collections/" + self.collection
            try:
                resp = self.http_client.open(url, headers=headers)
            except HTTPClientError as e:
                log.warning(f"Cannot retrieve {url}: %s", e)
                reraise_exception(SourceError(e.args[0]), sys.exc_info())
            try:
                j = json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                log.warning(f"Cannot parse response to {url} as JSON: %s", e)
                reraise_exception(SourceError(e.args[0]), sys.exc_info())

            if "links" not in j:
                ex = SourceError(f"Could not retrieve 'links' in {url} response")
                log.error(ex)
                raise ex

            tilesets_map_href = _find_href_in_links(
                j["links"],
                "http://www.opengis.net/def/rel/ogc/1.0/tilesets-map",
                "application/json",
            )
            if not tilesets_map_href:
                ex = SourceError(
                    f"Could not retrieve a tilesets-map link in {url} response"
                )
                log.error(ex)
                raise ex

            tilesets_map_url = self._build_url(tilesets_map_href)
            try:
                resp = self.http_client.open(tilesets_map_url, headers=headers)
            except HTTPClientError as e:
                log.warning(f"Cannot retrieve {tilesets_map_url}: %s", e)
                reraise_exception(SourceError(e.args[0]), sys.exc_info())
            try:
                j = json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                log.warning(
                    f"Cannot parse response to {tilesets_map_url} as JSON: %s", e
                )
                reraise_exception(SourceError(e.args[0]), sys.exc_info())

            if "tilesets" not in j:
                ex = SourceError(
                    f"Could not retrieve 'tilesets' in {tilesets_map_url} response"
                )
                log.error(ex)
                raise ex

            for tileset in j["tilesets"]:
                if tileset["dataType"] != "map":
                    continue
                crs = _normalize_srs_code(ogc_crs_url_to_auth_code(tileset["crs"]))
                if crs not in self.map_crs_to_tilesets_list:
                    self.map_crs_to_tilesets_list[crs] = []
                self.map_crs_to_tilesets_list[crs].append(tileset)

    def _get_grid_and_template_url_from_tileset(self, tileset, image_mime_type):
        links = tileset["links"]
        tiling_scheme_href = _find_href_in_links(
            links,
            "http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme",
            "application/json",
        )
        if not tiling_scheme_href:
            ex = SourceError(
                f"Could not retrieve a 'tiling-scheme' link for tileset {tileset}"
            )
            log.error(ex)
            raise ex

        tiling_scheme_url = self._build_url(tiling_scheme_href)

        tileset_href = _find_href_in_links(links, "self", "application/json")
        if not tiling_scheme_href:
            ex = SourceError(f"Could not retrieve a 'self' link for tileset {tileset}")
            log.error(ex)
            raise ex

        tileset_url = self._build_url(tileset_href)

        headers = {"Accept": "application/json"}
        try:
            resp = self.http_client.open(tiling_scheme_url, headers=headers)
        except HTTPClientError as e:
            log.warning(f"Cannot retrieve {tiling_scheme_url}: %s", e)
            reraise_exception(SourceError(e.args[0]), sys.exc_info())
        try:
            tile_matrix_set = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            log.warning(f"Cannot parse response to {tiling_scheme_url} as JSON: %s", e)
            reraise_exception(SourceError(e.args[0]), sys.exc_info())
        grid = tile_grid_from_ogc_tile_matrix_set(tile_matrix_set)

        try:
            resp = self.http_client.open(tileset_url, headers=headers)
        except HTTPClientError as e:
            log.warning(f"Cannot retrieve {tileset_url}: %s", e)
            reraise_exception(SourceError(e.args[0]), sys.exc_info())
        try:
            tileset_full = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            log.warning(f"Cannot parse response to {tileset_url} as JSON: %s", e)
            reraise_exception(SourceError(e.args[0]), sys.exc_info())

        template_href = _find_href_in_links(
            tileset_full["links"], "item", image_mime_type
        )
        if not template_href:
            ex = SourceError(
                f"Could not retrieve a tile template URL for tileset {tileset}"
            )
            log.error(ex)
            raise ex

        return grid, self._build_url(template_href)

    def _get_grid_and_template_url_from_srs(self, query_srs, image_mime_type):
        srs_code = _normalize_srs_code(query_srs.srs_code)
        key = srs_code + "/" + image_mime_type

        with self.lock:
            grid_and_template_url = self.map_srs_to_grid_and_template_url.get(key, None)
            if grid_and_template_url:
                return grid_and_template_url

            if srs_code in self.map_crs_to_tilesets_list:
                for tileset in self.map_crs_to_tilesets_list[srs_code]:
                    try:
                        grid_and_template_url = (
                            self._get_grid_and_template_url_from_tileset(
                                tileset, image_mime_type
                            )
                        )
                        break
                    except Exception as e:
                        log.info(f"Exception while evaluating tileset {tileset}: {e}")
                        pass

            else:
                # Iterate over all potential tilesets and select the first one
                # we can parse
                for tileset_crs, tileset_list in self.map_crs_to_tilesets_list.items():
                    for tileset in tileset_list:
                        try:
                            grid_and_template_url = (
                                self._get_grid_and_template_url_from_tileset(
                                    tileset, image_mime_type
                                )
                            )
                            break
                        except Exception as e:
                            log.info(
                                f"Exception while evaluating tileset {tileset}: {e}"
                            )
                            pass
                    if grid_and_template_url:
                        break

            if grid_and_template_url is None:
                ex = SourceError(f"Cannot find a valid tile matrix set for {query_srs}")
                log.error(ex)
                raise ex

            self.map_srs_to_grid_and_template_url[key] = grid_and_template_url
            return grid_and_template_url

    def get_map(self, query):
        print(query)

        self._initialize()
        image_mime_type = "image/" + query.format
        grid, template_url = self._get_grid_and_template_url_from_srs(
            query.srs, image_mime_type
        )

        if grid.tile_size == query.size and grid.srs == query.srs:
            if self.res_range and not self.res_range.contains(
                query.bbox, query.size, query.srs
            ):
                raise BlankImage()
            if self.coverage and not self.coverage.intersects(query.bbox, query.srs):
                raise BlankImage()

            _bbox, grid, tiles = grid.get_affected_tiles(query.bbox, query.size)

            if grid == (1, 1):
                x, y, z = next(tiles)
                tile_url = (
                    template_url.replace("{tileMatrix}", str(z))
                    .replace("{tileRow}", str(y))
                    .replace("{tileCol}", str(x))
                )
                print(tile_url)
                try:
                    return self.http_client.open_image(tile_url)
                except HTTPClientError as e:
                    if self.error_handler:
                        resp = self.error_handler.handle(e.response_code, query)
                        if resp:
                            return resp
                    log.warning("could not retrieve tile: %s", e)
                    reraise_exception(SourceError(e.args[0]), sys.exc_info())

        class FakeGridConfig:
            def __init__(self, grid):
                self.grid = grid
                self.conf = {"srs": grid.srs.srs_code}

            def tile_grid(self):
                return self.grid

        class FakeSourceConfig:
            def __init__(self, sourceObj):
                self.sourceObj = sourceObj

            def source(self, params=None):
                return self.sourceObj

        cache_conf = {
            "name": "my_cache",
            "sources": ["this_source"],
            "grids": [grid.name],
        }
        configuration_context = copy.copy(self.configuration_context)
        configuration_context.grids = {grid.name: FakeGridConfig(grid)}
        configuration_context.sources = {"this_source": FakeSourceConfig(self)}
        cache_config = CacheConfiguration(cache_conf, configuration_context)
        _, _, tile_manager = cache_config.caches()[0]
        cacheMapLayer = CacheMapLayer(tile_manager)

        # query.tiled_only = True
        return cacheMapLayer.get_map(query)

# SPDX-License-Identifier: MIT
# Copyright (C) 2021 CNES

import optparse
import sys

from mapproxy.config import local_base_config
from mapproxy.config.loader import load_configuration, ConfigurationError
from mapproxy.service.hips import HIPSServer

def hipsallsky_command(args=None):
    parser = optparse.OptionParser("%prog hips-allsky [options] -f mapproxy_conf -l layer")
    parser.add_option("-f", "--mapproxy-conf", dest="mapproxy_conf",
        help="MapProxy configuration.")
    parser.add_option("-l", "--layer", dest="layer", help="Layer")
    parser.add_option("-o", "--norder", dest="norder", type=int, default=3, help="Order")
    parser.add_option("-c", "--concurrency", type="int",
                      dest="concurrency", default=10,
                      help="number of parallel processes")

    from mapproxy.script.util import setup_logging
    import logging
    setup_logging(logging.INFO)

    if args:
        args = args[1:] # remove script name

    (options, args) = parser.parse_args(args)
    if not options.mapproxy_conf or not options.layer:
        parser.print_help()
        sys.exit(1)

    try:
        proxy_configuration = load_configuration(options.mapproxy_conf)
    except IOError as e:
        print('ERROR: ', "%s: '%s'" % (e.strerror, e.filename), file=sys.stderr)
        sys.exit(2)
    except ConfigurationError as e:
        print(e, file=sys.stderr)
        print('ERROR: invalid configuration (see above)', file=sys.stderr)
        sys.exit(2)

    with local_base_config(proxy_configuration.base_config):
        for service in proxy_configuration.configured_services():
            if isinstance(service, HIPSServer):
                service.generate_allsky_file(options.mapproxy_conf,
                                             options.layer,
                                             options.norder,
                                             options.concurrency)


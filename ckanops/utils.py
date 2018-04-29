#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import urllib2
import urlparse
import ssl


def dcat_to_utf8_dict(url):
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return json.loads(urllib2.urlopen(url, context=ctx).read().decode('utf-8'))


def get_extension_from_url(u):
    # SEE: https://docs.python.org/2/library/urlparse.html
    path = urlparse(u)[2]
    return path.split('.')[-1].strip().lower()

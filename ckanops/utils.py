#!/usr/bin/env python
# -*- coding: utf-8 -*-


def dcat_to_utf8_dict(url):
    return json.loads(urllib2.urlopen(url).read().decode('utf-8'))


def get_extension_from_url(u):
    # SEE: https://docs.python.org/2/library/urlparse.html
    path = urlparse(u)[2]
    return path.split('.')[-1].strip().lower()

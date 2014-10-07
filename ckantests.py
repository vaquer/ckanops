#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import ckanapi
from ckanapi.errors import CKANAPIError
import ckanops
import webtest
import converters
import urllib2
import json
import random


host = os.environ['CKAN_HOST']
token = os.environ['CKAN_API_TOKEN']


test_app = webtest.TestApp(host)
demo = ckanapi.TestAppCKAN(test_app, apikey=token)


def create_should_succeed():
    try:
        pkg = demo.action.package_create(name='my-dataset', title='not going to work')
    except ckanapi.NotAuthorized:
        print 'denied'


def list_datasets():
    return demo.action.package_list()


def list_groups():
    return demo.action.group_list(id='data-explorer')


def package(_id):
    return demo.action.package_show(id=_id)


def package_to_dcat(package):
    return converters.ckan_to_dcat(package)


def dcat_to_utf8_dict(url):
    return json.loads(urllib2.urlopen(url).read().decode('utf-8'))


# pkg = package('ciclones')
# print pkg
# print package_to_dcat(pkg)

# print 'Datasets'
# print list_datasets()
# print 'Groups'
# print list_groups()

catalog = dcat_to_utf8_dict("http://adela.datos.gob.mx/conapo/catalogo.json")
print catalog.get('title')

for dataset in catalog.get('dataset', []):
    d = converters.dcat_to_ckan(dataset)
    d['name'] = 'foo-%s' % random.randrange(0,31337)
    print "Creating dataset '%s'" % d['title'], "with %d resources" % len(d['resources'])
    print "---"
    print d
    ckanops.create_dataset(demo, d)



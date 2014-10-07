#!/usr/bin/env python

import os
import ckanapi
from ckanapi.errors import CKANAPIError
import ckanops
import webtest
import converters
import urllib2
import json


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
    return urllib2.urlopen(url).read().decode('utf-8')


pkg = package('ciclones')
# print pkg
# print package_to_dcat(pkg)

# print 'Datasets'
# print list_datasets()
# print 'Groups'
# print list_groups()

print dcat_to_utf8_dict("http://adela.datos.gob.mx/nafin/catalogo.json")


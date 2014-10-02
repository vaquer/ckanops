#!/usr/bin/env python

import os
import ckanapi
from ckanapi.errors import CKANAPIError
import ckanops
import webtest
import converters


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
    print demo.action.package_list()


def list_groups():
    print demo.action.group_list(id='data-explorer')


def package(_id):
    return demo.action.package_show(id=_id)


def package_to_dcat(package):
    return converters.ckan_to_dcat(package)


pkg = package('ciclones')
print pkg
print package_to_dcat(pkg)


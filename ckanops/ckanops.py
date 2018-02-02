#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import operator
import ckanapi
from ckanapi.errors import CKANAPIError
from urlparse import urlparse

import sys
import getopt
import datetime

import urllib2
import json
import converters
import munge
from dataset import update_dataset, create_dataset


def upsert_dataset(remote, dataset):
    print 'upsert_dataset', dataset
    if get_package(remote, dataset['name']):
        print 'EXISTE'
        new_pkg = update_dataset(remote, dataset)
    else:
        print 'NO EXISTE'
        new_pkg = create_dataset(remote, dataset)
    return new_pkg


def get_package(remote, _id):
    pkg = None
    try:
        pkg = remote.action.package_show(id=_id)
        print 'Log:Dataset', pkg
    except ckanapi.NotFound, e:
        print 'get_package: ', e
    return pkg


def get_dataset_groups(remote, name):
    groups = []
    try:
        dataset = remote.action.package_show(id=name)
        print 'Dataset Groups', dataset['groups']
        groups = dataset['groups']
    except ckanapi.NotFound, e:
        print 'get_package: ', e
    return groups


# Looks for datasets matching attributes defined as a string of the form field:term
def find_datasets_with_query(remote, query):
    datasets = remote.call_action('package_search', data_dict={'fq': query})
    return datasets


def update_group_for_datasets(remote, datasets_names, group):
    print 'Log:groups', datasets_names, groups
    try:
        for d in datasets_names:
            remote.call_action('member_create', data_dict={
                'id': group,
                'object': d,
                'object_type': 'package',
                'capacity': 'member'})
    except CKANAPIError, e:
        print 'update_group_for_datasets: ', e

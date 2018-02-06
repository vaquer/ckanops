#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ckanapi
from ckanapi import CKANAPIError


def create_dataset(remote, dataset):
    pkg = None
    try:
        pkg = remote.call_action('package_create', data_dict=dataset)
    except ckanapi.NotAuthorized, e:
        print 'create_dataset: ', e
    except ckanapi.ValidationError, e:
        print 'create_dataset: ', e
    except CKANAPIError, e:
        print 'create_dataset: ', e
    return pkg


def update_dataset(remote, dataset, attributes={}):
    pkg = None
    try:
        # Remove duplicate metadata fields
        # NOTE: This happened in a few experiments in the extra fields
        unique_extras = set(str(e) for e in dataset['extras'])
        dataset['extras'] = [eval(e) for e in unique_extras]

        unique_groups = set(str(e) for e in dataset['groups'])
        print 'unique_groups', unique_groups
        dataset['groups'] = [eval(e) for e in unique_groups]

        # Merge new attributes and update package
        dataset = dict(dataset.items() + attributes.items())
        pkg = remote.call_action('package_update',
                                 data_dict=dataset)
    except CKANAPIError, e:
        print 'update_dataset: ', e
    return pkg


# Extract DCAT publisher name from metadata
def get_dcat_publisher(dataset):
    extra_metadata = dataset.get('extras', [])
    for e in extra_metadata:
        if e['key'] == 'dcat_publisher_name':
            return e['value']
    print "Couldn't find dcat fields metadata for", "'%s'" % dataset['title']
    return ''


# Update the owner package to be the same as DCAT publisher
# NOTE: publisher name comes in uppercase, org name in lowercase
def update_dataset_owner_as_dcat_publisher(remote, dataset):
    organizations = remote.action.organization_list()
    publisher = get_dcat_publisher(dataset).lower()
    # TODO: owner_org is a hash id so it will never match publisher name
    if publisher in organizations and dataset['owner_org'] != publisher:
        success = update_dataset(remote,
                                 dataset,
                                 {'owner_org': publisher})
        if success:
            print publisher.upper(), "now owns", "'%s'" % dataset['title']


def clear_dataset_license(remote, dataset):
    attributes = {'license_id': 'notspecified'}
    if dataset['license_id'] == 'notspecified':
        return True
    if update_dataset(remote, dataset, attributes):
        title = "'%s'" % dataset['title']
        print "Updated", dataset['license_id'], "to notspecified", "for", title


def update_bbox(remote, dataset, region):
    regions = {
        'Mexico': [
            {'key': 'spatial-text', 'value': 'Mexico'},
            {'key': 'spatial', 'value': '{"type": "Polygon","coordinates": [[[-118.30078125,13.667338259654947], \
            [-118.30078125,33.35806161277885], [-85.95703125,33.35806161277885], [-85.95703125,13.667338259654947], \
            [-118.30078125,13.667338259654947]]]}'},
            {'key': 'spatial-uri', 'value': 'http://www.geonames.org/3996063'}
        ]
    }

    print "Updating", "'%s'" % dataset['title']
    extras = dataset['extras']
    for h in regions[region]:
        extras.append(h)
    update_dataset(remote, dataset, {'extras': extras})


def update_group_for_datasets(remote, datasets_names, group):
    try:
        for d in datasets_names:
            remote.call_action('member_create', data_dict={
                'id': group,
                'object': d,
                'object_type': 'package',
                'capacity': 'member'})
    except CKANAPIError, e:
        print 'update_group_for_datasets: ', e

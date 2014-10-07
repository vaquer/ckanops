#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import operator
import ckanapi
from ckanapi.errors import CKANAPIError
from urlparse import urlparse


host = os.environ['CKAN_HOST']
token = os.environ['CKAN_API_TOKEN']


def create_dataset(remote, dataset):
    pkg = None
    try:
        pkg = remote.call_action('package_create', data_dict=dataset)
    except ckanapi.NotAuthorized, e:
        print e
    except ckanapi.ValidationError, e:
        print e
    except CKANAPIError, e:
        print e
    return pkg


def update_dataset(remote, dataset, attributes={}):
    pkg = None
    try:
        # Remove duplicate metadata fields
        # NOTE: This happened in a few experiments in the extra fields
        unique_extras = set(str(e) for e in dataset['extras'])
        dataset['extras'] = [eval(e) for e in unique_extras]

        # Merge new attributes and update package
        dataset = dict(dataset.items() + attributes.items())
        pkg = remote.call_action('package_update',
                data_dict=dataset,
                apikey=token)
    except CKANAPIError, e:
        print "CKANAPIError for dataset", "'%s'" % dataset['title']
        print e
    return pkg


def update_resource(remote, resource, attributes):
    try:
        # Merge new attributes and update package
        resource = dict(resource.items() + attributes.items())
        remote.call_action('resource_update',
                data_dict=resource,
                apikey=token)
        return True
    except CKANAPIError, e:
        print "CKANAPIError for resource", "'%s'" % resource['url']
        print e
    return False


def upsert_dataset(remote, dataset):
    if get_package(remote, dataset['name']):
        new_pkg = update_dataset(remote, dataset)
    else:
        new_pkg = create_dataset(remote, dataset)
    return new_pkg


def get_package(remote, _id):
    pkg = None
    try:
        pkg = remote.action.package_show(id=_id)
    except ckanapi.NotFound, e:
        print e
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
    if (publisher in organizations and dataset['owner_org'] != publisher):
        success = update_dataset(remote,
                dataset,
                { 'owner_org': publisher })
        if success:
            print publisher.upper(), "now owns", "'%s'" % dataset['title']


def clear_dataset_license(remote, dataset):
    attributes = { 'license_id': 'notspecified' }
    if dataset['license_id'] == 'notspecified':
        return True
    if update_dataset(remote, dataset, attributes):
        title = "'%s'" % dataset['title']
        print "Updated", dataset['license_id'], "to notspecified", "for", title


def update_resources_format_based_on_extension(remote, dataset):
    formats = {
        'csv':      'CSV',
        'geojson':  'GeoJSON',
        'gif':      'GIF',
        'json':     'JSON',
        'kml+xml':  'KML',
        'kmz':      'KMZ',
        'pdf':      'PDF',
        'png':      'PNG',
        'xls':      'XLS',
        'xlsx':     'XLSX',
        'xml':      'XML',
        'zip':      'ZIP',
    }
    for r in dataset['resources']:
        if len(r['format']) > 0:
            continue
        extension = get_extension_from_url(r['url'])
        try:
            f = formats[extension]
            update_resource(remote,
                    r,
                    { 'format': f })
        except KeyError, e:
            print extension, "is not a known file extension"


def get_extension_from_url(u):
    # SEE: https://docs.python.org/2/library/urlparse.html
    path = urlparse(u)[2]
    return path.split('.')[-1].strip().lower()


def update_bbox(remote, dataset, region):
    regions = {
        'Mexico': [
            {'key':'spatial-text', 'value':'Mexico'},
            {'key':'spatial', 'value':'{"type":"Polygon","coordinates":[[[-118.30078125,13.667338259654947],[-118.30078125,33.35806161277885],[-85.95703125,33.35806161277885],[-85.95703125,13.667338259654947],[-118.30078125,13.667338259654947]]]}'},
            {'key':'spatial-uri', 'value':'http://www.geonames.org/3996063'}
        ]
    }

    print "Updating", "'%s'" % dataset['title']
    extras = dataset['extras']
    for h in regions[region]:
        extras.append(h)
    update_dataset(remote, dataset, {'extras': extras})


def tags_covered_by_an_organization(o):
    tags = []
    for p in o['packages']:
        tag_names = [t['name'].encode('UTF8') for t in p['tags']]
        tags.append(tag_names)
    if len(tags) == 0:
        return []
    else:
        return reduce(operator.add, tags)


def all_tags_by_organization(remote):
    tags_by_organization = {}

    organizations_names = remote.action.organization_list()
    for name in organizations_names:
        organization = remote.action.organization_show(id=name)
        tags = tags_covered_by_an_organization(organization)
        tags_by_organization[name] = tags

    return tags_by_organization


# shows people in charge per dataset
def who_is_in_charge(remote):
    return 1


# deletes duplicated harvester jobs
def wtf_harvester(remote):
    return 1


def main():
    remote = ckanapi.RemoteCKAN(host, user_agent='ckanops/1.0', apikey=token)

    datasets = remote.action.package_list()

    # all_tags = all_tags_by_organization(remote)
    # datasets = [d['name'] for d in remote.action.package_search(q='nacional')['results']]

    print "Will update", len(datasets), "datasets"
    for d in datasets:
        # Get dataset metadata
        pkg = remote.action.package_show(id=d)
        # Could stumble upon harvesters
        if pkg['type'] == 'dataset':
            # update_dataset_owner_as_dcat_publisher(remote, pkg)
            # clear_dataset_license(remote, pkg)
            update_resources_format_based_on_extension(remote, pkg)
            # update_bbox(remote, pkg, 'Mexico')



if __name__ == "__main__":
    main()



#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import operator
import ckanapi
from ckanapi.errors import CKANAPIError
from urlparse import urlparse

import sys
import getopt

import urllib2
import json
import converters
import munge


host = os.environ['CKAN_HOST']
token = os.environ['CKAN_API_TOKEN']


def dcat_to_utf8_dict(url):
    return json.loads(urllib2.urlopen(url).read().decode('utf-8'))


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

        # Merge new attributes and update package
        dataset = dict(dataset.items() + attributes.items())
        pkg = remote.call_action('package_update',
                data_dict=dataset,
                apikey=token)
    except CKANAPIError, e:
        print 'update_dataset: ', e
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
        print 'update_resource: ', e
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
        print 'get_package: ', e
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


# Looks for datasets matching attributes defined as a string of the form field:term
def find_datasets_with_query(remote, query):
    datasets = remote.call_action('package_search', data_dict={'fq':query})
    return datasets


# Looks for resources matching attributes defined as a string of the form field:term
def find_resources_with_query(remote, query):
    resources = remote.call_action('resource_search', data_dict={'query': query})
    return resources


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
            print 'update_resources_format_based_on_extension: ', extension, "is not a known file extension"


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


def update_group_for_datasets(remote, datasets_names, group):
    try:
        for d in datasets_names:
            remote.call_action('member_create', data_dict={
                'id':group,
                'object':d,
                'object_type':'package',
                'capacity':'member'})
    except CKANAPIError, e:
        print 'update_group_for_datasets: ', e


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


def get_organization_datasets(remote, org_id):
    organization = remote.action.organization_show(id=org_id)
    return map(lambda x:x["name"], organization['packages'])


def delete_organization_datasets(remote, org_id):
    datasets_names = get_organization_datasets(remote, org_id)
    for name in datasets_names:
        try:
            remote.action.package_delete(id=name)
        except CKANAPIError, e:
            print 'delete_organization_datasets: ', e


# shows people in charge per dataset
def who_is_in_charge(remote):
    return 1


# deletes duplicated harvester jobs
def wtf_harvester(remote):
    return 1


def usage():
    print 'Usage:'
    print sys.argv[0], '--datasets'
    print sys.argv[0], '--harvest <URI>'
    print sys.argv[0], '--purge-harvest <URI>'
    print sys.argv[0], '--find-datasets <field>:<value>'
    print sys.argv[0], '--find <field:value>'
    print sys.argv[0], '--replace <dataset|resource> <field> <old_value> <new_value>'
    print sys.argv[0], '--group <group> <dataset-name-1> <dataset-name-2> <...>'


def main(argv):
    remote = ckanapi.RemoteCKAN(host, user_agent='ckanops/1.0', apikey=token)

    try:
        opts, args = getopt.getopt(argv, "hds:p:q:f:r:g:", ["help", "datasets", "harvest=", "purge-harvest=", "find-datasets=", "find=", "replace=", "group="])
    except getopt.GetoptError, e:
        print str(e)
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-d", "--datasets"):
            datasets = remote.action.package_list()
            for d in datasets:
                print d
        elif opt in ("-s", "--harvest", "-p", "--purge-harvest"):
            catalog = dcat_to_utf8_dict(arg)

            # If purge mode is activated, then delete all org's datasets
            if opt in ("-p", "--purge-harvest"):
                org_name = catalog['dataset'][0]['publisher']['name']
                org_id = munge.munge_name(org_name)
                delete_organization_datasets(remote, org_id)

            for dcat_dataset in catalog.get('dataset', []):
                ckan_dataset = converters.dcat_to_ckan(dcat_dataset)
                ckan_dataset['name'] = munge.munge_title_to_name(ckan_dataset['title'])
                ckan_dataset['state'] = 'active'
                print 'Creating dataset "%s"' % ckan_dataset['title'], 'with %d resources' % len(ckan_dataset['resources'])
                new_dataset = upsert_dataset(remote, ckan_dataset)
                if new_dataset:
                    print 'Dataset upserted'
                else:
                    print 'Something went wrong'
        elif opt in ("-q", "--find-datasets"):
            datasets = find_datasets_with_query(remote, arg)
            for d in datasets['results']:
                print d['title']
        elif opt in ("-f", "--find"):
            resources = find_resources_with_query(remote, arg)
            for r in resources['results']:
                print r['name']
        elif opt in ("-r", "--replace"):
            # ✗ python ckanops.py --replace dataset license_id cc-by notspecified
            # ✗ python ckanops.py --replace resource format csvx CSV
            object_type = arg
            field       = args[0]
            old_value   = args[1]
            new_value   = args[2]
            query = "{0}:{1}".format(field, old_value)
            if object_type == 'dataset':
                datasets = find_datasets_with_query(remote, query)
                for d in datasets['results']:
                    update_dataset(remote, d, { field: new_value })
            if object_type == 'resource':
                resources = find_resources_with_query(remote, query)
                for r in resources['results']:
                    update_resource(remote, r, { field: new_value })
        elif opt in ("-g", "--group"):
            update_group_for_datasets(remote, args, arg)



if __name__ == "__main__":
    main(sys.argv[1:])



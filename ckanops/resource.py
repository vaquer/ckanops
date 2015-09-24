#!/usr/bin/env python
# -*- coding: utf-8 -*-


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
                            {'format': f})
        except KeyError, e:
            print 'update_resources_format_based_on_extension: ', extension, "is not a known file extension"

#!/usr/bin/env python
# -*- coding: utf-8 -*-


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
    return map(lambda x: x["name"], organization['packages'])


def delete_organization_datasets(remote, org_id):
    datasets_names = get_organization_datasets(remote, org_id)
    for name in datasets_names:
        try:
            remote.action.package_delete(id=name)
        except CKANAPIError, e:
            print 'delete_organization_datasets: ', e

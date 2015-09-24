# -*- coding: utf-8 -*-

__author__ = 'Rod Wilhelmy'
__email__ = 'rwilhelmy@gmail.com'
__version__ = '0.1.0'


from ckanops import *
from dataset import *
from organization import *
from resource import *
from utils import *


# def usage():
#     print 'Usage:'
#     print sys.argv[0], '--datasets'
#     print sys.argv[0], '--harvest <URI>'
#     print sys.argv[0], '--purge-harvest <URI>'
#     print sys.argv[0], '--find-datasets <field>:<value>'
#     print sys.argv[0], '--find <field:value>'
#     print sys.argv[0], '--replace <dataset|resource> <field> <old_value> <new_value>'
#     print sys.argv[0], '--group <group> <dataset-name-1> <dataset-name-2> <...>'
#
#
#def main(argv):
#    remote = ckanapi.RemoteCKAN(host, user_agent='ckanops/1.0', apikey=token)
#
#    try:
#        opts, args = getopt.getopt(argv, "hds:p:q:f:r:g:", ["help", "datasets", "harvest=", "purge-harvest=",
#                                                            "find-datasets=", "find=", "replace=", "group="])
#    except getopt.GetoptError, e:
#        print str(e)
#        usage()
#        sys.exit(2)
#    for opt, arg in opts:
#        if opt in ("-h", "--help"):
#            usage()
#            sys.exit()
#        elif opt in ("-d", "--datasets"):
#            datasets = remote.action.package_list()
#            for d in datasets:
#                print d
#        elif opt in ("-s", "--harvest", "-p", "--purge-harvest"):
#            catalog = dcat_to_utf8_dict(arg)
#
#            # If purge mode is activated, then delete all org's datasets
#            if opt in ("-p", "--purge-harvest"):
#                org_name = catalog['dataset'][0]['publisher']['name']
#                org_id = munge.munge_name(org_name)
#                delete_organization_datasets(remote, org_id)
#
#            for dcat_dataset in catalog.get('dataset', []):
#                ckan_dataset = converters.dcat_to_ckan(dcat_dataset)
#                ckan_dataset['name'] = munge.munge_title_to_name(ckan_dataset['title'])
#                ckan_dataset['state'] = 'active'
#                print 'Dataset "%s"' % ckan_dataset['title'], 'with %d resources' % len(ckan_dataset['resources'])
#                print datetime.datetime.utcnow()
#                new_dataset = upsert_dataset(remote, ckan_dataset)
#                if new_dataset:
#                    print 'Dataset upserted'
#                else:
#                    print 'Something went wrong'
#        elif opt in ("-q", "--find-datasets"):
#            datasets = find_datasets_with_query(remote, arg)
#            for d in datasets['results']:
#                print d['title']
#        elif opt in ("-f", "--find"):
#            resources = find_resources_with_query(remote, arg)
#            for r in resources['results']:
#                print r['name']
#        elif opt in ("-r", "--replace"):
#            # ✗ python ckanops.py --replace dataset license_id cc-by notspecified
#            # ✗ python ckanops.py --replace resource format csvx CSV
#            object_type = arg
#            field = args[0]
#            old_value = args[1]
#            new_value = args[2]
#            query = "{0}:{1}".format(field, old_value)
#            if object_type == 'dataset':
#                datasets = find_datasets_with_query(remote, query)
#                for d in datasets['results']:
#                    update_dataset(remote, d, {field: new_value})
#            if object_type == 'resource':
#                resources = find_resources_with_query(remote, query)
#                for r in resources['results']:
#                    update_resource(remote, r, {field: new_value})
#        elif opt in ("-g", "--group"):
#            update_group_for_datasets(remote, args, arg)
#
#
#if __name__ == "__main__":
#    main(sys.argv[1:])

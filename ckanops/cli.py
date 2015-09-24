import os
import click
import ckanapi
from utils import dcat_to_utf8_dict
from ckanops import munge, converters, upsert_dataset


HOST = os.getenv('CKAN_HOST')
TOKEN = os.getenv('CKAN_API_TOKEN')

remote = ckanapi.RemoteCKAN(HOST, user_agent='ckanops/1.0', apikey=TOKEN)


@click.group()
def main():
    pass


@click.command()
def datasets():
    d = remote.action.package_list()
    click.echo(d)


@click.command()
@click.argument('URI', metavar='<URI>')
@click.option('-p', '--purge', help='Delete all organizations datasets')
def harvest(uri, purge):
    catalog = dcat_to_utf8_dict(uri)
    for dcat_dataset in catalog.get('dataset', []):
        ckan_dataset = converters.dcat_to_ckan(dcat_dataset)
        ckan_dataset['name'] = munge.munge_title_to_name(ckan_dataset['title'])
        ckan_dataset['state'] = 'active'
        #click.echo('Dataset "%s"' % ckan_dataset['title'], 'with %d resources' % len(ckan_dataset['resources']))
        #click.echo(datetime.datetime.utcnow())
        new_dataset = upsert_dataset(remote, ckan_dataset)
        if new_dataset:
            click.echo('Datasets upserted')
        else:
            click.echo('Something went wrong')

    if purge:
        org_name = catalog['dataset'][0]['publisher']['name']
        org_id = munge.munge_name(org_name)
        delete_organization_datasets(remote, org_id)
        click.echo('Purged')

main.add_command(datasets)
main.add_command(harvest)

if __name__ == "__main__":
    main()

import os
import click
import ckanapi
import ckanops


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


main.add_command(datasets)

if __name__ == "__main__":
    main()

from setuptools import setup


def get_required_packages():
    with open('./requirements.txt', 'r') as file:
        return [line.rstrip('\n') for line in file.readlines()]


setup(
    name='replica',
    version='1.0',
    packages=['test', 'util', 'alert', 'cache', 'config', 'logger', 'tables', 'arangodb', 'clickhouse', 'replication',
              'replication.schema', 'replication.updater', 'replication.consumer', 'replication.producer',
              'replication.replicator'],
    url='http://ias-01.redbus.in/capi/UGC/-/tree/open-source/Arango-CH',
    license='',
    author='ajith.a',
    author_email='ajith.a@redbus.com',
    description='Auto replicate the data from Arango to Clickhouse',
    install_requires=get_required_packages()
)

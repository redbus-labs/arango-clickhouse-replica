import click

from replication.replicator.sync import synchronizer
from util.basic_utils import get_basic_utilities, CONFIG
from util.common import get_supported_producers


def validate_table_names(_, __, value):
    config = get_basic_utilities().get(CONFIG)
    allowed_tables = sorted(config['producer']['sync'])
    if value.strip() == '':
        tables = []
    else:
        tables = [table.strip() for table in value.split(',')]
    not_allowed = []
    for table in tables:
        if table not in allowed_tables:
            not_allowed.append(table)
    if len(not_allowed) > 0:
        raise click.BadParameter(
            'tables {} are not allowed.\nAllowed tables:\n{}'.format(', '.join(not_allowed),
                                                                     ',\n'.join(allowed_tables)))
    return tables


@click.command()
@click.option('--tables', '-t', callback=validate_table_names, default=f"{','.join(get_supported_producers())}",
              help='Names of the tables in arango', show_default=True)
@click.option('--clear', '-c', is_flag=True, help='flush cache', default=False)
def main(tables, clear):
    if len(tables) > 0:
        synchronizer(tables=tables, clear=clear)


if __name__ == '__main__':
    main()

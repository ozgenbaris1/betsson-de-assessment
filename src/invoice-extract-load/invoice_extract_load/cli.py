import click

from invoice_extract_load.app import run


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--from",
    "-f",
    "from_",
    help="The path/url to fetch the csv. Should be in the format acceptable by pandas.read_csv",
    required=True,
    type=str,
)
@click.option(
    "--encoding",
    "-e",
    help="The encoding that will be passed into pandas.read_csv",
    default="ISO-8859-1",
    required=True,
    type=str,
)
@click.option(
    "--schema",
    "-s",
    help="The database schema name to create the target table",
    default="raw",
    required=True,
    type=str,
)
@click.option(
    "--table",
    "-t",
    help="The table name to be created",
    default="invoices",
    required=True,
    type=str,
)
def start(from_: str, encoding: str, schema: str, table: str):
    click.echo(f"Initializing invoice-extract-load pipeline")

    run(
        from_=from_,
        encoding=encoding,
        target_schema=schema,
        target_table=table,
    )


@cli.command()
@click.option(
    "--message",
    "-m",
    help="Prints the message to the console. Used for testing the CLI.",
    required=True,
    type=str,
)
def echo(message: str):
    click.echo(message=message)

import typer


cli = typer.Typer()

@cli.command("convert")
def convert():
    print('static')


if __name__ == "__main__":
    cli()
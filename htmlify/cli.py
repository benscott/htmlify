import typer
from typing import List, Optional
import luigi

from htmlify.tasks.site import SiteTask
from htmlify.tasks.migrate import MigrateTask


cli = typer.Typer()
count = 0

@cli.command("migrate")
def migrate(limit: Optional[int] = None):

    if limit:
        typer.secho(f"Migrating: {limit} sites", fg=typer.colors.YELLOW) 
    else:
        typer.secho(f"Migrating all sites", fg=typer.colors.YELLOW) 

    def status_update(task):
        global count
        count += 1
        typer.secho(f'{count}. {task.domain} complete', fg=typer.colors.GREEN)    

    @SiteTask.event_handler(luigi.Event.SUCCESS)
    def on_success(task):
        status_update(task)
            
    @SiteTask.event_handler(luigi.Event.DEPENDENCY_PRESENT)
    def on_dependency_present(task):
        status_update(task) 

    task = MigrateTask(force=True, limit=limit) if limit else MigrateTask(force=True)
    luigi.build([task], local_scheduler=True, workers=10) 


if __name__ == "__main__":
    cli()
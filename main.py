from dotenv import load_dotenv
from os import getenv
import typer
from typing import Optional
from commands import send_command, nps_command, report_command, edit_command, commit_command
from datetime import datetime

load_dotenv()
app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


@app.command()
def send(month: int, year: Optional[int] = datetime.now().year,  count: Optional[int] = 20):
    args = {
        'MONTH': month,
        'YEAR': year,
        'COUNT': count
    }
    send_command.execute(args)


@app.command()
def nps(month: int, year: Optional[int] = datetime.now().year,  count: Optional[int] = 20):
    args = {
        'MONTH': month,
        'YEAR': year,
        'COUNT': count
    }
    nps_command.execute(args)


@app.command()
def report(month: int, year: Optional[int] = datetime.now().year):
    args = {
        'MONTH': month,
        'YEAR': year
    }

    report_command.execute(args)


@app.command()
def edit(month: int, year: Optional[int] = datetime.now().year):
    args = {
        'MONTH': month,
        'YEAR': year
    }

    edit_command.execute(args)


@app.command()
def commit():
    commit_command.execute()


if __name__ == '__main__':
    app()

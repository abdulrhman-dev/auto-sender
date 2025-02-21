from dotenv import load_dotenv
from os import getenv
from playwright.sync_api import sync_playwright
import typer
from typing import Optional
from commands import send_command, nps_command, report_command, edit_command, commit_command, send_local_command
from datetime import datetime

load_dotenv()
app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


def initilize_browser(func, args):
    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            getenv('USER_DATA_LOCATION'), headless=False)
        func(browser, args)


@app.command()
def send(month: int, year: Optional[int] = datetime.now().year,  count: Optional[int] = 20):
    args = {
        'MONTH': month,
        'YEAR': year,
        'COUNT': count
    }
    initilize_browser(send_command.execute, args)


@app.command()
def send_local():
    initilize_browser(send_local_command.execute, {})


@app.command()
def nps(month: int, year: Optional[int] = datetime.now().year,  count: Optional[int] = 20):
    args = {
        'MONTH': month,
        'YEAR': year,
        'COUNT': count
    }
    initilize_browser(nps_command.execute, args)


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

from dotenv import load_dotenv
from os import getenv
from playwright.sync_api import sync_playwright
import typer
from typing import Optional
from commands import send_messages, store_nps
load_dotenv()

app = typer.Typer()


def initilize_browser(func, args):
    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            getenv('USER_DATE_LOCATION'), headless=False)
        func(browser, args)


@app.command()
def send(month: int, year: Optional[int] = 2024,  count: Optional[int] = 20):
    args = {
        'MONTH': month,
        'YEAR': year,
        'COUNT': count
    }
    initilize_browser(send_messages, args)


@app.command()
def nps(month: int, year: Optional[int] = 2024,  count: Optional[int] = 20):
    args = {
        'MONTH': month,
        'YEAR': year,
        'COUNT': count
    }
    initilize_browser(store_nps, args)


if __name__ == '__main__':
    app()

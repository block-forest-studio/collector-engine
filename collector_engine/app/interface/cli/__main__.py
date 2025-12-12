import asyncio
import typer
import inspect
from dotenv import load_dotenv
from InquirerPy import inquirer
from collector_engine.app.infrastructure.registry.registry import (
    get_protocols,
    get_chains,
    get_protocol_info,
)
from collector_engine.app.interface.tasks import collector_tasks


load_dotenv()

app = typer.Typer()
collector_app = typer.Typer(help="cli for collecting on chain data.")
app.add_typer(collector_app, name="collector")


@collector_app.command("run")
def run() -> None:
    protocol = inquirer.select(
        message="Select a protocol:",
        choices=get_protocols(),
        pointer="❯",
        instruction="Use ↑/↓ to move, Enter to select",
    ).execute()

    chain = inquirer.select(
        message="Select a chain:",
        choices=get_chains(protocol),
        pointer="❯",
        instruction="Use ↑/↓ to move, Enter to select",
    ).execute()

    contract_name = inquirer.select(
        message="Select a contract:",
        choices=[c.name for c in get_protocol_info(chain_id=chain, protocol=protocol).contracts],
        pointer="❯",
        instruction="Use ↑/↓ to move, Enter to select",
    ).execute()

    task = inquirer.select(
        message="Select task:",
        choices=[
            name
            for name, _ in inspect.getmembers(collector_tasks, inspect.iscoroutinefunction)
            if name.endswith("_task")
        ],
        pointer="❯",
        instruction="Use ↑/↓ to move, Enter to select",
    ).execute()

    async_functions = {
        name: func
        for name, func in inspect.getmembers(collector_tasks, inspect.iscoroutinefunction)
    }
    task_func = async_functions[task]

    asyncio.run(task_func(chain_id=chain, protocol=protocol, contract_name=contract_name))


if __name__ == "__main__":
    LOGO = r"""

    /$$$$$$$  /$$                     /$$       /$$$$$$$$                                        /$$
    | $$__  $$| $$                    | $$      | $$_____/                                       | $$
    | $$  \ $$| $$  /$$$$$$   /$$$$$$$| $$   /$$| $$     /$$$$$$   /$$$$$$   /$$$$$$   /$$$$$$$ /$$$$$$
    | $$$$$$$ | $$ /$$__  $$ /$$_____/| $$  /$$/| $$$$$ /$$__  $$ /$$__  $$ /$$__  $$ /$$_____/|_  $$_/
    | $$__  $$| $$| $$  \ $$| $$      | $$$$$$/ | $$__/| $$  \ $$| $$  \__/| $$$$$$$$|  $$$$$$   | $$
    | $$  \ $$| $$| $$  | $$| $$      | $$_  $$ | $$   | $$  | $$| $$      | $$_____/ \____  $$  | $$ /$$
    | $$$$$$$/| $$|  $$$$$$/|  $$$$$$$| $$ \  $$| $$   |  $$$$$$/| $$      |  $$$$$$$ /$$$$$$$/  |  $$$$/
    |_______/ |__/ \______/  \_______/|__/  \__/|__/    \______/ |__/       \_______/|_______/    \___/

               Tailor-made Web3 tools studio
    """
    typer.echo(LOGO)
    app()

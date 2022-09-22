import os
from dotenv import load_dotenv
from pathlib import Path

def load_os_dotenv() :
    path = Path(f'{os.getcwd()}/config/.{os.environ.get("app_env")}.env')
    load_dotenv(path)
    if os.path.exists(path):
        load_dotenv(path)
    # print(os.getenv('POSTGRES_HOST'))

# path = f"./config/.{os.environ.get('env')}.env"
# load_dotenv(dotenv_path=path)
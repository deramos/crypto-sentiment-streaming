from pathlib import Path
from flask.cli import load_dotenv

env_path = Path.joinpath(Path(__file__).parent, '.env')
load_success = load_dotenv(env_path)

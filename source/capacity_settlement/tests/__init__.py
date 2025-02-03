from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent

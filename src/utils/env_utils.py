import os
from pathlib import Path

from dotenv import load_dotenv


def setup_environment(logger=None):
    """
    Load environment variables from the .env file located at the project root.

    This function calculates the project root directory relative to the
    location of this script and loads the .env file if it exists.

    Raises:
        FileNotFoundError: If the .env file is missing, halting the pipeline.
    """
    # Calculate the project root by navigating up the directory tree
    # Start searching from the directory containing this script
    current_dir = Path(__file__).resolve().parent

    # Traverse upwards up to 5 levels to locate the project root
    root = current_dir
    found_path = None

    for _ in range(5):
        potential_env = root / ".env"
        if potential_env.exists():
            found_path = potential_env
            break
        root = root.parent

    # If the .env file is found, load it; otherwise, raise an informative error
    if found_path:
        if "ENV_LOADED" not in os.environ:
            load_dotenv(dotenv_path=found_path)
            os.environ["ENV_LOADED"] = "true"
            if logger:
                logger.info(f"Environment successfully loaded from: {found_path}")
    else:
        # Halt execution if critical configuration is missing
        raise FileNotFoundError(
            f"Critical error: .env file not found after scanning 5 levels up "
            f"from {current_dir}. Please ensure it is mounted to the project root."
        )
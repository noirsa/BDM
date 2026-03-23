import os
from dotenv import load_dotenv

def setup_environment():
    # Only load if we haven't already
    if "ENV_LOADED" not in os.environ:
        load_dotenv()
        # Mark as loaded so subsequent imports don't re-read the file
        os.environ["ENV_LOADED"] = "true"
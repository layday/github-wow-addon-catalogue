`github-wow-addon-catalogue` requires Python 3.11 or later.  To get set up:

1. Create a venv: `python -m venv venv`
1. Load it up: `source venv/bin/activate`
1. Install this project: `python -m pip install -e .`
1. Generate a GitHub token and expose it in your environment as
   `RELEASE_JSON_ADDONS_GITHUB_TOKEN`
1. Run `python -m collect` or just `collect`

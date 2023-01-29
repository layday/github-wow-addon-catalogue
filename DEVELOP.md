`github-wow-addon-catalogue` requires Python 3.11 or later.  To get set up:

1. Create a venv: `python -m venv venv`
2. Load it up: `source venv/bin/activate`
3. Install this project: `python -m pip install -e .`
4. Generate a GitHub token and expose it in your environment as `RELEASE_JSON_ADDONS_GITHUB_TOKEN`
5. Run `python -m cataloguer collect --merge`

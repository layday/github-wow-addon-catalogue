1. Install Python 3.9 or greater
1. Create a venv: `python -m venv venv`
    1. Ignore it in git: `echo venv >> .git/info/exclude`
1. Load it: `source venv/bin/activate`
1. Upgrade pip: `python -m pip install -U pip`
1. Install this project: `python -m pip install -e .`
1. Run it with `python -m collect` or just `collect`

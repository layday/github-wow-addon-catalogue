import nox

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint"]


@nox.session(name="format")
def format_(session):
    session.install("--group", "format")
    session.run("ruff", "check", "--select", "I", "--fix")
    session.run("ruff", "format")


@nox.session
def lint(session):
    session.install("--group", "lint")
    session.run("ruff", "check")

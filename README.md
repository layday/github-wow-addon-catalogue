# github-wow-addon-catalogue

Script to harvest World of Warcraft add-on medata which are hosted on GitHub
and co-located catalogue.
Requires Python 3.11 or later to run.

## Usage

1. Generate a GitHub token and expose it in your environment as
   `RELEASE_JSON_ADDONS_GITHUB_TOKEN`
1. Provided that you have [`pipx`](https://github.com/pypa/pipx) installed:
   `pipx run --spec . cataloguer`

## Adoption

Derivatives of this catalogue are used by the following add-on managers:

* [instawow](https://github.com/layday/instawow)
* [strongbox](https://github.com/ogri-la/strongbox)
* [CurseBreaker](https://github.com/AcidWeb/CurseBreaker)

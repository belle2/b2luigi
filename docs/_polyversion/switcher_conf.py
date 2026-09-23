# POLYVERSION_SWITCHER -- version switcher configuration for versioned builds.
#
# Executed at the end of docs/conf.py on the main branch and appended to the
# conf.py of older tags by docs/_polyversion/prepare.sh, so it must be
# self-contained. It only acts when the build runs under sphinx-polyversion.
import os as _os

# pydata-sphinx-theme >= 0.20 adds a header search bar unless told otherwise
html_theme_options = globals().get("html_theme_options") or {}
html_theme_options["navbar_persistent"] = []

try:
    import sphinx_polyversion.git  # noqa: F401  registers the GitRef decoder
    from sphinx_polyversion import load as _load_polyversion

    _current = _load_polyversion(globals())["current"]
    _current_name = _current["name"] if isinstance(_current, dict) else _current.name
except Exception:  # not a sphinx-polyversion build
    _current_name = None

if _current_name is not None:
    _base_url = _os.environ.get("B2LUIGI_DOCS_BASE_URL", "https://b2luigi.belle2.org/")
    # A relative URL is resolved by the theme against the root of this version, which lets a
    # local or CI preview build carry its own switcher.json (see docs/poly.py).
    _switcher_url = _os.environ.get("B2LUIGI_DOCS_SWITCHER_URL", f"{_base_url}switcher.json")
    _name = "dev" if _current_name == "main" else _current_name
    _here = _os.path.dirname(_os.path.abspath(__file__))

    version = release = _name
    html_baseurl = f"{_base_url}{_name}/"
    html_theme_options["switcher"] = {"json_url": _switcher_url, "version_match": _name}
    html_static_path = list(globals().get("html_static_path", [])) + [_os.path.join(_here, "_polyversion", "static")]
    html_css_files = list(globals().get("html_css_files", [])) + ["version-switcher.css"]
    templates_path = list(globals().get("templates_path", [])) + [
        _os.path.join(_here, "_polyversion", "sphinx_templates")
    ]
    html_sidebars = {
        "**": [
            "navbar-logo.html",
            "icon-links.html",
            "search-button-field.html",
            "version-switcher-titled.html",
            "sbt-sidebar-nav.html",
        ]
    }

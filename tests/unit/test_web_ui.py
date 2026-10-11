import os
import re

import quickjs

INDEX = os.path.join(os.path.dirname(__file__), "..", "..", "web", "index.html")


def test_page_script_parses():
    """The UI is a single file with no build step, so a syntax error would silently break the whole page."""
    html = open(INDEX, encoding="utf-8").read()
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert scripts
    for source in scripts:
        quickjs.Context().eval("(function () {" + source + "\n})")  # compiles only; the function is never called

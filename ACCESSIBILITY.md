# Accessibility

Etcqlite has CLI and protocol interfaces, not a desktop/web GUI.
No WCAG audit, screen-reader certification or GUI accessibility claim is made.

Keep CLI help/errors readable as plain text, with operation, cause and a
nonzero exit code for failure. Do not depend on color, animation or terminal
cursor control for meaning. Preserve machine-readable JSON results separately
from stderr diagnostics and support noninteractive use.

Use descriptive Markdown headings, link text, text equivalents for diagrams,
and readable code examples. Any future UI should target WCAG 2.2 AA and native
keyboard/screen-reader APIs, with focus, contrast and non-color feedback
verified using synthetic data, not merely claimed by a lint score.

Contributor checks cover real `--help`, errors, JSON results and documented
quickstart flows. Human screen-reader behavior and untested terminals remain
unverified. Report non-security accessibility barriers through the existing
[issue chooser](https://github.com/mbianchidev/etcqlite/issues/new/choose);
follow [SECURITY.md](SECURITY.md) for private vulnerability reports.

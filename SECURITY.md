# Security policy

Etcqlite is experimental, not production-qualified or independently certified.
This policy does not promise a secure deployment, full compatibility or an SLA.

## Supported code

| Version / branch | Security fixes |
| --- | --- |
| Current `main` | Best effort; experimental |
| Latest published release, when one exists | Best effort |
| Older releases | Not supported |

Local developer archives and successful qualification components are not
published or supported production releases.

## Private reports

Email **security@mb-consulting.dev**. The maintainer selected this private
reporting channel; GitHub private vulnerability reporting is unavailable for
this private repository. Do not put vulnerabilities or exploit details in
public issues, PRs or discussions.

Include affected commit/version/platform, impact, a minimal synthetic
reproduction, relevant error text and any proposed mitigation. Do not send real
customer data, credentials, raw stores/snapshots or unnecessary personal data.

Responses and fixes are best-effort, with no guaranteed acknowledgment,
update or fix deadline. Coordinate public disclosure privately, case by case,
with no fixed disclosure window. Do not assume a bounty or a recognition
commitment; agree on attribution privately where relevant.

See [security and operations](docs/security-and-operations.md) for trust
boundaries and [recovery](docs/recovery-and-migration.md) for data-safety limits.

# Security Policy

## Supported versions

Security fixes are applied to the latest release on the default branch.

## Reporting a vulnerability

Please do not open a public issue for an undisclosed vulnerability. Use GitHub's **Report a vulnerability** / private security advisory flow for this repository. Include affected version, impact, reproduction steps, and a proposed mitigation when available.

Time Strike is a local stdio MCP server and does not provide a security boundary. It does not intentionally make network calls, execute commands, or read repositories. Optional persistence can store objective and checkpoint text; users must avoid secrets and protect the state file with appropriate filesystem permissions.

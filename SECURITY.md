# Security

## Reporting Vulnerabilities

If you discover a security vulnerability in this project, please **do not** open a public issue.

Report vulnerabilities securely by:

1. **Email**: Send a description to support@journl.dk with details about the vulnerability
2. **Private issue**: If the project has private reporting channels, use the designated channel

Include in your report:

- Description of the vulnerability
- Steps to reproduce
- Potential impact (scope, data exposure, etc.)
- Suggested fix (if you have one)

We aim to respond within a reasonable time and will keep you informed of the status. After confirmation and any fix, we will credit you in a security advisory (with your consent), unless you prefer to remain anonymous.

## Security Practices

- Never commit credentials, API keys or secrets to the repository
- Use `.env` for local environment variables (and ensure `.env` is in `.gitignore`)
- Follow the principle of least privilege for database and API connections

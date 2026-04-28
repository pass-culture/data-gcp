# Environment Variables Setup

This directory contains environment variable configuration for the Metabase local development environment.

## Quick Start

1. Copy the example files to create your local `.env` files:
   ```bash
   cp docker/.env.example docker/.env
   cp scripts/.env.example scripts/.env
   ```

2. Update the passwords in both `.env` files with secure values (optional for local development)

3. Ensure both `.env` files have matching values for:
   - `SAMPLE_DB`
   - `SAMPLE_USER`
   - `SAMPLE_PASSWORD`

## Files

- **`docker/.env`** - Environment variables for Docker Compose (PostgreSQL credentials)
- **`scripts/.env`** - Environment variables for setup scripts (Metabase admin and database credentials)
- **`docker/.env.example`** - Template for Docker environment variables
- **`scripts/.env.example`** - Template for scripts environment variables

## Security

- `.env` files are excluded from git via `.gitignore`
- Never commit `.env` files containing real passwords or sensitive data
- The `.env.example` files should only contain placeholder values like `changeme`

## Environment Variables

### Docker (.env in docker/)

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTGRES_DB` | Metabase application database name | `metabase` |
| `POSTGRES_USER` | Metabase application database user | `metabase` |
| `POSTGRES_PASSWORD` | Metabase application database password | `changeme` |
| `SAMPLE_DB` | Sample database name | `sample` |
| `SAMPLE_USER` | Sample database user | `sample` |
| `SAMPLE_PASSWORD` | Sample database password | `changeme` |

### Scripts (.env in scripts/)

| Variable | Description | Default |
|----------|-------------|---------|
| `METABASE_URL` | Metabase server URL | `http://localhost:3000` |
| `ADMIN_EMAIL` | Admin account email | `admin@test.com` |
| `ADMIN_PASSWORD` | Admin account password | `changeme` |
| `ADMIN_FIRST_NAME` | Admin first name | `Admin` |
| `ADMIN_LAST_NAME` | Admin last name | `User` |
| `SAMPLE_DB` | Sample database name (must match docker/.env) | `sample` |
| `SAMPLE_USER` | Sample database user (must match docker/.env) | `sample` |
| `SAMPLE_PASSWORD` | Sample database password (must match docker/.env) | `changeme` |

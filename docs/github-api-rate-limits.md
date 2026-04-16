# GitHub API Rate Limits

## REST API — Primary Limits (per hour)

| Auth method | Standard | Enterprise Cloud |
|---|---|---|
| Unauthenticated | 60 req/hr | — |
| Personal Access Token | 5,000 | 15,000 |
| GitHub App installation | 5,000–12,500* | 15,000 |
| OAuth App (client ID/secret) | 5,000 | 15,000 |
| `GITHUB_TOKEN` (Actions) | 1,000/repo | 15,000/repo |

*Non-Enterprise GitHub Apps scale: +50 req/hr per repo/user above 20, capped at 12,500.

## REST API — Secondary Limits (hard caps)

| Limit | Value |
|---|---|
| Concurrent requests | 100 |
| Points per minute | 900 |
| CPU time | 90s per 60s real time |
| Content-creating requests | 80/min, 500/hr |
| OAuth token requests | 2,000/hr |

**Point cost:** GET/HEAD/OPTIONS = 1pt, POST/PATCH/PUT/DELETE = 5pts

---

## GraphQL API — Primary Limits (per hour, in points)

| Auth method | Standard | Enterprise Cloud |
|---|---|---|
| User | 5,000 | 10,000 |
| GitHub App installation | 5,000–12,500* | 10,000 |
| OAuth App | 5,000 | 10,000 |
| `GITHUB_TOKEN` (Actions) | 1,000/repo | 15,000/repo |

*Same scaling formula as REST.

## GraphQL API — Secondary Limits

| Limit | Value |
|---|---|
| Concurrent requests (shared with REST) | 100 |
| Points per minute | 2,000 |
| CPU time | 90s per 60s real time, max 60s for GraphQL |
| Content-creating requests | 80/min, 500/hr |

**Point cost:** queries = 1pt, mutations = 5pts

## GraphQL Query/Node Limits

- Max nodes per call: 500,000
- `first`/`last` pagination args: 1–100
- Request timeout: 10 seconds

---

## gh CLI

The `gh` CLI uses both REST and GraphQL depending on the command:

- **Built-in commands** (`gh pr list`, `gh issue view`, etc.) use GraphQL internally for many operations.
- **`gh api`** defaults to REST; pass `graphql` as the endpoint to use GraphQL.

All `gh` commands consume from the same primary hourly bucket as direct API calls for whichever token is authenticated. If `gh` CLI calls and direct API calls share a token, they share the same 5,000 pts/hr limit.

---

## Sources

- [Rate limits for the REST API — GitHub Docs](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api)
- [Rate limits and query limits for the GraphQL API — GitHub Docs](https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api)
- [gh api manual](https://cli.github.com/manual/gh_api)

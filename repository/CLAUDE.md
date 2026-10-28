# repository/

## Database backend compatibility (REQUIRED for any DB-related change)

**Any change touching the persistent layer MUST be verified against real databases
before being considered done** — this means changes to `repository/{mysql,postgres,dm8}/`,
the models (including `model/` changes that affect persistence), time handling,
schema/migration (`Init`), or any repository interface method.

Mock unit tests CANNOT catch these bugs — they live inside the real driver/DB boundary
(e.g. DM8 case-sensitive `Has*` failing on restart → -2124/-2116/-2140; MySQL rejecting
Go zero-time `0000-00-00` → Error 1292; Postgres migration never creating some tables).
The integration scaffold `repository/dbtest/` (build tag `dbintegration`) exercises the
real `repository.Ps` methods against real MySQL/DM8/Postgres: connect-start ×3 (migration
idempotency), zero-time backup_run, and full CRUD per entity.

```bash
# one-command real PostgreSQL regression (spins up postgres:16, tests, removes container)
./repository/dbtest/pg-local-test.sh

# or point env vars at real DBs and run all three (see repository/dbtest/README.md)
go test -tags dbintegration -v ./repository/dbtest/ -run TestBackends
```

Run it for all three backends when the change is cross-backend (models, time, interface
shape); at minimum run the affected backend. Do not rely on mocks or a single backend.

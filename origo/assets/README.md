# How to add new assets?

1) Add the necessary asset file/s following the current naming convention
2) Make changes in `origo/definitions.py`
3) Follow the instructions at the head of `origo/definitions.py`
4) Make Pull Request with the changes committed

NOTE: Changes merged to `main` deploy the repo-defined production services automatically
through GitHub Actions. The workflow builds the Dagster and ClickHouse images, pushes
them to GitHub Container Registry, and deploys `clickhouse`, `dagster`, and `dagit`
to the remote host over SSH without relying on a server-side `.env` file.

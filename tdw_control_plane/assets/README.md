# How to add new assets?

1) Add the necessary asset file/s following the current naming convention
2) Make changes in `tdw_control_plane/definitions.py`
3) Follow the instructions at the head of `tdw_control_plane/definitions.py`
4) Make Pull Request with the changes committed

NOTE: Changes merged to `main` deploy the Dagster application services automatically
through GitHub Actions. The workflow builds the app image, pushes it to GitHub Container
Registry, and deploys `dagster` plus `dagit` to the remote host over SSH without relying
on a server-side `.env` file.

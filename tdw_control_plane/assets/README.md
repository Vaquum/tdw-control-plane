# How to add new assets?

1) Add the necessary asset file/s following the current naming convention
2) Make changes in `tdw_control_plane/definitions.py`
3) Follow the instructions at the head of `tdw_control_plane/definitions.py`
4) Make Pull Request with the changes committed

NOTE: For the changes to take effect in `tdw`, user `root` must run the command `@deploy` on the server. 

The Hugging Face publisher depends on `HF_TOKEN`, which is synced into `/opt/tdw-control-plane/.env`
from the GitHub repo secret by the `Sync Runtime Secrets` workflow. `docker-compose` reads that `.env`
file automatically during `@deploy`, so the server does not need manual shell exports for this token.

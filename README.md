<h1 align="center">
  <br>
  <a href="https://github.com/Vaquum"><img src="https://github.com/Vaquum/Home/raw/main/assets/Logo.png" alt="Vaquum" width="150"></a>
  <br>
</h1>

<h3 align="center">TDW Control Plane</h3>

<p align="center">
  <a href="#description">Description</a> •
  <a href="#owner">Owner</a> •
  <a href="#integrations">Integrations</a> •
  <a href="#docs">Docs</a>
</p>
<hr>

## Description

Control plane for `trade-warehouse` (tdw). 

## Owner

- [@mikkokotila](https://github.com/mikkokotila)

## Integrations

- https://github.com/Vaquum/tdw-docker

## Docs

`main` deploys automatically through GitHub Actions. The workflow builds the Dagster
and ClickHouse images, pushes them to GitHub Container Registry, and deploys them
to the remote host over SSH with runtime secrets injected from GitHub repo secrets.

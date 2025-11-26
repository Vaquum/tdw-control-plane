# Makefile for managing the tdw-control-plane stack

# Use bash as the shell
SHELL := /bin/bash

# Phony targets don't represent files
.PHONY: all up down build clean setup

all: up

# Ensures the .env file is present and populated before certain commands are run.
# If the .env file does not exist, it creates one and generates random passwords.
.env:
	@if [ ! -f .env ]; then \
		echo "Creating .env file with new random passwords..."; \
		echo "CLICKHOUSE_PASSWORD=$$(openssl rand -base64 16)" > .env; \
		echo "METABASE_ADMIN_PASSWORD=$$(openssl rand -base64 16)" >> .env; \
		echo "JUPYTER_TOKEN=$$(openssl rand -base64 16)" >> .env; \
		echo ".env file created successfully."; \
	fi

# Run this once to add the .env file to .gitignore
setup:
	@echo "" >> .gitignore
	@echo "# Secrets file" >> .gitignore
	@echo ".env" >> .gitignore
	@echo "Added .env to .gitignore to prevent committing secrets."

# Build the docker images.
# Depends on .env to ensure environment variables are available for build-time arguments if needed.
build: .env
	@echo "Building Docker images..."
	docker-compose build

# Start the services in detached mode.
# Depends on .env to ensure passwords are set for the services at runtime.
up: .env
	@echo "Starting Docker services in detached mode..."
	docker-compose up -d

# Stop the services.
down:
	@echo "Stopping Docker services..."
	docker-compose down

# Clean up the environment by stopping containers, removing them, and deleting volumes.
clean:
	@echo "Stopping and cleaning up Docker environment (containers, volumes)..."
	docker-compose down -v

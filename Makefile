# Makefile for managing the tdw-control-plane stack

# Use bash as the shell
SHELL := /bin/bash

# Phony targets don't represent files
.PHONY: all up down build clean setup certs

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

# Generates self-signed SSL certificates for local development if they don't exist.
certs:
	@mkdir -p certs
	@if [ ! -f certs/nginx-selfsigned.crt ]; then \
		echo "Generating self-signed SSL certificates for local development..."; \
		openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
			-keyout certs/nginx-selfsigned.key \
			-out certs/nginx-selfsigned.crt \
			-subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"; \
		echo "Certificates generated in certs/"; \
	fi

# Run this once to add the .env and certs to .gitignore (idempotent check)
setup:
	@grep -qF ".env" .gitignore || echo ".env" >> .gitignore
	@grep -qF "certs/" .gitignore || echo "certs/" >> .gitignore
	@echo "Ensure .env and certs/ are ignored by git."

# Build the docker images.
# Depends on .env and certs to ensure secrets and SSL are available.
build: .env certs
	@echo "Building Docker images..."
	docker-compose build

# Start the services in detached mode.
# Depends on .env and certs.
up: .env certs
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
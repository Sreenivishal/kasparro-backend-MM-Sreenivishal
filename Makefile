# Makefile for Crypto ETL System

# P0.3: Dockerized, Runnable System
# "The Docker image must automatically start the ETL service and expose API endpoints immediately." [cite: 47]

.PHONY: up down test logs clean

# Builds and starts the system in detached mode
up:
	docker-compose up -d --build

# Stops the system and removes containers
down:
	docker-compose down

# P0.4 & P1.4: Run tests
test:
	docker-compose run --rm app pytest

# Helper: View logs
logs:
	docker-compose logs -f app

# Helper: Clean up everything (volumes, orphans)
clean:
	docker-compose down -v --remove-orphans
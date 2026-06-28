#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

trap 'echo -e "\n${RED}--- Shutting down cluster ---${NC}"; docker compose down' INT

echo -e "${GREEN}--- Cleaning Docker environment ---${NC}"
docker compose down --volumes --remove-orphans

echo -e "${GREEN}--- Building and starting containers ---${NC}"
docker compose up --build -d

echo -e "${GREEN}--- Streaming logs (CTRL+C to stop) ---${NC}"
docker compose logs -f

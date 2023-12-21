#!/bin/bash

# Stop and remove all containers
echo "Stopping and removing all containers..."
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

# Remove all images
echo "Removing all images..."
docker rmi $(docker images -a -q)

# Remove all volumes
echo "Removing all volumes..."
docker volume prune -f

# Remove all networks
echo "Removing all networks..."
docker network prune -f

echo "Cleanup complete!"

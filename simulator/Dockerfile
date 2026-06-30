FROM node:18-alpine

# Install Docker CLI so the simulator can query `docker stats` of sibling containers, and curl for healthcheck
RUN apk add --no-cache docker-cli curl

WORKDIR /app

# Copy package info and install dependencies
COPY package*.json ./
RUN npm ci

# Copy source code
COPY tsconfig.json ./
COPY src/ ./src/

# Expose backend API port
EXPOSE 3001

# Start the simulator
CMD ["npm", "start"]

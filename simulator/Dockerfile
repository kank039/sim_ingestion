FROM node:18-alpine

# Install Docker CLI so the simulator can query `docker stats` of sibling containers
RUN apk add --no-cache docker-cli

WORKDIR /app

# Copy package info and install dependencies
COPY package*.json ./
RUN npm install

# Copy source code
COPY tsconfig.json ./
COPY src/ ./src/

# Expose backend API port
EXPOSE 3001

# Start the simulator
CMD ["npm", "start"]

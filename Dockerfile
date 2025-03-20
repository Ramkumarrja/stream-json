# Use Node.js base image
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files and install dependencies
COPY package.json package-lock.json ./
RUN npm install

# Copy source code
COPY . .

# Expose the correct port for Cloud Run
EXPOSE 8080

# Start the server
CMD ["node", "server.js"]

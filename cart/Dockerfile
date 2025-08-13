FROM node:20-alpine3.21 AS builder

# Set working directory
WORKDIR /opt/server

# Optional: Set environment variable
ENV MONGO=true

# Copy only package.json and package-lock.json first (better caching)
COPY package.json package-lock.json* ./

# Clean npm cache before install
RUN rm -rf /root/.npm && npm cache clean --force && npm install --force

# Now copy the rest of the files
COPY server.js .

# Expose port
EXPOSE 8080

# Start the app
CMD ["node", "server.js"]

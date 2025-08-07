FROM node:20

# App port
EXPOSE 8080

# Set working directory
WORKDIR /opt/server

# Enable Mongo flag
ENV MONGO=true

# Copy dependencies first (for layer caching)
COPY package.json ./

# Install dependencies
RUN npm install

# Copy the rest of the code
COPY server.js ./

# Start the app
CMD ["node", "server.js"]

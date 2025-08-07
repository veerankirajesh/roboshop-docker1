# Stage 1: Build with Node
FROM node:20-alpine3.21 AS builder

# Set working directory
WORKDIR /app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Copy the full source code and build
COPY . .
RUN npm run build

# Stage 2: Serve with NGINX
FROM nginx:1.29.0-alpine

# Remove default nginx config
RUN rm /etc/nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Add your custom nginx.conf
ADD nginx.conf /etc/nginx/nginx.conf

# Copy built static files from builder
COPY --from=builder /app/dist /usr/share/nginx/html/

# Set proper permissions (optional, depends on your setup)
RUN mkdir -p /var/cache/nginx && \
    chown -R nginx:nginx /var/cache/nginx /etc/nginx /usr/share/nginx/html && \
    chmod -R 755 /etc/nginx

# Run as nginx user
USER nginx

# Start nginx
CMD ["nginx", "-g", "daemon off;"]

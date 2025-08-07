# Stage 1: Build with Node
FROM node:20-alpine3.21 AS builder

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build


# Stage 2: Serve with NGINX
FROM nginx:1.29.0-alpine

# Remove default nginx config
RUN rm -f /etc/nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Add your custom nginx config
COPY nginx.conf /etc/nginx/nginx.conf

# Copy static files from builder
COPY --from=builder /app/dist /usr/share/nginx/html/

# Optional permissions
RUN mkdir -p /var/cache/nginx && \
    chown -R nginx:nginx /var/cache/nginx /etc/nginx /usr/share/nginx/html && \
    chmod -R 755 /etc/nginx

USER nginx

CMD ["nginx", "-g", "daemon off;"]

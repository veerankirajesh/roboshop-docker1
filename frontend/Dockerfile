FROM nginx:1.29.0-alpine

# Remove default config
RUN rm /etc/nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Create necessary cache/temp directories and fix permissions
RUN mkdir -p /var/cache/nginx/{client_temp,proxy_temp,fastcgi_temp,uwsgi_temp,scgi_temp} && \
    mkdir -p /etc/nginx/ssl && \
    chown -R nginx:nginx /var/cache/nginx /etc/nginx /var/log/nginx /etc/nginx/ssl && \
    chmod -R 755 /etc/nginx /etc/nginx/ssl

# Create PID file for nginx
RUN touch /var/run/nginx.pid && \
    chown -R nginx:nginx /var/run/nginx.pid /run/nginx.pid

# Copy custom nginx config and static files
ADD nginx.conf /etc/nginx/nginx.conf
ADD static/ /usr/share/nginx/html/

# Use non-root user
USER nginx

# Start nginx in foreground
CMD ["nginx", "-g", "daemon off;"]

# FROM nginx
# RUN rm -rf /usr/share/nginx/html/index.html
# RUN rm -rf /etc/nginx/nginx.conf
# RUN rm -rf /etc/nginx/conf.d/default.conf
# ADD nginx.conf /etc/nginx/nginx.conf
# ADD static /usr/share/nginx/html/
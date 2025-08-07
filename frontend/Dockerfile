FROM nginx:1.25.3-alpine  

RUN rm -f /etc/nginx/nginx.conf /etc/nginx/conf.d/default.conf && \
    mkdir -p /var/cache/nginx/client_temp \
             /var/cache/nginx/proxy_temp \
             /var/cache/nginx/fastcgi_temp \
             /var/cache/nginx/uwsgi_temp \
             /var/cache/nginx/scgi_temp \
             /etc/nginx/ssl && \
    chown -R nginx:nginx /var/cache/nginx /etc/nginx /var/log/nginx /etc/nginx/ssl && \
    chmod -R 755 /etc/nginx /etc/nginx/ssl && \
    touch /var/run/nginx.pid && \
    chown -R nginx:nginx /var/run/nginx.pid /run/nginx.pid

ADD nginx.conf /etc/nginx/nginx.conf
ADD static /usr/share/nginx/html/

USER nginx
CMD ["nginx", "-g", "daemon off;"]

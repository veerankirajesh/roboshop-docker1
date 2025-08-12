FROM node:18.19.1-alpine3.19

EXPOSE 8080

# Create non-root user
RUN addgroup -S roboshop && adduser -S roboshop -G roboshop

WORKDIR /opt/server
ENV MONGO=true

# Copy files before install
COPY package.json /opt/server/
COPY server.js /opt/server/

# Install dependencies as root (avoids permission issues)
RUN npm install

# Change ownership for runtime
RUN chown -R roboshop:roboshop /opt/server

# Switch to non-root user for security
USER roboshop

CMD ["node", "server.js"]

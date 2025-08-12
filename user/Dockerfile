FROM node:18.19.1-alpine3.19

EXPOSE 8080

# Create roboshop user & group
RUN addgroup -S roboshop && adduser -S roboshop -G roboshop

WORKDIR /opt/server
ENV MONGO=true

# Copy files first
COPY package.json /opt/server/
COPY server.js /opt/server/

# Install dependencies as root (avoids permission errors)
RUN npm install

# Change ownership after install
RUN chown -R roboshop:roboshop /opt/server

# Switch to non-root user
USER roboshop

CMD ["node", "server.js"]

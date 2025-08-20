import json
import pika
import os

class Publisher:
    HOST = os.getenv('AMQP_HOST', 'rabbitmq')
    USER = os.getenv('AMQP_USER', 'guest')
    PASS = os.getenv('AMQP_PASS', 'guest')
    VIRTUAL_HOST = '/'
    EXCHANGE = 'robot-shop'
    TYPE = 'direct'
    ROUTING_KEY = 'orders'

    def __init__(self, logger):
        self._logger = logger
        self._params = pika.ConnectionParameters(
            host=self.HOST,
            virtual_host=self.VIRTUAL_HOST,
            credentials=pika.PlainCredentials(self.USER, self.PASS)
        )
        self._conn = None
        self._channel = None

    def _connect(self):
        """Establish connection and channel to RabbitMQ."""
        if not self._conn or self._conn.is_closed or self._channel is None or self._channel.is_closed:
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            self._channel.exchange_declare(exchange=self.EXCHANGE, exchange_type=self.TYPE, durable=True)
            self._logger.info('Connected to broker')

    def _publish(self, msg, headers):
        """Publish a single message to the exchange."""
        self._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            properties=pika.BasicProperties(
                headers=headers,
                delivery_mode=2  # make message persistent
            ),
            body=json.dumps(msg).encode()
        )
        self._logger.info('Message sent')

    def publish(self, msg, headers):
        """Publish message, reconnecting if necessary."""
        if self._channel is None or self._channel.is_closed or self._conn is None or self._conn.is_closed:
            self._connect()
        try:
            self._publish(msg, headers)
        except (pika.exceptions.ConnectionClosed, pika.exceptions.StreamLostError) as e:
            self._logger.warning(f'Publish failed ({e}), reconnecting...')
            self._connect()
            self._publish(msg, headers)

    def close(self):
        """Close connection cleanly."""
        if self._conn and self._conn.is_open:
            self._logger.info('Closing queue connection')
            self._conn.close()

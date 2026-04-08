import pika
import random
import string
from typing import Optional

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

def _random_name(prefix: str, n: int = 12) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return f"{prefix}_" + "".join(random.choice(alphabet) for _ in range(n))


class _BaseRabbitMQ:
    def __init__(self, host: str):
        self._host = host
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self._consumer_tag: Optional[str] = None
        self._consuming: bool = False

    def _ensure_connected(self) -> None:
        if self._connection and self._connection.is_open and self._channel and self._channel.is_open:
            return
        try:
            params = pika.ConnectionParameters(
                host=self._host,
                port=5672,
                virtual_host="/",
                heartbeat=30,
                blocked_connection_timeout=10,
                connection_attempts=3,
                retry_delay=1.0,
            )
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def stop_consuming(self):
        try:
            if not self._channel or not self._channel.is_open:
                self._consuming = False
                return

            if not self._consuming:
                return

            if self._consumer_tag:
                try:
                    self._channel.basic_cancel(self._consumer_tag)
                except Exception:
                    pass

            try:
                self._channel.stop_consuming()
            finally:
                self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def close(self):
        try:
            try:
                self.stop_consuming()
            except Exception:
                pass

            if self._channel:
                try:
                    if self._channel.is_open:
                        self._channel.close()
                except Exception:
                    pass

            if self._connection:
                try:
                    if self._connection.is_open:
                        self._connection.close()
                except Exception:
                    pass
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e)) from e


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue, _BaseRabbitMQ):
    """
    Work Queue middleware.
    """

    def __init__(self, host, queue_name):
        _BaseRabbitMQ.__init__(self, host)
        self._queue_name = queue_name

        self._ensure_connected()
        try:
            assert self._channel is not None
            self._channel.queue_declare(
                queue=self._queue_name,
                durable=False,
                exclusive=False,
                auto_delete=False,
            )
            self._channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def send(self, message):
        self._ensure_connected()
        try:
            if not isinstance(message, (bytes, bytearray)):
                raise MessageMiddlewareMessageError("message must be bytes")

            assert self._channel is not None
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=bytes(message),
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except MessageMiddlewareMessageError:
            raise
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def start_consuming(self, on_message_callback):
        self._ensure_connected()
        try:
            assert self._channel is not None

            def _cb(ch, method, properties, body):
                def ack():
                    try:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except pika.exceptions.AMQPConnectionError as e:
                        raise MessageMiddlewareDisconnectedError(str(e)) from e
                    except Exception as e:
                        raise MessageMiddlewareMessageError(str(e)) from e

                def nack(requeue: bool = True):
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
                    except pika.exceptions.AMQPConnectionError as e:
                        raise MessageMiddlewareDisconnectedError(str(e)) from e
                    except Exception as e:
                        raise MessageMiddlewareMessageError(str(e)) from e

                on_message_callback(body, ack, nack)

            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_cb,
                auto_ack=False,
            )
            self._consuming = True
            self._channel.start_consuming()
            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e)) from e

    def stop_consuming(self):
        return _BaseRabbitMQ.stop_consuming(self)

    def close(self):
        return _BaseRabbitMQ.close(self)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange, _BaseRabbitMQ):
    """
    Direct exchange middleware with per-consumer exclusive queue bound to routing keys.
    """

    def __init__(self, host, exchange_name, routing_keys):
        _BaseRabbitMQ.__init__(self, host)
        self._exchange_name = exchange_name
        self._routing_keys = list(routing_keys) if routing_keys is not None else []
        self._queue_name: Optional[str] = None

        self._ensure_connected()
        try:
            assert self._channel is not None
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type="direct",
                durable=False,
                auto_delete=False,
            )

            self._queue_name = _random_name(f"{self._exchange_name}_q")
            self._channel.queue_declare(
                queue=self._queue_name,
                durable=False,
                exclusive=True,
                auto_delete=True,
            )

            for key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=key,
                )

            self._channel.basic_qos(prefetch_count=1)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def send(self, message):
        self._ensure_connected()
        try:
            if not isinstance(message, (bytes, bytearray)):
                raise MessageMiddlewareMessageError("message must be bytes")

            if len(self._routing_keys) != 1:
                raise MessageMiddlewareMessageError(
                    "send() requires exactly one routing key for producer instances"
                )
            routing_key = self._routing_keys[0]

            assert self._channel is not None
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=routing_key,
                body=bytes(message),
                properties=pika.BasicProperties(delivery_mode=1),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except MessageMiddlewareMessageError:
            raise
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def start_consuming(self, on_message_callback):
        self._ensure_connected()
        try:
            assert self._channel is not None
            if not self._queue_name:
                raise MessageMiddlewareMessageError("consumer queue not initialized")

            def _cb(ch, method, properties, body):
                def ack():
                    try:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    except pika.exceptions.AMQPConnectionError as e:
                        raise MessageMiddlewareDisconnectedError(str(e)) from e
                    except Exception as e:
                        raise MessageMiddlewareMessageError(str(e)) from e

                def nack(requeue: bool = True):
                    try:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
                    except pika.exceptions.AMQPConnectionError as e:
                        raise MessageMiddlewareDisconnectedError(str(e)) from e
                    except Exception as e:
                        raise MessageMiddlewareMessageError(str(e)) from e

                on_message_callback(body, ack, nack)

            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_cb,
                auto_ack=False,
            )
            self._consuming = True
            self._channel.start_consuming()
            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e)) from e

    def stop_consuming(self):
        return _BaseRabbitMQ.stop_consuming(self)

    def close(self):
        return _BaseRabbitMQ.close(self)

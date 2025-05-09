import amqpstorm
import threading
from amqpstorm import Message
from urllib.parse import urlparse, unquote

class RpcClient:
    """Cliente RPC para enviar solicitudes de traducción a través de RabbitMQ."""

    def __init__(self, amqp_url, rpc_queue):
        self.queue = {}  # Diccionario para almacenar respuestas
        self.rpc_queue = rpc_queue
        self.amqp_url = amqp_url
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self._parse_amqp_url()
        self.open()

    def _parse_amqp_url(self):
        """Parses the AMQP URL to extract host, username, and password."""
        # Asumiendo que el formato es amqp://username:password@host:port
        parsed = urlparse(self.amqp_url)
        self.username = unquote(parsed.username)
        self.password = unquote(parsed.password)
        self.host = parsed.hostname
        self.port = parsed.port or (5671 if parsed.scheme == "amqps" else 5672)
        self.virtual_host = unquote(parsed.path[1:])  # remove leading '/'

    def open(self):
        """Establece la conexión a RabbitMQ y declara la cola."""
        self.connection = amqpstorm.Connection(
            hostname=self.host,
            username=self.username,
            password=self.password,
            port=self.port,
            virtual_host=self.virtual_host,
            ssl=True,
            heartbeat=30
        )
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']

        # Inicia el consumidor de respuestas
        self.channel.basic.consume(self._on_response, no_ack=True, queue=self.callback_queue)

        # Crea un hilo para escuchar las respuestas
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        """Inicia la espera de mensajes."""
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        """Recibe la respuesta de RabbitMQ y la almacena en la cola."""
        self.queue[message.correlation_id] = message.body  # Ya es str

    def send_request(self, payload):
        """Envía una solicitud de traducción y devuelve el correlation_id."""
        if not self.connection or not self.connection.is_open:
            print("Conexión caída. Reconectando...")
            try:
                self.open()
            except Exception as e:
                print(f"No se pudo reconectar: {e}")
                raise

        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue  # Especifica la cola de respuesta

        # Crea una entrada en el diccionario para la respuesta con el correlation_id
        self.queue[message.correlation_id] = None

        # Publica la solicitud en la cola de traducción
        message.publish(routing_key=self.rpc_queue)

        return message.correlation_id

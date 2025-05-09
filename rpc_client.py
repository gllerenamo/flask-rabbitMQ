import amqpstorm
import threading
from amqpstorm import Message

class RpcClient:
    """Cliente RPC para enviar solicitudes de traducción a través de RabbitMQ."""

    def __init__(self, host, username, password, rpc_queue):
        self.queue = {}  # Diccionario para almacenar respuestas
        self.host = host
        self.username = username
        self.password = password
        self.rpc_queue = rpc_queue
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.open()

    def open(self):
        """Establece la conexión a RabbitMQ y declara la cola."""
        self.connection = amqpstorm.Connection(self.host, self.username, self.password)
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
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue  # Especifica la cola de respuesta

        # Crea una entrada en el diccionario para la respuesta con el correlation_id
        self.queue[message.correlation_id] = None

        # Publica la solicitud en la cola de traducción
        message.publish(routing_key=self.rpc_queue)

        return message.correlation_id

import amqpstorm
from deep_translator import GoogleTranslator
from urllib.parse import urlparse, unquote
import ssl
import time
import os

AMQP_URL = os.environ.get('AMQP_URL')

def parse_amqp_url(url):
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 5671,
        'username': parsed.username,
        'password': parsed.password,
        'virtual_host': unquote(parsed.path[1:]),
    }


def on_request(message):
    cuerpo = message.body

    try:
        # Espera el formato: origen|destino|texto
        idioma_origen, idioma_destino, texto = cuerpo.split("|", 2)

        translated_text = GoogleTranslator(
            source=idioma_origen,
            target=idioma_destino
        ).translate(texto)

    except Exception as e:
        translated_text = f"Error en la traducción: {str(e)}"

    message.channel.basic.publish(
        body=translated_text,
        routing_key=message.reply_to,
        properties={
            'correlation_id': message.correlation_id
        }
    )

    message.ack()

def main():
    cfg = parse_amqp_url(AMQP_URL)
    connection = amqpstorm.Connection(
        hostname=cfg['host'],
        username=cfg['username'],
        password=cfg['password'],
        port=cfg['port'],
        virtual_host=cfg['virtual_host'],
        ssl=True,
        heartbeat=30
    )
    channel = connection.channel()

    channel.queue.declare('translate_queue')
    channel.basic.consume(on_request, queue='translate_queue')

    print("Esperando solicitudes de traducción...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupción del servidor.")
        connection.close()
        exit(0)

if __name__ == '__main__':
    while True:
        try:
            main()
        except amqpstorm.AMQPConnectionError as e:
            print(f"Error de conexión: {e}. Reintentando en 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"Error inesperado: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

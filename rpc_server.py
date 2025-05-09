import amqpstorm
from deep_translator import GoogleTranslator

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
    connection = amqpstorm.Connection('localhost', 'guest', 'guest')
    channel = connection.channel()

    channel.queue.declare('translate_queue')
    channel.basic.consume(on_request, queue='translate_queue')

    print("Esperando solicitudes de traducción...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupción del servidor.")
        connection.close()

if __name__ == '__main__':
    main()

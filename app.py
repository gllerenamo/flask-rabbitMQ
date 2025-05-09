from flask import Flask, request, render_template
from rpc_client import RpcClient
from time import sleep

app = Flask(__name__)

# Instancia del cliente RPC
rpc_client = RpcClient('localhost', 'guest', 'guest', 'translate_queue')

@app.route('/', methods=['GET', 'POST'])
def index():
    resultado = None
    texto_original = ''
    idioma = 'es'
    idioma_origen = 'auto'

    if request.method == 'POST':
        texto_original = request.form['texto']
        idioma = request.form['idioma']
        idioma_origen = request.form['idioma_origen']

        # Mensaje: origen|destino|texto
        mensaje = f"{idioma_origen}|{idioma}|{texto_original}"

        corr_id = rpc_client.send_request(mensaje)
        while rpc_client.queue[corr_id] is None:
            sleep(0.1)

        resultado = rpc_client.queue[corr_id]

    return render_template(
        'index.html',
        resultado=resultado,
        texto_original=texto_original,
        idioma=idioma,
        idioma_origen=idioma_origen
    )

if __name__ == '__main__':
    app.run(debug=True)
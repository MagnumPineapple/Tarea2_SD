import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import pandas as pd
import time
import random
import json
import os

# Configuración del servidor SMTP
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_ADDRESS = "a.alpaca52@gmail.com" 
EMAIL_PASSWORD = "informatico123"

# Conexión a Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

def enviar_notificacion_email(correo_cliente, id_pedido, estado_actual):
    try:
     
        mensaje = MIMEMultipart()
        mensaje["From"] = EMAIL_ADDRESS
        mensaje["To"] = correo_cliente
        mensaje["Subject"] = f"Actualización del estado de tu pedido #{id_pedido}"

        # Contenido del mensaje
        cuerpo = f"Hola,\n\nTu pedido #{id_pedido} ha cambiado de estado. El nuevo estado es: 
        {estado_actual}.\n\nGracias por tu compra."
        mensaje.attach(MIMEText(cuerpo, "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as servidor:
            servidor.starttls()  # Activar TLS
            servidor.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            servidor.sendmail(EMAIL_ADDRESS, correo_cliente, mensaje.as_string())
            print(f"Notificación enviada a {correo_cliente} 
                  para el pedido {id_pedido} en estado {estado_actual}.")
    
    except Exception as e:
        print(f"Error al enviar el correo: {e}")

def crear_productor_kafka():
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        batch_size=16384,
        linger_ms=10
    )

def cargar_datos_csv(csv_path):
    data = pd.read_csv(csv_path)

    lista_pedidos = []
    for _, row in data.iterrows():
        pedido = {
            "id_pedido": row['transaction_id'],      
            "correo_cliente": "",                    
            "estado": "Procesando"             
        }
        lista_pedidos.append(pedido)

    return lista_pedidos

def simular_procesamiento_pedidos(pedidos, productor):
    estados = ["Procesando", "Preparación", "Enviado", "Entregado", "Finalizado"]
    for pedido in pedidos:
        for estado in estados:
            tiempo_procesamiento = random.uniform(1, 5)
            time.sleep(tiempo_procesamiento)
            data = {
                "id_pedido": pedido["id_pedido"],
                "estado": estado,
                "timestamp": pd.Timestamp.now().isoformat()
            }
            try:
                productor.send('estado_pedidos', value=data)
                productor.flush()
                print(f"Estado del pedido {pedido['id_pedido']} enviado a Kafka: {estado}")
                es.index(index="estado_pedidos", body=data)
                print(f"Estado del pedido {pedido['id_pedido']} registrado en Elasticsearch: {estado}")

            except Exception as e:
                print(f"Error al enviar el estado del pedido a Kafka o Elasticsearch: {e}")

            enviar_notificacion_email(pedido["correo_cliente"], pedido["id_pedido"], estado)
            pedido["estado"] = estado

if __name__ == "__main__":
    csv_path = 'online-retail-sales/online_retail_sales_dataset.csv'
    productor_kafka = crear_productor_kafka()
    lista_pedidos = cargar_datos_csv(csv_path)
    simular_procesamiento_pedidos(lista_pedidos, productor_kafka)

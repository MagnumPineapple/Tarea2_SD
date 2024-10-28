import grpc
import pandas as pd
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
from compra_pb2 import CompraRequest
import compra_pb2_grpc
import json
import time

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
def enviar_compras_a_grpc(compras):
    with grpc.insecure_channel('localhost:50051') as canal:
        stub = compra_pb2_grpc.CompraServiceStub(canal)
        
    
        for compra in compras:
            respuesta = stub.EnviarCompra(compra)
            print(f"Respuesta del servidor para pedido '{compra.nombre_producto}': 
                  {respuesta.message} - Éxito: {respuesta.success}")

def crear_productor_kafka():
    return Producer({
        'bootstrap.servers':'localhost:9092'
    })

def cargar_datos_csv(csv_path):
    data = pd.read_csv('/home/tomas/Descargas/online_retail_sales_dataset.csv', nrows=10000)

    lista_compras = []
    
    for _, row in data.iterrows():
        compra = CompraRequest(
            nombre_producto=row['product_category'], 
            precio=row['price'],                  
            pasarela_pago=row['payment_method'],   
            marca_tarjeta=row['marca_tarjeta'],                
            banco=row['banco'],                                
            region_envio=row['customer_location'],  
            direccion_envio=row['direccion_envio'],                   
            correo_cliente=row['correo_cliente']                     
        )
        lista_compras.append(compra)

    return lista_compras

def enviar_compras_a_grpc_kafka_elasticsearch(compras, productor_kafka):
    with grpc.insecure_channel('localhost:50051') as canal:
        stub = compra_pb2_grpc.CompraServiceStub(canal)
        for i, compra in enumerate(compras, start=1):
            try:
                respuesta = stub.EnviarCompra(compra)
                print(f"[{i}] Respuesta del servidor para pedido '{compra.nombre_producto}':
                       {respuesta.message} - Éxito: {respuesta.success}")
                data = {
                    "nombre_producto": compra.nombre_producto,
                    "precio": compra.precio,
                    "pasarela_pago": compra.pasarela_pago,
                    "region_envio": compra.region_envio,
                    "timestamp": time.time()
                }
                kafka_data =  json.dumps(compra).encode('utf-8')  
                productor_kafka.produce('orders_topic', value=kafka_data)
                productor_kafka.flush()  
                print(f"[{i}] Evento de compra enviado a Kafka con éxito.")

         
                es.index(index="compras", body=data)
                print(f"[{i}] Métrica de compra registrada en Elasticsearch.")

            except grpc.RpcError as e:
                print(f"[{i}] Error de comunicación gRPC: {e}")
            except Exception as e:
                print(f"[{i}] Error en el envío a Kafka o Elasticsearch: {e}")

if __name__ == "__main__":
    csv_path = 'online-retail-sales/online_retail_sales_dataset.csv'
    lista_compras = cargar_datos_csv(csv_path)
    productor_kafka = crear_productor_kafka()
    enviar_compras_a_grpc_kafka_elasticsearch(lista_compras, productor_kafka)  

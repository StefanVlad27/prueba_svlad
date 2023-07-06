from confluent_kafka import Producer, KafkaException
import time

kafka_conf = {
    'bootstrap.servers': 'b-1-public.arq6kafka.vhgjgn.c6.kafka.eu-west-1.amazonaws.com:9196,b-2-public.arq6kafka.vhgjgn.c6.kafka.eu-west-1.amazonaws.com:9196',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'kafka',
    'sasl.password': 'kafka1'
}

topic_name = 'Valores'

print('Creando el productor de Kafka...')
producer = Producer(kafka_conf)
print('Productor de Kafka creado.')

counter = 1

while True:
    value = f'stefan.{counter}'

    try:
        producer.produce(topic_name, value=value)

        producer.flush()

        print(f'Valor enviado exitosamente al tópico: {value}')

    except KafkaException as ex:
        print(f'Error al enviar el valor al tópico: {ex}')
    except Exception as ex:
        print(f'Ocurrió un error inesperado: {ex}')

    counter += 1
    time.sleep(10)
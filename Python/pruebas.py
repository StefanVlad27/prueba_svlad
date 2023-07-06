# lee a partir de donde se quedó la última vez

from confluent_kafka import Consumer, KafkaException, KafkaError

kafka_conf = {
    'bootstrap.servers': 'b-1-public.arq6kafka.vhgjgn.c6.kafka.eu-west-1.amazonaws.com:9196,b-2-public.arq6kafka.vhgjgn.c6.kafka.eu-west-1.amazonaws.com:9196',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'kafka',
    'sasl.password': 'kafka1',
    'group.id': 'kafka1',
    'auto.offset.reset': 'latest'
}

topic_name = 'Valores'

print('Creando el consumidor de Kafka...')
consumer = Consumer(kafka_conf)
print('Consumidor de Kafka creado.')

print('Subscribiéndose al tópico...')
consumer.subscribe([topic_name])

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:

                continue
            else:
                raise KafkaException(message.error())


        value = message.value().decode('utf-8')
        print(f'{value}')
except KafkaException as ex:
    print(f'Error al consumir mensajes de Kafka: {ex}')
except Exception as ex:
    print(f'Ocurrió un error inesperado: {ex}')
finally:
    consumer.close()

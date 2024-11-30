import json
from time import sleep

import typer
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

app = typer.Typer()


@app.command()
def produce(
    message: str = typer.Option(..., help="Message to produce"),
    topic: str = typer.Option(..., help="Kafka topic"),
    kafka: str = typer.Option(..., help="Kafka broker address"),
):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[kafka],
            api_version=(0, 10, 0),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(topic, message)
        producer.flush()
        producer.close()
        typer.echo(
            f"Message '{message}' sent to topic '{topic}' "
            f"on Kafka broker '{kafka}'."
        )
    except Exception as e:
        typer.echo(f"Failed to produce message: {e}")


@app.command()
def consume(
    topic: str = typer.Option(..., help="Kafka topic"),
    kafka: str = typer.Option(..., help="Kafka broker address"),
):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[kafka],
                auto_offset_reset="earliest",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            typer.echo(
                f"Subscribed to topic '{topic}' on Kafka broker '{kafka}'."
                f" Waiting for messages..."
            )
            for message in consumer:
                typer.echo(f"Received message: {message.value}")
        except NoBrokersAvailable:
            typer.echo("No brokers available")
            sleep(5)


if __name__ == "__main__":
    app()

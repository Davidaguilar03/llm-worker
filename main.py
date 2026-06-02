import pika
import json
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
GROQ_API_KEY = os.getenv("DEEPSEEK_API_KEY")

client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=os.getenv("GROQ_API_KEY")
)

def construir_prompt(evento: dict) -> str:
    status = evento.get("status", "")
    message = evento.get("message", "")
    detail = evento.get("detail", {})
    event_info = evento.get("event", {})

    nombre_evento = event_info.get("nombre", "el evento")
    categoria = event_info.get("categoria", "")
    fecha = event_info.get("fecha", "")
    precio = event_info.get("precio", "")

    if status == "success":
        return f"""Eres un asistente amigable de una plataforma de venta de boletas llamada Ticketeo.
Un usuario acaba de comprar exitosamente una boleta para: {nombre_evento}.
Categoría del evento: {categoria}.
Fecha: {fecha}.
Precio pagado: {precio}.

Genera un mensaje de confirmación corto en nombre de Ticketeo (usa "nosotros", no "yo").
Máximo 2 oraciones. No uses markdown, solo texto plano."""
    else:
        provider = detail.get("provider", "")
        status_code = detail.get("details", {}).get("status_code", "")
        return f"""Eres un asistente amigable de una plataforma de venta de boletas llamada Ticketeo.
Un usuario intentó comprar una boleta para: {nombre_evento} pero el pago falló.
Mensaje técnico del error: {message}.
Proveedor de tarjeta: {provider}.
Código de error: {status_code}.

Genera un mensaje empático y persuasivo en nombre de Ticketeo (usa "nosotros", no "yo").
Máximo 2 oraciones. No menciones códigos técnicos. No uses markdown, solo texto plano."""


def procesar_evento(ch, method, properties, body):
    try:
        evento = json.loads(body)
        tracking_id = evento.get("trackingId", "")
        print(f"Evento recibido: trackingId={tracking_id}, status={evento.get('status')}")

        prompt = construir_prompt(evento)
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        mensaje_llm = response.choices[0].message.content.strip()

        print(f"Respuesta LLM: {mensaje_llm}")

        respuesta = {
            "trackingId": tracking_id,
            "phase": "resultado_final",
            "status": evento.get("status"),
            "message": mensaje_llm,
            "detail": evento.get("detail", {})
        }

        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue="mensajes_llm", durable=True)
        channel.basic_publish(
            exchange="",
            routing_key="mensajes_llm",
            body=json.dumps(respuesta),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json"
            )
        )
        connection.close()
        print(f"Respuesta publicada en mensajes_llm para trackingId={tracking_id}")

    except Exception as e:
        print(f"Error procesando evento: {e}")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()
    channel.queue_declare(queue="eventos_pago", durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="eventos_pago", on_message_callback=procesar_evento)
    print("Worker escuchando eventos_pago...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
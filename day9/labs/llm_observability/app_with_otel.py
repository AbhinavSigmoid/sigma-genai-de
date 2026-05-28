import json
import boto3
import phoenix as px

from openinference.instrumentation.bedrock import BedrockInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# ─────────────────────────────────────────────
# Launch Phoenix
# ─────────────────────────────────────────────

print("Launching local Phoenix tracing server...")

session = px.launch_app(port=6006)

# ─────────────────────────────────────────────
# Setup OpenTelemetry
# ─────────────────────────────────────────────

provider = TracerProvider()

provider.add_span_processor(
    SimpleSpanProcessor(
        OTLPSpanExporter(
            endpoint="http://localhost:6006/v1/traces"
        )
    )
)

trace.set_tracer_provider(provider)

# ─────────────────────────────────────────────
# Instrument Bedrock
# ─────────────────────────────────────────────

BedrockInstrumentor().instrument()

# ─────────────────────────────────────────────
# LLM App
# ─────────────────────────────────────────────

def run_support_agent():

    print("\nRunning support agent inquiry...")

    bedrock = boto3.client(
        "bedrock-runtime",
        region_name="us-east-1"
    )

    prompt = """
    You are a customer support agent.

    Customer Query:
    I was charged $50 twice for my order #1048.
    I need refund assistance.
    """

    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "text": prompt
                    }
                ]
            }
        ],
        "inferenceConfig": {
            "maxTokens": 200,
            "temperature": 0.2
        }
    }

    response = bedrock.invoke_model(
        modelId="amazon.nova-lite-v1:0",
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json"
    )

    response_body = json.loads(
        response["body"].read()
    )

    output_text = response_body["output"]["message"]["content"][0]["text"]

    print("\nLLM RESPONSE:\n")
    print(output_text)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":

    run_support_agent()

    print("\nPhoenix running at http://localhost:6006")
    print("Keep this terminal open.")

    import time

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down tracing server.")

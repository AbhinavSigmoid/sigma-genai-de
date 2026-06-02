"""
Phase 2 Investigation Tool — CloudWatch
Shows Lambda errors, Firehose failures, Lambda version changes,
and S3 zero-byte files for the last 8 hours.
"""

import boto3, os, sys, json
from datetime import datetime, timezone, timedelta

def run_investigation(fn_name, hours_back, region):
    cw  = boto3.client("cloudwatch", region_name=region)
    lam = boto3.client("lambda", region_name=region)
    s3  = boto3.client("s3", region_name=region)

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours_back)

    print(f"\nCLOUDWATCH INVESTIGATION (last {hours_back} hours)")
    print("=" * 65)

    # ── Lambda version history ────────────────────────────────────────────────────
    print(f"\n  Lambda: {fn_name} — version/alias history")
    try:
        alias_name = os.getenv("PRODUCER_LAMBDA_ALIAS", "LIVE")
        alias = lam.get_alias(FunctionName=fn_name, Name=alias_name)

        print(f"    Current alias LIVE → version {alias['FunctionVersion']}")

        versions = lam.list_versions_by_function(FunctionName=fn_name)["Versions"]

        numbered = sorted(
            [v for v in versions if v["Version"] != "$LATEST"],
            key=lambda x: int(x["Version"]),
        )

        for v in numbered:
            ts = v.get("LastModified", "?")
            print(
                f"    Version {v['Version']:>3}  modified: {ts}  "
                f"{v.get('Description','')[:40]}"
            )

    except Exception as e:
        print(f"    ERROR: {e}")

    # ── Lambda errors ─────────────────────────────────────────────────────────────
    print(f"\n  Lambda errors per 5-min interval:")

    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start,
            EndTime=now,
            Period=300,
            Statistics=["Sum"],
        )

        errors = sorted(resp["Datapoints"], key=lambda x: x["Timestamp"])

        error_found = False

        for dp in errors:
            if dp["Sum"] > 0:
                ts = dp["Timestamp"].strftime("%H:%M UTC")
                print(f"    {ts}  {int(dp['Sum'])} errors  ← INVESTIGATE")
                error_found = True

        if not error_found:
            print("    None — Lambda reporting no errors")
            print("    NOTE: Lambda can run successfully but produce bad output.")
            print("          No errors here does NOT mean the pipeline is healthy.")
    except Exception as e:
        print(f"    ERROR: {e}")

    # ── Lambda invocation count ───────────────────────────────────────────────────
    print(f"\n  Lambda invocations per hour:")

    try:
        resp2 = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start,
            EndTime=now,
            Period=3600,
            Statistics=["Sum"],
        )

        invocations = sorted(resp2["Datapoints"], key=lambda x: x["Timestamp"])

        for dp in invocations:
            ts = dp["Timestamp"].strftime("%Y-%m-%d %H:%M UTC")
            cnt = int(dp["Sum"])
            print(f"    {ts}  {cnt:>6,} invocations")
    except Exception as e:
        print(f"    ERROR: {e}")

    # ── Firehose delivery freshness ───────────────────────────────────────────────
    stream_name = os.getenv("SIGMA_STREAM", "sigma-transactions")

    print(f"\n  Firehose data freshness (seconds) — high = delivery delay:")

    try:
        resp3 = cw.get_metric_statistics(
            Namespace="AWS/Firehose",
            MetricName="DeliveryToS3.DataFreshness",
            Dimensions=[
                {
                    "Name": "DeliveryStreamName",
                    "Value": f"{stream_name}-firehose",
                }
            ],
            StartTime=start,
            EndTime=now,
            Period=300,
            Statistics=["Maximum"],
        )

        freshness = sorted(resp3["Datapoints"], key=lambda x: x["Timestamp"])

        for dp in freshness:
            ts = dp["Timestamp"].strftime("%H:%M UTC")
            val = int(dp["Maximum"])
            flag = "  ← DELAYED" if val > 600 else ""
            print(f"    {ts}  {val:>6} sec{flag}")

        if not freshness:
            print("    No Firehose metrics found")
    except Exception as e:
        print(f"    ERROR: {e}")

    # ── EXTENSION: S3 Zero-byte File Detection ───────────────────────────────────
    print(f"\n  S3 Zero-byte File Detection:")

    bucket = os.getenv("SIGMA_S3_BUCKET")

    if not bucket:
        print("    SIGMA_S3_BUCKET not configured in .env")
    else:
        try:
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix="bronze/"
            )

            zero_files = []

            for obj in response.get("Contents", []):
                if obj["Size"] == 0:
                    zero_files.append(obj)

            if zero_files:
                print("    WARNING: Zero-byte files detected")

                for obj in zero_files:
                    print(
                        f"    {obj['Key']} | "
                        f"Size={obj['Size']} | "
                        f"Modified={obj['LastModified']}"
                    )
            else:
                print("    No zero-byte files detected")

        except Exception as e:
            print(f"    ERROR checking S3: {e}")

    print()
    print("  KEY QUESTION: Is there a timestamp where Lambda version changed")
    print("  AND Firehose freshness spiked AND Lambda errors appeared?")
    print("  AND S3 contains zero-byte files?")
    print("  That window is the root cause.")
    print()


def lambda_handler(event, context):
    import io
    from contextlib import redirect_stdout

    params = {p["name"]: p["value"] for p in event.get("parameters", [])}
    fn_name = params.get("function_name") or os.getenv("PRODUCER_LAMBDA_NAME", "sigma-data-producer")
    
    # Try parsing hours_back safely
    try:
        hours_back = int(params.get("hours_back") or 8)
    except (ValueError, TypeError):
        hours_back = 8
        
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    f = io.StringIO()
    with redirect_stdout(f):
        run_investigation(fn_name, hours_back, region)
    
    output = f.getvalue()
    result = {"status": "SUCCESS", "report": output}

    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": event.get("actionGroup"),
            "function":    event.get("function"),
            "functionResponse": {
                "responseBody": {"TEXT": {"body": json.dumps(result, default=str)}}
            },
        },
    }


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    region      = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    fn_name     = os.getenv("PRODUCER_LAMBDA_NAME", "sigma-data-producer")
    hours_back  = int(sys.argv[1]) if len(sys.argv) > 1 else 8

    run_investigation(fn_name, hours_back, region)
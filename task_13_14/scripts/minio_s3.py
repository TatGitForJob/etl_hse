import datetime as dt
import hashlib
import hmac
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


def _sign(key, message):
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signature_key(secret_key, date_stamp, region, service):
    key_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    key_region = _sign(key_date, region)
    key_service = _sign(key_region, service)
    return _sign(key_service, "aws4_request")


def _request(method, endpoint_url, bucket, key, access_key, secret_key, body=b""):
    parsed = urlparse(endpoint_url)
    host = parsed.netloc
    region = "us-east-1"
    service = "s3"
    now = dt.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    canonical_uri = "/" + quote(bucket) + "/" + quote(key)
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [method, canonical_uri, "", canonical_headers, signed_headers, payload_hash]
    )
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signing_key = _signature_key(secret_key, date_stamp, region, service)
    signature = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    request = Request(
        endpoint_url.rstrip("/") + canonical_uri,
        data=body if method in {"PUT", "POST"} else None,
        method=method,
        headers={
            "Authorization": authorization,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        },
    )
    with urlopen(request, timeout=30) as response:
        return response.read()


def get_text(endpoint_url, bucket, key, access_key, secret_key):
    return _request("GET", endpoint_url, bucket, key, access_key, secret_key).decode(
        "utf-8"
    )


def put_text(endpoint_url, bucket, key, access_key, secret_key, text):
    _request(
        "PUT",
        endpoint_url,
        bucket,
        key,
        access_key,
        secret_key,
        text.encode("utf-8"),
    )

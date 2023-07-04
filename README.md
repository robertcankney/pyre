## pyre

Pyre is a rate-limiting service, intended to be used as a sidecar for services that need high performance and do not need limits to persist across reboots. It stores all rates in memory, partitioned into collections.

## Configuring pyre

Pyre takes a config for collections as the first argument to the executable. Collection configs are separated by commas:

`collection_name=rate:time period,collection_name_2=rate2:time period2`.

Rate is an integer, and time period should be a `systemd.time`-compatible value with no commas.

## Using pyre

All requests to pyre are done via GET requests a single URL path: `rate/{collection}/{key}`. All responses are JSON, and are either the rate limit response or an error response.

Rate limit response:
```
{
    "allowed": boolean
}
```

Error response:
```
{
    "error": "message"
}
```
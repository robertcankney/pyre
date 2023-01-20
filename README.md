## pyre

Pyre provides rate-limiting as a service, with a number of useful features:
- the ability to link different rate limiting contexts, including linking at different weights
- easily configurable bucketing/TTLs

It's feature set can be best illustrated by using a sample configuration file:

```json
{
    "linkers": [
        {
            "name": "foo",
            "contexts": ["bar"],
            "rate": {
                "count": 10,
                "ttl_seconds": 60,
                "bucket_size": 60,
            }
        },
        {
            "name": "bar",
            "contexts": ["foo"],
            "rate": {
                "count": 10,
                "ttl_seconds": 30
            }
        }
    ],
    "sweep_seconds": 30
}
```

In the above configuration, two contexts are configured, accessible at `${address_and_port}/#{context}/#{key}` - foo, and bar. `count` and `ttl_seconds` are defined for each context - these define how many requests can be done in a given window, and Foo and bar are linked, meaning the total for bar will be added to foo when assessi
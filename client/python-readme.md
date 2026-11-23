# NonoDB Python client

`nonodb-client` is the Python client for [NonoDB](https://nonodb.com), a small
MongoDB-compatible JSON database with a single-writer LMDB core.

- synchronous `NonoDB` and `Collection` APIs;
- asynchronous `AsyncNonoDB` watch and stream helpers;
- Unix socket, TCP, TLS, and mutual TLS transports;
- optional write routing to a primary replica;
- optional caching, retries, batch lookups, SQL, search, TTL, history, vault,
  and event helpers.

```python
from nonodb import NonoDB

with NonoDB("/run/nonodb/nonodb.sock") as db:
    users = db.collection("users")
    users.insert({"name": "Ada", "age": 36})
    print(users.find({"age": {"$gte": 18}}, sort=[("age", -1)]))
```

Requires Python 3.9+ and `httpx`. Install `orjson` for faster JSON handling:

```bash
pip install nonodb-client
pip install "nonodb-client[speed]"
```

Full documentation: <https://nonodb.com/python-client/>.

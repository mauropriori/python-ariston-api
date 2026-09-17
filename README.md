[![CodeQL](https://github.com/mauropriori/python-ariston-api/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/mauropriori/python-ariston-api/actions/workflows/codeql.yml)

# ariston-net-api
A namespaced Python module for controlling Ariston devices with cloud polling.

This is a focused maintenance fork of
[fustom/python-ariston-api](https://github.com/fustom/python-ariston-api). The
original implementation and device support remain credited to fustom and its
contributors.

This fork adds account-wide request serialization and pacing, a 30-second
network timeout, bounded HTTP 429 backoff and avoids immediate retries of HTTP
5xx responses. Galevo's diagnostic menu is refreshed every 30 minutes instead
of on every state poll, while core temperatures and modes continue to refresh
normally. It uses the independent `ariston_net_api` import namespace so it can
coexist with the upstream `ariston` package used by legacy Home Assistant
integrations.

For Lydos Hybrid, the fork also preserves the last stable operating mode
across a temporary BOOST cycle. If the cloud returns `null`, `0`, an unknown
code or malformed data when BOOST ends, consumers continue to see the mode
that preceded BOOST. This recovery is read-only and never writes a mode back
to the appliance.

The following devices are currently supported:
- Ariston Alteas One 24
- Ariston Velis Evo
- Ariston Velis Lux
- Ariston Lydos Hybrid
- Ariston Genus One
- Ariston Nuos Split
- Ariston Thision S
- Chaffoteaux INOA S 24

## Installation
Use pip3 to install the latest version of this module.
```
pip3 install "ariston-net-api @ git+https://github.com/mauropriori/python-ariston-api.git"
```

## The easy way (recommended for testing the module)
First, open Python 3 and import the `ariston_net_api` module.
```
python3
```
```python3
import ariston_net_api
```
### Syncronous
Discover devices if you dont know your gateway id. You can skip this step.
```python3
raw_devices = ariston_net_api.discover("username", "password")
```
For example the gateway id for your first device.
```python3
 raw_devices[0]['gw']
```
Get your device
```python3
device = ariston_net_api.hello("username", "password", "gateway", is_metric, "location")
```
[Go use your device section](#use-your-device)
### Asyncronous
```python3
raw_devices = await ariston_net_api.async_discover("username", "password")
device = await ariston_net_api.async_hello("username", "password", "gateway", is_metric, "location")
```
[Go use your device section](#use-your-device)
## The ariston class way (recommended for integrate the module)
First, open Python 3 and import Ariston class from this module.
```
python3
```
```python3
from ariston_net_api import Ariston
```
Create a new Ariston instance
```python3
ariston = Ariston()
```
Now let's try some functions

### Connect
The cloud requests are asynchronous, so if you call them from a synchronous function or not even from function, you should use asyncio.
```python3
import asyncio
```

Sync
```python3
asyncio.run(ariston.async_connect("username", "password"))
```
Async
```python3
await ariston.async_connect("username", "password")
```
- username: Your ariston cloud username.
- password: Your ariston cloud password.

### Discovery
Use this function to discover devices. You can skip this step if you already know the gateway id.

Sync
```python3
devices = asyncio.run(ariston.async_discover())
```
Async
```python3
devices = await ariston.async_discover()
```

## Say hello
Use this function to create the device object.

Sync
```python3
device = asyncio.run(ariston.async_hello("gateway", is_metric, "location"))
```
Async
```python3
device = await ariston.async_hello("gateway", is_metric, "location")
```
- gateway: You can find the value in the returned discover dictionary name 'gw'
- is_metric: Optional. True or False. True means metric, False means imperial. Only works with Galevo (Alteas One, Genus One, etc) system. Default is True.
- language_tag: Optional. Check https://en.wikipedia.org/wiki/IETF_language_tag Only works with Galevo (Alteas One, Genus One, etc) system. Default is "en-US".

## Use your device
### Get device features
Sync
```python3
device.get_features()
```
Async
```python3
await device.async_get_features()
```
### Get device data
Sync
```python3
device.update_state()
```
Async
```python3
await device.async_update_state()
```
### Get device energy
Sync
```python3
device.update_energy()
```
Async
```python3
await device.async_update_energy()
```

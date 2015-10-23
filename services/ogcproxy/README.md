# OGCProxy

The OGCProxy component is a reverse proxy intended to multiplex between many GeoServer instances.
Rather than keep all datasets in the system in all GeoServer instances, the intent is to limit the number of layers deployed to any individual GeoServer and use the OGCProxy to present all layers as if they are coming from the same service.

In order to do this, the OGCProxy needs to partially parse incoming OGC requests to determine which dataset they reference.
This requires special support for each protocol.
Currently supported are WFS, WCS, and WMS, but only KVP Get requests (query parameters.)
This has been tested with the QGIS WMS client, but more work is needed around GetCapabilities responses before it will be generally usable.

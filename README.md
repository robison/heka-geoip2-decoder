# Heka GeoIp2Decoder plugin

This plugin is a rewrite of the original GeoIpDecoder plugin that is shipped with Heka.
It has the following benefits over the official plugin:
* does not depend on the libgeoip C-libraries
* supports Maxmind's GeoIP2 format (mmdb), which is supposed to be more accurate
* supports hostname lookups
* somewhat configurable output

## Dependencies
* https://github.com/oschwald/geoip2-golang

## Configuraton
* **db_file**
The location of the GeoLite2-City.mmdb database. Defaults to {HEKA_SHARE_DIR}/GeoLite2-City.mmdb
* **source_host_field**
The message field containing the hostname or IP to be looked up
* **target_field_prefix**
The prefix for all the new fields created by the decoder or the name
of the field containing the JSON object (see raw_json_object)
* **raw_json_object**
When true, all the location info is put into a separate JSON object. The "target_field_prefix" is the name of the field that will contain the raw bytes of the object. When false (the default), all the info is put into separate
fields with the prefix "target_field_prefix"
* **dns_lookup**
If true, it will do a DNS lookup on the string contained in "source_host_field" otherwise it will consider the contents of "source_host_field" to be an IP address (default)

## Example
```
[GeoIp2Decoder]
db_file = "/var/cache/GeoLite2-City.mmdb
source_host_field = "remote_addr"
target_field_prefix = "geoip"
dns_lookup = false
```

## Installation
In order to use the plugin you have to recompile Heka, as all Go plugins have to be compiled into it because of Go binaries being *mostly* statically linked.
* Check out the Heka source from git
* In the source root directory edit (or create) the cmake/plugin_loader.cmake file and add
`add_external_plugin(git https://github.com/mart1nl/heka-geoip2-decoder master)`
* Then add the needed extra packages' get into build.sh right before `make`
* `go get github.com/oschwald/geoip2-golang`




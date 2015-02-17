# Heka GeoIp2Decoder plugin

This plugin is a rewrite of the original GeoIpDecoder plugin that is shipped with Heka.
It has the following benefits over the official plugin:
* does not depend on the libgeoip C-libraries
* supports Maxmind's GeoIP2 format (mmdb), which is supposed to be more accurate
* supports hostname lookups
* somewhat configurable output

# Dependencies
* https://github.com/oschwald/geoip2-golang


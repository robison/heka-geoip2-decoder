/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Michael Gibson (michael.gibson79@gmail.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package geoip2

import (
        "errors"
        "fmt"
        "github.com/oschwald/geoip2-golang"
        "github.com/mozilla-services/heka/message"
        . "github.com/mozilla-services/heka/pipeline"
        "net"
        "bytes"
        "strconv"
)

type GeoIpDecoderConfig struct {
        DatabaseFile  string `toml:"db_file"`
        SourceHostField string `toml:"source_host_field"`
        TargetField   string `toml:"target_field_prefix"`

        //When true, all the location info is put into a separate
        //JSON object. The "target_field_prefix" is the name of the field
        //that will contain the raw bytes of the object.
        //When false (the default), all the info is put into separate
        //fields with the prefix "target_field_prefix"
        JSONObject   bool `toml:"raw_json_object"`

        //If true, it will do a DNS lookup on the string contained
        //in "source_host_field" otherwise it will consider the contents
        //of "source_host_field" to be a IP address (default)
        DNSLookup    bool    `toml:"dns_lookup"`
}

type GeoIpDecoder struct {
        DatabaseFile  string
        SourceHostField string
        TargetField   string
        JSONObject bool
        DNSLookup bool
        db            *geoip2.Reader
        pConfig       *PipelineConfig
}

// Heka will call this before calling any other methods to give us access to
// the pipeline configuration.
func (ld *GeoIpDecoder) SetPipelineConfig(pConfig *PipelineConfig) {
        ld.pConfig = pConfig
}

func (ld *GeoIpDecoder) ConfigStruct() interface{} {
        globals := ld.pConfig.Globals
        return &GeoIpDecoderConfig{
                DatabaseFile:  globals.PrependShareDir("GeoLite2-City.mmdb"),
                SourceHostField: "",
                TargetField:   "geoip",
        }
}

func (ld *GeoIpDecoder) Init(config interface{}) (err error) {
        conf := config.(*GeoIpDecoderConfig)

        if string(conf.SourceHostField) == "" {
                return errors.New("`source_host_field` must be specified")
        }

        if conf.TargetField == "" {
                return errors.New("`target_field` must be specified")
        }

        ld.TargetField = conf.TargetField
        ld.SourceHostField = conf.SourceHostField
        ld.JSONObject = conf.JSONObject
        ld.DNSLookup = conf.DNSLookup

        if ld.db == nil {
                ld.db, err = geoip2.Open(conf.DatabaseFile)
        }
        if err != nil {
                return fmt.Errorf("Could not open GeoIP database: %s\n")
        }

        return
}

//Creates new Heka Message fields for the following location info
//(if they are contained in the record): location coordinates,
//country ISO code, country name in English, city name in English
func (ld *GeoIpDecoder) CreateMessageFields(record *geoip2.City, pack *PipelinePack) (err error) {
        countrycode := record.Country.IsoCode
        country := record.Country.Names["en"]
        city := record.City.Names["en"]

        lat := strconv.FormatFloat(record.Location.Latitude,'g', 16, 32)
        lon := strconv.FormatFloat(record.Location.Longitude,'g', 16, 32)

        if ld.JSONObject {
                buf := bytes.Buffer{}
                buf.WriteString(`{`)

                buf.WriteString(`"location":[`)
                buf.WriteString(lon)
                buf.WriteString(`,`)
                buf.WriteString(lat)
                buf.WriteString(`]`)

                if countrycode != "" {
                        buf.WriteString(`,"country_code":"`)
                        buf.WriteString(countrycode)
                        buf.WriteString(`"`)
                }
                if country != "" {
                        buf.WriteString(`,"country":"`)
                        buf.WriteString(country)
                        buf.WriteString(`"`)
                }
                if city != "" {
                        buf.WriteString(`,"city":"`)
                        buf.WriteString(city)
                        buf.WriteString(`"`)
                }

                buf.WriteString(`}`)

                var nf *message.Field
                nf, err = message.NewField(ld.TargetField, buf.Bytes(), "")
                pack.Message.AddField(nf)
        } else {
                //Since Heka message cannot have an array as a field
                //value, we encode the [lon,lat] JSON array directly
                //as bytes
                buf := bytes.Buffer{}
                buf.WriteString(`[`)
                buf.WriteString(lon)
                buf.WriteString(`,`)
                buf.WriteString(lat)
                buf.WriteString(`]`)

                var nf *message.Field
                nf, err = message.NewField(
                        fmt.Sprintf("%s_location",ld.TargetField),
                        buf.Bytes(),"")
                pack.Message.AddField(nf)

                if countrycode != "" {
                        nf, err = message.NewField(
                                fmt.Sprintf("%s_countrycode",ld.TargetField),
                                countrycode, "")
                        pack.Message.AddField(nf)
                }
                if country != "" {
                        nf, err = message.NewField(
                                fmt.Sprintf("%s_country",ld.TargetField),
                                country, "")
                        pack.Message.AddField(nf)
                }
                if city != "" {
                        nf, err = message.NewField(
                                fmt.Sprintf("%s_city",ld.TargetField),
                                city, "")
                        pack.Message.AddField(nf)
                }

        }

        return
}

func (ld *GeoIpDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, fail error) {
        var hostAddr, _ = pack.Message.GetFieldValue(ld.SourceHostField)
        var ip net.IP

        host, ok := hostAddr.(string)

        if !ok {
                // IP field was not a string. Field could just be blank. Return without error.
                packs = []*PipelinePack{pack}
                return
        }
        
        if ld.DNSLookup {
            ips, err := net.LookupIP(host)
            if err != nil {
                    // Could not get an IP for the host, can happen.
                    packs = []*PipelinePack{pack}
                    return
            }       
            ip = ips[0]
        } else {
            ip = net.ParseIP(host)
            if ip == nil {
                //Not a valid IP address in the host string, still we don't
                //want to send an error
                packs = []*PipelinePack{pack}
                return
            }
        }
        if ld.db != nil {
                //We only check the first IP returned from the local resolver
                //TODO implement a configuration option to allow checking all IPs
                rec, err := ld.db.City(ip)
                if err != nil || 
                (rec.Location.Longitude == 0.0 && rec.Location.Latitude == 0.0){
                        // IP address did not return a valid GeoIp record but that's ok sometimes(private ip?). Return without error.
                        packs = []*PipelinePack{pack}
                        return
                }
                ld.CreateMessageFields(rec, pack)
        }

        packs = []*PipelinePack{pack}

        return
}

func init() {
        RegisterPlugin("GeoIp2Decoder", func() interface{} {
                return new(GeoIpDecoder)
        })
}

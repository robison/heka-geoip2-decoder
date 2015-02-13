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
)

type GeoIpDecoderConfig struct {
        DatabaseFile  string `toml:"db_file"`
        SourceHostField string `toml:"source_host_field"`
        TargetField   string `toml:"target_field"`
}

type GeoIpDecoder struct {
        DatabaseFile  string
        SourceHostField string
        TargetField   string
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

        if ld.db == nil {
                ld.db, err = geoip2.Open(conf.DatabaseFile)
        }
        if err != nil {
                return fmt.Errorf("Could not open GeoIP database: %s\n")
        }

        return
}

func (ld *GeoIpDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, fail error) {
        var hostAddr, _ = pack.Message.GetFieldValue(ld.SourceHostField)

        host, ok := hostAddr.(string)

        if !ok {
                // IP field was not a string. Field could just be blank. Return without error.
                packs = []*PipelinePack{pack}
                return
        }

        ips, err := net.LookupIP(host)
        if err != nil {
                // Could not get an IP for the host, can happen.
                packs = []*PipelinePack{pack}
                return
        }       

        if ld.db != nil {
                //We only check the first IP returned from the local resolver
                //TODO implement a configuration option to allow checking all IPs
                rec, err := ld.db.City(ips[0])
                if err != nil {
                        // IP address did not return a valid GeoIp record but that's ok sometimes(private ip?). Return without error.
                        packs = []*PipelinePack{pack}
                        return
                }
                location := []float64{rec.Location.Latitude, rec.Location.Longitude}

                var nf *message.Field
                nf, err = message.NewField(ld.TargetField, location, "")
                pack.Message.AddField(nf)
        }

        packs = []*PipelinePack{pack}

        return
}

func init() {
        RegisterPlugin("GeoIp2Decoder", func() interface{} {
                return new(GeoIpDecoder)
        })
}

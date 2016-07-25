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
        "fmt"
        "github.com/oschwald/geoip2-golang"
        "github.com/mozilla-services/heka/message"
        . "github.com/mozilla-services/heka/pipeline"
        "net"
        "bytes"
        "strconv"
)

type GeoIp2Decoder struct {
        AnonDatabaseFile      string
        CityDatabaseFile      string
        ConnDatabaseFile      string
        ISPDatabaseFile       string
        SourceAddrFields      []string
        TargetField           string
        Language              string
        JSONObject            bool
        DNSLookup             bool
        anon_db               *geoip2.Reader
        city_db               *geoip2.Reader
        conn_db               *geoip2.Reader
        isp_db                *geoip2.Reader
        Config                *GeoIp2DecoderConfig
        pConfig               *PipelineConfig
}

type GeoIp2DecoderConfig struct {
        AnonDatabaseFile   string   `toml:"db_anon"`
        CityDatabaseFile   string   `toml:"db_city"`
        ConnDatabaseFile   string   `toml:"db_conn"`
        ISPDatabaseFile    string   `toml:"db_isp"`
        SourceAddrFields   []string `toml:"source_addr_fields"`
        TargetField        string   `toml:"target_field_prefix"`
        Language           string   `toml:"language"`

        // When true, all the location info is put into a separate
        // JSON object. The "target_field_prefix" is the name of the field
        // that will contain the raw bytes of the object.
        // When false (the default), all the info is put into separate
        // fields with the prefix "target_field_prefix"
        JSONObject         bool `toml:"raw_json_object"`

        // If true, it will do a DNS lookup on the string contained
        // in "source_host_field".
        // When false, it will consider the contents
        // of "source_host_field" to be a IP address (default)
        DNSLookup          bool `toml:"dns_lookup"`
}

// Heka will call this before calling any other methods to give us access to
// the pipeline configuration.
func (gi2 *GeoIp2Decoder) SetPipelineConfig(pConfig *PipelineConfig) {
        gi2.pConfig = pConfig
}

func (gi2 *GeoIp2Decoder) ConfigStruct() interface{} {
        globals := gi2.pConfig.Globals
        safs := make([]string, 1)
        safs[0] = "remote_addr"
        return &GeoIp2DecoderConfig{
                CityDatabaseFile:      globals.PrependShareDir("GeoLite2-City.mmdb"),
                SourceAddrFields:      safs,
                TargetField:           "geoip",
                Language:              "en",
        }
}

func (gi2 *GeoIp2Decoder) Init(config interface{}) (err error) {
        gi2.Config = config.(*GeoIp2DecoderConfig)

        if len(gi2.Config.SourceAddrFields) == 0 {
                gi2.LogError(fmt.Errorf("At least one source address field must be specified."))
        }

        gi2.SourceAddrFields = make([]string, len(gi2.Config.SourceAddrFields))

        for i, name := range gi2.Config.SourceAddrFields {
                gi2.SourceAddrFields[i] = name
        }

        if gi2.Config.TargetField == "" {
                gi2.LogError(fmt.Errorf("`target_field` must be specified"))
        }

        gi2.DNSLookup          = gi2.Config.DNSLookup
        gi2.JSONObject         = gi2.Config.JSONObject
        gi2.Language           = gi2.Config.Language
        gi2.TargetField        = gi2.Config.TargetField

        if gi2.anon_db == nil && gi2.Config.AnonDatabaseFile != "" {
                gi2.anon_db, err = geoip2.Open(gi2.Config.AnonDatabaseFile)
        }
        if err != nil {
                gi2.LogError(fmt.Errorf("Error: Could not open GeoIP2-Anonymous-IP database: %s, skipping\n", gi2.Config.AnonDatabaseFile))
        }
        if gi2.city_db == nil && gi2.Config.CityDatabaseFile != "" {
                gi2.city_db, err = geoip2.Open(gi2.Config.CityDatabaseFile)
        }
        if err != nil {
                gi2.LogError(fmt.Errorf("Error: Could not open GeoIP2-City database: %s, skipping\n", gi2.Config.CityDatabaseFile))
        }
        if gi2.conn_db == nil && gi2.Config.ConnDatabaseFile != "" {
                gi2.conn_db, err = geoip2.Open(gi2.Config.ConnDatabaseFile)
        }
        if err != nil {
                gi2.LogError(fmt.Errorf("Error: Could not open GeoIP2-Connection-Type database: %s, skipping\n", gi2.Config.ConnDatabaseFile))
        }
        if gi2.isp_db == nil && gi2.Config.ISPDatabaseFile != "" {
                gi2.isp_db, err = geoip2.Open(gi2.Config.ISPDatabaseFile)
        }
        if err != nil {
                gi2.LogError(fmt.Errorf("Error: Could not open GeoIP2-ISP database: %s, skipping\n", gi2.Config.ISPDatabaseFile))
        }

        return
}

//Creates new Heka Message fields for the following location info
//(if they are contained in the record): location coordinates,
//country ISO code, country name in English, city name in English
func (gi2 *GeoIp2Decoder) CreateMessageFieldsCity(record *geoip2.City, pack *PipelinePack) (err error) {
        countrycode := record.Country.IsoCode
        country     := record.Country.Names[gi2.Language]
        city        := record.City.Names[gi2.Language]

        lat := strconv.FormatFloat(record.Location.Latitude,'g', 16, 32)
        lon := strconv.FormatFloat(record.Location.Longitude,'g', 16, 32)

        if gi2.JSONObject {
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

                gi2.AddField(pack, gi2.TargetField, buf.Bytes())
        } else {
                gi2.AddField(pack, fmt.Sprintf("%s_location",gi2.TargetField), fmt.Sprintf("%s, %s", lat, lon))

                if countrycode != "" {
                        gi2.AddField(pack, fmt.Sprintf("%s_country_code",gi2.TargetField), countrycode)
                }
                if country != "" {
                        gi2.AddField(pack, fmt.Sprintf("%s_country",gi2.TargetField), country)
                }
                if city != "" {
                        gi2.AddField(pack, fmt.Sprintf("%s_city",gi2.TargetField), city)
                }
        }

        return
}

func (gi2 *GeoIp2Decoder) CreateMessageFieldsISP(record *geoip2.ISP, pack *PipelinePack) (err error) {
        asnum          := record.AutonomousSystemNumber
        asname         := record.AutonomousSystemOrganization
        isp            := record.ISP
        organization   := record.Organization

        if gi2.JSONObject {
                buf := bytes.Buffer{}
                buf.WriteString(`{`)

                if asnum != 0 {
                        buf.WriteString(`"asnum":`)
                        buf.WriteString(strconv.FormatUint(uint64(asnum), 10))
                }
                if asname != "" {
                        buf.WriteString(`,"asname":"`)
                        buf.WriteString(asname)
                        buf.WriteString(`"`)
                }
                if isp != "" {
                        buf.WriteString(`,"isp":"`)
                        buf.WriteString(isp)
                        buf.WriteString(`"`)
                }
                if organization != "" {
                        buf.WriteString(`,"organization":"`)
                        buf.WriteString(organization)
                        buf.WriteString(`"`)
                }

                buf.WriteString(`}`)

                gi2.AddField(pack, gi2.TargetField, buf.Bytes())

        } else {
                if asnum != 0 {
                        gi2.AddField(pack, fmt.Sprintf("asnum"), gi2.GetData(asnum))
                }
                if asname != "" {
                        gi2.AddField(pack, fmt.Sprintf("asname"), asname)
                }
                if isp != "" {
                        gi2.AddField(pack, fmt.Sprintf("isp"), isp)
                }
                if organization != "" {
                        gi2.AddField(pack, fmt.Sprintf("organization"), organization)
                }
        }

        return
}

func (gi2 *GeoIp2Decoder) CreateMessageFieldsAnonymousIP(record *geoip2.AnonymousIP, pack *PipelinePack) (err error) {
        anon        := record.IsAnonymous
        anonvpn     := record.IsAnonymousVPN
        hostingpro  := record.IsHostingProvider
        publicproxy := record.IsPublicProxy
        torexitnode := record.IsTorExitNode

        if gi2.JSONObject {
                buf := bytes.Buffer{}
                buf.WriteString(`{`)

                if anon {
                        buf.WriteString(`,"anonymous_ip": true,`)
                }
                if anonvpn {
                        buf.WriteString(`,"anonymous_vpn": true`)
                }
                if hostingpro {
                        buf.WriteString(`,"hosting_provider": true`)
                }
                if publicproxy {
                        buf.WriteString(`,"public_proxy": true`)
                }
                if torexitnode {
                        buf.WriteString(`,"tor_exit_node": true`)
                }

                buf.WriteString(`}`)

                gi2.AddField(pack, gi2.TargetField, buf.Bytes())

        } else {
                if anon {
                        gi2.AddField(pack, fmt.Sprintf("anonymous_ip"), anon)
                }
                if anonvpn {
                        gi2.AddField(pack, fmt.Sprintf("anonymous_vpn"), anonvpn)
                }
                if hostingpro {
                        gi2.AddField(pack, fmt.Sprintf("hosting_provider"), hostingpro)
                }
                if publicproxy {
                        gi2.AddField(pack, fmt.Sprintf("public_proxy"), publicproxy)
                }
                if torexitnode {
                        gi2.AddField(pack, fmt.Sprintf("tor_exit_node"), torexitnode)
                }
        }

        return
}

func (gi2 *GeoIp2Decoder) CreateMessageFieldsConnectionType(record *geoip2.ConnectionType, pack *PipelinePack) (err error) {
        conntype := record.ConnectionType

        if gi2.JSONObject {
                buf := bytes.Buffer{}
                buf.WriteString(`{`)

                if conntype != "" {
                        buf.WriteString(`"connection_type":"`)
                        buf.WriteString(conntype)
                        buf.WriteString(`"`)
                }

                buf.WriteString(`}`)

                gi2.AddField(pack, gi2.TargetField, buf.Bytes())

        } else {
                if conntype != "" {
                        gi2.AddField(pack, fmt.Sprintf("connection_type"), conntype)
                }
        }

        return
}

func (gi2 *GeoIp2Decoder) Decode(pack *PipelinePack) (packs []*PipelinePack, fail error) {
        var ip net.IP
        var found bool = false
        for _, hostAddr := range gi2.SourceAddrFields {
                var hostValue, _ = pack.Message.GetFieldValue(hostAddr)
                host, ok := hostValue.(string)

                if !ok {
                        // IP field was not a string. Field could just be blank. Continue processing in loop.
                        continue
                }

                if gi2.DNSLookup {
                    ips, err := net.LookupIP(host)
                    if err != nil {
                            // Could not get an IP for the host, can happen. Continue processing in loop.
                            continue
                    }
                    ip = ips[0]
                } else {
                    ip = net.ParseIP(host)
                    if ip == nil {
                            // Not a valid IP address in the host string. Continue processing in loop.
                            continue
                    }
                }
                if gi2.anon_db != nil {
                        rec, err := gi2.anon_db.AnonymousIP(ip)
                        if err == nil &&
                        (rec.IsAnonymous || rec.IsAnonymousVPN || rec.IsHostingProvider || rec.IsPublicProxy || rec.IsTorExitNode){
                                found = true
                                gi2.CreateMessageFieldsAnonymousIP(rec, pack)
                        }
                }
                if gi2.city_db != nil {
                        rec, err := gi2.city_db.City(ip)
                        if err == nil &&
                        (rec.Location.Longitude != 0.0 && rec.Location.Latitude != 0.0){
                                found = true
                                gi2.CreateMessageFieldsCity(rec, pack)
                        }
                }
                if gi2.conn_db != nil {
                        rec, err := gi2.conn_db.ConnectionType(ip)
                        if err == nil &&
                        (rec.ConnectionType != ""){
                                found = true
                                gi2.CreateMessageFieldsConnectionType(rec, pack)
                        }
                }
                if gi2.isp_db != nil {
                        rec, err := gi2.isp_db.ISP(ip)
                        if err != nil ||
                        (rec.AutonomousSystemNumber != 0 || rec.AutonomousSystemOrganization != "" || rec.ISP != "" || rec.Organization != ""){
                                found = true
                                gi2.CreateMessageFieldsISP(rec, pack)
                        }
                }
                if found { break }
        }

        packs = []*PipelinePack{pack}

        return
}

func (gi2 *GeoIp2Decoder) AddField(pack *PipelinePack, name string, value interface{}) error {

        field, err := message.NewField(name, value, "")
        if err != nil {
                gi2.LogError(fmt.Errorf("error adding field '%s': %s", name, err))
        } else {
                pack.Message.AddField(field)
        }
        return nil
}

// getData converts uint64 to int64 for Heka supported data type
func (gi2 *GeoIp2Decoder) GetData(v interface{}) interface{} {
        switch d := v.(type) {
        case uint64:
                return int64(d)
        case uint32:
                return int32(d)
        case uint:
                return int(d)
        default:
                return d
        }
}

func (gi2 *GeoIp2Decoder) LogError(err error) {
        LogError.Printf("GeoIp2Decoder: %s", err)
}

func init() {
        RegisterPlugin("GeoIp2Decoder", func() interface{} {
                return new(GeoIp2Decoder)
        })
}


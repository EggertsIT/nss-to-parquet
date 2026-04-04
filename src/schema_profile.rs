use anyhow::Result;

use crate::schema::{FieldDef, LogicalType, SchemaDef};

pub const DEFAULT_SCHEMA_PROFILE: &str = "zscaler_web_v1";
pub const OPS_SCHEMA_PROFILE: &str = "zscaler_web_v2_ops";

pub const ZSCALER_WEB_V1_FEED_TEMPLATE: &str = "\"%s{time}\",\"%s{ologin}\",\"%s{proto}\",\"%s{eurl}\",\"%s{action}\",\"%s{appname}\",\"%s{app_status}\",\"%s{appclass}\",\"%d{reqsize}\",\"%d{respsize}\",\"%s{urlclass}\",\"%s{urlsupercat}\",\"%s{urlcat}\",\"%s{malwarecat}\",\"%s{threatname}\",\"%d{riskscore}\",\"%s{threatseverity}\",\"%s{dlpeng}\",\"%s{dlpdict}\",\"%s{location}\",\"%s{dept}\",\"%s{cip}\",\"%s{sip}\",\"%s{reqmethod}\",\"%s{respcode}\",\"%s{ua}\",\"%s{ereferer}\",\"%s{ruletype}\",\"%s{rulelabel}\",\"%s{contenttype}\",\"%s{unscannabletype}\",\"%s{deviceowner}\",\"%s{devicehostname}\",\"%s{keyprotectiontype}\"";
pub const ZSCALER_WEB_V2_OPS_FEED_TEMPLATE: &str = "\"%s{time}\",\"%s{tz}\",\"%d{epochtime}\",\"%s{login}\",\"%s{dept}\",\"%s{action}\",\"%s{ruletype}\",\"%s{rulelabel}\",\"%s{reason}\",\"%s{proto}\",\"%s{reqmethod}\",\"%s{respcode}\",\"%s{url}\",\"%s{host}\",\"%s{referer}\",\"%s{ua}\",\"%s{appname}\",\"%s{appclass}\",\"%s{app_status}\",\"%s{urlclass}\",\"%s{urlsupercat}\",\"%s{urlcat}\",\"%s{threatname}\",\"%s{malwarecat}\",\"%s{malwareclass}\",\"%d{riskscore}\",\"%s{threatseverity}\",\"%s{dlpeng}\",\"%s{dlpdict}\",\"%s{ssldecrypted}\",\"%s{ssl_rulename}\",\"%s{externalspr}\",\"%s{keyprotectiontype}\",\"%s{clienttlsversion}\",\"%s{srvtlsversion}\",\"%s{clientsslcipher}\",\"%s{srvsslcipher}\",\"%s{cltsslfailreason}\",\"%d{cltsslfailcount}\",\"%s{srvocspresult}\",\"%s{srvcertchainvalpass}\",\"%s{srvcertvalidationtype}\",\"%s{is_ssluntrustedca}\",\"%s{is_sslselfsigned}\",\"%s{is_sslexpiredca}\",\"%s{cip}\",\"%s{cintip}\",\"%s{cpubip}\",\"%s{sip}\",\"%d{clt_sport}\",\"%d{srv_dport}\",\"%s{srcip_country}\",\"%s{dstip_country}\",\"%s{is_src_cntry_risky}\",\"%s{is_dst_cntry_risky}\",\"%s{location}\",\"%s{userlocationname}\",\"%s{datacenter}\",\"%s{datacentercountry}\",\"%s{trafficredirectmethod}\",\"%s{alpnprotocol}\",\"%d{reqsize}\",\"%d{respsize}\",\"%d{totalsize}\",\"%s{devicehostname}\",\"%s{deviceowner}\"";

#[derive(Debug, Clone)]
pub struct SchemaProfile {
    pub id: &'static str,
    pub description: &'static str,
    pub time_field: &'static str,
    pub time_format: &'static str,
    pub timezone: &'static str,
    pub feed_template: &'static str,
    pub schema: SchemaDef,
}

pub fn supported_profiles() -> &'static [&'static str] {
    &[DEFAULT_SCHEMA_PROFILE, OPS_SCHEMA_PROFILE]
}

pub fn resolve_profile(id: &str) -> Result<SchemaProfile> {
    match id {
        DEFAULT_SCHEMA_PROFILE => Ok(zscaler_web_v1()),
        OPS_SCHEMA_PROFILE => Ok(zscaler_web_v2_ops()),
        _ => anyhow::bail!(
            "unsupported schema.profile '{}'; supported: {}",
            id,
            supported_profiles().join(", ")
        ),
    }
}

fn zscaler_web_v1() -> SchemaProfile {
    SchemaProfile {
        id: DEFAULT_SCHEMA_PROFILE,
        description: "Canonical Zscaler NSS Web feed profile with fixed 34-field order for nss-ingestor/nss-quarry.",
        time_field: "time",
        time_format: "%a %b %d %H:%M:%S %Y",
        timezone: "UTC",
        feed_template: ZSCALER_WEB_V1_FEED_TEMPLATE,
        schema: SchemaDef {
            fields: vec![
                field("time", LogicalType::Timestamp, false),
                field("ologin", LogicalType::String, true),
                field("proto", LogicalType::String, true),
                field("eurl", LogicalType::String, true),
                field("action", LogicalType::String, true),
                field("appname", LogicalType::String, true),
                field("app_status", LogicalType::String, true),
                field("appclass", LogicalType::String, true),
                field("reqsize", LogicalType::Int64, true),
                field("respsize", LogicalType::Int64, true),
                field("urlclass", LogicalType::String, true),
                field("urlsupercat", LogicalType::String, true),
                field("urlcat", LogicalType::String, true),
                field("malwarecat", LogicalType::String, true),
                field("threatname", LogicalType::String, true),
                field("riskscore", LogicalType::Int64, true),
                field("threatseverity", LogicalType::String, true),
                field("dlpeng", LogicalType::String, true),
                field("dlpdict", LogicalType::String, true),
                field("location", LogicalType::String, true),
                field("dept", LogicalType::String, true),
                field("cip", LogicalType::Ip, true),
                field("sip", LogicalType::Ip, true),
                field("reqmethod", LogicalType::String, true),
                field("respcode", LogicalType::String, true),
                field("ua", LogicalType::String, true),
                field("ereferer", LogicalType::String, true),
                field("ruletype", LogicalType::String, true),
                field("rulelabel", LogicalType::String, true),
                field("contenttype", LogicalType::String, true),
                field("unscannabletype", LogicalType::String, true),
                field("deviceowner", LogicalType::String, true),
                field("devicehostname", LogicalType::String, true),
                field("keyprotectiontype", LogicalType::String, true),
            ],
        },
    }
}

fn zscaler_web_v2_ops() -> SchemaProfile {
    SchemaProfile {
        id: OPS_SCHEMA_PROFILE,
        description: "Operations-focused Zscaler NSS Web feed profile optimized for blocked traffic, connection issues, SSL/TLS diagnostics, and geo reporting.",
        time_field: "time",
        time_format: "%a %b %d %H:%M:%S %Y",
        timezone: "UTC",
        feed_template: ZSCALER_WEB_V2_OPS_FEED_TEMPLATE,
        schema: SchemaDef {
            fields: vec![
                field("time", LogicalType::Timestamp, false),
                field("tz", LogicalType::String, true),
                field("epochtime", LogicalType::Int64, true),
                field("login", LogicalType::String, true),
                field("dept", LogicalType::String, true),
                field("action", LogicalType::String, true),
                field("ruletype", LogicalType::String, true),
                field("rulelabel", LogicalType::String, true),
                field("reason", LogicalType::String, true),
                field("proto", LogicalType::String, true),
                field("reqmethod", LogicalType::String, true),
                field("respcode", LogicalType::String, true),
                field("url", LogicalType::String, true),
                field("host", LogicalType::String, true),
                field("referer", LogicalType::String, true),
                field("ua", LogicalType::String, true),
                field("appname", LogicalType::String, true),
                field("appclass", LogicalType::String, true),
                field("app_status", LogicalType::String, true),
                field("urlclass", LogicalType::String, true),
                field("urlsupercat", LogicalType::String, true),
                field("urlcat", LogicalType::String, true),
                field("threatname", LogicalType::String, true),
                field("malwarecat", LogicalType::String, true),
                field("malwareclass", LogicalType::String, true),
                field("riskscore", LogicalType::Int64, true),
                field("threatseverity", LogicalType::String, true),
                field("dlpeng", LogicalType::String, true),
                field("dlpdict", LogicalType::String, true),
                field("ssldecrypted", LogicalType::String, true),
                field("ssl_rulename", LogicalType::String, true),
                field("externalspr", LogicalType::String, true),
                field("keyprotectiontype", LogicalType::String, true),
                field("clienttlsversion", LogicalType::String, true),
                field("srvtlsversion", LogicalType::String, true),
                field("clientsslcipher", LogicalType::String, true),
                field("srvsslcipher", LogicalType::String, true),
                field("cltsslfailreason", LogicalType::String, true),
                field("cltsslfailcount", LogicalType::Int64, true),
                field("srvocspresult", LogicalType::String, true),
                field("srvcertchainvalpass", LogicalType::String, true),
                field("srvcertvalidationtype", LogicalType::String, true),
                field("is_ssluntrustedca", LogicalType::String, true),
                field("is_sslselfsigned", LogicalType::String, true),
                field("is_sslexpiredca", LogicalType::String, true),
                field("cip", LogicalType::Ip, true),
                field("cintip", LogicalType::Ip, true),
                field("cpubip", LogicalType::Ip, true),
                field("sip", LogicalType::Ip, true),
                field("clt_sport", LogicalType::Int64, true),
                field("srv_dport", LogicalType::Int64, true),
                field("srcip_country", LogicalType::String, true),
                field("dstip_country", LogicalType::String, true),
                field("is_src_cntry_risky", LogicalType::String, true),
                field("is_dst_cntry_risky", LogicalType::String, true),
                field("location", LogicalType::String, true),
                field("userlocationname", LogicalType::String, true),
                field("datacenter", LogicalType::String, true),
                field("datacentercountry", LogicalType::String, true),
                field("trafficredirectmethod", LogicalType::String, true),
                field("alpnprotocol", LogicalType::String, true),
                field("reqsize", LogicalType::Int64, true),
                field("respsize", LogicalType::Int64, true),
                field("totalsize", LogicalType::Int64, true),
                field("devicehostname", LogicalType::String, true),
                field("deviceowner", LogicalType::String, true),
            ],
        },
    }
}

fn field(name: &str, logical_type: LogicalType, nullable: bool) -> FieldDef {
    FieldDef {
        name: name.to_string(),
        logical_type,
        nullable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_has_expected_shape() {
        let profile = resolve_profile(DEFAULT_SCHEMA_PROFILE).expect("profile");
        assert_eq!(profile.schema.fields.len(), 34);
        assert_eq!(profile.schema.fields[0].name, "time");
        assert!(!profile.schema.fields[0].nullable);
        assert_eq!(profile.schema.fields[21].name, "cip");
        assert_eq!(profile.schema.fields[21].logical_type, LogicalType::Ip);
    }

    #[test]
    fn ops_profile_has_expected_shape() {
        let profile = resolve_profile(OPS_SCHEMA_PROFILE).expect("profile");
        assert_eq!(profile.schema.fields.len(), 66);
        assert_eq!(profile.schema.fields[0].name, "time");
        assert!(!profile.schema.fields[0].nullable);
        assert_eq!(profile.schema.fields[3].name, "login");
        assert_eq!(profile.schema.fields[45].name, "cip");
        assert_eq!(profile.schema.fields[45].logical_type, LogicalType::Ip);
        assert_eq!(profile.schema.fields[49].name, "clt_sport");
        assert_eq!(profile.schema.fields[49].logical_type, LogicalType::Int64);
        assert_eq!(profile.schema.fields[65].name, "deviceowner");
    }
}

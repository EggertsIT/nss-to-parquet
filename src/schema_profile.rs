use anyhow::Result;

use crate::schema::{FieldDef, LogicalType, SchemaDef};

pub const DEFAULT_SCHEMA_PROFILE: &str = "zscaler_web_v1";

pub const ZSCALER_WEB_V1_FEED_TEMPLATE: &str = "\"%s{time}\",\"%s{ologin}\",\"%s{proto}\",\"%s{eurl}\",\"%s{action}\",\"%s{appname}\",\"%s{app_status}\",\"%s{appclass}\",\"%d{reqsize}\",\"%d{respsize}\",\"%s{urlclass}\",\"%s{urlsupercat}\",\"%s{urlcat}\",\"%s{malwarecat}\",\"%s{threatname}\",\"%d{riskscore}\",\"%s{threatseverity}\",\"%s{dlpeng}\",\"%s{dlpdict}\",\"%s{location}\",\"%s{dept}\",\"%s{cip}\",\"%s{sip}\",\"%s{reqmethod}\",\"%s{respcode}\",\"%s{ua}\",\"%s{ereferer}\",\"%s{ruletype}\",\"%s{rulelabel}\",\"%s{contenttype}\",\"%s{unscannabletype}\",\"%s{deviceowner}\",\"%s{devicehostname}\",\"%s{keyprotectiontype}\"";

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
    &[DEFAULT_SCHEMA_PROFILE]
}

pub fn resolve_profile(id: &str) -> Result<SchemaProfile> {
    match id {
        DEFAULT_SCHEMA_PROFILE => Ok(zscaler_web_v1()),
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
}

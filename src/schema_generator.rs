use std::collections::HashSet;

use anyhow::Result;
use csv::StringRecord;

use crate::schema::{FieldDef, LogicalType, SchemaDef};

const IP_FIELDS: &[&str] = &["cip", "cintip", "cpubip", "sip"];

pub fn generate_schema_from_feed_template(template: &str) -> Result<SchemaDef> {
    let line = first_non_empty_line(template)
        .ok_or_else(|| anyhow::anyhow!("feed template is empty"))?
        .trim();
    let raw_fields = parse_csv_fields(line)?;
    if raw_fields.is_empty() {
        anyhow::bail!("feed template contains no fields");
    }

    let mut names = HashSet::new();
    let mut fields = Vec::with_capacity(raw_fields.len());
    for raw in raw_fields {
        let (name, logical_type) = parse_field_spec(&raw)?;
        if !names.insert(name.clone()) {
            anyhow::bail!("duplicate field name '{}' in feed template", name);
        }
        let nullable = name != "time";
        fields.push(FieldDef {
            name,
            logical_type,
            nullable,
        });
    }

    let schema = SchemaDef { fields };
    schema.validate()?;
    Ok(schema)
}

fn first_non_empty_line(input: &str) -> Option<&str> {
    input.lines().map(str::trim).find(|line| !line.is_empty())
}

fn parse_csv_fields(line: &str) -> Result<Vec<String>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(line.as_bytes());

    let mut records = reader.records();
    let record: StringRecord = records
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty CSV line"))??;
    if records.next().transpose()?.is_some() {
        anyhow::bail!("expected a single CSV record for feed template");
    }
    Ok(record.iter().map(|s| s.trim().to_string()).collect())
}

fn parse_field_spec(spec: &str) -> Result<(String, LogicalType)> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        anyhow::bail!("empty field spec");
    }

    if !trimmed.starts_with('%') {
        return Ok((sanitize_name(trimmed), LogicalType::String));
    }

    let open = trimmed
        .find('{')
        .ok_or_else(|| anyhow::anyhow!("invalid NSS field spec '{}': missing '{{'", trimmed))?;
    if !trimmed.ends_with('}') {
        anyhow::bail!("invalid NSS field spec '{}': missing '}}'", trimmed);
    }

    let name = sanitize_name(&trimmed[(open + 1)..(trimmed.len() - 1)]);
    if name.is_empty() {
        anyhow::bail!("invalid NSS field spec '{}': empty field name", trimmed);
    }

    let prefix = &trimmed[..open];
    let fmt = prefix
        .chars()
        .rev()
        .find(|c| c.is_ascii_alphabetic())
        .unwrap_or('s')
        .to_ascii_lowercase();
    let logical_type = infer_type(fmt, &name);
    Ok((name, logical_type))
}

fn sanitize_name(name: &str) -> String {
    name.trim().trim_matches('"').to_string()
}

fn infer_type(fmt: char, name: &str) -> LogicalType {
    if name == "time" {
        return LogicalType::Timestamp;
    }
    if IP_FIELDS.contains(&name) {
        return LogicalType::Ip;
    }

    match fmt {
        'd' => LogicalType::Int64,
        'f' => LogicalType::Float64,
        _ => LogicalType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_schema_for_user_template() {
        let tpl = r#""%s{time}","%s{ologin}","%s{proto}","%s{eurl}","%s{action}","%s{appname}","%s{app_status}","%s{appclass}","%d{reqsize}","%d{respsize}","%s{urlclass}","%s{urlsupercat}","%s{urlcat}","%s{malwarecat}","%s{threatname}","%d{riskscore}","%s{threatseverity}","%s{dlpeng}","%s{dlpdict}","%s{location}","%s{dept}","%s{cip}","%s{sip}","%s{reqmethod}","%s{respcode}","%s{ua}","%s{ereferer}","%s{ruletype}","%s{rulelabel}","%s{contenttype}","%s{unscannabletype}","%s{deviceowner}","%s{devicehostname}","%s{keyprotectiontype}""#;
        let schema = generate_schema_from_feed_template(tpl).expect("schema generation failed");
        assert_eq!(schema.fields.len(), 34);
        assert_eq!(schema.fields[0].name, "time");
        assert_eq!(schema.fields[0].logical_type, LogicalType::Timestamp);
        assert!(!schema.fields[0].nullable);
        assert_eq!(schema.fields[8].name, "reqsize");
        assert_eq!(schema.fields[8].logical_type, LogicalType::Int64);
        assert_eq!(schema.fields[21].name, "cip");
        assert_eq!(schema.fields[21].logical_type, LogicalType::Ip);
        assert_eq!(schema.fields[24].name, "respcode");
        assert_eq!(schema.fields[24].logical_type, LogicalType::String);
    }

    #[test]
    fn supports_percent_width_int_fields() {
        let tpl = r#""%02d{hh}","%04d{yyyy}","%d{epochtime}""#;
        let schema = generate_schema_from_feed_template(tpl).expect("schema generation failed");
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].logical_type, LogicalType::Int64);
        assert_eq!(schema.fields[1].logical_type, LogicalType::Int64);
        assert_eq!(schema.fields[2].logical_type, LogicalType::Int64);
    }
}

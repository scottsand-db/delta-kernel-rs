//! Parse a SQL string into a kernel [`Expression`] or [`Predicate`].
//!
//! Delta stores column defaults, check constraints, and generated column definitions as SQL
//! strings in table metadata. This module turns those strings into kernel expressions so the
//! kernel can interpret them without depending on a full SQL parser.
//!
//! The grammar follows the Spark SQL standard: this parser implements a subset of Spark's SQL
//! grammar rather than defining a kernel-specific dialect, so the forms it accepts match what
//! Spark reads and writes.
//!
//! This is an intentionally light start. [`parse_sql`] covers the literal forms Delta metadata
//! contains today; [`parse_predicate`] adds the small boolean grammar (`col = literal`, `AND`,
//! `OR`) that CHECK constraints need, resolving column references against a schema. If the
//! supported SQL surface grows, options include moving parsing behind the
//! [`Engine`](crate::Engine) trait or adopting an existing SQL parser library.

// Not every literal sub-parser has an in-crate caller yet (e.g. timestamp/binary), and the
// column-defaults work (#2630) will wire up more entry points.
#![allow(dead_code)]

use crate::expressions::{Expression, Predicate, Scalar};
use crate::schema::{DataType, PrimitiveType, StructType};
use crate::{DeltaResult, Error};

/// Parse a SQL string into an [`Expression`] that yields a value of the given [`DataType`]
/// (e.g. the type of the column whose default is being parsed).
///
/// Leading and trailing whitespace are ignored. `NULL` (case-insensitive) is accepted for any
/// data type. All other input is parsed as a typed literal, which is supported only for primitive
/// types.
///
/// # Errors
///
/// Returns an error if the input is not a SQL form this parser accepts, or if the parsed value
/// is not compatible with `data_type` (incompatible type, out of range, etc.).
pub(crate) fn parse_sql(sql: &str, data_type: &DataType) -> DeltaResult<Expression> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(Error::generic("empty SQL literal"));
    }
    // NULL is valid for any data type, including non-primitive ones.
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(Expression::literal(Scalar::Null(data_type.clone())));
    }
    parse_literal(trimmed, data_type, sql)
}

/// Parse a boolean SQL `predicate` (a CHECK constraint expression) into a kernel [`Predicate`],
/// resolving column references against `schema`.
///
/// Supported grammar (Spark subset): `OR` of `AND` of equality comparisons, where each comparison
/// is `<operand> = <operand>` and an operand is either a column name present in `schema` or a
/// literal. The literal operand is parsed at the type of the column it is compared against.
///
/// # Errors
///
/// Returns an error for any form outside this grammar (other operators, two-literal comparisons,
/// unparsable literals, unknown columns on both sides).
pub(crate) fn parse_predicate(sql: &str, schema: &StructType) -> DeltaResult<Predicate> {
    parse_or(sql.trim(), schema)
}

/// `OR` binds loosest, so split on it first; each operand is an `AND` expression.
fn parse_or(sql: &str, schema: &StructType) -> DeltaResult<Predicate> {
    let parts = split_top_keyword(sql, "OR");
    if parts.len() > 1 {
        let preds = parts
            .iter()
            .map(|p| parse_and(p, schema))
            .collect::<DeltaResult<Vec<_>>>()?;
        return Ok(Predicate::or_from(preds));
    }
    parse_and(sql, schema)
}

/// `AND` binds tighter than `OR`; each operand is a single comparison.
fn parse_and(sql: &str, schema: &StructType) -> DeltaResult<Predicate> {
    let parts = split_top_keyword(sql, "AND");
    if parts.len() > 1 {
        let preds = parts
            .iter()
            .map(|p| parse_comparison(p, schema))
            .collect::<DeltaResult<Vec<_>>>()?;
        return Ok(Predicate::and_from(preds));
    }
    parse_comparison(sql, schema)
}

/// Parse a single `<operand> = <operand>` comparison. One side is resolved as a column (so we
/// know its type), and the other is parsed as a literal of that type; `col = col` is also allowed.
fn parse_comparison(sql: &str, schema: &StructType) -> DeltaResult<Predicate> {
    let sql = sql.trim();
    let (lhs, rhs) = sql
        .split_once('=')
        .ok_or_else(|| Error::generic(format!("unsupported constraint expression: {sql}")))?;
    let (lhs, rhs) = (lhs.trim(), rhs.trim());
    // Reject the compound operators that also contain '=' (>=, <=, <>, !=, ==).
    if lhs.ends_with(['<', '>', '!', '=']) || rhs.starts_with('=') {
        return Err(Error::generic(format!(
            "unsupported comparison operator in: {sql}"
        )));
    }

    match (schema.field(lhs), schema.field(rhs)) {
        (Some(l), Some(r)) => Ok(Predicate::eq(
            Expression::column([l.name().clone()]),
            Expression::column([r.name().clone()]),
        )),
        (Some(col), None) => {
            let lit = parse_sql(rhs, col.data_type())?;
            Ok(Predicate::eq(Expression::column([col.name().clone()]), lit))
        }
        (None, Some(col)) => {
            let lit = parse_sql(lhs, col.data_type())?;
            Ok(Predicate::eq(Expression::column([col.name().clone()]), lit))
        }
        (None, None) => Err(Error::generic(format!(
            "comparison references no known column: {sql}"
        ))),
    }
}

/// Split `sql` on a whole-word, space-delimited `keyword` (case-insensitive), ignoring occurrences
/// inside single-quoted string literals. Returns the original (untrimmed) segments.
///
/// Byte offsets are taken on an ASCII-lowercased copy; `to_ascii_lowercase` preserves byte length,
/// so the offsets index `sql` correctly even with multi-byte characters in string literals.
fn split_top_keyword<'a>(sql: &'a str, keyword: &str) -> Vec<&'a str> {
    let lower = sql.to_ascii_lowercase();
    let needle = format!(" {} ", keyword.to_ascii_lowercase());
    let mut parts = Vec::new();
    let mut start = 0;
    let mut search = 0;
    while let Some(rel) = lower[search..].find(&needle) {
        let pos = search + rel;
        // Even number of quotes before `pos` => not inside a string literal => a real boundary.
        if sql[..pos].bytes().filter(|&b| b == b'\'').count() % 2 == 0 {
            parts.push(&sql[start..pos]);
            start = pos + needle.len();
            search = start;
        } else {
            search = pos + 1;
        }
    }
    parts.push(&sql[start..]);
    parts
}

/// Dispatch a SQL literal to the per-type parser for its primitive `data_type`, then wrap the
/// resulting [`Scalar`] in an [`Expression`]. Errors on a non-primitive `data_type` (only `NULL`,
/// handled in [`parse_sql`], is valid for complex types).
fn parse_literal(trimmed: &str, data_type: &DataType, sql: &str) -> DeltaResult<Expression> {
    let DataType::Primitive(primitive) = data_type else {
        return Err(Error::generic(format!(
            "SQL literal parsing only supports primitive types, got {data_type:?}"
        )));
    };
    let scalar = match primitive {
        PrimitiveType::Binary => parse_binary_literal(trimmed)?,
        PrimitiveType::String => parse_string_literal(trimmed)?,
        PrimitiveType::Date => parse_date_literal(trimmed, sql)?,
        PrimitiveType::Timestamp => parse_timestamp_ltz_literal(trimmed, sql)?,
        PrimitiveType::TimestampNtz => parse_timestamp_ntz_literal(trimmed, sql)?,
        PrimitiveType::Float | PrimitiveType::Double => {
            parse_double_or_float(primitive, trimmed, sql)?
        }
        _ => primitive.parse_scalar(trimmed)?,
    };
    Ok(Expression::literal(scalar))
}

/// Build a `Scalar::String` from a single-quoted body via [`unquote_string`] (e.g. `'it''s'` ->
/// `it's`). Bypasses `parse_scalar`, which maps an empty input to SQL NULL (partition-value
/// convention), so an empty literal `''` round-trips here as `Scalar::String("")`, distinct from
/// NULL.
fn parse_string_literal(trimmed: &str) -> DeltaResult<Scalar> {
    Ok(Scalar::String(unquote_string(trimmed)?))
}

/// Build a `Scalar::Binary` from an `X'deadbeef'` literal (even number of hex digits).
fn parse_binary_literal(trimmed: &str) -> DeltaResult<Scalar> {
    Ok(Scalar::Binary(decode_binary_literal(trimmed)?))
}

/// Parse a `Scalar::Date` from `'2024-01-01'`, `DATE '2024-01-01'`, or `DATE'2024-01-01'` (the
/// keyword is optional and may touch the quote).
fn parse_date_literal(trimmed: &str, sql: &str) -> DeltaResult<Scalar> {
    let raw = unwrap_quoted_body(trimmed, &["DATE"], &PrimitiveType::Date, sql)?;
    PrimitiveType::Date.parse_scalar(&raw)
}

/// Parse a zoneless (wall-clock) `Scalar::TimestampNtz` from `'2024-01-01 12:00:00[.fff]'` or
/// `TIMESTAMP_NTZ '...'`.
fn parse_timestamp_ntz_literal(trimmed: &str, sql: &str) -> DeltaResult<Scalar> {
    let raw = unwrap_quoted_body(
        trimmed,
        &["TIMESTAMP_NTZ"],
        &PrimitiveType::TimestampNtz,
        sql,
    )?;
    PrimitiveType::TimestampNtz.parse_scalar(&raw)
}

/// Parse a `Scalar::Timestamp` (local-time-zone) in ISO 8601 / RFC 3339 form with an explicit UTC
/// `Z` suffix, e.g. `'1970-01-01T00:00:00.123Z'`, `TIMESTAMP '...Z'`, or `TIMESTAMP_LTZ '...Z'`.
fn parse_timestamp_ltz_literal(trimmed: &str, sql: &str) -> DeltaResult<Scalar> {
    let raw = unwrap_quoted_body(
        trimmed,
        &["TIMESTAMP", "TIMESTAMP_LTZ"],
        &PrimitiveType::Timestamp,
        sql,
    )?;
    require_utc_z_suffix(&raw, sql)?;
    PrimitiveType::Timestamp.parse_scalar(&raw)
}

/// Strip the typed-literal envelope and return the inner literal value, ready for `parse_scalar`.
fn unwrap_quoted_body(
    trimmed: &str,
    keywords: &[&str],
    primitive: &PrimitiveType,
    sql: &str,
) -> DeltaResult<String> {
    let body = strip_typed_prefix_and_unquote(trimmed, keywords)?;
    let body = body.trim();
    if body.is_empty() {
        return Err(Error::generic(format!(
            "empty {primitive:?} literal: {sql}"
        )));
    }
    Ok(body.to_string())
}

/// Require a TIMESTAMP (LTZ) literal to pin an absolute instant with an explicit UTC `Z` suffix.
fn require_utc_z_suffix(raw: &str, sql: &str) -> DeltaResult<()> {
    if raw.contains(['t', 'z']) {
        return Err(Error::generic(
            "TIMESTAMP literal must use uppercase 'T' and or 'Z'",
        ));
    }
    if raw.ends_with('Z') {
        return Ok(());
    }
    let has_offset = raw
        .split_once(['T', ' '])
        .is_some_and(|(_, time)| time.contains(['+', '-']));
    Err(if has_offset {
        Error::generic(format!(
            "TIMESTAMP literal with an explicit offset is not yet supported; use 'Z' (UTC): {sql}"
        ))
    } else {
        Error::generic(
            "zoneless TIMESTAMP literal is not yet supported; use an explicit 'Z' (UTC) suffix",
        )
    })
}

/// Parse a bare FLOAT or DOUBLE literal, matching Spark's literal-typing + cast semantics.
fn parse_double_or_float(primitive: &PrimitiveType, raw: &str, sql: &str) -> DeltaResult<Scalar> {
    let has_exponent = raw.contains(['e', 'E']);
    if !has_exponent && exceeds_decimal_precision(raw) {
        return Err(Error::generic(format!(
            "numeric literal exceeds maximum DECIMAL precision 38: {sql}"
        )));
    }
    let scalar = if *primitive == PrimitiveType::Float && has_exponent {
        let value: f64 = raw
            .parse()
            .map_err(|_| Error::generic(format!("invalid FLOAT literal: {sql}")))?;
        Scalar::Float(value as f32)
    } else {
        primitive.parse_scalar(raw)?
    };
    let normalize_neg_zero = !has_exponent;
    let non_finite_error = || Error::generic("non-finite float literals are not supported");
    Ok(match scalar {
        Scalar::Float(f) if !f.is_finite() => return Err(non_finite_error()),
        Scalar::Double(d) if !d.is_finite() => return Err(non_finite_error()),
        Scalar::Float(f) if normalize_neg_zero => Scalar::Float(f + 0.0),
        Scalar::Double(d) if normalize_neg_zero => Scalar::Double(d + 0.0),
        other => other,
    })
}

/// Whether a bare non-exponent numeric literal exceeds Spark's DECIMAL precision cap of 38.
fn exceeds_decimal_precision(raw: &str) -> bool {
    let unsigned = raw.strip_prefix(['+', '-']).unwrap_or(raw);
    let scale = match unsigned.split_once('.') {
        Some((_, frac)) => frac.chars().filter(|c| c.is_ascii_digit()).count(),
        None => 0,
    };
    let significant = unsigned
        .chars()
        .filter(|c| c.is_ascii_digit())
        .skip_while(|&c| c == '0')
        .count();
    significant.max(scale) > 38
}

/// Unquote a SQL string literal: strip the surrounding single quotes and un-escape each `''` into
/// a single `'`.
fn unquote_string(input: &str) -> DeltaResult<String> {
    let body = input.strip_prefix('\'').ok_or_else(|| {
        Error::generic(format!("expected a single-quoted SQL string, got: {input}"))
    })?;

    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            return Err(Error::generic(format!(
                "backslash escapes in SQL string literals are not yet supported: {input}"
            )));
        }
        if c != '\'' {
            out.push(c);
            continue;
        }
        match chars.next() {
            None => return Ok(out),
            Some('\'') => out.push('\''),
            Some(_) => {
                return Err(Error::generic(format!(
                    "unexpected characters after closing quote in SQL string literal: {input}"
                )))
            }
        }
    }
    Err(Error::generic(format!(
        "unterminated SQL string literal: {input}"
    )))
}

/// Strip an optional typed-literal keyword prefix and unwrap the required `'...'` quoted body.
fn strip_typed_prefix_and_unquote(input: &str, keywords: &[&str]) -> DeltaResult<String> {
    let body = keywords.iter().find_map(|kw| {
        let prefix = input.get(..kw.len())?;
        let rest = &input[kw.len()..];
        let is_token = rest.starts_with('\'') || rest.starts_with(char::is_whitespace);
        (prefix.eq_ignore_ascii_case(kw) && is_token).then(|| rest.trim_start())
    });
    unquote_string(body.unwrap_or(input))
}

/// Decode a `X'hex'` SQL binary literal into a byte vector.
fn decode_binary_literal(input: &str) -> DeltaResult<Vec<u8>> {
    let err = || {
        Error::generic(format!(
            "expected a SQL binary literal like X'..', got: {input}"
        ))
    };
    let hex = input
        .strip_prefix(['x', 'X'])
        .and_then(|rest| rest.strip_prefix('\''))
        .and_then(|rest| rest.strip_suffix('\''))
        .ok_or_else(err)?;
    if !hex.len().is_multiple_of(2) {
        return Err(Error::generic(format!(
            "binary literal must contain an even number of hex digits: {input}"
        )));
    }
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char)
                .to_digit(16)
                .ok_or_else(|| Error::generic(format!("invalid hex digit in {input}")))?;
            let lo = (pair[1] as char)
                .to_digit(16)
                .ok_or_else(|| Error::generic(format!("invalid hex digit in {input}")))?;
            Ok((hi << 4 | lo) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::expressions::column_expr;
    use crate::schema::StructField;

    fn schema() -> StructType {
        StructType::new_unchecked([
            StructField::nullable("age", DataType::INTEGER),
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("limit", DataType::INTEGER),
        ])
    }

    // === literal parsing (vendored parser) ===
    #[rstest]
    #[case("42", DataType::INTEGER, Scalar::Integer(42))]
    #[case(" -7 ", DataType::INTEGER, Scalar::Integer(-7))]
    #[case("'bob'", DataType::STRING, Scalar::String("bob".into()))]
    #[case("TRUE", DataType::BOOLEAN, Scalar::Boolean(true))]
    fn parse_sql_literal(#[case] sql: &str, #[case] ty: DataType, #[case] expected: Scalar) {
        assert_eq!(parse_sql(sql, &ty).unwrap(), Expression::literal(expected));
    }

    #[test]
    fn parse_sql_null_for_any_type() {
        let expr = parse_sql("null", &DataType::INTEGER).unwrap();
        assert_eq!(expr, Expression::literal(Scalar::Null(DataType::INTEGER)));
    }

    // === predicate parsing (column resolution + = / AND / OR) ===
    #[test]
    fn parse_predicate_column_eq_literal_resolves_type() {
        let p = parse_predicate("age = 21", &schema()).unwrap();
        assert_eq!(
            p,
            Predicate::eq(column_expr!("age"), Expression::literal(21))
        );
    }

    #[test]
    fn parse_predicate_literal_on_left_normalizes_to_column_first() {
        let p = parse_predicate("21 = age", &schema()).unwrap();
        assert_eq!(
            p,
            Predicate::eq(column_expr!("age"), Expression::literal(21))
        );
    }

    #[test]
    fn parse_predicate_string_literal() {
        let p = parse_predicate("name = 'bob'", &schema()).unwrap();
        assert_eq!(
            p,
            Predicate::eq(column_expr!("name"), Expression::literal("bob"))
        );
    }

    #[test]
    fn parse_predicate_and_of_two_comparisons() {
        let p = parse_predicate("age = 21 AND name = 'bob'", &schema()).unwrap();
        let expected = Predicate::and_from([
            Predicate::eq(column_expr!("age"), Expression::literal(21)),
            Predicate::eq(column_expr!("name"), Expression::literal("bob")),
        ]);
        assert_eq!(p, expected);
    }

    #[test]
    fn parse_predicate_or_binds_looser_than_and() {
        // a AND b OR c  ==  (a AND b) OR c
        let p = parse_predicate("age = 1 AND limit = 2 OR age = 3", &schema()).unwrap();
        let expected = Predicate::or_from([
            Predicate::and_from([
                Predicate::eq(column_expr!("age"), Expression::literal(1)),
                Predicate::eq(column_expr!("limit"), Expression::literal(2)),
            ]),
            Predicate::eq(column_expr!("age"), Expression::literal(3)),
        ]);
        assert_eq!(p, expected);
    }

    #[test]
    fn parse_predicate_column_eq_column() {
        let p = parse_predicate("age = limit", &schema()).unwrap();
        assert_eq!(p, Predicate::eq(column_expr!("age"), column_expr!("limit")));
    }

    #[rstest]
    #[case("age >= 1")] // unsupported operator
    #[case("age <> 1")]
    #[case("1 = 2")] // no column referenced
    #[case("age = notparsable")] // bad literal for INTEGER
    fn parse_predicate_rejects(#[case] sql: &str) {
        assert!(parse_predicate(sql, &schema()).is_err());
    }
}

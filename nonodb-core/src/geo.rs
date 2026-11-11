//! Geospatial primitives and the single-field GeoJSON point index.
//!
//! Documents store a GeoJSON `Point`:
//!
//! ```json
//! {"type": "Point", "coordinates": [lon, lat]}
//! ```
//!
//! A geo index stores one entry per document at each precision level in a
//! small hierarchical lon/lat grid. A query selects the finest level that
//! keeps the number of scanned cells bounded, then the residual matcher
//! verifies the exact distance or geometry. This keeps the index independent
//! of an external GIS dependency while still avoiding a full collection scan
//! for ordinary radius and polygon queries.

use std::ops::Bound;

use serde_json::Value;

use crate::index;
use crate::query::path;

/// Mean Earth radius used by MongoDB's legacy spherical calculations.
pub const EARTH_RADIUS_M: f64 = 6_378_100.0;

/// Index key tag reserved for geo cells. Existing scalar tags use 0x01..0x07.
pub const GEO_TAG: u8 = 0x08;

/// Grid cell size in degrees, from coarse to fine.
const GEO_LEVELS: [f64; 4] = [10.0, 1.0, 0.1, 0.01];

/// Maximum number of contiguous index ranges generated for one query.
pub const MAX_GEO_RANGES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GeoPoint {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Clone, Debug)]
pub enum Geometry {
    Point(GeoPoint),
    Polygon(Vec<Vec<GeoPoint>>),
    MultiPolygon(Vec<Vec<Vec<GeoPoint>>>),
    Circle { center: GeoPoint, radius_m: f64 },
}

#[derive(Clone, Debug)]
pub struct GeoNear {
    pub center: GeoPoint,
    pub min_m: f64,
    pub max_m: Option<f64>,
}

#[derive(Clone, Debug)]
pub enum GeoQuery {
    Near(GeoNear),
    Within(Geometry),
    Intersects(Geometry),
}

#[derive(Clone, Copy, Debug)]
struct BBox {
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
    wrap: bool,
}

fn finite_number(value: &Value) -> Option<f64> {
    let n = value.as_f64()?;
    n.is_finite().then_some(n)
}

/// Parses a `[lon, lat]` coordinate pair.
pub fn parse_point(value: &Value) -> Option<GeoPoint> {
    if let Some(obj) = value.as_object() {
        if obj.get("type")?.as_str()? != "Point" {
            return None;
        }
        return parse_point(obj.get("coordinates")?);
    }
    let arr = value.as_array()?;
    if arr.len() < 2 {
        return None;
    }
    let lon = finite_number(&arr[0])?;
    let lat = finite_number(&arr[1])?;
    if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
        return None;
    }
    Some(GeoPoint { lon, lat })
}

fn parse_ring(value: &Value) -> Option<Vec<GeoPoint>> {
    let arr = value.as_array()?;
    if arr.len() < 3 {
        return None;
    }
    let mut ring: Vec<GeoPoint> = arr.iter().map(parse_point).collect::<Option<_>>()?;
    if !same_point(ring[0], *ring.last()?) {
        ring.push(ring[0]);
    }
    Some(ring)
}

fn same_point(a: GeoPoint, b: GeoPoint) -> bool {
    (a.lon - b.lon).abs() < 1e-9 && (a.lat - b.lat).abs() < 1e-9
}

fn parse_polygon(value: &Value) -> Option<Vec<Vec<GeoPoint>>> {
    let rings = value.as_array()?;
    if rings.is_empty() {
        return None;
    }
    rings.iter().map(parse_ring).collect()
}

/// Parses GeoJSON `Point`, `Polygon`, and `MultiPolygon` geometry.
pub fn parse_geometry(value: &Value) -> Option<Geometry> {
    let Some(obj) = value.as_object() else {
        return parse_point(value).map(Geometry::Point);
    };
    match obj.get("type")?.as_str()? {
        "Point" => Some(Geometry::Point(parse_point(value)?)),
        "Polygon" => Some(Geometry::Polygon(parse_polygon(obj.get("coordinates")?)?)),
        "MultiPolygon" => {
            let polygons = obj.get("coordinates")?.as_array()?;
            if polygons.is_empty() {
                return None;
            }
            let polygons = polygons
                .iter()
                .map(parse_polygon)
                .collect::<Option<Vec<_>>>()?;
            Some(Geometry::MultiPolygon(polygons))
        }
        _ => None,
    }
}

fn parse_optional_distance(value: Option<&Value>) -> Option<Option<f64>> {
    match value {
        None => Some(None),
        Some(v) => {
            let n = finite_number(v)?;
            (n >= 0.0).then_some(Some(n))
        }
    }
}

/// Parses `$near` / `$nearSphere` argument.
pub fn parse_near(value: &Value) -> Option<GeoNear> {
    let obj = value.as_object()?;
    let center = parse_point(obj.get("$geometry")?)?;
    let min_m = parse_optional_distance(obj.get("$minDistance"))?.unwrap_or(0.0);
    let max_m = parse_optional_distance(obj.get("$maxDistance"))?;
    if max_m.is_some_and(|max| max < min_m) {
        return None;
    }
    Some(GeoNear {
        center,
        min_m,
        max_m,
    })
}

fn rectangle(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Geometry {
    Geometry::Polygon(vec![vec![
        GeoPoint {
            lon: min_lon,
            lat: min_lat,
        },
        GeoPoint {
            lon: max_lon,
            lat: min_lat,
        },
        GeoPoint {
            lon: max_lon,
            lat: max_lat,
        },
        GeoPoint {
            lon: min_lon,
            lat: max_lat,
        },
        GeoPoint {
            lon: min_lon,
            lat: min_lat,
        },
    ]])
}

/// Parses the argument of `$geoWithin`.
pub fn parse_within(value: &Value) -> Option<Geometry> {
    let obj = value.as_object()?;
    if let Some(geometry) = obj.get("$geometry") {
        return parse_geometry(geometry);
    }
    if let Some(spec) = obj.get("$centerSphere") {
        let arr = spec.as_array()?;
        if arr.len() != 2 {
            return None;
        }
        let center = parse_point(&arr[0])?;
        let radians = finite_number(&arr[1])?;
        if radians < 0.0 {
            return None;
        }
        return Some(Geometry::Circle {
            center,
            radius_m: radians * EARTH_RADIUS_M,
        });
    }
    if let Some(spec) = obj.get("$box") {
        let arr = spec.as_array()?;
        if arr.len() != 2 {
            return None;
        }
        let a = parse_point(&arr[0])?;
        let b = parse_point(&arr[1])?;
        return Some(rectangle(
            a.lon.min(b.lon),
            a.lat.min(b.lat),
            a.lon.max(b.lon),
            a.lat.max(b.lat),
        ));
    }
    if let Some(spec) = obj.get("$polygon") {
        return Some(Geometry::Polygon(vec![parse_ring(spec)?]));
    }
    None
}

/// Parses the argument of `$geoIntersects`.
pub fn parse_intersects(value: &Value) -> Option<Geometry> {
    parse_geometry(value.as_object()?.get("$geometry")?)
}

fn parse_condition(value: &Value) -> Option<GeoQuery> {
    let obj = value.as_object()?;
    if let Some(spec) = obj.get("$near").or_else(|| obj.get("$nearSphere")) {
        return Some(GeoQuery::Near(parse_near(spec)?));
    }
    if let Some(spec) = obj.get("$geoWithin") {
        return Some(GeoQuery::Within(parse_within(spec)?));
    }
    if let Some(spec) = obj.get("$geoIntersects") {
        return Some(GeoQuery::Intersects(parse_intersects(spec)?));
    }
    None
}

/// Returns the first top-level geo condition that may use an index.
pub fn index_query(filter: &Value) -> Option<(String, GeoQuery)> {
    let obj = filter.as_object()?;
    for (field, cond) in obj {
        if field.starts_with('$') || field.is_empty() {
            continue;
        }
        if let Some(query) = parse_condition(cond) {
            return Some((field.clone(), query));
        }
    }
    None
}

/// Returns a `$near` condition, including one nested in a top-level `$and`.
pub fn extract_near(filter: &Value) -> Option<(String, GeoNear)> {
    if let Some((field, GeoQuery::Near(near))) = index_query(filter) {
        return Some((field, near));
    }
    let obj = filter.as_object()?;
    for branch in obj.get("$and")?.as_array()? {
        if let Some(found) = extract_near(branch) {
            return Some(found);
        }
    }
    None
}

/// Spherical distance in meters.
pub fn distance_m(a: GeoPoint, b: GeoPoint) -> f64 {
    let lat1 = a.lat.to_radians();
    let lat2 = b.lat.to_radians();
    let dlat = lat2 - lat1;
    let dlon = (b.lon - a.lon).to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.sqrt().clamp(0.0, 1.0).asin()
}

fn point_on_segment(p: GeoPoint, a: GeoPoint, b: GeoPoint) -> bool {
    let cross = (p.lat - a.lat) * (b.lon - a.lon) - (p.lon - a.lon) * (b.lat - a.lat);
    if cross.abs() > 1e-12 {
        return false;
    }
    p.lon >= a.lon.min(b.lon) - 1e-12
        && p.lon <= a.lon.max(b.lon) + 1e-12
        && p.lat >= a.lat.min(b.lat) - 1e-12
        && p.lat <= a.lat.max(b.lat) + 1e-12
}

fn point_in_ring(p: GeoPoint, ring: &[GeoPoint]) -> bool {
    let mut inside = false;
    for pair in ring.windows(2) {
        let a = pair[0];
        let b = pair[1];
        if point_on_segment(p, a, b) {
            return true;
        }
        if (a.lat > p.lat) != (b.lat > p.lat) {
            let x = (b.lon - a.lon) * (p.lat - a.lat) / (b.lat - a.lat) + a.lon;
            if p.lon < x {
                inside = !inside;
            }
        }
    }
    inside
}

fn point_in_polygon(p: GeoPoint, rings: &[Vec<GeoPoint>]) -> bool {
    let Some(outer) = rings.first() else {
        return false;
    };
    if !point_in_ring(p, outer) {
        return false;
    }
    !rings.iter().skip(1).any(|hole| point_in_ring(p, hole))
}

impl Geometry {
    fn contains_point(&self, p: GeoPoint) -> bool {
        match self {
            Self::Point(q) => distance_m(*q, p) <= 1e-3,
            Self::Polygon(rings) => point_in_polygon(p, rings),
            Self::MultiPolygon(polygons) => polygons.iter().any(|rings| point_in_polygon(p, rings)),
            Self::Circle { center, radius_m } => distance_m(*center, p) <= *radius_m + 1e-6,
        }
    }

    fn bbox(&self) -> Option<BBox> {
        match self {
            Self::Point(p) => BBox::point(*p),
            Self::Polygon(rings) => {
                let outer = rings.first()?;
                BBox::from_points(outer.iter().copied())
            }
            Self::MultiPolygon(polygons) => BBox::from_points(
                polygons
                    .iter()
                    .flat_map(|rings| rings.first())
                    .flatten()
                    .copied(),
            ),
            Self::Circle { center, radius_m } => bbox_for_radius(*center, *radius_m),
        }
    }
}

impl BBox {
    fn point(p: GeoPoint) -> Option<Self> {
        Some(Self {
            min_lat: p.lat,
            max_lat: p.lat,
            min_lon: p.lon,
            max_lon: p.lon,
            wrap: false,
        })
    }

    fn from_points(points: impl Iterator<Item = GeoPoint>) -> Option<Self> {
        let mut min_lat = f64::INFINITY;
        let mut max_lat = f64::NEG_INFINITY;
        let mut min_lon = f64::INFINITY;
        let mut max_lon = f64::NEG_INFINITY;
        let mut any = false;
        for p in points {
            any = true;
            min_lat = min_lat.min(p.lat);
            max_lat = max_lat.max(p.lat);
            min_lon = min_lon.min(p.lon);
            max_lon = max_lon.max(p.lon);
        }
        any.then_some(Self {
            min_lat,
            max_lat,
            min_lon,
            max_lon,
            wrap: false,
        })
    }
}

fn bbox_for_radius(center: GeoPoint, radius_m: f64) -> Option<BBox> {
    if radius_m < 0.0 {
        return None;
    }
    let lat_delta = (radius_m / EARTH_RADIUS_M).to_degrees();
    let min_lat = (center.lat - lat_delta).max(-90.0);
    let max_lat = (center.lat + lat_delta).min(90.0);
    let cos_lat = center.lat.to_radians().cos().abs();
    let lon_delta = if cos_lat < 1e-9 {
        180.0
    } else {
        (radius_m / (EARTH_RADIUS_M * cos_lat)).to_degrees()
    };
    if lon_delta >= 180.0 {
        return Some(BBox {
            min_lat,
            max_lat,
            min_lon: -180.0,
            max_lon: 180.0,
            wrap: false,
        });
    }
    let raw_min = center.lon - lon_delta;
    let raw_max = center.lon + lon_delta;
    if raw_min < -180.0 {
        Some(BBox {
            min_lat,
            max_lat,
            min_lon: raw_min + 360.0,
            max_lon: raw_max,
            wrap: true,
        })
    } else if raw_max > 180.0 {
        Some(BBox {
            min_lat,
            max_lat,
            min_lon: raw_min,
            max_lon: raw_max - 360.0,
            wrap: true,
        })
    } else {
        Some(BBox {
            min_lat,
            max_lat,
            min_lon: raw_min,
            max_lon: raw_max,
            wrap: false,
        })
    }
}

fn grid_dims(size: f64) -> (u32, u32) {
    let cols = (360.0 / size).ceil().max(1.0) as u32;
    let rows = (180.0 / size).ceil().max(1.0) as u32;
    (rows, cols)
}

fn cell_indices(p: GeoPoint, size: f64) -> (u32, u32) {
    let (rows, cols) = grid_dims(size);
    let row = (((p.lat + 90.0) / size).floor().max(0.0) as u64).min(rows as u64 - 1) as u32;
    let lon = p.lon.clamp(-180.0, 180.0);
    let col = (((lon + 180.0) / size).floor().max(0.0) as u64).min(cols as u64 - 1) as u32;
    (row, col)
}

/// Encodes one geo key per resolution level for a point.
pub fn encode_point_keys(point: GeoPoint) -> Vec<Vec<u8>> {
    GEO_LEVELS
        .iter()
        .enumerate()
        .map(|(level, size)| {
            let (row, col) = cell_indices(point, *size);
            let mut key = Vec::with_capacity(10);
            key.push(GEO_TAG);
            key.push(level as u8);
            key.extend_from_slice(&row.to_be_bytes());
            key.extend_from_slice(&col.to_be_bytes());
            key
        })
        .collect()
}

fn row_range(bbox: BBox, size: f64) -> (u32, u32) {
    let (rows, _) = grid_dims(size);
    let start =
        ((((bbox.min_lat + 90.0) / size).floor().max(0.0) as u64).min(rows as u64 - 1)) as u32;
    let end =
        ((((bbox.max_lat + 90.0) / size).floor().max(0.0) as u64).min(rows as u64 - 1)) as u32;
    (start, end)
}

fn col_ranges(bbox: BBox, size: f64) -> Vec<(u32, u32)> {
    let (_, cols) = grid_dims(size);
    let last = cols - 1;
    if bbox.min_lon <= -180.0 && bbox.max_lon >= 180.0 {
        return vec![(0, last)];
    }
    let col_of = |lon: f64| {
        ((((lon.clamp(-180.0, 180.0) + 180.0) / size).floor().max(0.0) as u64).min(last as u64))
            as u32
    };
    if bbox.wrap {
        vec![(col_of(bbox.min_lon), last), (0, col_of(bbox.max_lon))]
    } else {
        vec![(col_of(bbox.min_lon), col_of(bbox.max_lon))]
    }
}

fn cell_count(bbox: BBox, size: f64) -> u128 {
    let (start, end) = row_range(bbox, size);
    let rows = u128::from(end.saturating_sub(start) + 1);
    let cols: u128 = col_ranges(bbox, size)
        .into_iter()
        .map(|(lo, hi)| u128::from(hi.saturating_sub(lo) + 1))
        .sum();
    rows.saturating_mul(cols.max(1))
}

fn level_for_bbox(bbox: BBox, max_ranges: usize) -> usize {
    let max_ranges = max_ranges.max(1) as u128;
    for (level, size) in GEO_LEVELS.iter().enumerate().rev() {
        if cell_count(bbox, *size) <= max_ranges {
            return level;
        }
    }
    0
}

fn bbox_ranges(bbox: BBox, max_ranges: usize) -> Option<Vec<index::RangeBytes>> {
    if bbox.min_lat > bbox.max_lat {
        return None;
    }
    let level = level_for_bbox(bbox, max_ranges);
    let size = GEO_LEVELS[level];
    let (row_start, row_end) = row_range(bbox, size);
    let cols = col_ranges(bbox, size);
    let (_, col_count) = grid_dims(size);
    let estimated = u128::from(row_end.saturating_sub(row_start) + 1)
        * cols
            .iter()
            .map(|(lo, hi)| u128::from(hi.saturating_sub(*lo) + 1))
            .sum::<u128>();
    if estimated > max_ranges.max(1) as u128 {
        return None;
    }

    let mut ranges = Vec::with_capacity(estimated as usize);
    for row in row_start..=row_end {
        for (col_start, col_end) in &cols {
            let mut prefix = vec![GEO_TAG, level as u8];
            prefix.extend_from_slice(&row.to_be_bytes());
            let mut lower = prefix.clone();
            lower.extend_from_slice(&col_start.to_be_bytes());
            let upper = if *col_end >= col_count - 1 {
                Bound::Excluded(index::after_value(&prefix))
            } else {
                let mut end = prefix;
                end.extend_from_slice(&(col_end + 1).to_be_bytes());
                Bound::Excluded(end)
            };
            ranges.push((Bound::Included(lower), upper));
        }
    }
    Some(ranges)
}

/// Builds index ranges for a geo query. `None` means the query should use a
/// full collection scan (for example `$near` without `$maxDistance`).
pub fn index_ranges(query: &GeoQuery, max_ranges: usize) -> Option<Vec<index::RangeBytes>> {
    let bbox = match query {
        GeoQuery::Near(near) => bbox_for_radius(near.center, near.max_m?)?,
        GeoQuery::Within(geometry) | GeoQuery::Intersects(geometry) => geometry.bbox()?,
    };
    bbox_ranges(bbox, max_ranges)
}

fn point_values(value: &Value, out: &mut Vec<GeoPoint>) {
    if let Some(point) = parse_point(value) {
        out.push(point);
        return;
    }
    if let Some(items) = value.as_array() {
        for item in items {
            if let Some(point) = parse_point(item) {
                out.push(point);
            }
        }
    }
}

fn min_distance(value: Option<&Value>, center: GeoPoint) -> Option<f64> {
    let mut points = Vec::new();
    point_values(value?, &mut points);
    points
        .into_iter()
        .map(|point| distance_m(point, center))
        .min_by(f64::total_cmp)
}

/// Distance from the nearest point stored in a document field.
pub fn field_distance(doc: &Value, field: &str, center: GeoPoint) -> Option<f64> {
    path::get_values(doc, field)
        .into_iter()
        .filter_map(|value| min_distance(Some(value), center))
        .min_by(f64::total_cmp)
}

/// Matcher implementation for `$near` / `$nearSphere`.
pub fn match_near(actual: Option<&Value>, spec: &Value) -> bool {
    let Some(near) = parse_near(spec) else {
        return false;
    };
    let Some(distance) = min_distance(actual, near.center) else {
        return false;
    };
    distance >= near.min_m && near.max_m.is_none_or(|max| distance <= max)
}

/// Matcher implementation for `$geoWithin`.
pub fn match_within(actual: Option<&Value>, spec: &Value) -> bool {
    let Some(geometry) = parse_within(spec) else {
        return false;
    };
    let Some(actual) = actual else {
        return false;
    };
    let mut points = Vec::new();
    point_values(actual, &mut points);
    points
        .into_iter()
        .any(|point| geometry.contains_point(point))
}

/// Matcher implementation for `$geoIntersects`. Point documents are supported;
/// a query polygon or circle is tested against each point in the field.
pub fn match_intersects(actual: Option<&Value>, spec: &Value) -> bool {
    let Some(geometry) = parse_intersects(spec) else {
        return false;
    };
    let Some(actual) = actual else {
        return false;
    };
    let mut points = Vec::new();
    point_values(actual, &mut points);
    points
        .into_iter()
        .any(|point| geometry.contains_point(point))
}

/// Validates a geo operator argument before a query runs.
pub fn validate_condition(op: &str, value: &Value) -> crate::Result<()> {
    let ok = match op {
        "$near" | "$nearSphere" => parse_near(value).is_some(),
        "$geoWithin" => parse_within(value).is_some(),
        "$geoIntersects" => parse_intersects(value).is_some(),
        _ => true,
    };
    if ok {
        Ok(())
    } else {
        Err(crate::Error::Query(format!("invalid {op} argument")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_geojson_point() {
        let p = parse_point(&json!({"type": "Point", "coordinates": [19.94, 50.06]})).unwrap();
        assert!((p.lon - 19.94).abs() < 1e-9);
        assert!((p.lat - 50.06).abs() < 1e-9);
        assert!(parse_point(&json!({"type": "Point", "coordinates": [200, 50]})).is_none());
    }

    #[test]
    fn distance_and_near() {
        let center = parse_point(&json!([19.94, 50.06])).unwrap();
        let nearby = json!({"type": "Point", "coordinates": [19.941, 50.06]});
        assert!(match_near(
            Some(&nearby),
            &json!({"$geometry": {"type": "Point", "coordinates": [19.94, 50.06]}, "$maxDistance": 200})
        ));
        assert!(!match_near(
            Some(&nearby),
            &json!({"$geometry": {"type": "Point", "coordinates": [19.94, 50.06]}, "$maxDistance": 1})
        ));
        assert!(field_distance(&json!({"loc": nearby}), "loc", center).unwrap() < 200.0);
    }

    #[test]
    fn polygon_and_circle_within() {
        let point = json!({"type": "Point", "coordinates": [19.94, 50.06]});
        assert!(match_within(
            Some(&point),
            &json!({"$box": [[19.0, 50.0], [20.0, 51.0]]})
        ));
        assert!(match_within(
            Some(&point),
            &json!({"$centerSphere": [[19.94, 50.06], 0.001]})
        ));
        assert!(!match_within(
            Some(&point),
            &json!({"$box": [[20.0, 51.0], [21.0, 52.0]]})
        ));
    }

    #[test]
    fn index_ranges_cover_nearby_cells() {
        let query = GeoQuery::Near(GeoNear {
            center: GeoPoint {
                lon: 19.94,
                lat: 50.06,
            },
            min_m: 0.0,
            max_m: Some(5_000.0),
        });
        let ranges = index_ranges(&query, MAX_GEO_RANGES).unwrap();
        assert!(!ranges.is_empty());
    }
}

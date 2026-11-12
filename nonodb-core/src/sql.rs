//! SQL over document collections.
//!
//! `SELECT` translates to the same `find` and aggregation paths used by the
//! JSON API. `INSERT` and `UPDATE` translate to the normal single-writer
//! operations and stay inside one LMDB write transaction. This is not a second
//! storage engine: indexes, TTL filtering, snapshots, and aggregation limits
//! are inherited from the normal paths.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Map, Value};

use crate::aggregate;
use crate::collection::{Collection, FindQuery};
use crate::error::{Error, Result};
use crate::heed::{RoTxn, RwTxn};
use crate::query::path;

/// SQL statement category used by authorization and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlKind {
    Select,
    Insert,
    Update,
    Delete,
}

impl SqlKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Select => "select",
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// A parsed SQL statement that can be executed or explained.
#[derive(Debug, Clone)]
pub struct PreparedSql {
    parsed: ParsedStatement,
}

impl PreparedSql {
    pub fn parse(query: &str) -> Result<Self> {
        Ok(Self {
            parsed: parse(query)?,
        })
    }

    pub fn collection(&self) -> &str {
        self.parsed.collection()
    }

    /// True when the statement is a `SELECT`.
    pub fn is_read_only(&self) -> bool {
        self.parsed.is_read_only()
    }

    pub fn kind(&self) -> SqlKind {
        self.parsed.kind()
    }

    /// Execute a `SELECT` against the collection's normal read path. Simple
    /// field-only selects use `find` directly; grouping/distinct/expressions
    /// use the aggregation pipeline.
    pub fn execute(&self, c: &Collection<'_>, params: &[Value]) -> Result<Vec<Value>> {
        let ParsedStatement::Select(parsed) = &self.parsed else {
            return Err(Error::Sql("statement is not a SELECT".into()));
        };
        if let Some(query) = parsed.fast_find(params)? {
            c.find(&query)
        } else {
            c.aggregate(&parsed.pipeline(params)?)
        }
    }

    /// Execute a `SELECT` inside an existing read transaction.
    pub fn execute_in(
        &self,
        c: &Collection<'_>,
        rtxn: &RoTxn<'_>,
        params: &[Value],
    ) -> Result<Vec<Value>> {
        let ParsedStatement::Select(parsed) = &self.parsed else {
            return Err(Error::Sql("statement is not a SELECT".into()));
        };
        if let Some(query) = parsed.fast_find(params)? {
            c.find_in(rtxn, &query)
        } else {
            c.aggregate_in(rtxn, &parsed.pipeline(params)?)
        }
    }

    /// Execute an `INSERT` or `UPDATE` inside the writer's transaction.
    pub fn execute_write(
        &self,
        c: &Collection<'_>,
        wtxn: &mut RwTxn<'_>,
        params: &[Value],
    ) -> Result<Value> {
        match &self.parsed {
            ParsedStatement::Insert(insert) => insert.execute(c, wtxn, params),
            ParsedStatement::Update(update) => update.execute(c, wtxn, params),
            ParsedStatement::Delete(delete) => delete.execute(c, wtxn, params),
            ParsedStatement::Select(_) => Err(Error::Sql("statement is not a write".into())),
        }
    }

    /// Return the selected plan. Simple selects delegate to the normal
    /// `explain`; grouped/distinct/expression queries expose the SQL pipeline.
    pub fn explain(&self, c: &Collection<'_>, params: &[Value]) -> Result<Value> {
        let ParsedStatement::Select(parsed) = &self.parsed else {
            return Err(Error::Sql("only SELECT statements can be explained".into()));
        };
        if let Some(query) = parsed.fast_find(params)? {
            return c.explain(&query);
        }
        parsed.explain_pipeline(params)
    }

    /// Explain a `SELECT` inside an existing read transaction.
    pub fn explain_in(
        &self,
        c: &Collection<'_>,
        rtxn: &RoTxn<'_>,
        params: &[Value],
    ) -> Result<Value> {
        let ParsedStatement::Select(parsed) = &self.parsed else {
            return Err(Error::Sql("only SELECT statements can be explained".into()));
        };
        if let Some(query) = parsed.fast_find(params)? {
            return c.explain_in(rtxn, &query);
        }
        parsed.explain_pipeline(params)
    }
}

/// Parse the collection name out of a SQL statement without needing parameter
/// values. This is used for authorization and auditing before execution.
pub fn collection_name(query: &str) -> Result<String> {
    Ok(parse(query)?.collection().to_string())
}

/// Return the target collection and whether the statement is read-only.
pub fn statement_access(query: &str) -> Result<(String, bool)> {
    let parsed = parse(query)?;
    Ok((parsed.collection().to_string(), parsed.is_read_only()))
}

/// Return the statement category without parameter values.
pub fn statement_kind(query: &str) -> Result<SqlKind> {
    Ok(parse(query)?.kind())
}

/// Return whether a SQL statement only reads.
pub fn is_read_only(query: &str) -> Result<bool> {
    Ok(parse(query)?.is_read_only())
}

/// Execute a read-only SELECT statement against one collection.
pub fn execute(c: &Collection<'_>, query: &str, params: &[Value]) -> Result<Vec<Value>> {
    PreparedSql::parse(query)?.execute(c, params)
}

/// Execute a SELECT statement inside an existing LMDB read transaction.
pub fn execute_in(
    c: &Collection<'_>,
    rtxn: &RoTxn<'_>,
    query: &str,
    params: &[Value],
) -> Result<Vec<Value>> {
    PreparedSql::parse(query)?.execute_in(c, rtxn, params)
}

/// Explain a SQL SELECT.
pub fn explain(c: &Collection<'_>, query: &str, params: &[Value]) -> Result<Value> {
    PreparedSql::parse(query)?.explain(c, params)
}

/// Explain a SQL SELECT inside an existing read transaction.
pub fn explain_in(
    c: &Collection<'_>,
    rtxn: &RoTxn<'_>,
    query: &str,
    params: &[Value],
) -> Result<Value> {
    PreparedSql::parse(query)?.explain_in(c, rtxn, params)
}

#[derive(Debug, Clone)]
enum ParsedStatement {
    Select(ParsedSql),
    Insert(InsertSql),
    Update(UpdateSql),
    Delete(DeleteSql),
}

impl ParsedStatement {
    fn collection(&self) -> &str {
        match self {
            Self::Select(parsed) => &parsed.collection,
            Self::Insert(parsed) => &parsed.collection,
            Self::Update(parsed) => &parsed.collection,
            Self::Delete(parsed) => &parsed.collection,
        }
    }

    fn is_read_only(&self) -> bool {
        matches!(self, Self::Select(_))
    }

    fn kind(&self) -> SqlKind {
        match self {
            Self::Select(_) => SqlKind::Select,
            Self::Insert(_) => SqlKind::Insert,
            Self::Update(_) => SqlKind::Update,
            Self::Delete(_) => SqlKind::Delete,
        }
    }
}

#[derive(Debug, Clone)]
struct InsertSql {
    collection: String,
    columns: Vec<String>,
    rows: Vec<Vec<ValueExpr>>,
}

impl InsertSql {
    fn execute(&self, c: &Collection<'_>, wtxn: &mut RwTxn<'_>, params: &[Value]) -> Result<Value> {
        let mut inserted_ids = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let mut doc = json!({});
            for (column, value) in self.columns.iter().zip(row) {
                path::set(&mut doc, column, value.resolve(params)?)?;
            }
            let id = c.insert_one_txn(wtxn, doc)?;
            if let Some(after) = c.find_one_in(&*wtxn, &json!({ "_id": id }), None)? {
                c.append_change_event(wtxn, "insert", &id, None, Some(&after))?;
            }
            inserted_ids.push(id);
        }
        Ok(json!({
            "inserted": inserted_ids.len(),
            "inserted_ids": inserted_ids,
        }))
    }
}

#[derive(Debug, Clone)]
struct UpdateSql {
    collection: String,
    assignments: Vec<(String, ScalarExpr)>,
    filter: Option<WhereExpr>,
}

impl UpdateSql {
    fn execute(&self, c: &Collection<'_>, wtxn: &mut RwTxn<'_>, params: &[Value]) -> Result<Value> {
        let filter = match &self.filter {
            Some(expr) => expr.to_filter(params)?,
            None => json!({}),
        };
        let uses_document = self
            .assignments
            .iter()
            .any(|(_, expr)| expr.uses_document());

        if !uses_document {
            let events_enabled = c.events_enabled_in(wtxn)?.is_some();
            let before_docs = if events_enabled {
                c.find_in(
                    &*wtxn,
                    &FindQuery {
                        filter: filter.clone(),
                        projection: None,
                        sort: Vec::new(),
                        skip: 0,
                        limit: None,
                    },
                )?
            } else {
                Vec::new()
            };
            let mut set = Map::new();
            for (field, expr) in &self.assignments {
                set.insert(field.clone(), expr.eval(&json!({}), params)?);
            }
            let out = c.update_opts_txn_filtered(
                wtxn,
                &filter,
                &json!({ "$set": set }),
                true,
                false,
                &[],
            )?;
            if events_enabled {
                for before in &before_docs {
                    let id = before
                        .get("_id")
                        .and_then(Value::as_str)
                        .ok_or(Error::InvalidId)?;
                    let after = c.find_one_in(&*wtxn, &json!({ "_id": id }), None)?;
                    c.append_change_event(wtxn, "update", id, Some(before), after.as_ref())?;
                }
            }
            return Ok(json!({
                "matched": out.matched,
                "modified": out.modified,
            }));
        }

        let docs = c.find_in(
            wtxn,
            &FindQuery {
                filter,
                projection: None,
                sort: Vec::new(),
                skip: 0,
                limit: None,
            },
        )?;
        let mut modified = 0usize;
        for doc in &docs {
            let mut set = Map::new();
            for (field, expr) in &self.assignments {
                set.insert(field.clone(), expr.eval(doc, params)?);
            }
            let id = doc
                .get("_id")
                .and_then(Value::as_str)
                .ok_or(Error::InvalidId)?
                .to_string();
            let out = c.update_opts_txn_filtered(
                wtxn,
                &json!({ "_id": id }),
                &json!({ "$set": set }),
                false,
                false,
                &[],
            )?;
            if out.modified > 0 {
                let after = c.find_one_in(&*wtxn, &json!({ "_id": id }), None)?;
                c.append_change_event(wtxn, "update", &id, Some(doc), after.as_ref())?;
            }
            modified += out.modified;
        }
        Ok(json!({
            "matched": docs.len(),
            "modified": modified,
        }))
    }
}

#[derive(Debug, Clone)]
struct DeleteSql {
    collection: String,
    filter: Option<WhereExpr>,
}

impl DeleteSql {
    fn execute(&self, c: &Collection<'_>, wtxn: &mut RwTxn<'_>, params: &[Value]) -> Result<Value> {
        let filter = match &self.filter {
            Some(expr) => expr.to_filter(params)?,
            None => json!({}),
        };
        let events_enabled = c.events_enabled_in(wtxn)?.is_some();
        let before_docs = if events_enabled {
            c.find_in(
                &*wtxn,
                &FindQuery {
                    filter: filter.clone(),
                    projection: None,
                    sort: Vec::new(),
                    skip: 0,
                    limit: None,
                },
            )?
        } else {
            Vec::new()
        };
        let deleted = c.delete_txn(wtxn, &filter, true)?;
        if events_enabled {
            for before in &before_docs {
                let id = before
                    .get("_id")
                    .and_then(Value::as_str)
                    .ok_or(Error::InvalidId)?;
                c.append_change_event(wtxn, "delete", id, Some(before), None)?;
            }
        }
        Ok(json!({ "deleted": deleted }))
    }
}

#[derive(Debug, Clone)]
struct ParsedSql {
    collection: String,
    distinct: bool,
    select: Vec<SelectItem>,
    filter: Option<WhereExpr>,
    group_by: Vec<String>,
    having: Option<ScalarExpr>,
    order_by: Vec<OrderBy>,
    limit: Option<usize>,
    offset: usize,
}

#[derive(Debug, Clone)]
enum SelectItem {
    Star,
    Field {
        field: String,
        alias: Option<String>,
    },
    Expr {
        expr: ScalarExpr,
        alias: Option<String>,
    },
    Aggregate {
        kind: AggregateKind,
        field: Option<String>,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AggregateKind {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
struct OrderBy {
    field: String,
    descending: bool,
}

/// Scalar SQL expression used by computed columns, CASE, functions, and
/// HAVING conditions.
#[derive(Debug, Clone)]
enum ScalarExpr {
    Literal(ValueExpr),
    Field(String),
    Aggregate(AggregateKind, Option<String>),
    Unary {
        op: UnaryOp,
        expr: Box<ScalarExpr>,
    },
    Binary {
        op: ScalarOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    Call {
        func: ScalarFunc,
        args: Vec<ScalarExpr>,
    },
    Case {
        when_then: Vec<(ScalarExpr, ScalarExpr)>,
        else_expr: Option<Box<ScalarExpr>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarOp {
    Add,
    Sub,
    Mul,
    Div,
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarFunc {
    Lower,
    Upper,
    Length,
    Substr,
    Trim,
    Ltrim,
    Rtrim,
    Coalesce,
    NullIf,
    Abs,
    Ceil,
    Floor,
    Round,
}

#[derive(Debug, Clone)]
enum WhereExpr {
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
    Compare {
        field: String,
        op: CompareOp,
        value: ValueExpr,
    },
    In {
        field: String,
        values: Vec<ValueExpr>,
        negated: bool,
    },
    Between {
        field: String,
        low: ValueExpr,
        high: ValueExpr,
        negated: bool,
    },
    IsNull {
        field: String,
        negated: bool,
    },
    Like {
        field: String,
        pattern: ValueExpr,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy)]
enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
enum ValueExpr {
    Literal(Value),
    Param(usize),
}

impl ValueExpr {
    fn resolve(&self, params: &[Value]) -> Result<Value> {
        match self {
            Self::Literal(v) => Ok(v.clone()),
            Self::Param(i) => params.get(*i).cloned().ok_or_else(|| {
                Error::Sql(format!(
                    "missing SQL parameter ${} ({} supplied)",
                    i + 1,
                    params.len()
                ))
            }),
        }
    }

    fn string(&self, params: &[Value], what: &str) -> Result<String> {
        match self.resolve(params)? {
            Value::String(s) => Ok(s),
            _ => Err(Error::Sql(format!("{what} must be a string"))),
        }
    }
}

impl ParsedSql {
    fn has_aggregate(&self) -> bool {
        self.select
            .iter()
            .any(|item| matches!(item, SelectItem::Aggregate { .. }))
    }

    fn grouped(&self) -> bool {
        !self.group_by.is_empty() || self.has_aggregate()
    }

    fn validate(&self) -> Result<()> {
        if self
            .select
            .iter()
            .any(|item| matches!(item, SelectItem::Star))
            && self.select.len() > 1
        {
            return Err(Error::Sql(
                "SELECT * must be the only item in the select list".into(),
            ));
        }
        if self
            .select
            .iter()
            .any(|item| matches!(item, SelectItem::Star))
            && self.grouped()
        {
            return Err(Error::Sql(
                "SELECT * cannot be combined with GROUP BY or aggregates".into(),
            ));
        }
        if self.grouped()
            && self
                .select
                .iter()
                .any(|item| matches!(item, SelectItem::Expr { .. }))
        {
            return Err(Error::Sql(
                "scalar expressions in a grouped SELECT are not supported yet".into(),
            ));
        }
        if self.having.is_some() && !self.grouped() {
            return Err(Error::Sql(
                "HAVING requires GROUP BY or an aggregate function".into(),
            ));
        }
        Ok(())
    }

    fn filter_value(&self, params: &[Value]) -> Result<Value> {
        match &self.filter {
            Some(expr) => expr.to_filter(params),
            None => Ok(json!({})),
        }
    }

    /// `SELECT DISTINCT *` is a no-op because `SELECT *` always includes the
    /// unique `_id`. Keep `DISTINCT` for every other select list.
    fn distinct_is_noop(&self) -> bool {
        self.distinct
            && self
                .select
                .iter()
                .any(|item| matches!(item, SelectItem::Star))
    }

    fn effective_distinct(&self) -> bool {
        self.distinct && !self.distinct_is_noop()
    }

    /// Simple field-only SELECTs can avoid the aggregation pipeline and its
    /// intermediate-document cap. Returns `None` when the query needs grouping,
    /// DISTINCT, HAVING, or a computed expression.
    fn fast_find(&self, params: &[Value]) -> Result<Option<FindQuery>> {
        self.validate()?;
        let needs_alias_projection = self.select.iter().any(|item| match item {
            SelectItem::Field { field, alias } => alias.as_deref().is_some_and(|a| a != field),
            _ => false,
        });
        if self.grouped()
            || self.effective_distinct()
            || self.having.is_some()
            || needs_alias_projection
            || self
                .select
                .iter()
                .any(|item| matches!(item, SelectItem::Expr { .. }))
        {
            return Ok(None);
        }
        let filter = self.filter_value(params)?;
        let sort = self.source_sort_fields()?;
        let projection = self.project_stage(params)?;
        // `find` keeps a long-standing special case where `{_id: 1}` alone is
        // an exclusion projection. SQL must return only `_id`, so use the
        // aggregation path for that shape instead.
        if projection.as_ref().is_some_and(|p| {
            p.as_object()
                .is_some_and(|obj| obj.len() == 1 && obj.contains_key("_id"))
        }) {
            return Ok(None);
        }
        Ok(Some(FindQuery {
            filter,
            projection,
            sort,
            skip: self.offset,
            limit: self.limit,
        }))
    }

    fn pipeline(&self, params: &[Value]) -> Result<Vec<Value>> {
        self.validate()?;
        let filter = self.filter_value(params)?;
        let filter_empty = matches!(&filter, Value::Object(map) if map.is_empty());
        let grouped = self.grouped();
        let distinct = self.effective_distinct();
        let has_expr = self
            .select
            .iter()
            .any(|item| matches!(item, SelectItem::Expr { .. }));

        let mut stages = Vec::new();
        if !filter_empty {
            stages.push(json!({ "$match": filter }));
        }

        if grouped {
            let (group_stage, project_stage, output_map, hidden) = self.group_stages()?;
            stages.push(group_stage);
            stages.push(project_stage);
            if let Some(having) = &self.having {
                stages.push(json!({
                    "$match": having_to_filter(having, &output_map, params)?
                }));
            }
            if !hidden.is_empty() {
                stages.push(json!({ "$unset": hidden }));
            }
            if distinct {
                let names = self.output_names()?;
                stages.push(self.output_distinct_group(&names));
                stages.push(self.output_distinct_project(&names));
            }
            if !self.order_by.is_empty() {
                let names: HashSet<String> = output_map.values().cloned().collect();
                stages.push(json!({ "$sort": self.sort_object(&names, true)? }));
            }
            if self.offset > 0 {
                stages.push(json!({ "$skip": self.offset }));
            }
            if let Some(limit) = self.limit {
                stages.push(json!({ "$limit": limit }));
            }
        } else if distinct {
            let (group_stage, project_stage, output_names) = self.distinct_stages(params)?;
            stages.push(group_stage);
            stages.push(project_stage);
            if !self.order_by.is_empty() {
                stages.push(json!({ "$sort": self.sort_object(&output_names, true)? }));
            }
            if self.offset > 0 {
                stages.push(json!({ "$skip": self.offset }));
            }
            if let Some(limit) = self.limit {
                stages.push(json!({ "$limit": limit }));
            }
        } else if has_expr {
            let project = self
                .project_stage(params)?
                .ok_or_else(|| Error::Sql("computed SELECT requires a projection".into()))?;
            stages.push(json!({ "$project": project }));
            if !self.order_by.is_empty() {
                let names = self.output_names()?;
                let set: HashSet<String> = names.into_iter().collect();
                stages.push(json!({ "$sort": self.sort_object(&set, true)? }));
            }
            if self.offset > 0 {
                stages.push(json!({ "$skip": self.offset }));
            }
            if let Some(limit) = self.limit {
                stages.push(json!({ "$limit": limit }));
            }
        } else {
            if !self.order_by.is_empty() {
                stages.push(json!({ "$sort": self.source_sort_object()? }));
            }
            if self.offset > 0 {
                stages.push(json!({ "$skip": self.offset }));
            }
            if let Some(limit) = self.limit {
                stages.push(json!({ "$limit": limit }));
            }
            if let Some(project) = self.project_stage(params)? {
                stages.push(json!({ "$project": project }));
            }
        }

        if stages.is_empty() {
            stages.push(json!({ "$match": {} }));
        }
        Ok(stages)
    }

    fn explain_pipeline(&self, params: &[Value]) -> Result<Value> {
        self.validate()?;
        let filtered = matches!(self.pipeline(params)?, ref stages if stages.first().and_then(|s| s.get("$match")).is_some());
        Ok(json!({
            "collection": self.collection,
            "access_path": if self.grouped() {
                "aggregation_group"
            } else if self.effective_distinct() {
                "aggregation_distinct"
            } else {
                "aggregation"
            },
            "distinct": self.effective_distinct(),
            "grouped": self.grouped(),
            "has_having": self.having.is_some(),
            "has_filter": filtered,
            "pipeline": self.pipeline(params)?,
        }))
    }

    fn output_names(&self) -> Result<Vec<String>> {
        let mut used = HashSet::new();
        let mut names = Vec::new();
        for (idx, item) in self.select.iter().enumerate() {
            let base = match item {
                SelectItem::Star => continue,
                SelectItem::Field { field, alias } => {
                    alias.clone().unwrap_or_else(|| field.clone())
                }
                SelectItem::Expr { alias, .. } => {
                    alias.clone().unwrap_or_else(|| format!("expr{}", idx + 1))
                }
                SelectItem::Aggregate { kind, field, alias } => {
                    aggregate_base_name(*kind, field.as_deref(), alias.as_deref(), idx)
                }
            };
            let mut name = base.clone();
            let mut n = 2;
            while !used.insert(name.clone()) {
                name = format!("{base}_{n}");
                n += 1;
            }
            names.push(name);
        }
        Ok(names)
    }

    fn project_stage(&self, params: &[Value]) -> Result<Option<Value>> {
        let mut project = Map::new();
        let mut selects_id = false;
        let mut has_non_star = false;
        let names = self.output_names()?;
        let mut name_idx = 0;
        for item in &self.select {
            match item {
                SelectItem::Star => return Ok(None),
                SelectItem::Field { field, .. } => {
                    has_non_star = true;
                    let key = names[name_idx].clone();
                    name_idx += 1;
                    if key == "_id" {
                        selects_id = true;
                    }
                    if key == *field {
                        project.insert(key, json!(1));
                    } else {
                        project.insert(key, json!(format!("${field}")));
                    }
                }
                SelectItem::Expr { expr, .. } => {
                    has_non_star = true;
                    let key = names[name_idx].clone();
                    name_idx += 1;
                    if key == "_id" {
                        selects_id = true;
                    }
                    project.insert(key, expr.to_agg(None, params)?);
                }
                SelectItem::Aggregate { .. } => unreachable!(),
            }
        }
        if !has_non_star {
            return Ok(None);
        }
        if !selects_id {
            project.insert("_id".into(), json!(0));
        }
        Ok(Some(Value::Object(project)))
    }

    fn output_distinct_group(&self, names: &[String]) -> Value {
        let mut id = Map::new();
        for (idx, name) in names.iter().enumerate() {
            id.insert(format!("d{idx}"), json!(format!("${name}")));
        }
        json!({ "$group": { "_id": Value::Object(id) } })
    }

    fn output_distinct_project(&self, names: &[String]) -> Value {
        let mut project = Map::new();
        project.insert("_id".into(), json!(0));
        for (idx, name) in names.iter().enumerate() {
            project.insert(name.clone(), json!(format!("$_id.d{idx}")));
        }
        json!({ "$project": Value::Object(project) })
    }

    fn distinct_stages(&self, params: &[Value]) -> Result<(Value, Value, HashSet<String>)> {
        if self
            .select
            .iter()
            .any(|item| matches!(item, SelectItem::Star))
        {
            return Err(Error::Sql("SELECT DISTINCT * is not supported".into()));
        }
        let names = self.output_names()?;
        let mut id = Map::new();
        let mut project = Map::new();
        project.insert("_id".into(), json!(0));
        let mut output_names = HashSet::new();
        for (idx, item) in self.select.iter().enumerate() {
            let expr = match item {
                SelectItem::Field { field, .. } => json!(format!("${field}")),
                SelectItem::Expr { expr, .. } => expr.to_agg(None, params)?,
                SelectItem::Aggregate { .. } => {
                    return Err(Error::Sql(
                        "DISTINCT with aggregates is not supported".into(),
                    ))
                }
                SelectItem::Star => unreachable!(),
            };
            let key = format!("d{idx}");
            id.insert(key.clone(), expr);
            let name = names[idx].clone();
            project.insert(name.clone(), json!(format!("$_id.{key}")));
            output_names.insert(name);
        }
        Ok((
            json!({ "$group": { "_id": Value::Object(id) } }),
            json!({ "$project": Value::Object(project) }),
            output_names,
        ))
    }

    fn group_stages(&self) -> Result<GroupStages> {
        let mut group = Map::new();
        group.insert("_id".into(), self.group_id_expr()?);

        let mut project = Map::new();
        project.insert("_id".into(), json!(0));
        let names = self.output_names()?;
        let mut used_names: HashSet<String> = names.iter().cloned().collect();
        let mut output_map: HashMap<String, String> = HashMap::new();
        let mut alias_to_field: HashMap<String, String> = HashMap::new();
        let mut alias_to_agg: HashMap<String, (AggregateKind, Option<String>)> = HashMap::new();
        let mut hidden = Vec::new();
        let mut select_idx = 0;

        for (idx, item) in self.select.iter().enumerate() {
            match item {
                SelectItem::Star => unreachable!(),
                SelectItem::Field { field, alias } => {
                    let group_idx = self
                        .group_by
                        .iter()
                        .position(|f| f == field)
                        .ok_or_else(|| {
                            Error::Sql(format!(
                                "column `{field}` must appear in GROUP BY or be used in an aggregate"
                            ))
                        })?;
                    let key = names[select_idx].clone();
                    select_idx += 1;
                    let expr = if self.group_by.len() == 1 {
                        json!("$_id")
                    } else {
                        json!(format!("$_id.g{group_idx}"))
                    };
                    project.insert(key.clone(), expr);
                    output_map.insert(field.clone(), key.clone());
                    if let Some(alias) = alias {
                        if alias != field {
                            alias_to_field.insert(alias.clone(), field.clone());
                            output_map.insert(alias.clone(), key);
                        }
                    }
                }
                SelectItem::Expr { .. } => {
                    return Err(Error::Sql(
                        "scalar expressions in a grouped SELECT are not supported yet".into(),
                    ))
                }
                SelectItem::Aggregate { kind, field, alias } => {
                    let key = names[select_idx].clone();
                    select_idx += 1;
                    group.insert(key.clone(), aggregate_spec(*kind, field.as_deref())?);
                    project.insert(key.clone(), aggregate_projection(*kind, &key));
                    output_map.insert(aggregate_key(*kind, field.as_deref()), key.clone());
                    if let Some(alias) = alias {
                        alias_to_agg.insert(alias.clone(), (*kind, field.clone()));
                        output_map.insert(alias.clone(), key);
                    }
                }
            }
            let _ = idx;
        }

        if let Some(having) = &self.having {
            let mut fields = Vec::new();
            let mut aggregates = Vec::new();
            having_refs(having, &mut fields, &mut aggregates);
            for raw_field in fields {
                if let Some((kind, field)) = alias_to_agg.get(&raw_field) {
                    aggregates.push((*kind, field.clone()));
                    continue;
                }
                let field = alias_to_field.get(&raw_field).cloned().unwrap_or(raw_field);
                if !self.group_by.iter().any(|f| f == &field) {
                    return Err(Error::Sql(format!(
                        "HAVING field `{field}` must appear in GROUP BY"
                    )));
                }
                if output_map.contains_key(&field) {
                    continue;
                }
                let group_idx = self
                    .group_by
                    .iter()
                    .position(|f| f == &field)
                    .expect("checked above");
                let key = unique_hidden_name(&format!("__sql_g{group_idx}"), &mut used_names);
                let expr = if self.group_by.len() == 1 {
                    json!("$_id")
                } else {
                    json!(format!("$_id.g{group_idx}"))
                };
                project.insert(key.clone(), expr);
                output_map.insert(field, key.clone());
                hidden.push(key);
            }
            for (kind, field) in aggregates {
                let agg_key = aggregate_key(kind, field.as_deref());
                if output_map.contains_key(&agg_key) {
                    continue;
                }
                let key = unique_hidden_name("__sql_h", &mut used_names);
                group.insert(key.clone(), aggregate_spec(kind, field.as_deref())?);
                project.insert(key.clone(), aggregate_projection(kind, &key));
                output_map.insert(agg_key, key.clone());
                hidden.push(key);
            }
        }

        Ok((
            json!({ "$group": Value::Object(group) }),
            json!({ "$project": Value::Object(project) }),
            output_map,
            hidden,
        ))
    }

    fn group_id_expr(&self) -> Result<Value> {
        if self.group_by.is_empty() {
            return Ok(Value::Null);
        }
        if self.group_by.len() == 1 {
            return Ok(json!(format!("${}", self.group_by[0])));
        }
        let mut id = Map::new();
        for (i, field) in self.group_by.iter().enumerate() {
            id.insert(format!("g{i}"), json!(format!("${field}")));
        }
        Ok(Value::Object(id))
    }

    fn source_sort_fields(&self) -> Result<Vec<(String, i32)>> {
        let mut out = Vec::new();
        for order in &self.order_by {
            let field = self
                .select
                .iter()
                .find_map(|item| match item {
                    SelectItem::Field {
                        field,
                        alias: Some(a),
                    } if a == &order.field => Some(field.clone()),
                    SelectItem::Expr { alias: Some(a), .. } if a == &order.field => {
                        Some(order.field.clone())
                    }
                    _ => None,
                })
                .unwrap_or_else(|| order.field.clone());
            out.push((field, if order.descending { -1 } else { 1 }));
        }
        Ok(out)
    }

    fn source_sort_object(&self) -> Result<Value> {
        let mut sort = Map::new();
        for (field, direction) in self.source_sort_fields()? {
            sort.insert(field, json!(direction));
        }
        Ok(Value::Object(sort))
    }

    fn sort_object(&self, names: &HashSet<String>, grouped: bool) -> Result<Value> {
        let mut sort = Map::new();
        for order in &self.order_by {
            let field = if grouped {
                if names.contains(&order.field) {
                    order.field.clone()
                } else {
                    let alias = self
                        .select
                        .iter()
                        .find_map(|item| match item {
                            SelectItem::Field {
                                field,
                                alias: Some(a),
                            } if field == &order.field => Some(a.clone()),
                            _ => None,
                        })
                        .or_else(|| {
                            self.group_by
                                .iter()
                                .position(|f| f == &order.field)
                                .and_then(|_| {
                                    self.select.iter().find_map(|item| match item {
                                        SelectItem::Field { field, alias }
                                            if field == &order.field =>
                                        {
                                            Some(alias.clone().unwrap_or_else(|| field.clone()))
                                        }
                                        _ => None,
                                    })
                                })
                        });
                    alias.ok_or_else(|| {
                        Error::Sql(format!(
                            "ORDER BY `{}` must reference a selected output column",
                            order.field
                        ))
                    })?
                }
            } else {
                self.select
                    .iter()
                    .find_map(|item| match item {
                        SelectItem::Field {
                            field,
                            alias: Some(a),
                        } if a == &order.field => Some(field.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| order.field.clone())
            };
            sort.insert(field, json!(if order.descending { -1 } else { 1 }));
        }
        Ok(Value::Object(sort))
    }

    fn apply_table_alias(&mut self, alias: &str) {
        let strip = |s: &mut String| {
            let prefix = format!("{alias}.");
            if let Some(rest) = s.strip_prefix(&prefix) {
                *s = rest.to_string();
            }
        };
        for item in &mut self.select {
            match item {
                SelectItem::Field { field, .. } => strip(field),
                SelectItem::Expr { expr, .. } => strip_scalar(expr, alias),
                SelectItem::Aggregate { field: Some(f), .. } => strip(f),
                _ => {}
            }
        }
        strip_all(&mut self.filter, alias);
        if let Some(having) = &mut self.having {
            strip_scalar(having, alias);
        }
        for f in &mut self.group_by {
            strip(f);
        }
        for o in &mut self.order_by {
            strip(&mut o.field);
        }
    }
}

type GroupStages = (Value, Value, HashMap<String, String>, Vec<String>);

fn aggregate_base_name(
    kind: AggregateKind,
    field: Option<&str>,
    alias: Option<&str>,
    idx: usize,
) -> String {
    match alias {
        Some(a) => a.to_string(),
        None => match (kind, field) {
            (AggregateKind::Count | AggregateKind::CountDistinct, _) => "count".to_string(),
            (AggregateKind::Sum, Some(f)) => format!("sum_{}", sanitize_name(f)),
            (AggregateKind::Avg, Some(f)) => format!("avg_{}", sanitize_name(f)),
            (AggregateKind::Min, Some(f)) => format!("min_{}", sanitize_name(f)),
            (AggregateKind::Max, Some(f)) => format!("max_{}", sanitize_name(f)),
            _ => format!("expr{}", idx + 1),
        },
    }
}

fn aggregate_key(kind: AggregateKind, field: Option<&str>) -> String {
    format!(
        "{}:{}",
        match kind {
            AggregateKind::Count => "count",
            AggregateKind::CountDistinct => "count_distinct",
            AggregateKind::Sum => "sum",
            AggregateKind::Avg => "avg",
            AggregateKind::Min => "min",
            AggregateKind::Max => "max",
        },
        field.unwrap_or("*")
    )
}

fn unique_hidden_name(base: &str, used: &mut HashSet<String>) -> String {
    if used.insert(base.to_string()) {
        return base.to_string();
    }
    let mut n = 2;
    loop {
        let name = format!("{base}_{n}");
        if used.insert(name.clone()) {
            return name;
        }
        n += 1;
    }
}

impl ScalarExpr {
    fn uses_document(&self) -> bool {
        match self {
            Self::Field(_) | Self::Aggregate(..) => true,
            Self::Unary { expr, .. } => expr.uses_document(),
            Self::Binary { left, right, .. } => left.uses_document() || right.uses_document(),
            Self::Call { args, .. } => args.iter().any(Self::uses_document),
            Self::Case {
                when_then,
                else_expr,
            } => {
                when_then
                    .iter()
                    .any(|(when, then)| when.uses_document() || then.uses_document())
                    || else_expr.as_deref().is_some_and(Self::uses_document)
            }
            Self::Literal(_) => false,
        }
    }

    fn eval(&self, doc: &Value, params: &[Value]) -> Result<Value> {
        let expr = self.to_agg(None, params)?;
        aggregate::eval_expr(doc, &expr)
    }

    fn to_agg(&self, fields: Option<&HashMap<String, String>>, params: &[Value]) -> Result<Value> {
        Ok(match self {
            Self::Literal(value) => json!({ "$literal": value.resolve(params)? }),
            Self::Field(field) => {
                if let Some(map) = fields {
                    if let Some(mapped) = map.get(field) {
                        return Ok(json!(format!("${mapped}")));
                    }
                }
                json!(format!("${field}"))
            }
            Self::Aggregate(..) => {
                return Err(Error::Sql(
                    "aggregate expressions are only supported at the top level of SELECT or HAVING"
                        .into(),
                ))
            }
            Self::Unary { op, expr } => match op {
                UnaryOp::Not => json!({ "$not": [ expr.to_agg(fields, params)? ] }),
                UnaryOp::Neg => {
                    json!({ "$multiply": [ { "$literal": -1 }, expr.to_agg(fields, params)? ] })
                }
            },
            Self::Binary { op, left, right } => {
                let l = left.to_agg(fields, params)?;
                let r = right.to_agg(fields, params)?;
                let name = match op {
                    ScalarOp::Add => "$add",
                    ScalarOp::Sub => "$subtract",
                    ScalarOp::Mul => "$multiply",
                    ScalarOp::Div => "$divide",
                    ScalarOp::Concat => "$concat",
                    ScalarOp::Eq => "$eq",
                    ScalarOp::Ne => "$ne",
                    ScalarOp::Lt => "$lt",
                    ScalarOp::Le => "$lte",
                    ScalarOp::Gt => "$gt",
                    ScalarOp::Ge => "$gte",
                    ScalarOp::And => "$and",
                    ScalarOp::Or => "$or",
                };
                json!({ name: [l, r] })
            }
            Self::Call { func, args } => {
                let args: Result<Vec<_>> = args.iter().map(|a| a.to_agg(fields, params)).collect();
                scalar_call(*func, args?)?
            }
            Self::Case {
                when_then,
                else_expr,
            } => {
                let mut out = match else_expr {
                    Some(expr) => expr.to_agg(fields, params)?,
                    None => Value::Null,
                };
                for (when, then) in when_then.iter().rev() {
                    out = json!({
                        "$cond": [ when.to_agg(fields, params)?, then.to_agg(fields, params)?, out ]
                    });
                }
                out
            }
        })
    }
}

fn scalar_call(func: ScalarFunc, args: Vec<Value>) -> Result<Value> {
    match func {
        ScalarFunc::Lower | ScalarFunc::Upper | ScalarFunc::Length => {
            if args.len() != 1 {
                return Err(Error::Sql("function requires exactly one argument".into()));
            }
            let op = match func {
                ScalarFunc::Lower => "$toLower",
                ScalarFunc::Upper => "$toUpper",
                ScalarFunc::Length => "$length",
                _ => unreachable!(),
            };
            Ok(json!({ op: [ args[0].clone() ] }))
        }
        ScalarFunc::Substr => {
            if args.len() != 3 {
                return Err(Error::Sql("SUBSTR requires three arguments".into()));
            }
            Ok(json!({ "$substr": args }))
        }
        ScalarFunc::Trim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            if args.len() != 1 {
                return Err(Error::Sql(
                    "trim function requires exactly one argument".into(),
                ));
            }
            let op = match func {
                ScalarFunc::Trim => "$trim",
                ScalarFunc::Ltrim => "$ltrim",
                ScalarFunc::Rtrim => "$rtrim",
                _ => unreachable!(),
            };
            Ok(json!({ op: [ args[0].clone() ] }))
        }
        ScalarFunc::Coalesce => {
            if args.is_empty() {
                return Err(Error::Sql("COALESCE requires at least one argument".into()));
            }
            let mut out = args.last().cloned().unwrap();
            for arg in args.iter().rev().skip(1) {
                out = json!({ "$ifNull": [arg, out] });
            }
            Ok(out)
        }
        ScalarFunc::NullIf => {
            if args.len() != 2 {
                return Err(Error::Sql("NULLIF requires two arguments".into()));
            }
            Ok(
                json!({ "$cond": [ { "$eq": [args[0].clone(), args[1].clone()] }, null, args[0].clone() ] }),
            )
        }
        ScalarFunc::Abs | ScalarFunc::Ceil | ScalarFunc::Floor | ScalarFunc::Round => {
            if args.is_empty() || args.len() > 2 {
                return Err(Error::Sql("numeric function has invalid arity".into()));
            }
            let op = match func {
                ScalarFunc::Abs => "$abs",
                ScalarFunc::Ceil => "$ceil",
                ScalarFunc::Floor => "$floor",
                ScalarFunc::Round => "$round",
                _ => unreachable!(),
            };
            Ok(json!({ op: args }))
        }
    }
}

fn having_refs(
    expr: &ScalarExpr,
    fields: &mut Vec<String>,
    aggregates: &mut Vec<(AggregateKind, Option<String>)>,
) {
    match expr {
        ScalarExpr::Field(field) => fields.push(field.clone()),
        ScalarExpr::Aggregate(kind, field) => aggregates.push((*kind, field.clone())),
        ScalarExpr::Unary { expr, .. } => having_refs(expr, fields, aggregates),
        ScalarExpr::Binary { left, right, .. } => {
            having_refs(left, fields, aggregates);
            having_refs(right, fields, aggregates);
        }
        ScalarExpr::Call { args, .. } => {
            for arg in args {
                having_refs(arg, fields, aggregates);
            }
        }
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                having_refs(when, fields, aggregates);
                having_refs(then, fields, aggregates);
            }
            if let Some(expr) = else_expr {
                having_refs(expr, fields, aggregates);
            }
        }
        ScalarExpr::Literal(_) => {}
    }
}

fn having_to_filter(
    expr: &ScalarExpr,
    output_map: &HashMap<String, String>,
    params: &[Value],
) -> Result<Value> {
    match expr {
        ScalarExpr::Binary { op, left, right } => match op {
            ScalarOp::And => Ok(json!({
                "$and": [
                    having_to_filter(left, output_map, params)?,
                    having_to_filter(right, output_map, params)?
                ]
            })),
            ScalarOp::Or => Ok(json!({
                "$or": [
                    having_to_filter(left, output_map, params)?,
                    having_to_filter(right, output_map, params)?
                ]
            })),
            ScalarOp::Eq
            | ScalarOp::Ne
            | ScalarOp::Lt
            | ScalarOp::Le
            | ScalarOp::Gt
            | ScalarOp::Ge => {
                let (field, value, op) = if let Some(field) = having_field(left, output_map) {
                    let value = scalar_literal(right, params).ok_or_else(|| {
                        Error::Sql(
                            "HAVING comparison must compare a column/aggregate to a literal".into(),
                        )
                    })?;
                    (field, value, *op)
                } else if let Some(field) = having_field(right, output_map) {
                    let value = scalar_literal(left, params).ok_or_else(|| {
                        Error::Sql(
                            "HAVING comparison must compare a column/aggregate to a literal".into(),
                        )
                    })?;
                    (field, value, reverse_cmp(*op))
                } else {
                    return Err(Error::Sql(
                        "HAVING comparison must reference a selected column or GROUP BY field"
                            .into(),
                    ));
                };
                let mongo_op = match op {
                    ScalarOp::Eq => None,
                    ScalarOp::Ne => Some("$ne"),
                    ScalarOp::Lt => Some("$lt"),
                    ScalarOp::Le => Some("$lte"),
                    ScalarOp::Gt => Some("$gt"),
                    ScalarOp::Ge => Some("$gte"),
                    _ => unreachable!(),
                };
                Ok(match mongo_op {
                    None => json!({ field: value }),
                    Some(op) => json!({ field: { op: value } }),
                })
            }
            _ => Err(Error::Sql(
                "HAVING supports comparisons, AND, OR, and NOT".into(),
            )),
        },
        ScalarExpr::Unary {
            op: UnaryOp::Not,
            expr,
        } => Ok(json!({ "$nor": [ having_to_filter(expr, output_map, params)? ] })),
        _ => Err(Error::Sql(
            "HAVING supports comparisons, AND, OR, and NOT".into(),
        )),
    }
}

fn having_field(expr: &ScalarExpr, map: &HashMap<String, String>) -> Option<String> {
    match expr {
        ScalarExpr::Field(field) => map.get(field).cloned(),
        ScalarExpr::Aggregate(kind, field) => {
            map.get(&aggregate_key(*kind, field.as_deref())).cloned()
        }
        _ => None,
    }
}

fn scalar_literal(expr: &ScalarExpr, params: &[Value]) -> Option<Value> {
    match expr {
        ScalarExpr::Literal(value) => value.resolve(params).ok(),
        ScalarExpr::Unary {
            op: UnaryOp::Neg,
            expr,
        } => match scalar_literal(expr, params)? {
            Value::Number(n) => n
                .as_i64()
                .map(|i| json!(-i))
                .or_else(|| n.as_f64().map(|f| json!(-f))),
            _ => None,
        },
        _ => None,
    }
}

fn reverse_cmp(op: ScalarOp) -> ScalarOp {
    match op {
        ScalarOp::Lt => ScalarOp::Gt,
        ScalarOp::Le => ScalarOp::Ge,
        ScalarOp::Gt => ScalarOp::Lt,
        ScalarOp::Ge => ScalarOp::Le,
        other => other,
    }
}

fn strip_all(expr: &mut Option<WhereExpr>, alias: &str) {
    fn strip_value(v: &mut ValueExpr, alias: &str) {
        // SQL parameters and literals are never column references.
        let _ = (v, alias);
    }
    fn go(expr: &mut WhereExpr, alias: &str) {
        let strip = |s: &mut String| {
            let prefix = format!("{alias}.");
            if let Some(rest) = s.strip_prefix(&prefix) {
                *s = rest.to_string();
            }
        };
        match expr {
            WhereExpr::And(a, b) | WhereExpr::Or(a, b) => {
                go(a, alias);
                go(b, alias);
            }
            WhereExpr::Not(a) => go(a, alias),
            WhereExpr::Compare { field, value, .. } => {
                strip(field);
                strip_value(value, alias);
            }
            WhereExpr::In { field, values, .. } => {
                strip(field);
                for v in values {
                    strip_value(v, alias);
                }
            }
            WhereExpr::Between {
                field, low, high, ..
            } => {
                strip(field);
                strip_value(low, alias);
                strip_value(high, alias);
            }
            WhereExpr::IsNull { field, .. } => strip(field),
            WhereExpr::Like { field, pattern, .. } => {
                strip(field);
                strip_value(pattern, alias);
            }
        }
    }
    if let Some(e) = expr {
        go(e, alias);
    }
}

fn strip_scalar(expr: &mut ScalarExpr, alias: &str) {
    fn strip(s: &mut String, alias: &str) {
        let prefix = format!("{alias}.");
        if let Some(rest) = s.strip_prefix(&prefix) {
            *s = rest.to_string();
        }
    }
    match expr {
        ScalarExpr::Field(field) => strip(field, alias),
        ScalarExpr::Aggregate(_, Some(field)) => strip(field, alias),
        ScalarExpr::Unary { expr, .. } => strip_scalar(expr, alias),
        ScalarExpr::Binary { left, right, .. } => {
            strip_scalar(left, alias);
            strip_scalar(right, alias);
        }
        ScalarExpr::Call { args, .. } => {
            for arg in args {
                strip_scalar(arg, alias);
            }
        }
        ScalarExpr::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                strip_scalar(when, alias);
                strip_scalar(then, alias);
            }
            if let Some(expr) = else_expr {
                strip_scalar(expr, alias);
            }
        }
        ScalarExpr::Literal(_) | ScalarExpr::Aggregate(_, None) => {}
    }
}

fn aggregate_projection(kind: AggregateKind, key: &str) -> Value {
    if kind == AggregateKind::CountDistinct {
        json!({ "$size": [ { "$setDifference": [ format!("${key}"), [null] ] } ] })
    } else {
        json!(format!("${key}"))
    }
}

fn aggregate_spec(kind: AggregateKind, field: Option<&str>) -> Result<Value> {
    match kind {
        AggregateKind::Count => match field {
            None => Ok(json!({ "$count": {} })),
            Some(f) => Ok(json!({
                "$sum": { "$cond": [ { "$ne": [ format!("${f}"), null ] }, 1, 0 ] }
            })),
        },
        AggregateKind::CountDistinct => {
            let f =
                field.ok_or_else(|| Error::Sql("COUNT(DISTINCT ...) requires a field".into()))?;
            Ok(json!({ "$addToSet": format!("${f}") }))
        }
        AggregateKind::Sum | AggregateKind::Avg | AggregateKind::Min | AggregateKind::Max => {
            let f =
                field.ok_or_else(|| Error::Sql("aggregate function requires a field".into()))?;
            let op = match kind {
                AggregateKind::Sum => "$sum",
                AggregateKind::Avg => "$avg",
                AggregateKind::Min => "$min",
                AggregateKind::Max => "$max",
                AggregateKind::Count | AggregateKind::CountDistinct => unreachable!(),
            };
            Ok(json!({ op: format!("${f}") }))
        }
    }
}

fn sanitize_name(field: &str) -> String {
    field
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl WhereExpr {
    fn to_filter(&self, params: &[Value]) -> Result<Value> {
        Ok(match self {
            Self::And(a, b) => json!({ "$and": [a.to_filter(params)?, b.to_filter(params)?] }),
            Self::Or(a, b) => json!({ "$or": [a.to_filter(params)?, b.to_filter(params)?] }),
            Self::Not(a) => json!({ "$nor": [a.to_filter(params)?] }),
            Self::Compare { field, op, value } => {
                let value = value.resolve(params)?;
                if matches!(op, CompareOp::Eq) {
                    json!({ field: value })
                } else {
                    let op = match op {
                        CompareOp::Ne => "$ne",
                        CompareOp::Lt => "$lt",
                        CompareOp::Le => "$lte",
                        CompareOp::Gt => "$gt",
                        CompareOp::Ge => "$gte",
                        CompareOp::Eq => unreachable!(),
                    };
                    json!({ field: { op: value } })
                }
            }
            Self::In {
                field,
                values,
                negated,
            } => {
                let values: Result<Vec<_>> = values.iter().map(|v| v.resolve(params)).collect();
                let op = if *negated { "$nin" } else { "$in" };
                json!({ field: { op: values? } })
            }
            Self::Between {
                field,
                low,
                high,
                negated,
            } => {
                let low = low.resolve(params)?;
                let high = high.resolve(params)?;
                if *negated {
                    json!({
                        "$or": [
                            { field: { "$lt": low } },
                            { field: { "$gt": high } }
                        ]
                    })
                } else {
                    json!({ field: { "$gte": low, "$lte": high } })
                }
            }
            Self::IsNull { field, negated } => {
                if *negated {
                    json!({ field: { "$ne": null } })
                } else {
                    json!({ field: null })
                }
            }
            Self::Like {
                field,
                pattern,
                negated,
            } => {
                let pattern = pattern.string(params, "LIKE pattern")?;
                let regex = like_to_regex(&pattern);
                if *negated {
                    json!({ "$nor": [ { field: { "$regex": regex } } ] })
                } else {
                    json!({ field: { "$regex": regex } })
                }
            }
        })
    }
}

fn like_to_regex(pattern: &str) -> String {
    let mut out = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('$');
    out
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    QuotedIdent(String),
    Str(String),
    Num(String),
    Param(usize),
    Comma,
    Dot,
    LParen,
    RParen,
    Star,
    Plus,
    Minus,
    Slash,
    Percent,
    Concat,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Semi,
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    pos: usize,
}

struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
    next_param: usize,
}

impl<'a> Lexer<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            input: s.as_bytes(),
            pos: 0,
            next_param: 0,
        }
    }

    fn lex(mut self) -> Result<Vec<Token>> {
        let mut out = Vec::new();
        while self.pos < self.input.len() {
            let start = self.pos;
            let c = self.input[self.pos];
            if c.is_ascii_whitespace() {
                self.pos += 1;
                continue;
            }
            if c == b'-' && self.peek_byte(1) == Some(b'-') {
                self.pos += 2;
                while self.pos < self.input.len() && self.input[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            if c == b'/' && self.peek_byte(1) == Some(b'*') {
                self.pos += 2;
                while self.pos + 1 < self.input.len()
                    && !(self.input[self.pos] == b'*' && self.input[self.pos + 1] == b'/')
                {
                    self.pos += 1;
                }
                if self.pos + 1 >= self.input.len() {
                    return Err(sql_err(start, "unterminated block comment"));
                }
                self.pos += 2;
                continue;
            }
            let tok = match c {
                b',' => {
                    self.pos += 1;
                    Tok::Comma
                }
                b'.' => {
                    self.pos += 1;
                    Tok::Dot
                }
                b'(' => {
                    self.pos += 1;
                    Tok::LParen
                }
                b')' => {
                    self.pos += 1;
                    Tok::RParen
                }
                b'*' => {
                    self.pos += 1;
                    Tok::Star
                }
                b'+' => {
                    self.pos += 1;
                    Tok::Plus
                }
                b'/' => {
                    self.pos += 1;
                    Tok::Slash
                }
                b'%' => {
                    self.pos += 1;
                    Tok::Percent
                }
                b'|' => {
                    if self.peek_byte(1) == Some(b'|') {
                        self.pos += 2;
                        Tok::Concat
                    } else {
                        return Err(sql_err(start, "expected `||`"));
                    }
                }
                b';' => {
                    self.pos += 1;
                    Tok::Semi
                }
                b'-' => {
                    self.pos += 1;
                    Tok::Minus
                }
                b'=' => {
                    self.pos += 1;
                    Tok::Eq
                }
                b'!' => {
                    if self.peek_byte(1) == Some(b'=') {
                        self.pos += 2;
                        Tok::Ne
                    } else {
                        return Err(sql_err(start, "expected `!=`"));
                    }
                }
                b'<' => {
                    if self.peek_byte(1) == Some(b'=') {
                        self.pos += 2;
                        Tok::Le
                    } else if self.peek_byte(1) == Some(b'>') {
                        self.pos += 2;
                        Tok::Ne
                    } else {
                        self.pos += 1;
                        Tok::Lt
                    }
                }
                b'>' => {
                    if self.peek_byte(1) == Some(b'=') {
                        self.pos += 2;
                        Tok::Ge
                    } else {
                        self.pos += 1;
                        Tok::Gt
                    }
                }
                b'\'' => {
                    self.pos += 1;
                    let mut buf = Vec::new();
                    loop {
                        if self.pos >= self.input.len() {
                            return Err(sql_err(start, "unterminated string literal"));
                        }
                        if self.input[self.pos] == b'\'' {
                            if self.peek_byte(1) == Some(b'\'') {
                                buf.push(b'\'');
                                self.pos += 2;
                            } else {
                                self.pos += 1;
                                break;
                            }
                        } else {
                            buf.push(self.input[self.pos]);
                            self.pos += 1;
                        }
                    }
                    Tok::Str(
                        String::from_utf8(buf)
                            .map_err(|_| sql_err(start, "string literal is not valid UTF-8"))?,
                    )
                }
                b'"' | b'`' => {
                    let quote = c;
                    self.pos += 1;
                    let mut buf = Vec::new();
                    loop {
                        if self.pos >= self.input.len() {
                            return Err(sql_err(start, "unterminated quoted identifier"));
                        }
                        if self.input[self.pos] == quote {
                            if self.peek_byte(1) == Some(quote) {
                                buf.push(quote);
                                self.pos += 2;
                            } else {
                                self.pos += 1;
                                break;
                            }
                        } else {
                            buf.push(self.input[self.pos]);
                            self.pos += 1;
                        }
                    }
                    Tok::QuotedIdent(
                        String::from_utf8(buf)
                            .map_err(|_| sql_err(start, "quoted identifier is not valid UTF-8"))?,
                    )
                }
                b'?' => {
                    let idx = self.next_param;
                    self.next_param += 1;
                    self.pos += 1;
                    Tok::Param(idx)
                }
                b'$' if self.peek_byte(1).is_some_and(|b| b.is_ascii_digit()) => {
                    self.pos += 1;
                    let begin = self.pos;
                    while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                        self.pos += 1;
                    }
                    let n: usize = std::str::from_utf8(&self.input[begin..self.pos])
                        .map_err(|_| sql_err(start, "invalid parameter number"))?
                        .parse()
                        .map_err(|_| sql_err(start, "invalid parameter number"))?;
                    if n == 0 {
                        return Err(sql_err(start, "SQL parameters are numbered from 1"));
                    }
                    // Keep positional `?` placeholders after a preceding `$n`
                    // placeholder unless the caller mixes styles in reverse.
                    self.next_param = self.next_param.max(n);
                    Tok::Param(n - 1)
                }
                b'0'..=b'9' => {
                    self.pos += 1;
                    while self.pos < self.input.len()
                        && (self.input[self.pos].is_ascii_digit() || self.input[self.pos] == b'.')
                    {
                        self.pos += 1;
                    }
                    if self.pos < self.input.len() && matches!(self.input[self.pos], b'e' | b'E') {
                        self.pos += 1;
                        if self.pos < self.input.len()
                            && matches!(self.input[self.pos], b'+' | b'-')
                        {
                            self.pos += 1;
                        }
                        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                            self.pos += 1;
                        }
                    }
                    let s = std::str::from_utf8(&self.input[start..self.pos])
                        .map_err(|_| sql_err(start, "invalid number"))?;
                    Tok::Num(s.to_string())
                }
                c if c == b'_' || c == b'$' || c.is_ascii_alphabetic() => {
                    self.pos += 1;
                    while self.pos < self.input.len() {
                        let b = self.input[self.pos];
                        if b == b'_' || b == b'$' || b.is_ascii_alphanumeric() {
                            self.pos += 1;
                        } else {
                            break;
                        }
                    }
                    let s = std::str::from_utf8(&self.input[start..self.pos])
                        .map_err(|_| sql_err(start, "invalid identifier"))?;
                    Tok::Ident(s.to_string())
                }
                _ => return Err(sql_err(start, "unexpected character")),
            };
            out.push(Token { tok, pos: start });
        }
        Ok(out)
    }

    fn peek_byte(&self, offset: usize) -> Option<u8> {
        self.input.get(self.pos + offset).copied()
    }
}

fn sql_err(pos: usize, message: &str) -> Error {
    Error::Sql(format!("{message} at byte {pos}"))
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn parse(mut self) -> Result<ParsedStatement> {
        if self.eat_kw("INSERT") {
            return Ok(ParsedStatement::Insert(self.parse_insert()?));
        }
        if self.eat_kw("UPDATE") {
            return Ok(ParsedStatement::Update(self.parse_update()?));
        }
        if self.eat_kw("DELETE") {
            return Ok(ParsedStatement::Delete(self.parse_delete()?));
        }
        self.expect_kw("SELECT")?;
        Ok(ParsedStatement::Select(self.parse_select()?))
    }

    fn parse_select(&mut self) -> Result<ParsedSql> {
        let distinct = self.eat_kw("DISTINCT");
        let mut select = Vec::new();
        loop {
            if self.eat_tok(&Tok::Star) {
                select.push(SelectItem::Star);
            } else {
                select.push(self.parse_select_item()?);
            }
            if !self.eat_tok(&Tok::Comma) {
                break;
            }
        }
        self.expect_kw("FROM")?;
        let collection = self.parse_ident()?;
        let mut table_alias = None;
        if self.eat_kw("AS") {
            table_alias = Some(self.parse_ident()?);
        } else if let Some(Tok::Ident(s) | Tok::QuotedIdent(s)) = self.peek_tok() {
            if !is_reserved_keyword(s) {
                table_alias = Some(self.parse_ident()?);
            }
        }

        let mut parsed = ParsedSql {
            collection,
            distinct,
            select,
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: 0,
        };

        if self.eat_kw("WHERE") {
            parsed.filter = Some(self.parse_or()?);
        }
        if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            loop {
                parsed.group_by.push(self.parse_field()?);
                if !self.eat_tok(&Tok::Comma) {
                    break;
                }
            }
        }
        if self.eat_kw("HAVING") {
            parsed.having = Some(self.parse_scalar_or()?);
        }
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            loop {
                let field = self.parse_field()?;
                let descending = if self.eat_kw("DESC") {
                    true
                } else {
                    self.eat_kw("ASC");
                    false
                };
                parsed.order_by.push(OrderBy { field, descending });
                if !self.eat_tok(&Tok::Comma) {
                    break;
                }
            }
        }
        if self.eat_kw("LIMIT") {
            let first = self.parse_usize("LIMIT")?;
            if self.eat_tok(&Tok::Comma) {
                let count = self.parse_usize("LIMIT")?;
                parsed.offset = first;
                parsed.limit = Some(count);
            } else {
                parsed.limit = Some(first);
                if self.eat_kw("OFFSET") {
                    parsed.offset = self.parse_usize("OFFSET")?;
                }
            }
        }
        if self.eat_kw("OFFSET") {
            parsed.offset = self.parse_usize("OFFSET")?;
        }
        if let Some(alias) = table_alias.as_deref() {
            parsed.apply_table_alias(alias);
        }
        self.finish_statement()?;
        Ok(parsed)
    }

    fn parse_insert(&mut self) -> Result<InsertSql> {
        self.expect_kw("INTO")?;
        let collection = self.parse_ident()?;
        self.expect_tok(&Tok::LParen)?;
        let mut columns = Vec::new();
        let mut seen = HashSet::new();
        loop {
            let column = self.parse_field()?;
            if !seen.insert(column.clone()) {
                return Err(Error::Sql(format!("duplicate INSERT column `{column}`")));
            }
            columns.push(column);
            if !self.eat_tok(&Tok::Comma) {
                break;
            }
        }
        self.expect_tok(&Tok::RParen)?;
        if columns.is_empty() {
            return Err(Error::Sql("INSERT requires at least one column".into()));
        }
        self.expect_kw("VALUES")?;
        let mut rows = Vec::new();
        loop {
            self.expect_tok(&Tok::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_value()?);
                if !self.eat_tok(&Tok::Comma) {
                    break;
                }
            }
            self.expect_tok(&Tok::RParen)?;
            if row.len() != columns.len() {
                return Err(Error::Sql(format!(
                    "INSERT row has {} values but {} columns",
                    row.len(),
                    columns.len()
                )));
            }
            rows.push(row);
            if !self.eat_tok(&Tok::Comma) {
                break;
            }
        }
        self.finish_statement()?;
        Ok(InsertSql {
            collection,
            columns,
            rows,
        })
    }

    fn parse_update(&mut self) -> Result<UpdateSql> {
        let collection = self.parse_ident()?;
        let mut table_alias = None;
        if self.eat_kw("AS") {
            table_alias = Some(self.parse_ident()?);
        } else if let Some(Tok::Ident(s) | Tok::QuotedIdent(s)) = self.peek_tok() {
            if !is_reserved_keyword(s) {
                table_alias = Some(self.parse_ident()?);
            }
        }
        self.expect_kw("SET")?;
        let mut assignments = Vec::new();
        let mut seen = HashSet::new();
        loop {
            let mut field = self.parse_field()?;
            if let Some(alias) = table_alias.as_deref() {
                let prefix = format!("{alias}.");
                if let Some(rest) = field.strip_prefix(&prefix) {
                    field = rest.to_string();
                }
            }
            if !seen.insert(field.clone()) {
                return Err(Error::Sql(format!("duplicate UPDATE column `{field}`")));
            }
            self.expect_tok(&Tok::Eq)?;
            let mut value = self.parse_scalar_or()?;
            if let Some(alias) = table_alias.as_deref() {
                strip_scalar(&mut value, alias);
            }
            assignments.push((field, value));
            if !self.eat_tok(&Tok::Comma) {
                break;
            }
        }
        let mut filter = None;
        if self.eat_kw("WHERE") {
            filter = Some(self.parse_or()?);
        }
        if let (Some(alias), Some(filter)) = (table_alias.as_deref(), filter.as_mut()) {
            let mut wrapped = Some(filter.clone());
            strip_all(&mut wrapped, alias);
            *filter = wrapped.expect("filter exists");
        }
        self.finish_statement()?;
        Ok(UpdateSql {
            collection,
            assignments,
            filter,
        })
    }

    fn parse_delete(&mut self) -> Result<DeleteSql> {
        self.expect_kw("FROM")?;
        let collection = self.parse_ident()?;
        let mut table_alias = None;
        if self.eat_kw("AS") {
            table_alias = Some(self.parse_ident()?);
        } else if let Some(Tok::Ident(s) | Tok::QuotedIdent(s)) = self.peek_tok() {
            if !is_reserved_keyword(s) {
                table_alias = Some(self.parse_ident()?);
            }
        }
        let mut filter = None;
        if self.eat_kw("WHERE") {
            filter = Some(self.parse_or()?);
        }
        if let (Some(alias), Some(filter)) = (table_alias.as_deref(), filter.as_mut()) {
            let mut wrapped = Some(filter.clone());
            strip_all(&mut wrapped, alias);
            *filter = wrapped.expect("filter exists");
        }
        self.finish_statement()?;
        Ok(DeleteSql { collection, filter })
    }

    fn finish_statement(&mut self) -> Result<()> {
        self.eat_tok(&Tok::Semi);
        if self.pos != self.tokens.len() {
            let t = &self.tokens[self.pos];
            return Err(sql_err(t.pos, "unexpected trailing SQL token"));
        }
        Ok(())
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        let expr = self.parse_scalar_or()?;
        let alias = if self.eat_kw("AS") {
            Some(self.parse_ident()?)
        } else {
            None
        };
        Ok(match expr {
            ScalarExpr::Field(field) => SelectItem::Field { field, alias },
            ScalarExpr::Aggregate(kind, field) => SelectItem::Aggregate { kind, field, alias },
            expr => SelectItem::Expr { expr, alias },
        })
    }

    fn parse_scalar_or(&mut self) -> Result<ScalarExpr> {
        let mut left = self.parse_scalar_and()?;
        while self.eat_kw("OR") {
            let right = self.parse_scalar_and()?;
            left = ScalarExpr::Binary {
                op: ScalarOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_scalar_and(&mut self) -> Result<ScalarExpr> {
        let mut left = self.parse_scalar_not()?;
        while self.eat_kw("AND") {
            let right = self.parse_scalar_not()?;
            left = ScalarExpr::Binary {
                op: ScalarOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_scalar_not(&mut self) -> Result<ScalarExpr> {
        if self.eat_kw("NOT") {
            Ok(ScalarExpr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(self.parse_scalar_not()?),
            })
        } else {
            self.parse_scalar_compare()
        }
    }

    fn parse_scalar_compare(&mut self) -> Result<ScalarExpr> {
        let left = self.parse_scalar_add()?;
        if self.eat_kw("IS") {
            let negated = self.eat_kw("NOT");
            self.expect_kw("NULL")?;
            let op = if negated { ScalarOp::Ne } else { ScalarOp::Eq };
            return Ok(ScalarExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(ScalarExpr::Literal(ValueExpr::Literal(Value::Null))),
            });
        }
        let negated = self.eat_kw("NOT");
        if self.eat_kw("IN") {
            self.expect_tok(&Tok::LParen)?;
            let mut values = Vec::new();
            if !self.eat_tok(&Tok::RParen) {
                loop {
                    values.push(self.parse_scalar_add()?);
                    if !self.eat_tok(&Tok::Comma) {
                        break;
                    }
                }
                self.expect_tok(&Tok::RParen)?;
            }
            let mut out = None;
            for value in values {
                let eq = ScalarExpr::Binary {
                    op: ScalarOp::Eq,
                    left: Box::new(left.clone()),
                    right: Box::new(value),
                };
                out = Some(match out {
                    None => eq,
                    Some(prev) => ScalarExpr::Binary {
                        op: ScalarOp::Or,
                        left: Box::new(prev),
                        right: Box::new(eq),
                    },
                });
            }
            let out = out.unwrap_or(ScalarExpr::Literal(ValueExpr::Literal(Value::Bool(false))));
            return Ok(if negated {
                ScalarExpr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(out),
                }
            } else {
                out
            });
        }
        if self.eat_kw("BETWEEN") {
            let low = self.parse_scalar_add()?;
            self.expect_kw("AND")?;
            let high = self.parse_scalar_add()?;
            let between = ScalarExpr::Binary {
                op: ScalarOp::And,
                left: Box::new(ScalarExpr::Binary {
                    op: ScalarOp::Ge,
                    left: Box::new(left.clone()),
                    right: Box::new(low),
                }),
                right: Box::new(ScalarExpr::Binary {
                    op: ScalarOp::Le,
                    left: Box::new(left),
                    right: Box::new(high),
                }),
            };
            return Ok(if negated {
                ScalarExpr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(between),
                }
            } else {
                between
            });
        }
        if negated {
            return Err(Error::Sql(
                "NOT after an expression requires IN or BETWEEN".into(),
            ));
        }
        let op = match self.peek_tok() {
            Some(Tok::Eq) => Some(ScalarOp::Eq),
            Some(Tok::Ne) => Some(ScalarOp::Ne),
            Some(Tok::Lt) => Some(ScalarOp::Lt),
            Some(Tok::Le) => Some(ScalarOp::Le),
            Some(Tok::Gt) => Some(ScalarOp::Gt),
            Some(Tok::Ge) => Some(ScalarOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.pos += 1;
            let right = self.parse_scalar_add()?;
            return Ok(ScalarExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            });
        }
        Ok(left)
    }

    fn parse_scalar_add(&mut self) -> Result<ScalarExpr> {
        let mut left = self.parse_scalar_mul()?;
        loop {
            let op = match self.peek_tok() {
                Some(Tok::Plus) => ScalarOp::Add,
                Some(Tok::Minus) => ScalarOp::Sub,
                Some(Tok::Concat) => ScalarOp::Concat,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_scalar_mul()?;
            left = ScalarExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_scalar_mul(&mut self) -> Result<ScalarExpr> {
        let mut left = self.parse_scalar_unary()?;
        loop {
            let op = match self.peek_tok() {
                Some(Tok::Star) => ScalarOp::Mul,
                Some(Tok::Slash) => ScalarOp::Div,
                Some(Tok::Percent) => {
                    return Err(Error::Sql("SQL modulo is not supported yet".into()))
                }
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_scalar_unary()?;
            left = ScalarExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_scalar_unary(&mut self) -> Result<ScalarExpr> {
        if self.eat_tok(&Tok::Minus) {
            Ok(ScalarExpr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(self.parse_scalar_unary()?),
            })
        } else {
            if self.eat_tok(&Tok::Plus) {
                return self.parse_scalar_unary();
            }
            self.parse_scalar_primary()
        }
    }

    fn parse_scalar_primary(&mut self) -> Result<ScalarExpr> {
        if self.peek_tok().is_some_and(|t| matches!(t, Tok::LParen)) {
            self.pos += 1;
            let expr = self.parse_scalar_or()?;
            self.expect_tok(&Tok::RParen)?;
            return Ok(expr);
        }
        if let Some(Tok::QuotedIdent(_)) = self.peek_tok() {
            return Ok(ScalarExpr::Field(self.parse_field()?));
        }
        if let Some(Tok::Ident(name)) = self.peek_tok().cloned() {
            let upper = name.to_ascii_uppercase();
            if upper == "CASE" {
                return self.parse_case();
            }
            if matches!(upper.as_str(), "TRUE" | "FALSE" | "NULL") {
                return Ok(ScalarExpr::Literal(self.parse_value()?));
            }
            if matches!(self.peek_n(1), Some(Tok::LParen)) {
                self.pos += 2;
                if let Some(mut kind) = aggregate_kind(&upper) {
                    let field = if self.eat_tok(&Tok::Star) {
                        if kind != AggregateKind::Count {
                            return Err(Error::Sql(format!("{upper}(*) is not valid")));
                        }
                        None
                    } else {
                        let distinct = self.eat_kw("DISTINCT");
                        if distinct {
                            if kind != AggregateKind::Count {
                                return Err(Error::Sql(
                                    "only COUNT(DISTINCT field) is supported".into(),
                                ));
                            }
                            kind = AggregateKind::CountDistinct;
                        }
                        Some(self.parse_field()?)
                    };
                    self.expect_tok(&Tok::RParen)?;
                    return Ok(ScalarExpr::Aggregate(kind, field));
                }
                let func = scalar_func(&upper)
                    .ok_or_else(|| Error::Sql(format!("unsupported SQL function `{name}`")))?;
                let mut args = Vec::new();
                if !self.eat_tok(&Tok::RParen) {
                    loop {
                        args.push(self.parse_scalar_add()?);
                        if !self.eat_tok(&Tok::Comma) {
                            break;
                        }
                    }
                    self.expect_tok(&Tok::RParen)?;
                }
                return Ok(ScalarExpr::Call { func, args });
            }
            return Ok(ScalarExpr::Field(self.parse_field()?));
        }
        Ok(ScalarExpr::Literal(self.parse_value()?))
    }

    fn parse_case(&mut self) -> Result<ScalarExpr> {
        self.expect_kw("CASE")?;
        let mut when_then = Vec::new();
        while self.eat_kw("WHEN") {
            let when = self.parse_scalar_or()?;
            self.expect_kw("THEN")?;
            let then = self.parse_scalar_add()?;
            when_then.push((when, then));
        }
        if when_then.is_empty() {
            return Err(Error::Sql("CASE requires at least one WHEN".into()));
        }
        let else_expr = if self.eat_kw("ELSE") {
            Some(Box::new(self.parse_scalar_add()?))
        } else {
            None
        };
        self.expect_kw("END")?;
        Ok(ScalarExpr::Case {
            when_then,
            else_expr,
        })
    }

    fn parse_or(&mut self) -> Result<WhereExpr> {
        let mut left = self.parse_and()?;
        while self.eat_kw("OR") {
            let right = self.parse_and()?;
            left = WhereExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<WhereExpr> {
        let mut left = self.parse_not()?;
        while self.eat_kw("AND") {
            let right = self.parse_not()?;
            left = WhereExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<WhereExpr> {
        if self.eat_kw("NOT") {
            Ok(WhereExpr::Not(Box::new(self.parse_not()?)))
        } else {
            self.parse_predicate()
        }
    }

    fn parse_predicate(&mut self) -> Result<WhereExpr> {
        if self.eat_tok(&Tok::LParen) {
            let expr = self.parse_or()?;
            self.expect_tok(&Tok::RParen)?;
            return Ok(expr);
        }
        let field = self.parse_field()?;
        if self.eat_kw("IS") {
            let negated = self.eat_kw("NOT");
            self.expect_kw("NULL")?;
            return Ok(WhereExpr::IsNull { field, negated });
        }
        let negated = self.eat_kw("NOT");
        if self.eat_kw("IN") {
            self.expect_tok(&Tok::LParen)?;
            let mut values = Vec::new();
            if !self.eat_tok(&Tok::RParen) {
                loop {
                    values.push(self.parse_value()?);
                    if !self.eat_tok(&Tok::Comma) {
                        break;
                    }
                }
                self.expect_tok(&Tok::RParen)?;
            }
            return Ok(WhereExpr::In {
                field,
                values,
                negated,
            });
        }
        if self.eat_kw("BETWEEN") {
            let low = self.parse_value()?;
            self.expect_kw("AND")?;
            let high = self.parse_value()?;
            return Ok(WhereExpr::Between {
                field,
                low,
                high,
                negated,
            });
        }
        if self.eat_kw("LIKE") {
            let pattern = self.parse_value()?;
            return Ok(WhereExpr::Like {
                field,
                pattern,
                negated,
            });
        }
        if negated {
            return Err(Error::Sql(
                "NOT after a column requires IN, BETWEEN, or LIKE".into(),
            ));
        }
        let op = match self.next_tok() {
            Some(Tok::Eq) => CompareOp::Eq,
            Some(Tok::Ne) => CompareOp::Ne,
            Some(Tok::Lt) => CompareOp::Lt,
            Some(Tok::Le) => CompareOp::Le,
            Some(Tok::Gt) => CompareOp::Gt,
            Some(Tok::Ge) => CompareOp::Ge,
            _ => {
                return Err(Error::Sql(format!(
                    "expected a comparison operator after column `{field}`"
                )))
            }
        };
        let value = self.parse_value()?;
        Ok(WhereExpr::Compare { field, op, value })
    }

    fn parse_value(&mut self) -> Result<ValueExpr> {
        let mut negative = false;
        if self.eat_tok(&Tok::Minus) {
            negative = true;
        }
        let tok = self
            .next_tok()
            .ok_or_else(|| Error::Sql("expected a SQL value".into()))?;
        let value = match tok {
            Tok::Str(s) => Value::String(s),
            Tok::Num(s) => {
                let text = if negative { format!("-{s}") } else { s };
                serde_json::from_str(&text)
                    .map_err(|_| Error::Sql(format!("invalid numeric literal `{text}`")))?
            }
            Tok::Param(i) => {
                if negative {
                    return Err(Error::Sql(
                        "unary minus is not supported for SQL parameters".into(),
                    ));
                }
                return Ok(ValueExpr::Param(i));
            }
            Tok::Ident(s) => match s.to_ascii_uppercase().as_str() {
                "TRUE" => Value::Bool(true),
                "FALSE" => Value::Bool(false),
                "NULL" => Value::Null,
                _ => {
                    return Err(Error::Sql(format!(
                        "unsupported value `{s}`; use a quoted string, number, TRUE, FALSE, NULL, or parameter"
                    )))
                }
            },
            _ => return Err(Error::Sql("expected a SQL value".into())),
        };
        if negative {
            return Err(Error::Sql(
                "unary minus is only supported on numeric literals".into(),
            ));
        }
        Ok(ValueExpr::Literal(value))
    }

    fn parse_field(&mut self) -> Result<String> {
        let mut field = self.parse_ident()?;
        while self.eat_tok(&Tok::Dot) {
            field.push('.');
            field.push_str(&self.parse_ident()?);
        }
        Ok(field)
    }

    fn parse_ident(&mut self) -> Result<String> {
        match self.next_tok() {
            Some(Tok::Ident(s) | Tok::QuotedIdent(s)) => Ok(s),
            _ => Err(Error::Sql("expected an identifier".into())),
        }
    }

    fn parse_usize(&mut self, what: &str) -> Result<usize> {
        match self.next_tok() {
            Some(Tok::Num(s)) => s
                .parse()
                .map_err(|_| Error::Sql(format!("{what} requires a non-negative integer"))),
            _ => Err(Error::Sql(format!(
                "{what} requires a non-negative integer"
            ))),
        }
    }

    fn peek_tok(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|t| &t.tok)
    }

    fn peek_n(&self, n: usize) -> Option<&Tok> {
        self.tokens.get(self.pos + n).map(|t| &t.tok)
    }

    fn next_tok(&mut self) -> Option<Tok> {
        let tok = self.tokens.get(self.pos).map(|t| t.tok.clone());
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn eat_tok(&mut self, want: &Tok) -> bool {
        if self.peek_tok() == Some(want) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_tok(&mut self, want: &Tok) -> Result<()> {
        if self.eat_tok(want) {
            Ok(())
        } else {
            Err(Error::Sql(format!("expected `{want:?}`")))
        }
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if let Some(Tok::Ident(s)) = self.peek_tok() {
            if s.eq_ignore_ascii_case(kw) {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(Error::Sql(format!("expected keyword `{kw}`")))
        }
    }
}

fn aggregate_kind(name: &str) -> Option<AggregateKind> {
    match name {
        "COUNT" => Some(AggregateKind::Count),
        "SUM" => Some(AggregateKind::Sum),
        "AVG" => Some(AggregateKind::Avg),
        "MIN" => Some(AggregateKind::Min),
        "MAX" => Some(AggregateKind::Max),
        _ => None,
    }
}

fn scalar_func(name: &str) -> Option<ScalarFunc> {
    match name {
        "LOWER" => Some(ScalarFunc::Lower),
        "UPPER" => Some(ScalarFunc::Upper),
        "LENGTH" => Some(ScalarFunc::Length),
        "SUBSTR" | "SUBSTRING" => Some(ScalarFunc::Substr),
        "TRIM" => Some(ScalarFunc::Trim),
        "LTRIM" => Some(ScalarFunc::Ltrim),
        "RTRIM" => Some(ScalarFunc::Rtrim),
        "COALESCE" => Some(ScalarFunc::Coalesce),
        "NULLIF" => Some(ScalarFunc::NullIf),
        "ABS" => Some(ScalarFunc::Abs),
        "CEIL" | "CEILING" => Some(ScalarFunc::Ceil),
        "FLOOR" => Some(ScalarFunc::Floor),
        "ROUND" => Some(ScalarFunc::Round),
        _ => None,
    }
}

fn is_reserved_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "WHERE"
            | "SET"
            | "VALUES"
            | "INTO"
            | "GROUP"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "HAVING"
            | "UNION"
            | "JOIN"
            | "LEFT"
            | "RIGHT"
            | "INNER"
            | "OUTER"
            | "ON"
            | "AS"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
    )
}

fn parse(query: &str) -> Result<ParsedStatement> {
    let tokens = Lexer::new(query).lex()?;
    if tokens.is_empty() {
        return Err(Error::Sql("empty SQL statement".into()));
    }
    Parser::new(tokens).parse()
}

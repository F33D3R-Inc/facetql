//! `sum`, `avg`, `min` and `max` over the rows a filter selects, driven
//! over HTTP against the real binary.
//!
//! The engine could count rows and nothing else. Everything a page
//! actually shows beyond a count — an order's total, a seller's revenue,
//! the highest score — had to be computed by shipping the rows to the
//! caller and adding them up there, which is the N+1 `/nodes/count`
//! exists to close, one level up: the reply is a table when the answer is
//! a number, and it is the *wrong* number as soon as the rows stop
//! fitting in one page.
//!
//! These run over HTTP rather than against `StorageEngine` directly
//! because most of what is new is above the engine: the route, its place
//! in the authorization matrix and the rate-limit classes, and the
//! refusal of a function/field pair that has no answer. An engine-level
//! test would pass on all of that without touching it.

mod common;

use common::{free_port, scratch, Server};

/// A `data` JSON literal, escaped for [`common::node_body`]'s string
/// field.
fn body(address: &str, kind: &str, data: serde_json::Value) -> String {
    common::node_body(address, kind, &data.to_string().replace('"', "\\\""))
}

/// The fixture: six orders across two sellers, one of which carries no
/// `total` at all, plus one row whose `total` is text rather than a
/// number. The awkward rows are the point — a fixture of six clean
/// integers cannot tell a correct fold from a lucky one.
fn seeded(name: &str) -> Server {
    let dir = scratch(name);
    let server = Server::start(&dir, free_port());

    let rows = [
        // address        seller   total          rating
        ("Order:1", "ana", serde_json::json!(10), serde_json::json!(4.5)),
        ("Order:2", "ana", serde_json::json!(32), serde_json::json!(3.5)),
        ("Order:3", "ana", serde_json::json!(100), serde_json::json!(5.0)),
        ("Order:4", "bo", serde_json::json!(7), serde_json::json!(1.0)),
        ("Order:5", "bo", serde_json::json!(3), serde_json::json!(2.0)),
    ];

    for (address, seller, total, rating) in rows {
        let r = server
            .post(
                "/node",
                &body(
                    address,
                    "Order",
                    serde_json::json!({
                        "seller": seller,
                        "total": total,
                        "rating": rating,
                    }),
                ),
            )
            .expect("seed");

        assert_eq!(r.status, 201, "seed {address}: {}", r.body);
    }

    // A sixth order for `ana` with no `total` field. It matches every
    // predicate the others do, so it is what proves a missing value is
    // skipped rather than summed as zero — and, for `avg`, that it is not
    // in the divisor either.
    let r = server
        .post(
            "/node",
            &body(
                "Order:6",
                "Order",
                serde_json::json!({ "seller": "ana", "rating": 4.0 }),
            ),
        )
        .expect("seed");
    assert_eq!(r.status, 201, "seed Order:6: {}", r.body);

    // One row in a different kind whose `total` is text. Nothing here
    // aggregates `Broken` except the test that asserts the refusal.
    let r = server
        .post(
            "/node",
            &body(
                "Broken:1",
                "Broken",
                serde_json::json!({ "seller": "ana", "total": "eleven" }),
            ),
        )
        .expect("seed");
    assert_eq!(r.status, 201, "seed Broken:1: {}", r.body);

    server
}

/// `item.<field> == <value>`
fn eq(field: &str, value: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "kind": "bin",
        "op": "==",
        "l": { "kind": "get", "field": field, "obj": { "kind": "ref", "name": "item" } },
        "r": { "kind": "lit", "val": value },
    })
}

fn aggregate(server: &Server, request: serde_json::Value) -> (u16, serde_json::Value) {
    let r = server
        .post("/nodes/aggregate", &request.to_string())
        .expect("POST /nodes/aggregate");

    let parsed = serde_json::from_str(&r.body).unwrap_or(serde_json::Value::Null);

    (r.status, parsed)
}

fn aggregate_by(
    server: &Server,
    request: serde_json::Value,
) -> (u16, serde_json::Value) {
    let r = server
        .post("/nodes/aggregate_by", &request.to_string())
        .expect("POST /nodes/aggregate_by");

    let parsed = serde_json::from_str(&r.body).unwrap_or(serde_json::Value::Null);

    (r.status, parsed)
}

/// The result of a successful aggregate, or a panic naming the refusal.
fn result(server: &Server, request: serde_json::Value) -> serde_json::Value {
    let (status, parsed) = aggregate(server, request);

    assert_eq!(status, 200, "aggregate refused: {parsed}");

    parsed["result"].clone()
}

#[test]
fn a_filtered_sum_is_the_total_of_the_rows_the_filter_selects() {
    let server = seeded("a_filtered_sum_is_the_total_of_the_rows_the_filter_selects");

    // `sum(o in Order where o.seller == "ana")` — the shape an fct app
    // writes, and the one that had no answer before this endpoint.
    let total = result(
        &server,
        serde_json::json!({
            "kind": "Order",
            "where": eq("seller", serde_json::json!("ana")),
            "func": "sum",
            "field": "total",
        }),
    );

    assert_eq!(total, serde_json::json!(142), "10 + 32 + 100");

    // The row with no `total` is in the filter's result set — it is why
    // this number is a sum of three values over four rows.
    let rows = result(
        &server,
        serde_json::json!({
            "kind": "Order",
            "where": eq("seller", serde_json::json!("ana")),
            "func": "count",
        }),
    );

    assert_eq!(rows, serde_json::json!(4));
}

#[test]
fn an_integer_column_does_not_come_back_as_a_float() {
    let server = seeded("an_integer_column_does_not_come_back_as_a_float");

    let total = result(
        &server,
        serde_json::json!({ "kind": "Order", "func": "sum", "field": "total" }),
    );

    assert_eq!(total, serde_json::json!(152));
    assert!(
        total.is_i64(),
        "an int column rendered into a typed template must stay an int, got {total}",
    );
}

#[test]
fn an_average_divides_by_the_rows_that_had_a_value() {
    let server = seeded("an_average_divides_by_the_rows_that_had_a_value");

    // Four `ana` orders, three with a total: 142 / 3, not 142 / 4. A row
    // with no total is not an order worth nothing.
    let avg = result(
        &server,
        serde_json::json!({
            "kind": "Order",
            "where": eq("seller", serde_json::json!("ana")),
            "func": "avg",
            "field": "total",
        }),
    );

    let got = avg.as_f64().expect("a number");

    assert!(
        (got - 142.0 / 3.0).abs() < 1e-9,
        "expected 142/3, got {got}",
    );
}

#[test]
fn min_and_max_read_the_extremes_in_the_fields_own_type() {
    let server = seeded("min_and_max_read_the_extremes_in_the_fields_own_type");

    let ask = |func: &str, field: &str| {
        result(
            &server,
            serde_json::json!({ "kind": "Order", "func": func, "field": field }),
        )
    };

    assert_eq!(ask("min", "total"), serde_json::json!(3));
    assert_eq!(ask("max", "total"), serde_json::json!(100));

    // Ordering is the engine's own total order, so a text column has a
    // min and a max too, and they come back as text.
    assert_eq!(ask("min", "seller"), serde_json::json!("ana"));
    assert_eq!(ask("max", "seller"), serde_json::json!("bo"));

    assert_eq!(ask("min", "rating").as_f64(), Some(1.0));
}

#[test]
fn the_empty_cases_differ_by_function_and_say_so() {
    let server = seeded("the_empty_cases_differ_by_function_and_say_so");

    let nobody = eq("seller", serde_json::json!("nobody at all"));

    let ask = |func: &str| {
        result(
            &server,
            serde_json::json!({
                "kind": "Order",
                "where": nobody,
                "func": func,
                "field": "total",
            }),
        )
    };

    // A sum has an identity, and a typed caller rendering it into an
    // integer column wants the identity rather than a hole.
    assert_eq!(ask("sum"), serde_json::json!(0));

    // An average of nothing is not zero — that would be a number nobody
    // measured.
    assert_eq!(ask("avg"), serde_json::Value::Null);
    assert_eq!(ask("min"), serde_json::Value::Null);
    assert_eq!(ask("max"), serde_json::Value::Null);
}

#[test]
fn a_sum_over_a_field_that_is_not_a_number_is_refused_not_skipped() {
    let server = seeded("a_sum_over_a_field_that_is_not_a_number_is_refused_not_skipped");

    let (status, parsed) = aggregate(
        &server,
        serde_json::json!({ "kind": "Broken", "func": "sum", "field": "total" }),
    );

    assert_eq!(
        status, 400,
        "a total over a text column must not be reported as a total over \
         the rows that happened to be numeric: {parsed}",
    );
}

#[test]
fn a_request_with_no_answer_is_refused_before_a_row_is_read() {
    let server = seeded("a_request_with_no_answer_is_refused_before_a_row_is_read");

    // A sum needs a field.
    let (status, _) = aggregate(
        &server,
        serde_json::json!({ "kind": "Order", "func": "sum" }),
    );
    assert_eq!(status, 400, "sum with no field");

    // A count does not take one: silently ignoring it would answer a
    // different question than the caller asked.
    let (status, _) = aggregate(
        &server,
        serde_json::json!({ "kind": "Order", "func": "count", "field": "total" }),
    );
    assert_eq!(status, 400, "count with a field");

    // And a function that does not exist is named back.
    let (status, _) = aggregate(
        &server,
        serde_json::json!({ "kind": "Order", "func": "median", "field": "total" }),
    );
    assert_eq!(status, 400, "unknown function");
}

#[test]
fn a_grouped_sum_answers_every_value_the_page_asked_about() {
    let server = seeded("a_grouped_sum_answers_every_value_the_page_asked_about");

    let (status, parsed) = aggregate_by(
        &server,
        serde_json::json!({
            "kind": "Order",
            "group_by": "seller",
            "values": ["ana", "bo", "cy"],
            "func": "sum",
            "field": "total",
        }),
    );

    assert_eq!(status, 200, "{parsed}");

    let groups = parsed["groups"].as_array().expect("groups");
    let pairs: Vec<(String, serde_json::Value)> = groups
        .iter()
        .map(|g| {
            (
                g["value"].as_str().unwrap_or("<null>").to_string(),
                g["result"].clone(),
            )
        })
        .collect();

    assert_eq!(
        pairs,
        vec![
            ("ana".to_string(), serde_json::json!(142)),
            ("bo".to_string(), serde_json::json!(10)),
            // Asked about, so it is answered: a seller with no orders
            // sums to the identity. An absent key would be
            // indistinguishable from one the engine forgot.
            ("cy".to_string(), serde_json::json!(0)),
        ],
        "one entry per requested value, sorted, none duplicated",
    );
}

#[test]
fn the_groups_sum_to_the_ungrouped_total() {
    let server = seeded("the_groups_sum_to_the_ungrouped_total");

    let (status, parsed) = aggregate_by(
        &server,
        serde_json::json!({
            "kind": "Order",
            "group_by": "seller",
            "func": "sum",
            "field": "total",
        }),
    );

    assert_eq!(status, 200, "{parsed}");

    let grouped: i64 = parsed["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .map(|g| g["result"].as_i64().expect("an integer sum"))
        .sum();

    let plain = result(
        &server,
        serde_json::json!({ "kind": "Order", "func": "sum", "field": "total" }),
    );

    assert_eq!(
        serde_json::json!(grouped),
        plain,
        "grouping must partition the rows, not lose or double them",
    );
}

#[test]
fn a_count_still_answers_through_the_shared_traversal() {
    let server = seeded("a_count_still_answers_through_the_shared_traversal");

    // `count` now folds through the same accumulator as `sum`, over the
    // same access paths. The two endpoints must still agree — a count
    // and a sum that disagree about which rows exist is the failure the
    // shared traversal exists to make impossible.
    let via_count = server
        .post(
            "/nodes/count",
            &serde_json::json!({
                "kind": "Order",
                "where": eq("seller", serde_json::json!("bo")),
            })
            .to_string(),
        )
        .expect("POST /nodes/count");

    assert_eq!(via_count.status, 200, "{}", via_count.body);

    let counted: serde_json::Value =
        serde_json::from_str(&via_count.body).expect("json");

    let via_aggregate = result(
        &server,
        serde_json::json!({
            "kind": "Order",
            "where": eq("seller", serde_json::json!("bo")),
            "func": "count",
        }),
    );

    assert_eq!(counted["count"], serde_json::json!(2));
    assert_eq!(counted["count"], via_aggregate);
}

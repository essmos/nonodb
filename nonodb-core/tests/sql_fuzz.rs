use nonodb_core::sql::PreparedSql;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn sql_lexer_and_parser_never_panic(input in ".{0,512}") {
        let _ = PreparedSql::parse(&input);
    }

    #[test]
    fn sql_parser_handles_random_tokens(input in prop::collection::vec(
        prop::sample::select(vec![
            "SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "INTO", "SET", "VALUES",
            "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "OFFSET", "DISTINCT",
            "COUNT", "SUM", "CASE", "WHEN", "THEN", "ELSE", "END", "NULL", "TRUE",
            "users", "name", "age", "_id", "$1", "?", "'x'", "1", ",", "(", ")", "*",
            "=", "<", ">", "+", "-", "/", "||", ";",
        ]),
        0..80,
    )) {
        let query = input.join(" ");
        let _ = PreparedSql::parse(&query);
    }
}

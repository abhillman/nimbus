# `nimbus`

`nimbus` is a toy implementation of `sqlite3` in rust.

## Running Nimbus

```shell
cargo run --bin repl --manifest-path /Users/aryehh/Development/nimbus/nimbus/examples/repl/Cargo.toml
```

## Example

```sql
nimbus> CREATE TABLE foo (a INT, b TEXT)
CreateTableResult(
    true,
)
nimbus> INSERT INTO foo VALUES (1, 'hello!')
InsertResult
nimbus> INSERT INTO foo VALUES (2, 'byeeee')
InsertResult
nimbus> SELECT * FROM foo
SelectResult(
    [
        [
            Numeric(
                "1",
            ),
            String(
                "'hello!'",
            ),
        ],
        [
            Numeric(
                "2",
            ),
            String(
                "'byeeee'",
            ),
        ],
    ],
)
```

## monorepo Information

`foldhash/`, `hashbrown/`, `indexmap/` and `lemon-rs/` are used by permission of their authors by courtesy of their respect licenses. Some modifications to these packages have been made. 
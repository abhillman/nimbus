use anyhow::bail;
use fallible_iterator::FallibleIterator;
use sqlite3_parser::ast::fmt::{ToTokens, TokenStream};
use sqlite3_parser::ast::{Cmd, Literal, ResultColumn, Select, SelectBody, Stmt};
use sqlite3_parser::dialect::TokenType;

type ExecResult = anyhow::Result<Option<Vec<Vec<Literal>>>>;

fn exec(sql: &str) -> ExecResult {
    let mut parser = sqlite3_parser::lexer::sql::Parser::new(sql.as_ref());
    Ok(Some(parser.try_fold(vec![], |mut rows, cmd| {
        if let Cmd::Stmt(stmt) = cmd {
            if let Stmt::Select(Select {
                with: None,
                order_by: None,
                limit: None,
                body:
                    SelectBody {
                        compounds: None,
                        select:
                            sqlite3_parser::ast::OneSelect::Select {
                                distinctness: None,
                                from: None,
                                where_clause: None,
                                group_by: None,
                                window_clause: None,
                                columns,
                            },
                    },
            }) = &stmt
            {
                let row = columns.iter().try_fold(vec![], |mut row, column| {
                    if let ResultColumn::Expr(sqlite3_parser::ast::Expr::Literal(literal), None) =
                        column
                    {
                        row.push(literal.clone());
                        Ok(row)
                    } else {
                        bail!("Unexpected column {:?}", column)
                    }
                })?;
                rows.push(row);
                Ok(rows)
            } else {
                bail!("Unexpected stmt: {:?}", stmt);
            }
        } else {
            bail!("Unexpected cmd: {:?}", cmd);
        }
    })?))
}

struct TokenFormatter {
    result: String,
}

impl TokenStream for TokenFormatter {
    type Error = anyhow::Error;

    fn append(&mut self, _ty: TokenType, value: Option<&str>) -> Result<(), Self::Error> {
        self.result.push_str(value.unwrap_or(""));
        Ok(())
    }
}

impl TokenFormatter {
    fn format<Value: ToTokens>(value: &Value) -> String {
        let mut token_formatter = TokenFormatter {
            result: String::new(),
        };
        value.to_tokens(&mut token_formatter).unwrap();
        token_formatter.result
    }
}

fn as_str(exec_result: &ExecResult) -> String {
    match exec_result {
        Ok(None) => String::new(),
        Ok(Some(rows)) => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(TokenFormatter::format)
                    .collect::<Vec<String>>()
                    .join("|")
            })
            .collect::<Vec<String>>()
            .join("\n"),
        Err(e) => {
            format!("{e}")
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::as_str;
    use crate::exec;

    #[test]
    fn it_works() {
        assert_eq!(as_str(&exec("select 1, 2, 3")), "1|2|3");
    }
}

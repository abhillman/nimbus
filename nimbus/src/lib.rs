use std::collections::HashMap;
use log::trace;
use anyhow::bail;
use ctor::ctor;
use fallible_iterator::FallibleIterator;
use sqlite3_parser::ast::{Cmd, Expr, FromClause, InsertBody, Literal, Name, OneSelect, QualifiedName, ResultColumn, Select, SelectBody, SelectTable, Stmt};
use std::env;
use std::hash::{Hash, Hasher};
use log::info;
use indexmap::Entries;

#[ctor]
fn init_log() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "nimbus")
    }
    env_logger::init();
}

#[derive(Eq, PartialEq, Debug)]
struct NimbusTable {
    create_stmt: Stmt,
    data: Vec<Vec<Literal>>,
}

impl Hash for NimbusTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.create_stmt.hash(state)
    }
}

impl NimbusTable {
    fn from_create_stmt(create_stmt: Stmt) -> Self {
        if let Stmt::CreateTable { .. } = create_stmt {
            Self {
                create_stmt,
                data: Default::default(),
            }
        } else {
            panic!("developer error.")
        }
    }

    fn tbl_name(&self) -> &QualifiedName {
        if let Stmt::CreateTable { ref tbl_name, .. } = &self.create_stmt {
            tbl_name
        } else {
            panic!("developer error.")
        }
    }

    fn name(&self) -> &String {
        if let Stmt::CreateTable { ref tbl_name, .. } = &self.create_stmt {
            match tbl_name {
                QualifiedName { ref name, .. } => {
                    match name {
                        Name(n) => {
                            n
                        }
                    }
                }
            }
        } else {
            panic!("developer error.")
        }
    }
}


#[derive(Default, Debug)]
struct NimbusData {
    tables: indexmap::IndexSet<NimbusTable>,
}

impl NimbusData {
    fn get_table(&mut self, tbl_name: &QualifiedName) -> Option<&mut NimbusTable> {
        if let Some(mut bucket) = self.tables.as_entries_mut().iter_mut().find(|bucket| {
            *bucket.key.tbl_name() == *tbl_name
        }) {
            Some(&mut bucket.key)
        } else {
            None
        }
    }

    fn execute(&mut self, stmt: Stmt) -> anyhow::Result<NimbusExecuteResult> {
        match stmt {
            Stmt::CreateTable { .. } => {
                Ok(NimbusExecuteResult::CreateTableResult(self.tables.insert(NimbusTable::from_create_stmt(stmt))))
            }
            Stmt::Select(select) => {
                match &select {
                    Select { with, body, order_by, limit } => {
                        if with.is_some() | order_by.is_some() | limit.is_some() {
                            bail!("select-(with|limit|order_by) not supported");
                        }

                        match body {
                            SelectBody { select, compounds } => {
                                if compounds.is_some() {
                                    bail!("select-compounds not supported");
                                }
                                match select {
                                    OneSelect::Select { distinctness, columns, from, where_clause, group_by, window_clause } => {
                                        if distinctness.is_some() | where_clause.is_some() | group_by.is_some() | window_clause.is_some() {
                                            bail!("one-select-(distinctness|where|group_by|window_clause) not supported");
                                        }

                                        let tbl_name = match from {
                                            None => {
                                                bail!("missing table name");
                                            }
                                            Some(from_clause) => {
                                                match from_clause { FromClause { select, joins, .. } => {
                                                    match select {
                                                        None => {
                                                            bail!("missing select-table");
                                                        }
                                                        Some(select_table) => {
                                                            match select_table.as_ref() {
                                                                SelectTable::Table(name, as_, indexed_) => {
                                                                    if as_.is_some() | indexed_.is_some() {
                                                                        bail!("not supported")
                                                                    }
                                                                    name
                                                                }
                                                                SelectTable::TableCall(_, _, _) | SelectTable::Select(_, _) | SelectTable::Sub(_, _) => {
                                                                    bail!("not supported");
                                                                }
                                                            }
                                                        }
                                                    }
                                                } }
                                            }
                                        };

                                        if columns.len() > 1 {
                                            bail!("only select * supported")
                                        } else {
                                            match columns.get(0).unwrap() {
                                                ResultColumn::Star => {
                                                    // no-op
                                                }
                                                ResultColumn::TableStar(_) | ResultColumn::Expr(_, _) => {
                                                    bail!("not supported")
                                                }
                                            }
                                        }

                                        match self.get_table(tbl_name) {
                                            None => {
                                                bail!("table with name {} not found", tbl_name)
                                            }
                                            Some(nimbus_table) => {
                                                Ok(NimbusExecuteResult::SelectResult(nimbus_table.data.clone()))
                                            }
                                        }
                                    }
                                    OneSelect::Values(_values) => {
                                        bail!("select-one-select-values not supported");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Stmt::Update { .. } => {
                todo!()
            }
            Stmt::Insert { with, or_conflict, tbl_name, columns, body, returning } => {
                if with.is_some() | or_conflict.is_some() | columns.is_some() | returning.is_some() {
                    bail!("insert-(with|or_conflict|columns|returning) not supported");
                }
                if let Some(nimbus_table) = self.get_table(&tbl_name) {
                    if columns.is_some() {
                        bail!("stmt-insert-columns not supported");
                    }
                    match body {
                        InsertBody::Select(select, upsert) => {
                            if upsert.is_some() {
                                bail!("insert-body-select-upsert not supported");
                            }
                            match &select {
                                Select { with, body, order_by, limit } => {
                                    if with.is_some() | order_by.is_some() | limit.is_some() {
                                        bail!("insert-body-select-(with|limit|order_by) not supported");
                                    }
                                    match body {
                                        SelectBody { select, compounds } => {
                                            if compounds.is_some() {
                                                bail!("insert-body-select-compounds not supported");
                                            }
                                            match select.clone() {
                                                OneSelect::Select { .. } => {
                                                    bail!("insert-body-select-one-select not supported");
                                                }
                                                OneSelect::Values(values) => {
                                                    for row in values {
                                                        let mut insert_row = vec![];
                                                        for expr in row {
                                                            match expr {
                                                                Expr::Literal(literal) => {
                                                                    insert_row.push(literal.clone());
                                                                }
                                                                _ => bail!("only literal expressions supported")
                                                            }
                                                        }
                                                        nimbus_table.data.push(insert_row);
                                                    }
                                                    Ok(NimbusExecuteResult::InsertResult)
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        InsertBody::DefaultValues => {
                            bail!("insert-body-default-values not supported");
                        }
                    }
                } else {
                    bail!("Parse error: no such table: foo");
                }
            }
            _ => {
                bail!("unsupported statement");
            }
        }
    }
}

#[derive(Debug)]
enum NimbusExecuteResult {
    NoneResult,
    CreateTableResult(bool),
    InsertResult,
    SelectResult(Vec<Vec<Literal>>)
}

struct Nimbus {
    data: NimbusData,
}

impl Nimbus {
    fn new() -> Self {
        Self {
            data: NimbusData::default(),
        }
    }

    fn eval(&mut self, input: &str) -> anyhow::Result<NimbusExecuteResult> {
        let input: Vec<u8> = input.into();
        let mut parser = sqlite3_parser::lexer::sql::Parser::new(input.as_ref());

        match parser.next()? {
            None => {
                Ok(NimbusExecuteResult::NoneResult)
            }
            Some(cmd) => {
                let result = match cmd {
                    Cmd::Explain(_) => {
                        bail!("cmd-explain not supported");
                    }
                    Cmd::ExplainQueryPlan(s) => {
                        bail!("cmd-explain-query-plan not supported")
                    }
                    Cmd::Stmt(ref stmt) => {
                        Ok(self.data.execute(stmt.clone())?)
                    }
                };

                if result.is_ok() {
                    info!("{}", cmd)
                }
                result
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use fallible_iterator::FallibleIterator;
    use insta::assert_debug_snapshot;
    use crate::Nimbus;

    #[test]
    fn t0() {
        let mut nimbus = Nimbus::new();
        nimbus.eval("create table tbl1(one text, two int)").unwrap();
        nimbus.eval("insert into tbl1 values ('abc', 2)").unwrap();
        let select = nimbus.eval("select * from tbl1").unwrap();
        assert_debug_snapshot!(select);
        nimbus.eval("insert into tbl1 values ('def', 3)").unwrap();
        let select = nimbus.eval("select * from tbl1").unwrap();
        assert_debug_snapshot!(select);
        assert_debug_snapshot!(nimbus.data.tables.iter().map(|nt| {
            (nt.name(), nt.data.clone())
        }).collect::<Vec<_>>());
    }
}
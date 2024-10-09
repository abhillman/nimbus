use crate::NodeKind::{Catch, DoTest, ExecSql, ResultLiteral, SqlLiteral};
use crate::State::Parsing;
use crate::ValueKind::String_;
use anyhow::{anyhow, bail};
use log::trace;
use std::cmp::PartialEq;
use std::collections::VecDeque;

#[derive(PartialEq, Debug, Clone)]
enum NodeKind {
    ExecSql,
    DoTest,
    SqlLiteral,
    ResultLiteral,
    Catch,
}

#[derive(Debug, PartialEq, Clone)]
struct Node {
    kind: NodeKind,
    value: Option<ValueKind>,
    children: Option<Vec<Node>>,
}

#[derive(Debug, PartialEq, Clone)]
enum ValueKind {
    String_(String),
    Bool_(bool),
}

impl Node {
    fn mk_node(kind: NodeKind, value: ValueKind) -> Self {
        Self {
            kind,
            value: Some(value),
            children: None,
        }
    }
}

impl Node {
    fn add_child(&mut self, node: Node) {
        if self.children.is_none() {
            self.children = Some(vec![node]);
        } else {
            self.children.as_mut().unwrap().push(node);
        }
    }
}

#[derive(PartialEq, Debug)]
enum State {
    Toplevel,
    Parsing(Node),
}

impl State {
    fn is_toplevel(&self) -> bool {
        matches!(self, State::Toplevel)
    }

    fn mk_state(kind: NodeKind) -> Self {
        Parsing(Node {
            kind,
            value: None,
            children: None,
        })
    }

    fn set_value(&mut self, value: ValueKind) -> anyhow::Result<()> {
        match self {
            State::Toplevel => {
                bail!("cannot set value on top-level")
            }
            Parsing(node) => {
                node.value = Some(value);
                Ok(())
            }
        }
    }

    fn add_child(&mut self, node: Node) -> anyhow::Result<()> {
        match self {
            State::Toplevel => {
                bail!("cannot add child to top-level")
            }
            Parsing(parent) => {
                parent.add_child(node);
                Ok(())
            }
        }
    }

    fn get_node(&mut self) -> anyhow::Result<Node> {
        if let Parsing(node) = self {
            Ok(node.to_owned())
        } else {
            bail!("cannot get node from top-level")
        }
    }
}

fn parse(src: &str) -> anyhow::Result<Vec<Node>> {
    let mut state = State::Toplevel;

    struct Lines {
        lines: VecDeque<String>,
    }

    impl Lines {
        fn new(src: &str) -> Self {
            let lines = src.split("\n");
            let lines: Vec<_> = lines.map(str::to_string).collect();
            let lines = VecDeque::from(lines);
            Self { lines }
        }

        fn next(&mut self) -> Option<String> {
            let line = self.lines.pop_front();
            Some(line?.trim().to_string())
        }
    }

    let mut nodes = vec![];
    let mut lines = Lines::new(src);
    while let Some(line) = lines.next() {
        let line = line.trim();
        if line.starts_with("#")
            || line.is_empty()
            || (state.is_toplevel() && (line.starts_with("set") || line.starts_with("source")))
        {
            trace!("skipping {line}");
            continue;
        }

        if line.starts_with("do_test") {
            state = State::mk_state(DoTest);
            let line = line.replace("do_test ", "");
            if let Some(idx) = line.find("{") {
                let ident = &line[0..idx - 1].to_string();
                state.set_value(String_(ident.to_string()))?;

                if let Some(line) = lines.next() {
                    const START_CATCH: &str = "set v [catch {execsql {";
                    const END_CATCH: &str = "}} msg]";

                    const START_SQL: &str = "execsql {";
                    const END_SQL: &str = "}";

                    if let Some(idx_start) = line.find(START_CATCH) {
                        if let Some(idx_end) = line.find(END_CATCH) {
                            state.add_child(Node::mk_node(Catch, ValueKind::Bool_(true)))?;
                            let sql = line[idx_start + START_CATCH.len()..idx_end].to_string();
                            state.add_child(Node::mk_node(SqlLiteral, String_(sql)))?;
                            if !line[idx_end + END_CATCH.len()..line.len()].is_empty() {
                                bail!(
                                    "unexpected trailing chars: {}",
                                    line[idx_end..line.len()].to_string()
                                )
                            }
                        } else {
                            bail!("expected {END_CATCH}")
                        }
                    } else if let Some(idx_start) = line.find(START_SQL) {
                        if let Some(idx_end) = line.rfind(END_SQL) {
                            let sql = line[idx_start + START_SQL.len()..idx_end].to_string();
                            state.add_child(Node::mk_node(SqlLiteral, String_(sql)))?;
                            if !line[idx_end + END_SQL.len()..line.len()].is_empty() {
                                bail!(
                                    "unexpected trailing chars: {}",
                                    line[idx_end..line.len()].to_string()
                                )
                            }
                        } else {
                            let next_line = lines.next();
                            if next_line.is_none() {
                                bail!("expected line")
                            }
                            let line = [line, next_line.unwrap()].join(" ");
                            if let Some(idx_end) = line.rfind(END_SQL) {
                                let sql = line[idx_start + START_SQL.len()..idx_end].to_string();
                                state.add_child(Node::mk_node(SqlLiteral, String_(sql)))?;
                                if !line[idx_end + END_SQL.len()..line.len()].is_empty() {
                                    bail!(
                                        "unexpected trailing chars: {}",
                                        line[idx_end..line.len()].to_string()
                                    )
                                }
                            } else {
                                bail!("expected {END_SQL}")
                            }
                        }
                    } else {
                        bail!("expected {START_CATCH} or {START_SQL}")
                    }

                    if let Some(mut line) = lines.next() {
                        if line == "lappend v $msg" {
                            line = lines.next().ok_or(anyhow!("expected line"))?;
                        }

                        if line.starts_with("}") {
                            let line = line[1..line.len()].trim().to_string();
                            if line.starts_with("{") && line.ends_with("}") {
                                let result = line[1..line.len() - 1].to_string();
                                state.add_child(Node::mk_node(ResultLiteral, String_(result)))?;
                            } else {
                                bail!("expected result")
                            }
                        } else {
                            bail!("unexpected line {line}")
                        }
                    } else {
                        bail!("expected line");
                    }
                } else {
                    bail!("expected line")
                };
            } else {
                bail!("expected '{{'")
            }
        } else if line.starts_with("execsql {") {
            let line = line.replace("execsql {", "");
            if let Some(idx) = line.rfind("}") {
                let sql = line[0..idx].to_string();
                state = State::mk_state(ExecSql);
                state.set_value(String_(sql))?;
            }
        } else if state.is_toplevel() {
            bail!("could not parse")
        }

        if !state.is_toplevel() {
            trace!("{state:#?}");
            nodes.push(state.get_node()?);
            state = State::Toplevel;
        }
    }

    Ok(nodes)
}

#[derive(Debug)]
pub enum SqliteTestStatement {
    Test {
        name: String,
        catch: bool,
        sql: String,
        expected: String,
    },
    ExecSql {
        sql: String,
    },
}

impl TryFrom<Node> for SqliteTestStatement {
    type Error = anyhow::Error;

    fn try_from(node: Node) -> Result<Self, Self::Error> {
        let Node {
            ref kind,
            ref value,
            ref children,
        } = node;
        match kind {
            ExecSql => {
                if let String_(sql) = value.clone().unwrap() {
                    Ok(SqliteTestStatement::ExecSql { sql })
                } else {
                    bail!("could not parse ExecSql")
                }
            }
            DoTest => {
                let name = if let Some(String_(ref name)) = value {
                    name.clone()
                } else {
                    bail!("could not get name to parse Test")
                };

                let children = children
                    .clone()
                    .ok_or(anyhow!("children missing for parsing Test"))?;

                let catch = children
                    .iter()
                    .find(|node| node.kind == Catch)
                    .and_then(|node| node.value.as_ref())
                    .map_or(Ok(false), |v| match v {
                        ValueKind::Bool_(catch) => Ok(*catch),
                        _ => bail!("incorrect type"),
                    })?;

                let sql: String = match children
                    .iter()
                    .find(|node| node.kind == SqlLiteral)
                    .and_then(|node| node.value.as_ref())
                    .ok_or(anyhow!("missing sql"))?
                {
                    String_(sql) => Ok::<String, anyhow::Error>(sql.to_string()),
                    _ => bail!("invalid type for sql"),
                }?;

                let expected = match children
                    .iter()
                    .find(|node| node.kind == ResultLiteral)
                    .and_then(|node| node.value.as_ref())
                    .ok_or(anyhow!("missing result for {node:#?}"))?
                {
                    String_(expected) => Ok::<String, anyhow::Error>(expected.to_string()),
                    _ => bail!("invalid type for result"),
                }?;

                Ok(SqliteTestStatement::Test {
                    name,
                    catch,
                    sql,
                    expected,
                })
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

pub mod sqlite_test_suite {
    pub mod select1 {
        use crate::{parse, SqliteTestStatement};

        pub fn script() -> Vec<SqliteTestStatement> {
            let select1 = include_str!("../../sqlite/test/select1.test");
            let end_idx = select1
                .find("set long {This is a string that is too big to fit inside a NBFS buffer}")
                .unwrap();
            let ast = parse(&select1[0..end_idx]).unwrap();
            ast.iter()
                .map(|node| node.clone().try_into().unwrap())
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::NodeKind::{DoTest, SqlLiteral};
    use crate::ValueKind::String_;
    use crate::{parse, Node, NodeKind};
    use ctor::ctor;

    #[ctor]
    fn init_logger() {
        const RUST_LOG: &str = "RUST_LOG";
        if std::env::var(RUST_LOG).is_err() {
            std::env::set_var(RUST_LOG, "INFO");
        }
        env_logger::init()
    }

    #[test]
    fn test_select_1_1_4() {
        let expected = Node {
            kind: DoTest,
            value: Some(String_("select1-1.4".to_string())),
            children: Some(vec![
                Node {
                    kind: SqlLiteral,
                    value: Some(String_("SELECT f1 FROM test1".to_string())),
                    children: None,
                },
                Node {
                    kind: NodeKind::ResultLiteral,
                    value: Some(String_("11".to_string())),
                    children: None,
                },
            ]),
        };

        debug_assert_eq!(
            parse(
                r#"do_test select1-1.4 {
  execsql {SELECT f1 FROM test1}
} {11}"#
            )
            .unwrap()
            .first()
            .unwrap(),
            &expected
        )
    }

    #[test]
    fn test_select1() {
        let script = super::sqlite_test_suite::select1::script();
        assert_eq!(script.len(), 24);
    }
}

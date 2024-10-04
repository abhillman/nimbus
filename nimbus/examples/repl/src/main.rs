use nimbus::Nimbus;

use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};

fn main() -> Result<()> {
    let mut nimbus = Nimbus::new();

    let mut rl = DefaultEditor::new()?;
    loop {
        let readline = rl.readline("nimbus> ");
        match readline {
            Ok(line) => {
                match nimbus.eval(&line) {
                    Ok(result) => {
                        println!("{:#?}", result);
                        rl.add_history_entry(line.as_str())?;
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
    Ok(())
}

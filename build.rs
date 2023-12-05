use std::error::Error;
use ethers::prelude::Abigen;

fn main() -> Result<(), Box<dyn Error>> {
    Abigen::new("Erc20Token", "./src/abi/erc20.json")?.generate()?.write_to_file("./src/abi/erc_20_token.rs")?;
    Abigen::new("Pancake", "./src/abi/pancake.json")?.generate()?.write_to_file("./src/abi/pancake.rs")?;
    Ok(())
}
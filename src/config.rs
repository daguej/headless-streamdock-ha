use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub brightness: u8,
    pub timeout: u64,
    #[serde(default)]
    pub buttons: Vec<ImageConfig>,
    /// Segments of the secondary LCD strip, on devices that have one
    #[serde(default)]
    pub lcd: Vec<ImageConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ImageConfig {
    pub id: u8,
    pub icon: String,
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let config_text = std::fs::read_to_string("config.toml")?;
    let config: Config = toml::from_str(&config_text)?;
    Ok(config)
}

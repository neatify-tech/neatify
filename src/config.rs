use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RepositoryConfig {
	pub name: String,
	pub path: String,
	#[serde(default)]
	pub url: Option<String>,
	#[serde(default)]
	pub version: Option<String>,
	#[serde(default)]
	pub last_checked: Option<String>,
	#[serde(default)]
	pub last_synced: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NeatifyConfig {
	#[serde(default)]
	pub repositories: Vec<RepositoryConfig>,
	#[serde(default)]
	pub ignore: Vec<String>,
	#[serde(default = "default_strict")]
	pub strict: bool,
}

impl Default for NeatifyConfig {
	fn default() -> Self {
		Self {
			repositories: Vec::new(),
			ignore: Vec::new(),
			strict: true
		}
	}
}

fn default_strict() -> bool {
	true
}

pub fn read_config(path: &Path) -> Result<NeatifyConfig, String> {
	if !path.exists() {
		return Ok(NeatifyConfig::default());
	}
	let text = std::fs::read_to_string(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
	toml::from_str(&text).map_err(|e| e.to_string())
}

pub fn write_config(path: &Path, config: &NeatifyConfig) -> Result<(), String> {
	let text = toml::to_string_pretty(config).map_err(|e| e.to_string())?;
	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
	}
	std::fs::write(path, text).map_err(|e| format!("failed to write {}: {e}", path.display()))?;
	Ok(())
}

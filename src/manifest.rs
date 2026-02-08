use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
	#[serde(default)]
	pub version: Option<String>,
	pub files: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
	pub path: String,
	pub sha256: String,
	pub size: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum ManifestFormat {
	Toml,
	Json,
}

impl Manifest {
	pub fn sort_by_path(&mut self) {
		self.files.sort_by(|a, b| a.path.cmp(&b.path));
	}
}

pub fn parse_manifest(content: &str, format: ManifestFormat) -> Result<Manifest, String> {
	match format {
		ManifestFormat::Toml => toml::from_str(content).map_err(|e| e.to_string()),
		ManifestFormat::Json => serde_json::from_str(content).map_err(|e| e.to_string()),
	}
}

pub fn parse_manifest_auto(content: &str, path: &Path) -> Result<Manifest, String> {
	let format = if let Some(ext) = path.extension() {
		if ext.eq_ignore_ascii_case("json") {
			ManifestFormat::Json
		}
		else {
			ManifestFormat::Toml
		}
	}
	else {
		ManifestFormat::Toml
	};
	parse_manifest(content, format)
}

pub fn read_manifest_from_path(path: &Path) -> Result<Manifest, String> {
	let content = std::fs::read_to_string(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
	parse_manifest_auto(&content, path)
}

pub fn write_manifest_toml(manifest: &Manifest, path: &Path) -> Result<(), String> {
	let text = toml::to_string_pretty(manifest).map_err(|e| e.to_string())?;
	std::fs::write(path, text).map_err(|e| format!("failed to write {}: {e}", path.display()))?;
	Ok(())
}

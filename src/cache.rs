use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct RegistryCache {
	root: PathBuf,
}

impl RegistryCache {
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self { root: root.into() }
	}
	pub fn repository_root(&self, name: &str) -> PathBuf {
		self.root.join(name)
	}
	pub fn manifest_path(&self, repo_root: &Path) -> PathBuf {
		repo_root.join("manifest.toml")
	}
	pub fn artifact_path(&self, repo_root: &Path, registry_path: &str) -> PathBuf {
		let registry_path = registry_path.trim_start_matches('/');
		repo_root.join(registry_path)
	}
	pub fn ensure_repository_root(&self, name: &str) -> std::io::Result<PathBuf> {
		let root = self.repository_root(name);
		std::fs::create_dir_all(&root)?;
		Ok(root)
	}
	pub fn root(&self) -> &Path {
		&self.root
	}
}

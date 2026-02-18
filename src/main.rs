use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use clap::{Parser, ValueEnum};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::WalkBuilder;
use neatify::{
	parse_manifest,
	register_primitives,
	write_manifest_toml,
	Context,
	Manifest,
	ManifestEntry,
	ManifestFormat,
	ParseIssue,
	RegistryCache,
};
use libloading::Library;
use rhai::{Dynamic, Engine, Module, ModuleResolver, Scope, AST};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use serde_json::json;
use crate::config::{read_config, write_config, NeatifyConfig, RepositoryConfig};
use tree_sitter::{Language as TsLanguage, Parser as TsParser};

mod lsp;
mod config;

struct CachedAst {
	mtime: Option<SystemTime>,
	ast: AST,
}
thread_local! {
	static AST_CACHE: RefCell<HashMap<PathBuf, CachedAst>> = RefCell::new(HashMap::new());
}
fn entry_mtime(path: &Path) -> Option<SystemTime> {
	std::fs::metadata(path).and_then(|meta| meta.modified()).ok()
}

fn compile_ast_cached(engine: &Engine, entry_path: &Path) -> Result<AST, String> {
	let mtime = entry_mtime(entry_path);
	let key = entry_path.to_path_buf();
	AST_CACHE.with(
		|cache| {
			let mut cache = cache.borrow_mut();
			if let Some(existing) = cache.get(&key) {
				if existing.mtime == mtime {
					return Ok(existing.ast.clone());
				}
			}
			let ast = engine.compile_file(key.clone()).map_err(|e| format!("compile error: {e}"))?;
			cache.insert(key, CachedAst { mtime, ast: ast.clone() });
			Ok(ast)
		}
	)
}

#[derive(Parser, Debug)]
#[command(name = "neatify")]
#[command(about = "Lightweight, scriptable formatter", long_about = None)]
struct Cli {
	/// Override local cache root (defaults to ~/.neatify/repository)
	#[arg(long = "local", value_name = "PATH")]
	local_root: Option<String>,
	/// Add repository (name + url or local path)
	#[arg(long = "repository-add", value_names = ["NAME", "URL"], num_args = 2)]
	repository_add: Option<Vec<String>>,
	/// Remove repository by name
	#[arg(long = "repository-remove", value_name = "NAME")]
	repository_remove: Option<String>,
	/// Promote repository priority
	#[arg(long = "repository-promote", value_name = "NAME")]
	repository_promote: Option<String>,
	/// Demote repository priority
	#[arg(long = "repository-demote", value_name = "NAME")]
	repository_demote: Option<String>,
	/// List configured repositories
	#[arg(long = "repository-list")]
	repository_list: bool,
	/// Sync repositories from remote sources
	#[arg(long = "sync")]
	sync: bool,
	/// Sync repository tests
	#[arg(long = "sync-tests")]
	sync_tests: bool,
	/// Language spec (e.g., java)
	#[arg(value_name = "LANG|FILE", num_args = 0..)]
	targets: Vec<String>,
	/// Read from stdin and write to stdout
	#[arg(long = "stdin")]
	stdin: bool,
	/// Print formatted output to stdout instead of writing files
	#[arg(long = "stdout")]
	stdout: bool,
	/// Override script variables (key=value)
	#[arg(long = "set", value_name = "KEY=VALUE", action = clap::ArgAction::Append)]
	sets: Vec<String>,
	/// Enable debug logging for scripts
	#[arg(long = "debug")]
	debug: bool,
	/// Enable profiling logs for scripts
	#[arg(long = "profile")]
	profile: bool,
	/// Run linter only (no formatting)
	#[arg(long = "lint")]
	lint: bool,
	/// Show parse error context
	#[arg(long = "inspect")]
	inspect: bool,
	/// Output format (human, json)
	#[arg(long = "format", value_name = "FORMAT", default_value = "human")]
	format: OutputFormat,
	/// Allow formatting even with parse errors
	#[arg(long = "lax")]
	lax: bool,
	/// List available Tree-sitter node kinds and exit
	#[arg(long = "list-nodes")]
	list_nodes: bool,
	/// Dump Tree-sitter parse tree and exit
	#[arg(long = "dump-tree")]
	dump_tree: bool,
	/// Highlight test output (true/false)
	#[arg(long = "highlight", value_name = "BOOL")]
	highlight: Option<bool>,
	/// Run repository tests (all languages by default)
	#[arg(long = "test")]
	test: bool,
	/// Format only a line/column range (ROW[:COL],ROW[:COL])
	#[arg(long = "range", value_name = "ROW[:COL],ROW[:COL]")]
	range: Option<String>,
	/// Generate manifest.toml for local repository
	#[arg(long = "generate-manifest", value_name = "PATH", num_args = 0..=1, default_missing_value = ".")]
	generate_manifest: Option<String>,
	/// Run as an LSP server over stdio
	#[arg(long = "lsp")]
	lsp: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
enum OutputFormat {
	Human,
	Json,
}

const PARSE_ERROR_PREFIX: &str = "parse error: ";

fn main() {
	let cli = Cli::parse();
	let cache_root = cache_root_from_cli(&cli);
	let config_path = config_path_from_cache_root(&cache_root);
	let config_exists = config_path.exists();
	let test_group_env = env_flag_enabled("NEATIFY_TEST_GROUP");
	let range = parse_range_arg(cli.range.as_deref()).unwrap_or_else(|err| exit_with_error(&err));
	set_highlight_override(cli.highlight);
	let mut config = read_config(&config_path).unwrap_or_else(|err| exit_with_error(&err));
	let reporter = Reporter::new(cli.format, cli.inspect);
	let _ = OUTPUT_FORMAT.set(cli.format);
	let strict = if cli.lax {
		false
	}
	else {
		config.strict
	};
	if let Some(path) = cli.generate_manifest.as_ref() {
		if cli.test {
			exit_with_error("--generate-manifest cannot be combined with --test");
		}
		let root = PathBuf::from(path);
		generate_manifest_for_local_repository(&root).unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if let Some(args) = cli.repository_add.as_ref() {
		let name = args[0].clone();
		let url = args[1].clone();
		repository_add(&mut config, &cache_root, &name, &url, config_exists)
			.unwrap_or_else(|err| exit_with_error(&err));
		write_config(&config_path, &config).unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if let Some(name) = cli.repository_remove.as_ref() {
		repository_remove(&mut config, &cache_root, name).unwrap_or_else(|err| exit_with_error(&err));
		write_config(&config_path, &config).unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if let Some(name) = cli.repository_promote.as_ref() {
		repository_promote(&mut config, name).unwrap_or_else(|err| exit_with_error(&err));
		write_config(&config_path, &config).unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if let Some(name) = cli.repository_demote.as_ref() {
		repository_demote(&mut config, name).unwrap_or_else(|err| exit_with_error(&err));
		write_config(&config_path, &config).unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if cli.repository_list {
		repository_list(&config);
		return;
	}
	if cli.sync || cli.sync_tests {
		sync_repositories(&mut config, &cache_root, cli.sync_tests).unwrap_or_else(|err| exit_with_error(&err));
		write_config(&config_path, &config).unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	let bootstrap_sync = config.repositories.is_empty();
	if bootstrap_sync {
		ensure_core_repository_config(&mut config, &cache_root).unwrap_or_else(|err| exit_with_error(&err));
		write_config(&config_path, &config).unwrap_or_else(|err| exit_with_error(&err));
		eprintln!("no repositories detected, syncing core repository...");
		sync_repositories(&mut config, &cache_root, false).unwrap_or_else(|err| exit_with_error(&err));
		write_config(&config_path, &config).unwrap_or_else(|err| exit_with_error(&err));
	}
	let repositories = load_repositories(&config);
	let repo_roots = repository_roots(&repositories);
	let available_specs = collect_language_specs(&repo_roots).unwrap_or_else(|err| exit_with_error(&err));
	if available_specs.is_empty() {
		if config.repositories.is_empty() {
			exit_with_error("no repositories synced yet; run `neatify --sync`");
		}
		exit_with_error("no language specs found; run `neatify --sync`");
	}
	let (language, file_targets) = resolve_language_targets(&cli.targets, &repositories).unwrap_or_else(|err| exit_with_error(&err));
	if cli.lint {
		if cli.test {
			exit_with_error("--lint cannot be combined with --test");
		}
		if cli.stdout {
			exit_with_error("--lint cannot be combined with --stdout");
		}
		run_lint(
			&config,
			&repositories,
			language.clone(),
			&file_targets,
			strict,
			&reporter
		)
			.unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if cli.test {
		run_tests(&cli, &repositories, language.clone(), &file_targets, &reporter)
			.unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	let overrides = parse_overrides(&cli.sets).unwrap_or_else(|err| exit_with_error(&err));
	if cli.list_nodes {
		let Some(language) = language.as_ref() else {
			exit_with_error("--list-nodes requires a language");
		};
		let (language_name, language_roots) = resolve_language_roots(language, &repositories).unwrap_or_else(|err| exit_with_error(&err));
		let language_spec = resolve_language_spec(&language_name, &language_roots).unwrap_or_else(|err| exit_with_error(&err));
		list_language_nodes(&language_spec, &language_roots).unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if cli.dump_tree {
		let (language_name, language_roots) = if let Some(language) = language.as_ref() {
			let (name, roots) = resolve_language_roots(language, &repositories).unwrap_or_else(|err| exit_with_error(&err));
			(Some(name), roots)
		}
		else {
			(None, repo_roots.clone())
		};
		dump_trees(
			&config,
			&language_roots,
			language_name.as_deref(),
			&file_targets,
			range.as_ref(),
			cli.stdin
		)
			.unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if cli.lsp {
		let Some(language) = language.as_ref() else {
			exit_with_error("--lsp requires a language");
		};
		if cli.stdin || !file_targets.is_empty() || cli.stdout || range.is_some() {
			exit_with_error("--lsp cannot be combined with file/stdin/range flags");
		}
		let (language_name, language_roots) = resolve_language_roots(language, &repositories).unwrap_or_else(|err| exit_with_error(&err));
		let language_spec = resolve_language_spec(&language_name, &language_roots).unwrap_or_else(|err| exit_with_error(&err));
		lsp::run_lsp(language_spec, language_roots, overrides, cli.debug, test_group_env, strict)
			.unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if cli.stdin {
		let Some(language) = language.as_ref() else {
			exit_with_error("--stdin requires a language");
		};
		let (language_name, language_roots) = resolve_language_roots(language, &repositories).unwrap_or_else(|err| exit_with_error(&err));
		let language_spec = resolve_language_spec(&language_name, &language_roots).unwrap_or_else(|err| exit_with_error(&err));
		format_stdin(
			&language_spec,
			&language_roots,
			range.as_ref(),
			&overrides,
			cli.debug,
			cli.profile,
			test_group_env,
			strict,
			&reporter
		)
			.unwrap_or_else(|err| exit_with_error(&err));
		return;
	}
	if let Some(language) = language.as_ref() {
		let (language_name, language_roots) = resolve_language_roots(language, &repositories).unwrap_or_else(|err| exit_with_error(&err));
		let language_spec = resolve_language_spec(&language_name, &language_roots).unwrap_or_else(|err| exit_with_error(&err));
		let files = if file_targets.is_empty() {
			collect_files_for_language(&language_spec.extensions, &config.ignore)
				.unwrap_or_else(|err| exit_with_error(&err))
		}
		else {
			collect_files_from_targets(&file_targets, &config.ignore).unwrap_or_else(|err| exit_with_error(&err))
		};
		if files.is_empty() {
			exit_with_error("no files found");
		}
		let mut summary = FormatSummary::new();
		let mut runner = FormatRunner::new(
			&language_spec,
			&language_roots,
			&overrides,
			cli.debug,
			cli.profile,
			test_group_env,
			strict
		)
			.unwrap_or_else(|err| exit_with_error(&err));
		for file in files.iter() {
			match format_file(
				&mut runner,
				&language_spec,
				file,
				cli.stdout,
				range.as_ref(),
				strict,
				&reporter
			) {
				Ok(outcome) => summary.record(outcome),
				Err(_) => summary.record_failure(),
			}
		}
		summary.print_if_needed(&reporter);
		return;
	}
	let files_by_language = collect_files_all(&repo_roots, &config.ignore, &file_targets)
		.unwrap_or_else(|err| exit_with_error(&err));
	if files_by_language.is_empty() {
		exit_with_error("no files found");
	}
	let mut summary = FormatSummary::new();
	for (language, files) in files_by_language.iter() {
		let language_spec = resolve_language_spec(language, &repo_roots).unwrap_or_else(|err| exit_with_error(&err));
		let mut runner = FormatRunner::new(
			&language_spec,
			&repo_roots,
			&overrides,
			cli.debug,
			cli.profile,
			test_group_env,
			strict
		)
			.unwrap_or_else(|err| exit_with_error(&err));
		for file in files.iter() {
			match format_file(
				&mut runner,
				&language_spec,
				file,
				cli.stdout,
				range.as_ref(),
				strict,
				&reporter
			) {
				Ok(outcome) => summary.record(outcome),
				Err(_) => summary.record_failure(),
			}
		}
	}
	summary.print_if_needed(&reporter);
}

fn cache_root_from_cli(cli: &Cli) -> String {
	if let Some(path) = cli.local_root.as_ref() {
		return path.clone();
	}
	if let Ok(path) = std::env::var("NEATIFY_CACHE_DIR") {
		if !path.trim().is_empty() {
			return path;
		}
	}
	default_cache_root()
}

fn default_cache_root() -> String {
	if let Ok(home) = std::env::var("HOME") {
		return format!("{home}/.neatify/repository");
	}
	if let Ok(home) = std::env::var("USERPROFILE") {
		return format!("{home}\\.neatify\\repository");
	}
	"./.neatify/repository".to_string()
}

fn config_path_from_cache_root(cache_root: &str) -> PathBuf {
	let root = Path::new(cache_root);
	if let Some(name) = root.file_name().and_then(|v| v.to_str()) {
		if name == "repository" {
			if let Some(parent) = root.parent() {
				return parent.join("config.toml");
			}
		}
	}
	root.join("config.toml")
}

fn load_repositories(config: &NeatifyConfig) -> Vec<RepositoryRecord> {
	let mut records = Vec::new();
	for entry in config.repositories.iter() {
		let root = PathBuf::from(&entry.path);
		if !root.exists() {
			continue;
		}
		records.push(
			RepositoryRecord {
				name: entry.name.clone(),
				root,
				config: entry.clone()
			}
		);
	}
	records
}

fn repository_list(config: &NeatifyConfig) {
	if config.repositories.is_empty() {
		println!("no repositories configured");
		return;
	}
	for entry in config.repositories.iter() {
		if let Some(url) = entry.url.as_ref() {
			println!("{} {} ({})", entry.name, entry.path, url);
		}
		else {
			println!("{} {}", entry.name, entry.path);
		}
	}
}

fn repository_add(
	config: &mut NeatifyConfig,
	cache_root: &str,
	name: &str,
	url: &str,
	config_exists: bool) -> Result<(), String> {
	let was_empty = config.repositories.is_empty();
	let (root, url_opt) = if url.starts_with("http://") || url.starts_with("https://") {
		let repo_root = Path::new(cache_root).join(name);
		std::fs::create_dir_all(&repo_root).map_err(|e| format!("create {}: {e}", repo_root.display()))?;
		(repo_root, Some(url.to_string()))
	}
	else {
		let path = url.strip_prefix("file:").unwrap_or(url);
		let repo_root = PathBuf::from(path);
		if !repo_root.exists() {
			return Err(format!("repository path not found: {}", repo_root.display()));
		}
		(repo_root, None)
	};
	config.repositories.retain(|entry| entry.name != name);
	config.repositories
		.insert(
			0,
			RepositoryConfig {
				name: name.to_string(),
				path: root.to_string_lossy().to_string(),
				url: url_opt,
				version: None,
				last_checked: None,
				last_synced: None
			}
		);
	if !config_exists && was_empty && name != "core" {
		add_core_repository_config(config, cache_root)?;
	}
	Ok(())
}

fn repository_remove(
	config: &mut NeatifyConfig,
	_cache_root: &str,
	name: &str) -> Result<(), String> {
	let before = config.repositories.len();
	config.repositories.retain(|entry| entry.name != name);
	if config.repositories.len() == before {
		return Err(format!("repository not found: {name}"));
	}
	Ok(())
}

fn repository_promote(config: &mut NeatifyConfig, name: &str) -> Result<(), String> {
	let idx = config.repositories
		.iter()
		.position(|entry| entry.name == name)
		.ok_or_else(|| format!("repository not found: {name}"))?;
	if idx == 0 {
		return Ok(());
	}
	config.repositories.swap(idx, idx - 1);
	Ok(())
}

fn repository_demote(config: &mut NeatifyConfig, name: &str) -> Result<(), String> {
	let idx = config.repositories
		.iter()
		.position(|entry| entry.name == name)
		.ok_or_else(|| format!("repository not found: {name}"))?;
	if idx + 1 >= config.repositories.len() {
		return Ok(());
	}
	config.repositories.swap(idx, idx + 1);
	Ok(())
}

fn now_timestamp() -> String {
	let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
	now.as_secs().to_string()
}

fn sync_repositories(
	config: &mut NeatifyConfig,
	cache_root: &str,
	sync_tests: bool) -> Result<(), String> {
	if config.repositories.is_empty() {
		return Err("no repositories configured".to_string());
	}
	let cache = RegistryCache::new(cache_root);
	for entry in config.repositories.iter_mut() {
		let repo_root = PathBuf::from(&entry.path);
		if !repo_root.exists() {
			continue;
		}
		let Some(url) = entry.url.clone() else {
			continue;
		};
		eprintln!("syncing {}...", entry.name);
		let manifest = fetch_manifest_from_url(&url)?;
		if let Some(version) = manifest.version.as_ref() {
			let current = entry.version.clone();
			if current.as_ref() == Some(version) {
				eprintln!("{} is up to date ({})", entry.name, version);
				entry.last_checked = Some(now_timestamp());
				continue;
			}
			if !prompt_yes_no(&format!("update {} to {}?", entry.name, version))? {
				entry.last_checked = Some(now_timestamp());
				continue;
			}
		}
		sync_repository(&entry.name, &repo_root, &url, &manifest, &cache, sync_tests)?;
		entry.version = manifest.version.clone();
		entry.last_checked = Some(now_timestamp());
		entry.last_synced = Some(now_timestamp());
	}
	Ok(())
}

fn sync_repository(
	name: &str,
	repo_root: &Path,
	url: &str,
	manifest: &Manifest,
	cache: &RegistryCache,
	sync_tests: bool) -> Result<(), String> {
	let mut manifest = manifest.clone();
	manifest.sort_by_path();
	write_manifest_toml(&manifest, &cache.manifest_path(repo_root))?;
	let total_files = manifest.files
		.iter()
		.filter(|entry| sync_tests || !is_test_path(&entry.path))
		.count();
	let mut downloaded = 0usize;
	for entry in manifest.files.iter() {
		if !sync_tests && is_test_path(&entry.path) {
			continue;
		}
		let dest = cache.artifact_path(repo_root, &entry.path);
		if dest.exists() {
			let (hash, size) = hash_file(&dest)?;
			if hash == entry.sha256 && size == entry.size {
				continue;
			}
		}
		downloaded += 1;
		eprintln!("{} {}/{}: {}", name, downloaded, total_files, entry.path);
		if let Some(parent) = dest.parent() {
			std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
		}
		let file_url = join_url(url, &entry.path);
		let data = fetch_url_bytes(&file_url)?;
		std::fs::write(&dest, &data).map_err(|e| format!("write {}: {e}", dest.display()))?;
		let (hash, size) = hash_file(&dest)?;
		if hash != entry.sha256 || size != entry.size {
			return Err(
				format!(
					"hash mismatch for {} (expected {}, {})",
					dest.display(),
					entry.sha256,
					entry.size
				)
			);
		}
	}
	Ok(())
}

fn is_test_path(path: &str) -> bool {
	path.contains("/tests/") || path.contains("/test/")
}

fn fetch_manifest_from_url(url: &str) -> Result<Manifest, String> {
	let base = url.trim_end_matches('/');
	let toml_url = format!("{base}/manifest.toml");
	match fetch_url_string(&toml_url) {
		Ok(text) => parse_manifest(&text, ManifestFormat::Toml),
		Err(toml_err) => {
			let json_url = format!("{base}/manifest.json");
			let text = fetch_url_string(&json_url)
				.map_err(|json_err| format!("failed to fetch manifest: {toml_err}; {json_err}"))?;
			parse_manifest(&text, ManifestFormat::Json)
		}
	}
}

fn prompt_yes_no(message: &str) -> Result<bool, String> {
	eprint!("{} [Y/n]: ", message);
	let mut input = String::new();
	std::io::stdin().read_line(&mut input).map_err(|e| format!("read input: {e}"))?;
	let trimmed = input.trim().to_lowercase();
	if trimmed.is_empty() || trimmed == "y" || trimmed == "yes" {
		return Ok(true);
	}
	if trimmed == "n" || trimmed == "no" {
		return Ok(false);
	}
	Ok(true)
}

fn ensure_core_repository_config(config: &mut NeatifyConfig, cache_root: &str) -> Result<(), String> {
	add_core_repository_config(config, cache_root)
}

fn add_core_repository_config(config: &mut NeatifyConfig, cache_root: &str) -> Result<(), String> {
	if config.repositories
		.iter()
		.any(|repo| repo.name == "core") {
		return Ok(());
	}
	let core_url = "https://neatify-tech.github.io/repository";
	let repo_root = Path::new(cache_root).join("core");
	std::fs::create_dir_all(&repo_root).map_err(|e| format!("create {}: {e}", repo_root.display()))?;
	config.repositories
		.push(
			RepositoryConfig {
				name: "core".to_string(),
				path: repo_root.to_string_lossy().to_string(),
				url: Some(core_url.to_string()),
				version: None,
				last_checked: None,
				last_synced: None
			}
		);
	Ok(())
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct TreesitterSpec {
	pub(crate) language: String,
	pub(crate) binary: String,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct LanguageSpec {
	pub(crate) name: String,
	#[allow(dead_code)]
	pub(crate) version: String,
	pub(crate) formatter: String,
	#[serde(rename = "fragment-marker")]
	pub(crate) fragment_marker: Option<String>,
	pub(crate) extensions: Vec<String>,
	pub(crate) treesitter: TreesitterSpec,
}

fn repository_roots(repositories: &[RepositoryRecord]) -> Vec<PathBuf> {
	repositories.iter()
		.map(|repo| repo.root.clone())
		.collect()
}

fn resolve_language_spec(language: &str, roots: &[PathBuf]) -> Result<LanguageSpec, String> {
	let language_path = if language.contains('/') {
		format!("{language}/language.toml")
	}
	else {
		format!("{language}/language.toml")
	};
	let spec_path = resolve_registry_path(&language_path, roots)
		.ok_or_else(|| format!("language spec not found: {language_path}"))?;
	let content = std::fs::read_to_string(&spec_path)
		.map_err(|e| format!("failed to read {}: {e}", spec_path.display()))?;
	let spec: LanguageSpec = toml::from_str(&content).map_err(|e| format!("invalid language spec {}: {e}", spec_path.display()))?;
	Ok(spec)
}

fn split_language_namespace(language: &str) -> Option<(&str, &str)> {
	language.split_once('/')
}

fn find_repository<'a>(repositories: &'a [RepositoryRecord], name: &str) -> Result<&'a RepositoryRecord, String> {
	repositories.iter()
		.find(|repo| repo.name == name)
		.ok_or_else(|| format!("repository not found: {name}"))
}

fn resolve_language_roots(language: &str, repositories: &[RepositoryRecord]) -> Result<(String, Vec<PathBuf>), String> {
	if let Some((namespace, name)) = split_language_namespace(language) {
		let repo = find_repository(repositories, namespace)?;
		return Ok((name.to_string(), vec![repo.root.clone()]));
	}
	Ok((language.to_string(), repository_roots(repositories)))
}

fn resolve_registry_path(path: &str, roots: &[PathBuf]) -> Option<PathBuf> {
	for root in roots {
		let candidate = root.join(path);
		if candidate.exists() {
			return Some(candidate);
		}
	}
	None
}

enum FormatOutcome {
	Updated,
	Unchanged,
	Skipped,
	Stdout,
}

struct FormatRunner {
	engine: Engine,
	ctx: Context,
	ast: AST,
	entry_path: PathBuf,
	binary_path: PathBuf,
	language_name: String,
	roots: Vec<PathBuf>,
	overrides: Vec<(String, OverrideValue)>,
	debug: bool,
	profile: bool,
	test_group: bool,
	strict: bool,
	_libs: Vec<Library>,
}

impl FormatRunner {
	fn new(
		spec: &LanguageSpec,
		roots: &[PathBuf],
		overrides: &[(String, OverrideValue)],
		debug: bool,
		profile: bool,
		test_group: bool,
		strict: bool) -> Result<Self, String> {
		let entry_path = resolve_entry_path(&spec.formatter, roots)
			.ok_or_else(|| format!("formatter script not found: {}", spec.formatter))?;
		let binary_path = resolve_treesitter_binary(&spec.treesitter.binary, roots)?;
		let ctx = Context::new("");
		ctx.set_test_group(test_group);
		ctx.set_profile(profile);
		ctx.set_debug(debug);
		ctx.set_strict(strict);
		ctx.set_registry_roots(roots.to_vec());
		let mut engine = Engine::new();
		register_primitives(&mut engine, ctx.clone());
		engine.set_max_expr_depths(128, 64);
		engine.set_max_call_levels(128);
		engine.set_module_resolver(RegistryModuleResolver::new(roots.to_vec()));
		let mut libs = Vec::new();
		let lang = load_treesitter_language(&binary_path, &spec.treesitter.language, &mut libs)?;
		ctx.register_language(&spec.treesitter.language, lang);
		let settings = build_settings(overrides, profile);
		ctx.set_settings(settings.clone());
		let settings_for_get = settings.clone();
		engine.register_fn(
			"setting",
			move |key: &str, default: Dynamic| -> Dynamic {
				settings_for_get.get(key)
					.cloned()
					.unwrap_or(default)
			}
		);
		let debug_enabled = debug;
		engine.register_fn(
			"log",
			move |message: &str| {
				if debug_enabled {
					eprintln!("[neatify] {message}");
				}
			}
		);
		let ast = compile_ast_cached(&engine, &entry_path)?;
		Ok(
			Self {
				engine,
				ctx,
				ast,
				entry_path,
				binary_path,
				language_name: spec.treesitter
					.language
					.clone(),
				roots: roots.to_vec(),
				overrides: overrides.to_vec(),
				debug,
				profile,
				test_group,
				strict,
				_libs: libs
			}
		)
	}
	fn format_source(&mut self, source: &str) -> Result<String, String> {
		self.ctx.set_source(source.to_string());
		if self.profile {
			self.ctx.reset_trace_profile();
		}
		if self.strict {
			let issues = self.ctx.parse_issues(&self.language_name, 6)?;
			if !issues.is_empty() {
				return Err(format_parse_issues(&issues));
			}
		}
		let mut scope = Scope::new();
		self.ctx.clear_node_cache();
		self.ctx.precache_tree(&self.language_name)?;
		let eval_result = self.engine.eval_ast_with_scope::<()>(&mut scope, &self.ast);
		self.ctx.clear_node_cache();
		self.ctx.clear_cache();
		if self.profile {
			self.ctx.print_trace_report();
		}
		eval_result.map_err(|e| format!("script error: {e}"))?;
		Ok(self.ctx.source_text())
	}
}

struct FormatSummary {
	updated: usize,
	unchanged: usize,
	skipped: usize,
	failed: usize,
}

impl FormatSummary {
	fn new() -> Self {
		Self {
			updated: 0,
			unchanged: 0,
			skipped: 0,
			failed: 0
		}
	}
	fn record(&mut self, outcome: FormatOutcome) {
		match outcome {
			FormatOutcome::Updated => self.updated += 1,
			FormatOutcome::Unchanged => self.unchanged += 1,
			FormatOutcome::Skipped => self.skipped += 1,
			FormatOutcome::Stdout => {}
		}
	}
	fn record_failure(&mut self) {
		self.failed += 1;
	}
	fn print_if_needed(&self, reporter: &Reporter) {
		reporter.format_summary(self);
	}
}

struct LintSummary {
	checked: usize,
	approved: usize,
	rejected: usize,
}

impl LintSummary {
	fn new() -> Self {
		Self {
			checked: 0,
			approved: 0,
			rejected: 0
		}
	}
	fn record(&mut self, issues: usize) {
		self.checked += 1;
		if issues == 0 {
			self.approved += 1;
		}
		else {
			self.rejected += 1;
		}
	}
	fn add(&mut self, other: &LintSummary) {
		self.checked += other.checked;
		self.approved += other.approved;
		self.rejected += other.rejected;
	}
	fn print_if_needed(&self, reporter: &Reporter) {
		reporter.lint_summary(self);
	}
}

fn format_file(
	runner: &mut FormatRunner,
	spec: &LanguageSpec,
	file: &str,
	stdout: bool,
	range: Option<&RangeSpec>,
	strict: bool,
	reporter: &Reporter) -> Result<FormatOutcome, String> {
	let start = Instant::now();
	let source = std::fs::read_to_string(file).map_err(|e| format!("read {file}: {e}"))?;
	if !stdout {
		reporter.processing_file(file);
	}
	let output = if let Some(range) = range {
		let marker = spec.fragment_marker
			.as_ref()
			.ok_or_else(|| "fragment-marker missing in language spec".to_string())?;
		let offsets = range_offsets(&source, range)?;
		let fragment = match format_fragment(
			&source,
			&offsets,
			marker,
			&runner.entry_path,
			&runner.binary_path,
			&runner.language_name,
			&runner.roots,
			&runner.overrides,
			runner.debug,
			runner.profile,
			runner.test_group,
			strict
		) {
			Ok(fragment) => fragment,
			Err(err) => {
				if let Some(detail) = err.strip_prefix(PARSE_ERROR_PREFIX) {
					let contexts = if reporter.inspect {
						build_parse_issue_contexts(
							file,
							&source,
							&runner.binary_path,
							&runner.language_name
						)
					}
					else {
						Vec::new()
					};
					reporter.format_result(
						file,
						FormatStatus::Skipped,
						Some(start.elapsed()),
						Some(detail),
						if contexts.is_empty() {
							None
						}
						else {
							Some(contexts.as_slice())
						}
					);
					return Ok(FormatOutcome::Skipped);
				}
				reporter.format_result(
					file,
					FormatStatus::Failed,
					Some(start.elapsed()),
					Some(&err),
					None
				);
				return Err(err);
			}
		};
		let mut updated = source.clone();
		updated.replace_range(offsets.start..offsets.end, &fragment);
		updated
	}
	else {
		match runner.format_source(&source) {
			Ok(output) => output,
			Err(err) => {
				if let Some(detail) = err.strip_prefix(PARSE_ERROR_PREFIX) {
					let contexts = if reporter.inspect {
						build_parse_issue_contexts(
							file,
							&source,
							&runner.binary_path,
							&runner.language_name
						)
					}
					else {
						Vec::new()
					};
					reporter.format_result(
						file,
						FormatStatus::Skipped,
						Some(start.elapsed()),
						Some(detail),
						if contexts.is_empty() {
							None
						}
						else {
							Some(contexts.as_slice())
						}
					);
					return Ok(FormatOutcome::Skipped);
				}
				reporter.format_result(
					file,
					FormatStatus::Failed,
					Some(start.elapsed()),
					Some(&err),
					None
				);
				return Err(err);
			}
		}
	};
	if stdout {
		println!("{output}");
		return Ok(FormatOutcome::Stdout);
	}
	if output == source {
		reporter.format_result(
			file,
			FormatStatus::Unchanged,
			Some(start.elapsed()),
			None,
			None
		);
		return Ok(FormatOutcome::Unchanged);
	}
	std::fs::write(file, output).map_err(|e| format!("write {file}: {e}"))?;
	reporter.format_result(
		file,
		FormatStatus::Updated,
		Some(start.elapsed()),
		None,
		None
	);
	Ok(FormatOutcome::Updated)
}

fn format_stdin(
	spec: &LanguageSpec,
	roots: &[PathBuf],
	range: Option<&RangeSpec>,
	overrides: &[(String, OverrideValue)],
	debug: bool,
	profile: bool,
	test_group: bool,
	strict: bool,
	reporter: &Reporter) -> Result<(), String> {
	let entry_path = resolve_entry_path(&spec.formatter, roots)
		.ok_or_else(|| format!("formatter script not found: {}", spec.formatter))?;
	let binary_path = resolve_treesitter_binary(&spec.treesitter.binary, roots)?;
	let mut source = String::new();
	std::io::stdin().read_to_string(&mut source).map_err(|e| format!("read stdin: {e}"))?;
	let output = if let Some(range) = range {
		let marker = spec.fragment_marker
			.as_ref()
			.ok_or_else(|| "fragment-marker missing in language spec".to_string())?;
		let offsets = range_offsets(&source, range)?;
		let fragment = match format_fragment(
			&source,
			&offsets,
			marker,
			&entry_path,
			&binary_path,
			&spec.treesitter.language,
			roots,
			overrides,
			debug,
			profile,
			test_group,
			strict
		) {
			Ok(fragment) => fragment,
			Err(err) => {
				if reporter.inspect && err.starts_with(PARSE_ERROR_PREFIX) {
					let contexts = build_parse_issue_contexts(
						"<stdin>",
						&source,
						&binary_path,
						&spec.treesitter.language
					);
					reporter.print_contexts(&contexts);
				}
				return Err(err);
			}
		};
		let mut updated = source.clone();
		updated.replace_range(offsets.start..offsets.end, &fragment);
		updated
	}
	else {
		match format_source(
			&source,
			&entry_path,
			&binary_path,
			&spec.treesitter.language,
			roots,
			overrides,
			debug,
			profile,
			test_group,
			strict
		) {
			Ok(output) => output,
			Err(err) => {
				if reporter.inspect && err.starts_with(PARSE_ERROR_PREFIX) {
					let contexts = build_parse_issue_contexts(
						"<stdin>",
						&source,
						&binary_path,
						&spec.treesitter.language
					);
					reporter.print_contexts(&contexts);
				}
				return Err(err);
			}
		}
	};
	println!("{output}");
	Ok(())
}

fn build_settings(overrides: &[(String, OverrideValue)], profile: bool) -> HashMap<String, Dynamic> {
	let mut settings = HashMap::new();
	settings.insert("max_width".to_string(), Dynamic::from(100_i64));
	settings.insert("tab_width".to_string(), Dynamic::from(4_i64));
	settings.insert("profile".to_string(), Dynamic::from(profile));
	for (key, value) in overrides {
		match value {
			OverrideValue::Int(v) => {
				settings.insert(key.clone(), Dynamic::from(*v));
			}
			OverrideValue::Bool(v) => {
				settings.insert(key.clone(), Dynamic::from(*v));
			}
			OverrideValue::Str(v) => {
				settings.insert(key.clone(), Dynamic::from(v.clone()));
			}
		}
	}
	settings
}

#[derive(Clone, Debug)]
pub(crate) enum OverrideValue {
	Int(i64),
	Bool(bool),
	Str(String),
}

fn parse_overrides(items: &[String]) -> Result<Vec<(String, OverrideValue)>, String> {
	let mut overrides = Vec::new();
	for item in items {
		let (key, value) = item.split_once('=').ok_or_else(|| format!("invalid --set value: {item}"))?;
		if key.trim().is_empty() {
			return Err(format!("invalid --set key: {item}"));
		}
		let value = parse_override_value(value);
		overrides.push((key.trim().to_string(), value));
	}
	Ok(overrides)
}

fn parse_override_value(value: &str) -> OverrideValue {
	let trimmed = value.trim();
	if trimmed.eq_ignore_ascii_case("true") {
		return OverrideValue::Bool(true);
	}
	if trimmed.eq_ignore_ascii_case("false") {
		return OverrideValue::Bool(false);
	}
	if let Ok(int_val) = trimmed.parse::<i64>() {
		return OverrideValue::Int(int_val);
	}
	OverrideValue::Str(trimmed.to_string())
}

pub(crate) fn resolve_entry_path(entry: &str, roots: &[PathBuf]) -> Option<PathBuf> {
	let entry_path = Path::new(entry);
	if entry_path.is_absolute() || entry.starts_with("./") || entry.starts_with("../") {
		return Some(entry_path.to_path_buf());
	}
	resolve_registry_path(entry, roots)
}

pub(crate) fn resolve_treesitter_binary(path: &str, roots: &[PathBuf]) -> Result<PathBuf, String> {
	let os = std::env::consts::OS;
	let arch = std::env::consts::ARCH;
	let ext = match os {
		"windows" => "dll",
		"macos" => "dylib",
		_ => "so",
	};
	let resolved = path.replace("{os}", os)
		.replace("{arch}", arch)
		.replace("{ext}", ext);
	resolve_registry_path(&resolved, roots)
		.ok_or_else(|| format!("treesitter binary not found: {resolved}"))
}

fn load_treesitter_language(
	path: &Path,
	language_name: &str,
	libs: &mut Vec<Library>) -> Result<tree_sitter::Language, String> {
	let lib = unsafe { Library::new(path) }.map_err(|e| format!("load library {}: {e}", path.display()))?;
	let symbol = format!("tree_sitter_{}", language_name.replace('-', "_"));
	unsafe {
		let func: libloading::Symbol<unsafe extern "C" fn() -> tree_sitter::Language> = lib.get(symbol.as_bytes()).map_err(|e| format!("load symbol {symbol}: {e}"))?;
		let lang = func();
		libs.push(lib);
		Ok(lang)
	}
}

fn list_language_nodes(spec: &LanguageSpec, roots: &[PathBuf]) -> Result<(), String> {
	let binary_path = resolve_treesitter_binary(&spec.treesitter.binary, roots)?;
	let mut libs = Vec::new();
	let lang = load_treesitter_language(&binary_path, &spec.treesitter.language, &mut libs)?;
	let mut kinds = Vec::new();
	let count = lang.node_kind_count();
	for id in 0..count {
		let name = lang.node_kind_for_id(id as u16).unwrap_or("<unknown>");
		kinds.push((id as u16, name.to_string()));
	}
	println!("node kinds ({}):", kinds.len());
	for (id, name) in kinds {
		let id_text = color(&format!("#{id}"), Color::Gray, false);
		println!("- {name} ({id_text})");
	}
	Ok(())
}

struct DumpTreeRunner {
	parser: TsParser,
	language: TsLanguage,
	_libs: Vec<Library>,
}

impl DumpTreeRunner {
	fn new(spec: &LanguageSpec, roots: &[PathBuf]) -> Result<Self, String> {
		let binary_path = resolve_treesitter_binary(&spec.treesitter.binary, roots)?;
		let mut libs = Vec::new();
		let lang = load_treesitter_language(&binary_path, &spec.treesitter.language, &mut libs)?;
		let mut parser = TsParser::new();
		parser.set_language(&lang)
			.map_err(|e| format!("failed to set language '{}': {e}", spec.treesitter.language))?;
		Ok(
			Self {
				parser,
				language: lang,
				_libs: libs
			}
		)
	}
}

#[derive(Default)]
struct DumpTreeCounts {
	nodes: usize,
	errors: usize,
	missing: usize,
}

fn dump_trees(
	config: &NeatifyConfig,
	roots: &[PathBuf],
	language: Option<&str>,
	file_targets: &[String],
	range: Option<&RangeSpec>,
	stdin: bool) -> Result<(), String> {
	if stdin {
		let Some(language) = language else {
			return Err("--stdin requires a language".to_string());
		};
		if !file_targets.is_empty() {
			return Err("--stdin cannot be combined with file targets".to_string());
		}
		let language_spec = resolve_language_spec(language, roots)?;
		let mut runner = DumpTreeRunner::new(&language_spec, roots)?;
		let mut source = String::new();
		std::io::stdin().read_to_string(&mut source).map_err(|e| format!("read stdin: {e}"))?;
		dump_tree_for_source(&mut runner, "stdin", &source, range)?;
		return Ok(());
	}
	if let Some(language) = language {
		let language_spec = resolve_language_spec(language, roots)?;
		let files = if file_targets.is_empty() {
			collect_files_for_language(&language_spec.extensions, &config.ignore)?
		}
		else {
			collect_files_from_targets(file_targets, &config.ignore)?
		};
		if files.is_empty() {
			return Err("no files found".to_string());
		}
		let mut runner = DumpTreeRunner::new(&language_spec, roots)?;
		for (idx, file) in files.iter().enumerate() {
			if idx > 0 {
				println!();
			}
			dump_tree_for_file(&mut runner, file, range)?;
		}
		return Ok(());
	}
	let files_by_language = collect_files_all(roots, &config.ignore, file_targets)?;
	if files_by_language.is_empty() {
		return Err("no files found".to_string());
	}
	let mut first = true;
	for (language, files) in files_by_language.iter() {
		let language_spec = resolve_language_spec(language, roots)?;
		let mut runner = DumpTreeRunner::new(&language_spec, roots)?;
		for file in files.iter() {
			if first {
				first = false;
			}
			else {
				println!();
			}
			dump_tree_for_file(&mut runner, file, range)?;
		}
	}
	Ok(())
}

fn dump_tree_for_file(
	runner: &mut DumpTreeRunner,
	file: &str,
	range: Option<&RangeSpec>) -> Result<(), String> {
	let source = std::fs::read_to_string(file).map_err(|e| format!("read {file}: {e}"))?;
	dump_tree_for_source(runner, file, &source, range)
}

fn dump_tree_for_source(
	runner: &mut DumpTreeRunner,
	label: &str,
	source: &str,
	range: Option<&RangeSpec>) -> Result<(), String> {
	let tree = runner.parser
		.parse(source.as_bytes(), None)
		.ok_or_else(|| "failed to parse".to_string())?;
	let root = tree.root_node();
	let target = if let Some(range) = range {
		let offsets = range_offsets(source, range)?;
		root.descendant_for_byte_range(offsets.start, offsets.end).unwrap_or(root)
	}
	else {
		root
	};
	println!("== {label} ==");
	let mut counts = DumpTreeCounts::default();
	dump_tree_node(target, false, &runner.language, &mut counts);
	let node_text = color(&counts.nodes.to_string(), Color::Green, false);
	let error_text = color(&counts.errors.to_string(), Color::Red, counts.errors > 0);
	let missing_text = color(&counts.missing.to_string(), Color::Yellow, counts.missing > 0);
	println!("Nodes: {node_text}, errors: {error_text}, missing: {missing_text}");
	Ok(())
}

fn dump_tree_node(
	node: tree_sitter::Node,
	include_unnamed: bool,
	lang: &TsLanguage,
	counts: &mut DumpTreeCounts) {
	let mut stack: Vec<(tree_sitter::Node, usize)> = vec![(node, 0)];
	while let Some((node, depth)) = stack.pop() {
		let is_named = include_unnamed || node.is_named();
		if is_named {
			let kind = node.kind();
			let id_kind = lang.node_kind_for_id(node.kind_id()).unwrap_or("<unknown>");
			let has_mismatch = kind != id_kind;
			let color_kind = if node.is_error() {
				Color::Red
			}
			else if node.is_missing() {
				Color::Yellow
			}
			else if has_mismatch {
				Color::Yellow
			}
			else {
				Color::Cyan
			};
			let kind = color(kind, color_kind, false);
			let kind = if has_mismatch {
				let id_kind = color(id_kind, color_kind, false);
				format!("{kind} (id: {id_kind})")
			}
			else {
				kind
			};
			let id_text = color(&format!("#{}", node.kind_id()), Color::Gray, false);
			let start = node.start_position();
			let end = node.end_position();
			let indent = "  ".repeat(depth);
			println!(
				"{indent}{kind} ({id_text}) [{}:{}..{}:{}] bytes {}..{}",
				start.row + 1,
				start.column + 1,
				end.row + 1,
				end.column + 1,
				node.start_byte(),
				node.end_byte()
			);
			counts.nodes += 1;
			if node.is_error() {
				counts.errors += 1;
			}
			if node.is_missing() {
				counts.missing += 1;
			}
		}
		let next_depth = if is_named {
			depth + 1
		}
		else {
			depth
		};
		let mut cursor = node.walk();
		if cursor.goto_first_child() {
			let mut children = Vec::new();
			loop {
				children.push(cursor.node());
				if !cursor.goto_next_sibling() {
					break;
				}
			}
			for child in children.into_iter().rev() {
				stack.push((child, next_depth));
			}
		}
	}
}

pub(crate) fn format_source(
	source: &str,
	entry_path: &Path,
	binary_path: &Path,
	language: &str,
	roots: &[PathBuf],
	overrides: &[(String, OverrideValue)],
	debug: bool,
	profile: bool,
	test_group: bool,
	strict: bool) -> Result<String, String> {
	let ctx = Context::new(source.to_string());
	ctx.set_test_group(test_group);
	ctx.set_profile(profile);
	ctx.set_debug(debug);
	ctx.set_debug(debug);
	ctx.set_strict(strict);
	ctx.set_registry_roots(roots.to_vec());
	let mut engine = Engine::new();
	register_primitives(&mut engine, ctx.clone());
	engine.set_max_expr_depths(128, 64);
	engine.set_max_call_levels(128);
	engine.set_module_resolver(RegistryModuleResolver::new(roots.to_vec()));
	let mut _libs = Vec::new();
	let lang = load_treesitter_language(binary_path, language, &mut _libs)?;
	ctx.register_language(language, lang);
	let settings = build_settings(overrides, profile);
	ctx.set_settings(settings.clone());
	let settings_for_get = settings.clone();
	engine.register_fn(
		"setting",
		move |key: &str, default: Dynamic| -> Dynamic {
			settings_for_get.get(key)
				.cloned()
				.unwrap_or(default)
		}
	);
	let debug_enabled = debug;
	engine.register_fn(
		"log",
		move |message: &str| {
			if debug_enabled {
				eprintln!("[neatify] {message}");
			}
		}
	);
	let ast = compile_ast_cached(&engine, entry_path)?;
	if strict {
		let issues = ctx.parse_issues(language, 6)?;
		if !issues.is_empty() {
			return Err(format_parse_issues(&issues));
		}
	}
	let mut scope = Scope::new();
	ctx.clear_node_cache();
	ctx.precache_tree(language)?;
	let eval_result = engine.eval_ast_with_scope::<()>(&mut scope, &ast);
	ctx.clear_node_cache();
	ctx.clear_cache();
	eval_result.map_err(|e| format!("script error: {e}"))?;
	Ok(ctx.source_text())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RangeSpec {
	pub(crate) start_row: usize,
	pub(crate) start_col: usize,
	pub(crate) end_row: usize,
	pub(crate) end_col: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RangeOffsets {
	start: usize,
	end: usize,
}

fn parse_range_arg(input: Option<&str>) -> Result<Option<RangeSpec>, String> {
	let Some(input) = input else {
		return Ok(None);
	};
	let input = input.trim();
	if input.is_empty() {
		return Ok(None);
	}
	let (start_str, end_str) = input.split_once(',').ok_or_else(|| "invalid --range, expected ROW[:COL],ROW[:COL]".to_string())?;
	let (start_row, start_col) = parse_row_col(start_str, "start")?;
	let (end_row, end_col) = parse_row_col(end_str, "end")?;
	if start_row > end_row || (start_row == end_row && start_col > end_col) {
		return Err("invalid --range: start after end".to_string());
	}
	Ok(
		Some(
			RangeSpec {
				start_row,
				start_col,
				end_row,
				end_col
			}
		)
	)
}

fn parse_row_col(input: &str, label: &str) -> Result<(usize, usize), String> {
	let input = input.trim();
	if input.is_empty() {
		return Err(format!("invalid --range {label}"));
	}
	if let Some((row_str, col_str)) = input.split_once(':') {
		let row = row_str.trim().parse::<usize>().map_err(|_| format!("invalid --range {label} row"))?;
		let col = if col_str.trim().is_empty() {
			0
		}
		else {
			col_str.trim().parse::<usize>().map_err(|_| format!("invalid --range {label} col"))?
		};
		Ok((row, col))
	}
	else {
		let row = input.parse::<usize>().map_err(|_| format!("invalid --range {label} row"))?;
		Ok((row, 0))
	}
}

pub(crate) fn format_fragment(
	source: &str,
	range: &RangeOffsets,
	marker_template: &str,
	entry_path: &Path,
	binary_path: &Path,
	language: &str,
	roots: &[PathBuf],
	overrides: &[(String, OverrideValue)],
	debug: bool,
	profile: bool,
	test_group: bool,
	strict: bool) -> Result<String, String> {
	if range.end > source.len() {
		return Err("range end exceeds source length".to_string());
	}
	if !source.is_char_boundary(range.start) || !source.is_char_boundary(range.end) {
		return Err("range must align to UTF-8 boundaries".to_string());
	}
	if !marker_template.contains("{marker}") {
		return Err("fragment-marker must contain {marker} placeholder".to_string());
	}
	let uuid = uuid::Uuid::new_v4().to_string();
	let start_token = format!("neatify-fragment:{uuid}:start");
	let end_token = format!("neatify-fragment:{uuid}:end");
	let start_marker = marker_template.replace("{marker}", &start_token);
	let end_marker = marker_template.replace("{marker}", &end_token);
	let mut with_markers = source.to_string();
	with_markers.insert_str(range.end, &end_marker);
	with_markers.insert_str(range.start, &start_marker);
	let formatted = format_source(
		&with_markers,
		entry_path,
		binary_path,
		language,
		roots,
		overrides,
		debug,
		profile,
		test_group,
		strict
	)?;
	let start_idx = formatted.find(&start_marker).ok_or_else(|| "start marker not found after formatting".to_string())?;
	let after_start = start_idx + start_marker.len();
	let end_idx = formatted[after_start..].find(&end_marker)
		.map(|i| after_start + i)
		.ok_or_else(|| "end marker not found after formatting".to_string())?;
	Ok(formatted[after_start..end_idx].to_string())
}

pub(crate) fn range_offsets(source: &str, range: &RangeSpec) -> Result<RangeOffsets, String> {
	let starts = line_starts(source);
	let start = offset_for_position(source, &starts, range.start_row, range.start_col)?;
	let end = offset_for_position(source, &starts, range.end_row, range.end_col)?;
	if start > end {
		return Err("invalid --range: start after end".to_string());
	}
	Ok(RangeOffsets { start, end })
}

fn line_starts(source: &str) -> Vec<usize> {
	let mut starts = vec![0usize];
	let bytes = source.as_bytes();
	let mut i = 0usize;
	while i < bytes.len() {
		if bytes[i] == b'\n' {
			if i + 1 <= bytes.len() {
				starts.push(i + 1);
			}
		}
		i += 1;
	}
	starts
}

fn offset_for_position(
	source: &str,
	starts: &[usize],
	row: usize,
	col_utf16: usize) -> Result<usize, String> {
	if row >= starts.len() {
		return Err("range row out of bounds".to_string());
	}
	let start = starts[row];
	let end_exclusive = if row + 1 < starts.len() {
		starts[row + 1].saturating_sub(1)
	}
	else {
		source.len()
	};
	let line = &source[start..end_exclusive];
	let mut utf16_count = 0usize;
	for (byte_idx, ch) in line.char_indices() {
		if utf16_count == col_utf16 {
			return Ok(start + byte_idx);
		}
		let units = ch.len_utf16();
		if utf16_count + units > col_utf16 {
			return Err("range column falls inside a UTF-16 surrogate".to_string());
		}
		utf16_count += units;
	}
	if utf16_count == col_utf16 {
		return Ok(start + line.len());
	}
	Err("range column out of bounds".to_string())
}

fn collect_files_for_language(
	extensions: &[String],
	ignore_patterns: &[String]) -> Result<Vec<String>, String> {
	if extensions.is_empty() {
		return Err("language spec missing extensions".to_string());
	}
	let cwd = std::env::current_dir().map_err(|e| format!("read cwd: {e}"))?;
	let (ignore_set, use_gitignore) = build_ignore_set(&cwd, ignore_patterns)?;
	let mut files = Vec::new();
	let mut builder = WalkBuilder::new(&cwd);
	builder.git_ignore(use_gitignore)
		.git_exclude(use_gitignore)
		.git_global(use_gitignore);
	for result in builder.build() {
		let entry = result.map_err(|e| e.to_string())?;
		if !entry.file_type()
			.map(|t| t.is_file())
			.unwrap_or(false) {
			continue;
		}
		let path = entry.path();
		if is_ignored(path, &cwd, &ignore_set) {
			continue;
		}
		if let Some(path_str) = path.to_str() {
			if extensions.iter().any(|ext| path_str.ends_with(ext)) {
				files.push(path_str.to_string());
			}
		}
	}
	files.sort();
	Ok(files)
}

fn collect_files_from_targets(
	targets: &[String],
	ignore_patterns: &[String]) -> Result<Vec<String>, String> {
	let cwd = std::env::current_dir().map_err(|e| format!("read cwd: {e}"))?;
	let (ignore_set, use_gitignore) = build_ignore_set(&cwd, ignore_patterns)?;
	let (direct_files, target_set, has_globs) = build_target_set(targets, &cwd)?;
	let respect_ignore = targets.is_empty();
	let mut files = Vec::new();
	for file in direct_files.iter() {
		files.push(file.clone());
	}
	if !has_globs {
		files.sort();
		files.dedup();
		return Ok(files);
	}
	let mut builder = WalkBuilder::new(&cwd);
	builder.git_ignore(use_gitignore)
		.git_exclude(use_gitignore)
		.git_global(use_gitignore);
	for result in builder.build() {
		let entry = result.map_err(|e| e.to_string())?;
		if !entry.file_type()
			.map(|t| t.is_file())
			.unwrap_or(false) {
			continue;
		}
		let path = entry.path();
		if respect_ignore && is_ignored(path, &cwd, &ignore_set) {
			continue;
		}
		if !target_set.is_empty() {
			let rel = path.strip_prefix(&cwd).ok();
			let rel_match = rel.and_then(|p| p.to_str())
				.map(|p| target_set.is_match(p))
				.unwrap_or(false);
			if !target_set.is_match(path) && !rel_match {
				continue;
			}
		}
		if let Some(path_str) = path.to_str() {
			files.push(path_str.to_string());
		}
	}
	files.sort();
	files.dedup();
	Ok(files)
}

fn collect_files_all(
	roots: &[PathBuf],
	ignore_patterns: &[String],
	targets: &[String]) -> Result<HashMap<String, Vec<String>>, String> {
	let specs = collect_language_specs(roots)?;
	let mut by_language: HashMap<String, Vec<String>> = HashMap::new();
	let cwd = std::env::current_dir().map_err(|e| format!("read cwd: {e}"))?;
	let (ignore_set, use_gitignore) = build_ignore_set(&cwd, ignore_patterns)?;
	let (direct_files, target_set, has_globs) = build_target_set(targets, &cwd)?;
	let respect_ignore = targets.is_empty();
	for file in direct_files.iter() {
		let path = Path::new(file);
		let path_str = match path.to_str() {
			Some(p) => p,
			None => continue,
		};
		for (language, extensions) in specs.iter() {
			if extensions.iter().any(|ext| path_str.ends_with(ext)) {
				by_language.entry(language.clone())
					.or_default()
					.push(path_str.to_string());
				break;
			}
		}
	}
	if !targets.is_empty() && !has_globs {
		for files in by_language.values_mut() {
			files.sort();
			files.dedup();
		}
		return Ok(by_language);
	}
	let mut builder = WalkBuilder::new(&cwd);
	builder.git_ignore(use_gitignore)
		.git_exclude(use_gitignore)
		.git_global(use_gitignore);
	for result in builder.build() {
		let entry = result.map_err(|e| e.to_string())?;
		if !entry.file_type()
			.map(|t| t.is_file())
			.unwrap_or(false) {
			continue;
		}
		let path = entry.path();
		if respect_ignore && is_ignored(path, &cwd, &ignore_set) {
			continue;
		}
		if !target_set.is_empty() {
			let rel = path.strip_prefix(&cwd).ok();
			let rel_match = rel.and_then(|p| p.to_str())
				.map(|p| target_set.is_match(p))
				.unwrap_or(false);
			if !target_set.is_match(path) && !rel_match {
				continue;
			}
		}
		let path_str = match path.to_str() {
			Some(p) => p,
			None => continue,
		};
		for (language, extensions) in specs.iter() {
			if extensions.iter().any(|ext| path_str.ends_with(ext)) {
				by_language.entry(language.clone())
					.or_default()
					.push(path_str.to_string());
				break;
			}
		}
	}
	for files in by_language.values_mut() {
		files.sort();
		files.dedup();
	}
	Ok(by_language)
}

fn build_ignore_set(cwd: &Path, patterns: &[String]) -> Result<(GlobSet, bool), String> {
	let mut builder = GlobSetBuilder::new();
	let mut use_gitignore = true;
	let expand_patterns = |raw: &str| {
		let mut base = raw.to_string();
		if base.ends_with('/') {
			base.push_str("**");
		}
		let anchored = base.starts_with('/');
		if anchored {
			base = base.trim_start_matches('/').to_string();
		}
		if base.is_empty() {
			return Vec::new();
		}
		let has_slash = base.contains('/');
		let mut out = Vec::new();
		out.push(base.clone());
		if !has_slash {
			out.push(format!("**/{}", base));
		}
		else if !anchored && !base.starts_with("**/") {
			out.push(format!("**/{}", base));
		}
		out
	};
	for pattern in patterns.iter() {
		for expanded in expand_patterns(pattern) {
			let glob = Glob::new(&expanded).map_err(|e| e.to_string())?;
			builder.add(glob);
		}
	}
	let neatify_ignore = cwd.join(".neatifyignore");
	if neatify_ignore.exists() {
		let text = std::fs::read_to_string(&neatify_ignore)
			.map_err(|e| format!("read {}: {e}", neatify_ignore.display()))?;
		for line in text.lines() {
			let line = line.trim();
			if line.is_empty() || line.starts_with('#') {
				continue;
			}
			if line == ".gitignore" || line == "**/.gitignore" {
				use_gitignore = false;
				continue;
			}
			for expanded in expand_patterns(line) {
				let glob = Glob::new(&expanded).map_err(|e| e.to_string())?;
				builder.add(glob);
			}
		}
	}
	let ignore_set = builder.build().map_err(|e| e.to_string())?;
	Ok((ignore_set, use_gitignore))
}

fn is_glob_pattern(value: &str) -> bool {
	value.contains('*') || value.contains('?') || value.contains('[')
}

fn build_target_set(targets: &[String], cwd: &Path) -> Result<(Vec<String>, GlobSet, bool), String> {
	let mut builder = GlobSetBuilder::new();
	let mut direct_files = Vec::new();
	let mut has_globs = false;
	for target in targets.iter() {
		let path = Path::new(target);
		let resolved = if path.is_absolute() {
			path.to_path_buf()
		}
		else {
			cwd.join(path)
		};
		if resolved.is_file() {
			if let Some(path_str) = resolved.to_str() {
				direct_files.push(path_str.to_string());
			}
			continue;
		}
		if resolved.is_dir() {
			let pattern = format!("{}/**", resolved.display());
			let glob = Glob::new(&pattern).map_err(|e| e.to_string())?;
			builder.add(glob);
			has_globs = true;
			continue;
		}
		if !is_glob_pattern(target) {
			return Err(format!("file not found: {target}"));
		}
		let glob = Glob::new(target).map_err(|e| e.to_string())?;
		builder.add(glob);
		has_globs = true;
	}
	let set = builder.build().map_err(|e| e.to_string())?;
	Ok((direct_files, set, has_globs))
}

fn collect_language_specs(roots: &[PathBuf]) -> Result<HashMap<String, Vec<String>>, String> {
	let mut map: HashMap<String, Vec<String>> = HashMap::new();
	for root in roots {
		let mut builder = WalkBuilder::new(root);
		builder.git_ignore(false)
			.git_exclude(false)
			.git_global(false);
		for entry in builder.build() {
			let entry = entry.map_err(|e| e.to_string())?;
			if !entry.file_type()
				.map(|t| t.is_file())
				.unwrap_or(false) {
				continue;
			}
			let path = entry.path();
			if path.file_name().and_then(|v| v.to_str()) != Some("language.toml") {
				continue;
			}
			let content = std::fs::read_to_string(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
			let spec: LanguageSpec = toml::from_str(&content).map_err(|e| format!("invalid language spec {}: {e}", path.display()))?;
			if map.contains_key(&spec.name) {
				continue;
			}
			map.insert(spec.name.clone(), spec.extensions.clone());
		}
	}
	Ok(map)
}

fn resolve_language_targets(
	targets: &[String],
	repositories: &[RepositoryRecord]) -> Result<(Option<String>, Vec<String>), String> {
	if targets.is_empty() {
		return Ok((None, Vec::new()));
	}
	let first = &targets[0];
	if let Ok((language_name, roots)) = resolve_language_roots(first, repositories) {
		if resolve_language_spec(&language_name, &roots).is_ok() {
			return Ok((Some(first.clone()), targets[1..].to_vec()));
		}
	}
	Ok((None, targets.to_vec()))
}

fn exit_with_error(msg: &str) -> ! {
	if let Some(format) = OUTPUT_FORMAT.get() {
		if *format == OutputFormat::Json {
			println!("{}", json!({
				"type": "error",
				"message": msg,
			}));
			std::process::exit(2);
		}
	}
	eprintln!("{msg}");
	std::process::exit(2);
}

struct RegistryModuleResolver {
	roots: Vec<PathBuf>,
	cwd: PathBuf,
}

impl RegistryModuleResolver {
	fn new(roots: Vec<PathBuf>) -> Self {
		let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
		Self { roots, cwd }
	}
	fn resolve_path(&self, path: &str) -> Option<PathBuf> {
		let path_obj = Path::new(path);
		if path_obj.is_absolute() {
			return Some(path_obj.to_path_buf());
		}
		if path.starts_with("./") || path.starts_with("../") {
			let candidate = self.cwd.join(path);
			if candidate.exists() {
				return Some(candidate);
			}
		}
		let candidate = self.cwd.join(path);
		if candidate.exists() {
			return Some(candidate);
		}
		resolve_registry_path(path, &self.roots)
	}
}

impl ModuleResolver for RegistryModuleResolver {
	fn resolve(
		&self,
		engine: &Engine,
		_source: Option<&str>,
		path: &str,
		_pos: rhai::Position) -> Result<Rc<Module>, Box<rhai::EvalAltResult>> {
		let resolved = self.resolve_path(path).ok_or_else(|| format!("import not found: {path}"))?;
		let ast = engine.compile_file(resolved)?;
		let scope = Scope::new();
		Module::eval_ast_as_new(scope, &ast, engine).map(Rc::new)
	}
}

#[derive(Clone, Debug)]
struct RepositoryRecord {
	#[allow(dead_code)]
	name: String,
	root: PathBuf,
	#[allow(dead_code)]
	config: RepositoryConfig,
}

fn run_tests(
	cli: &Cli,
	repositories: &[RepositoryRecord],
	language: Option<String>,
	filters: &[String],
	reporter: &Reporter) -> Result<(), String> {
	let overrides = parse_overrides(&cli.sets)?;
	if repositories.is_empty() {
		return Err("no repository roots available".to_string());
	}
	let tests = if let Some(language) = language.as_ref() {
		if let Some((namespace, name)) = split_language_namespace(language) {
			let repo = find_repository(repositories, namespace)?;
			let roots = vec![repo.root.clone()];
			let language_spec = resolve_language_spec(name, &roots)?;
			discover_tests_from_fs(&repo.root, namespace, name, &language_spec.extensions)?
		}
		else {
			let mut tests = Vec::new();
			let mut found = false;
			for repo in repositories {
				let roots = vec![repo.root.clone()];
				let language_spec = match resolve_language_spec(language, &roots) {
					Ok(spec) => spec,
					Err(_) => continue
				};
				found = true;
				let mut repo_tests = discover_tests_from_fs(
					&repo.root,
					&repo.name,
					language,
					&language_spec.extensions
				)?;
				tests.append(&mut repo_tests);
			}
			if !found {
				return Err(format!("language spec not found: {language}/language.toml"));
			}
			tests
		}
	}
	else {
		discover_all_tests_from_fs(repositories)?
	};
	let tests = filter_tests(tests, filters)?;
	if tests.is_empty() {
		return Err("no tests found".to_string());
	}
	let mut failures = 0usize;
	let mut passed = 0usize;
	let mut grouped: HashMap<String, Vec<TestCase>> = HashMap::new();
	for test in tests.into_iter() {
		grouped.entry(test.language.clone())
			.or_default()
			.push(test);
	}
	for (language, tests) in grouped.iter() {
		let (namespace, name) = split_language_namespace(language).ok_or_else(|| format!("invalid test language: {language}"))?;
		let repo = find_repository(repositories, namespace)?;
		let roots = vec![repo.root.clone()];
		let language_spec = resolve_language_spec(name, &roots)?;
		let entry_path = resolve_entry_path(&language_spec.formatter, &roots)
			.ok_or_else(|| format!("formatter script not found: {}", language_spec.formatter))?;
		let binary_path = resolve_treesitter_binary(&language_spec.treesitter.binary, &roots)?;
		let strict = false;
		for test in tests.iter() {
			let input = std::fs::read_to_string(&test.input).map_err(|e| format!("read {}: {e}", test.input.display()))?;
			let expected = std::fs::read_to_string(&test.expected).map_err(|e| format!("read {}: {e}", test.expected.display()))?;
			let formatted = match format_source(
				&input,
				&entry_path,
				&binary_path,
				&language_spec.treesitter.language,
				&roots,
				&overrides,
				cli.debug,
				cli.profile,
				env_flag_enabled("NEATIFY_TEST_GROUP"),
				strict
			) {
				Ok(output) => output,
				Err(err) => {
					failures += 1;
					let contexts = if reporter.inspect && err.starts_with(PARSE_ERROR_PREFIX) {
						build_parse_issue_contexts(
							&format!("{}", test.input.display()),
							&input,
							&binary_path,
							&language_spec.treesitter.language
						)
					}
					else {
						Vec::new()
					};
					reporter.test_result(
						&test.name,
						TestStatus::Fail,
						Some(&err),
						if contexts.is_empty() {
							None
						}
						else {
							Some(contexts.as_slice())
						}
					);
					continue;
				}
			};
			if formatted != expected {
				failures += 1;
				reporter.test_result(
					&test.name,
					TestStatus::Fail,
					Some(&diff_summary(&expected, &formatted)),
					None
				);
			}
			else {
				passed += 1;
				reporter.test_result(&test.name, TestStatus::Pass, None, None);
			}
		}
	}
	reporter.test_summary(passed, failures);
	if failures > 0 {
		return Err(format!("{failures} test(s) failed"));
	}
	Ok(())
}

fn run_lint(
	config: &NeatifyConfig,
	repositories: &[RepositoryRecord],
	language: Option<String>,
	file_targets: &[String],
	strict: bool,
	reporter: &Reporter) -> Result<(), String> {
	let mut total_errors = 0usize;
	let mut summary = LintSummary::new();
	if let Some(language) = language.as_ref() {
		let (language_name, roots) = resolve_language_roots(language, repositories)?;
		let language_spec = resolve_language_spec(&language_name, &roots)?;
		let files = if file_targets.is_empty() {
			collect_files_for_language(&language_spec.extensions, &config.ignore)?
		}
		else {
			collect_files_from_targets(file_targets, &config.ignore)?
		};
		if files.is_empty() {
			return Err("no files found".to_string());
		}
		let result = lint_language_files(&language_spec, &roots, &files, strict, reporter)?;
		total_errors += result.errors;
		summary.add(&result.summary);
	}
	else {
		let repo_roots = repository_roots(repositories);
		let files_by_language = collect_files_all(&repo_roots, &config.ignore, file_targets)?;
		if files_by_language.is_empty() {
			return Err("no files found".to_string());
		}
		for (language, files) in files_by_language.iter() {
			let language_spec = resolve_language_spec(language, &repo_roots)?;
			let result = lint_language_files(&language_spec, &repo_roots, files, strict, reporter)?;
			total_errors += result.errors;
			summary.add(&result.summary);
		}
	}
	summary.print_if_needed(reporter);
	if total_errors > 0 {
		return Err(format!("{total_errors} lint error(s)"));
	}
	Ok(())
}

struct LintResult {
	summary: LintSummary,
	errors: usize,
}

fn lint_language_files(
	spec: &LanguageSpec,
	roots: &[PathBuf],
	files: &[String],
	strict: bool,
	reporter: &Reporter) -> Result<LintResult, String> {
	let binary_path = resolve_treesitter_binary(&spec.treesitter.binary, roots)?;
	let mut libs = Vec::new();
	let lang = load_treesitter_language(&binary_path, &spec.treesitter.language, &mut libs)?;
	let ctx = Context::new("");
	ctx.set_strict(false);
	ctx.register_language(&spec.treesitter.language, lang);
	let mut error_count = 0usize;
	let mut summary = LintSummary::new();
	for file in files.iter() {
		let source = std::fs::read_to_string(file).map_err(|e| format!("read {file}: {e}"))?;
		ctx.set_source(source.to_string());
		let issues = ctx.parse_issues(&spec.treesitter.language, 0)?;
		summary.record(issues.len());
		if !issues.is_empty() {
			let contexts = if reporter.inspect {
				build_issue_contexts_from_issues(file, &source, &issues)
			}
			else {
				Vec::new()
			};
			for (idx, issue) in issues.iter().enumerate() {
				let row = issue.position.row + 1;
				let col = issue.position.col + 1;
				let message = if issue.is_missing {
					format!("{file}:{row}:{col} missing {}", issue.kind)
				}
				else {
					format!("{file}:{row}:{col} parse error")
				};
				let context = contexts.get(idx).map(|ctx| std::slice::from_ref(ctx));
				reporter.lint_issue(file, &message, "error", context);
			}
			error_count += issues.len();
			if strict {
				continue;
			}
		}
		let _ = strict;
		// Linter execution will be added when language specs include a linter entry.
	}
	Ok(
		LintResult {
			summary,
			errors: error_count
		}
	)
}

struct TestCase {
	language: String,
	name: String,
	input: PathBuf,
	expected: PathBuf,
}

fn extract_language_from_test_path(path: &str) -> Result<String, String> {
	if let Some(idx) = path.find("/tests/") {
		let prefix = &path[..idx];
		if prefix.is_empty() {
			return Err(format!("invalid test path: {path}"));
		}
		return Ok(prefix.to_string());
	}
	Err(format!("invalid test path: {path}"))
}

fn filter_tests(tests: Vec<TestCase>, filters: &[String]) -> Result<Vec<TestCase>, String> {
	if filters.is_empty() {
		return Ok(tests);
	}
	let mut filtered = Vec::new();
	for test in tests.into_iter() {
		let input_str = test.input.to_string_lossy();
		let expected_str = test.expected.to_string_lossy();
		let mut matched = false;
		for filter in filters.iter() {
			if input_str.ends_with(filter)
				|| expected_str.ends_with(filter)
				|| test.name.contains(filter) {
				matched = true;
				break;
			}
		}
		if matched {
			filtered.push(test);
		}
	}
	if filtered.is_empty() {
		return Err("no matching tests found for provided filters".to_string());
	}
	Ok(filtered)
}

fn discover_tests_from_fs(
	root: &Path,
	namespace: &str,
	language: &str,
	extensions: &[String]) -> Result<Vec<TestCase>, String> {
	let tests_prefix = format!("{language}/tests/");
	let files = collect_registry_files(root)?;
	let mut set = HashMap::new();
	for file in files.iter() {
		let rel = registry_relative_path(root, file)?;
		set.insert(rel.clone(), file.clone());
	}
	let mut tests = Vec::new();
	for (rel, path) in set.iter() {
		if !rel.starts_with(&tests_prefix) {
			continue;
		}
		for ext in extensions {
			let in_suffix = format!(".in{ext}");
			if !rel.ends_with(&in_suffix) {
				continue;
			}
			let expected_rel = rel.replace(&in_suffix, &format!(".out{ext}"));
			let expected = set.get(&expected_rel).ok_or_else(
				|| {
					format!("missing expected test file: {expected_rel}")
				}
			)?;
			let name = format!("{namespace}/{rel}");
			tests.push(
				TestCase {
					language: format!("{namespace}/{language}"),
					name,
					input: path.clone(),
					expected: expected.clone()
				}
			);
		}
	}
	tests.sort_by(|a, b| a.name.cmp(&b.name));
	Ok(tests)
}

fn discover_all_tests_from_fs(repositories: &[RepositoryRecord]) -> Result<Vec<TestCase>, String> {
	let mut tests = Vec::new();
	for repo in repositories {
		let root = &repo.root;
		let files = collect_registry_files(root)?;
		let mut set = HashMap::new();
		for file in files.iter() {
			let rel = registry_relative_path(root, file)?;
			set.insert(rel.clone(), file.clone());
		}
		for (rel, path) in set.iter() {
			if !rel.contains("/tests/") {
				continue;
			}
			if !rel.contains(".in.") {
				continue;
			}
			let language = extract_language_from_test_path(rel)?;
			let expected_rel = rel.replace(".in.", ".out.");
			let expected = set.get(&expected_rel).ok_or_else(
				|| {
					format!("missing expected test file: {expected_rel}")
				}
			)?;
			tests.push(
				TestCase {
					language: format!("{}/{}", repo.name, language),
					name: format!("{}/{}", repo.name, rel),
					input: path.clone(),
					expected: expected.clone()
				}
			);
		}
	}
	tests.sort_by(|a, b| a.name.cmp(&b.name));
	Ok(tests)
}

fn fetch_url_string(url: &str) -> Result<String, String> {
	let response = ureq::get(url).call().map_err(|e| format!("fetch {url}: {e}"))?;
	response.into_string().map_err(|e| format!("read {url}: {e}"))
}

fn fetch_url_bytes(url: &str) -> Result<Vec<u8>, String> {
	let response = ureq::get(url).call().map_err(|e| format!("fetch {url}: {e}"))?;
	let mut reader = response.into_reader();
	let mut buffer = Vec::new();
	reader.read_to_end(&mut buffer).map_err(|e| format!("read {url}: {e}"))?;
	Ok(buffer)
}

fn join_url(base: &str, path: &str) -> String {
	let base = base.trim_end_matches('/');
	let path = path.trim_start_matches('/');
	format!("{base}/{path}")
}

fn hash_file(path: &Path) -> Result<(String, u64), String> {
	let data = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
	let mut hasher = Sha256::new();
	hasher.update(&data);
	let digest = hasher.finalize();
	Ok((hex::encode(digest), data.len() as u64))
}

fn generate_manifest_for_local_repository(root: &Path) -> Result<(), String> {
	if !root.exists() {
		return Err(format!("repository root not found: {}", root.display()));
	}
	let files = collect_registry_files(root)?;
	let mut entries = Vec::new();
	for file in files {
		let rel = registry_relative_path(root, &file)?;
		if rel == "manifest.toml" || rel == "manifest.json" {
			continue;
		}
		let (hash, size) = hash_file(&file)?;
		entries.push(
			ManifestEntry {
				path: rel,
				sha256: hash,
				size
			}
		);
	}
	let mut manifest = Manifest {
		version: None,
		files: entries
	};
	manifest.sort_by_path();
	let manifest_path = root.join("manifest.toml");
	write_manifest_toml(&manifest, &manifest_path)?;
	Ok(())
}

fn collect_registry_files(root: &Path) -> Result<Vec<PathBuf>, String> {
	let mut files = Vec::new();
	collect_registry_files_recursive(root, root, &mut files)?;
	Ok(files)
}

fn is_hidden_path(path: &Path) -> bool {
	path.components()
		.any(
			|comp| match comp {
				std::path::Component::Normal(os) => os.to_str()
					.map(|s| s.starts_with('.'))
					.unwrap_or(false),
				_ => false,
			}
		)
}

fn collect_registry_files_recursive(
	root: &Path,
	dir: &Path,
	files: &mut Vec<PathBuf>) -> Result<(), String> {
	let entries = std::fs::read_dir(dir).map_err(|e| format!("read dir {}: {e}", dir.display()))?;
	for entry in entries {
		let entry = entry.map_err(|e| format!("read dir entry: {e}"))?;
		let path = entry.path();
		let rel = match path.strip_prefix(root) {
			Ok(rel) => rel,
			Err(_) => continue,
		};
		if is_hidden_path(rel) {
			continue;
		}
		if path.is_dir() {
			collect_registry_files_recursive(root, &path, files)?;
		}
		else {
			files.push(path);
		}
	}
	Ok(())
}

fn registry_relative_path(root: &Path, path: &Path) -> Result<String, String> {
	let rel = path.strip_prefix(root).map_err(|_| format!("path {} outside root", path.display()))?;
	let rel = rel.to_string_lossy().replace('\\', "/");
	Ok(rel)
}

fn diff_summary(expected: &str, actual: &str) -> String {
	let exp_lines: Vec<&str> = expected.lines().collect();
	let act_lines: Vec<&str> = actual.lines().collect();
	let max = exp_lines.len().max(act_lines.len());
	let mut diffs = Vec::new();
	for i in 0..max {
		let exp = exp_lines.get(i).copied();
		let act = act_lines.get(i).copied();
		if exp != act {
			let line_no = i + 1;
			diffs.push(
				match (exp, act) {
					(Some(e), Some(a)) => format!(
						"line {line_no}:\n{}\n{}",
						color(&format!("- {e}"), Color::Red, false),
						color(&format!("+ {a}"), Color::Green, false)
					),
					(Some(e), None) => format!(
						"line {line_no}:\n{}\n{}",
						color(&format!("- {e}"), Color::Red, false),
						color("+ <missing>", Color::Green, false)
					),
					(None, Some(a)) => format!(
						"line {line_no}:\n{}\n{}",
						color("- <missing>", Color::Red, false),
						color(&format!("+ {a}"), Color::Green, false)
					),
					(None, None) => continue,
				}
			);
		}
	}
	if diffs.is_empty() {
		return "output differs".to_string();
	}
	let total = diffs.len();
	let limit = 5usize.min(total);
	let mut out = format!("diffs: {total}\n");
	for diff in diffs.into_iter().take(limit) {
		out.push_str(&diff);
		out.push('\n');
	}
	if total > limit {
		out.push_str(&format!("... {} more difference(s)", total - limit));
	}
	out
}

#[derive(Copy, Clone)]
enum Color {
	Red,
	Green,
	Gray,
	Yellow,
	Cyan,
}

enum FormatStatus {
	Updated,
	Unchanged,
	Skipped,
	Failed,
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum TestStatus {
	Pass,
	Fail,
}

struct IssueContextLine {
	line: usize,
	text: String,
	marker: String,
}

struct IssueContext {
	label: String,
	header: String,
	is_missing: bool,
	lines: Vec<IssueContextLine>,
}

struct Reporter {
	format: OutputFormat,
	inspect: bool,
}

impl Reporter {
	fn new(format: OutputFormat, inspect: bool) -> Self {
		Self { format, inspect }
	}
	fn is_human(&self) -> bool {
		self.format == OutputFormat::Human
	}
	fn emit_json_object(&self, value: serde_json::Map<String, serde_json::Value>) {
		if self.format == OutputFormat::Json {
			println!("{}", serde_json::Value::Object(value));
		}
	}
	fn processing_file(&self, path: &str) {
		if self.is_human() {
			eprint!("Processing file {path}... ");
		}
	}
	fn format_result(
		&self,
		path: &str,
		status: FormatStatus,
		elapsed: Option<Duration>,
		detail: Option<&str>,
		contexts: Option<&[IssueContext]>) {
		if self.is_human() {
			let elapsed = elapsed.map(|d| color(&format_elapsed(d), Color::Gray, false)).unwrap_or_else(|| "".to_string());
			let status_text = match status {
				FormatStatus::Updated => color("DONE", Color::Green, true),
				FormatStatus::Unchanged => color("UNCHANGED", Color::Gray, false),
				FormatStatus::Skipped => color("SKIPPED", Color::Gray, false),
				FormatStatus::Failed => color("FAILED", Color::Red, true),
			};
			if let Some(detail) = detail {
				eprintln!("{status_text} {elapsed} {}", color(detail, Color::Gray, false));
			}
			else if elapsed.is_empty() {
				eprintln!("{status_text}");
			}
			else {
				eprintln!("{status_text} {elapsed}");
			}
			if self.inspect {
				if let Some(contexts) = contexts {
					self.print_contexts(contexts);
				}
			}
		}
		else {
			let mut obj = serde_json::Map::new();
			obj.insert("type".to_string(), json!("format"));
			obj.insert("path".to_string(), json!(path));
			obj.insert("status".to_string(), json!(format_status_text(status)));
			if let Some(elapsed) = elapsed {
				obj.insert("elapsed_ms".to_string(), json!(elapsed.as_millis()));
			}
			if let Some(detail) = detail {
				obj.insert("detail".to_string(), json!(detail));
			}
			if let Some(contexts) = contexts {
				obj.insert("context".to_string(), contexts_to_json(contexts));
			}
			self.emit_json_object(obj);
		}
	}
	fn test_result(
		&self,
		name: &str,
		status: TestStatus,
		detail: Option<&str>,
		contexts: Option<&[IssueContext]>) {
		if self.is_human() {
			let status_text = match status {
				TestStatus::Pass => color("[PASS]", Color::Green, true),
				TestStatus::Fail => color("[FAIL]", Color::Red, true),
			};
			if status == TestStatus::Pass {
				eprintln!("{status_text} {name}");
			}
			else {
				eprintln!("{status_text} {}", bold(name));
				if let Some(detail) = detail {
					eprintln!("{}", detail);
				}
				if self.inspect {
					if let Some(contexts) = contexts {
						self.print_contexts(contexts);
					}
				}
			}
		}
		else {
			let mut obj = serde_json::Map::new();
			obj.insert("type".to_string(), json!("test"));
			obj.insert("name".to_string(), json!(name));
			obj.insert("status".to_string(), json!(test_status_text(status)));
			if let Some(detail) = detail {
				obj.insert("detail".to_string(), json!(detail));
			}
			if let Some(contexts) = contexts {
				obj.insert("context".to_string(), contexts_to_json(contexts));
			}
			self.emit_json_object(obj);
		}
	}
	fn format_summary(&self, summary: &FormatSummary) {
		if self.is_human() {
			if summary.updated == 0 && summary.unchanged == 0 && summary.skipped == 0 && summary.failed == 0 {
				return;
			}
			let updated = color(&summary.updated.to_string(), Color::Green, true);
			let unchanged = color(&summary.unchanged.to_string(), Color::Gray, false);
			let skipped = color(&summary.skipped.to_string(), Color::Gray, false);
			let failed = color(&summary.failed.to_string(), Color::Red, true);
			eprintln!("Summary: {updated} updated, {unchanged} unchanged, {skipped} skipped, {failed} failed");
		}
		else {
			self.emit_json_object(
				json!({
					"type": "summary",
					"updated": summary.updated,
					"unchanged": summary.unchanged,
					"skipped": summary.skipped,
					"failed": summary.failed,
				}).as_object()
					.unwrap()
					.clone()
			);
		}
	}
	fn test_summary(&self, passed: usize, failures: usize) {
		if self.is_human() {
			let passed = color(&passed.to_string(), Color::Green, true);
			let failed = color(&failures.to_string(), Color::Red, true);
			eprintln!("Summary: {passed} passed, {failed} failed");
			return;
		}
		self.emit_json_object(
			json!({
				"type": "summary",
				"passed": passed,
				"failed": failures,
			}).as_object()
				.unwrap()
				.clone()
		);
	}
	fn lint_issue(
		&self,
		path: &str,
		message: &str,
		severity: &str,
		contexts: Option<&[IssueContext]>) {
		if self.is_human() {
			let label = match severity {
				"warning" => color("LINT", Color::Yellow, true),
				_ => color("LINT", Color::Red, true),
			};
			eprintln!("{} {}", label, message);
			if self.inspect {
				if let Some(contexts) = contexts {
					self.print_contexts(contexts);
				}
			}
		}
		else {
			let mut obj = serde_json::Map::new();
			obj.insert("type".to_string(), json!("lint"));
			obj.insert("path".to_string(), json!(path));
			obj.insert("severity".to_string(), json!(severity));
			obj.insert("message".to_string(), json!(message));
			if let Some(contexts) = contexts {
				obj.insert("context".to_string(), contexts_to_json(contexts));
			}
			self.emit_json_object(obj);
		}
	}
	fn lint_summary(&self, summary: &LintSummary) {
		if !self.is_human() {
			return;
		}
		let checked = color(&summary.checked.to_string(), Color::Gray, false);
		let approved = color(&summary.approved.to_string(), Color::Green, true);
		let rejected = color(&summary.rejected.to_string(), Color::Red, true);
		eprintln!("Summary: {checked} checked, {approved} approved, {rejected} rejected");
	}
	fn print_contexts(&self, contexts: &[IssueContext]) {
		if !self.is_human() {
			return;
		}
		for context in contexts {
			let color_kind = if context.is_missing {
				Color::Yellow
			}
			else {
				Color::Red
			};
			eprintln!(
				"  {} {}",
				color(&context.label, Color::Gray, false),
				color(&context.header, color_kind, true)
			);
			for line in context.lines.iter() {
				eprintln!("  {}", line.text);
				eprintln!("  {}", color(&line.marker, color_kind, true));
			}
		}
	}
}

fn format_status_text(status: FormatStatus) -> &'static str {
	match status {
		FormatStatus::Updated => "updated",
		FormatStatus::Unchanged => "unchanged",
		FormatStatus::Skipped => "skipped",
		FormatStatus::Failed => "failed",
	}
}

fn test_status_text(status: TestStatus) -> &'static str {
	match status {
		TestStatus::Pass => "pass",
		TestStatus::Fail => "fail",
	}
}

fn contexts_to_json(contexts: &[IssueContext]) -> serde_json::Value {
	let items: Vec<serde_json::Value> = contexts.iter()
		.map(
			|context| {
				let lines: Vec<serde_json::Value> = context.lines
					.iter()
					.map(
						|line| {
							json!({
								"line": line.line,
								"text": line.text,
								"marker": line.marker,
							})
						})
					.collect();
				json!({
					"label": context.label,
					"header": context.header,
					"is_missing": context.is_missing,
					"lines": lines,
				})
			})
		.collect();
	json!(items)
}

fn color(text: &str, color: Color, bold: bool) -> String {
	if !supports_color() {
		return text.to_string();
	}
	let code = match (color, bold) {
		(Color::Red, true) => "1;31",
		(Color::Green, true) => "1;32",
		(Color::Gray, true) => "1;90",
		(Color::Yellow, true) => "1;33",
		(Color::Cyan, true) => "1;36",
		(Color::Red, false) => "31",
		(Color::Green, false) => "32",
		(Color::Gray, false) => "90",
		(Color::Yellow, false) => "33",
		(Color::Cyan, false) => "36",
	};
	format!("\x1b[{code}m{text}\x1b[0m")
}

fn format_elapsed(elapsed: Duration) -> String {
	if elapsed.as_millis() < 1000 {
		format!("{}ms", elapsed.as_millis())
	}
	else {
		format!("{:.2}s", elapsed.as_secs_f64())
	}
}

fn format_parse_issues(issues: &[ParseIssue]) -> String {
	let mut parts = Vec::new();
	for issue in issues {
		let row = issue.position.row + 1;
		let col = issue.position.col + 1;
		if issue.is_missing {
			parts.push(format!("missing {} at {row}:{col}", issue.kind));
		}
		else {
			parts.push(format!("error at {row}:{col}"));
		}
	}
	format!("{PARSE_ERROR_PREFIX}{}", parts.join("; "))
}

fn build_issue_contexts_from_issues(
	label: &str,
	source: &str,
	issues: &[ParseIssue]) -> Vec<IssueContext> {
	if issues.is_empty() {
		return Vec::new();
	}
	let starts = line_starts(source);
	let mut contexts = Vec::new();
	for issue in issues.iter() {
		let start_row = issue.position
			.row
			.min(starts.len().saturating_sub(1));
		let end_row = issue.end_position
			.row
			.min(starts.len().saturating_sub(1));
		let mut lines = Vec::new();
		let row_start = start_row.min(end_row);
		let row_end = start_row.max(end_row);
		for row in row_start..=row_end {
			let line_start = starts[row];
			let line_end = if row + 1 < starts.len() {
				starts[row + 1].saturating_sub(1)
			}
			else {
				source.len()
			};
			let line_text = &source[line_start..line_end];
			let highlight_start = if row == row_start {
				issue.range
					.start
					.max(line_start)
			}
			else {
				line_start
			};
			let highlight_end = if row == row_end {
				issue.range
					.end
					.min(line_end)
			}
			else {
				line_end
			};
			let start_rel = highlight_start.saturating_sub(line_start).min(line_text.len());
			let end_rel = highlight_end.saturating_sub(line_start).min(line_text.len());
			let fallback_col = if row == start_row {
				Some(
					issue.position
						.col
						.min(line_text.len())
				)
			}
			else {
				None
			};
			let marker = build_marker(line_text, start_rel, end_rel, fallback_col);
			lines.push(
				IssueContextLine {
					line: row + 1,
					text: line_text.to_string(),
					marker
				}
			);
		}
		let header = if issue.is_missing {
			format!("missing {}", issue.kind)
		}
		else {
			"error".to_string()
		};
		let label = format!("{}:{}:{}", label, issue.position.row + 1, issue.position.col + 1);
		contexts.push(
			IssueContext {
				label,
				header,
				is_missing: issue.is_missing,
				lines
			}
		);
	}
	contexts
}

fn build_parse_issue_contexts(
	label: &str,
	source: &str,
	binary_path: &Path,
	language: &str) -> Vec<IssueContext> {
	let ctx = Context::new(source.to_string());
	ctx.set_strict(false);
	let mut libs = Vec::new();
	let lang = match load_treesitter_language(binary_path, language, &mut libs) {
		Ok(lang) => lang,
		Err(_) => return Vec::new(),
	};
	ctx.register_language(language, lang);
	let issues = match ctx.parse_issues(language, 6) {
		Ok(issues) => issues,
		Err(_) => return Vec::new(),
	};
	build_issue_contexts_from_issues(label, source, &issues)
}

fn build_marker(
	line_text: &str,
	mut start: usize,
	mut end: usize,
	fallback_col: Option<usize>) -> String {
	let len = line_text.len();
	start = start.min(len);
	end = end.min(len);
	while start > 0 && !line_text.is_char_boundary(start) {
		start -= 1;
	}
	while end > start && !line_text.is_char_boundary(end) {
		end -= 1;
	}
	if start == end {
		if let Some(col) = fallback_col {
			start = col.min(len);
			end = (start + 1).min(len);
		}
		else if len > 0 {
			end = (start + 1).min(len);
		}
	}
	let prefix = &line_text[..start];
	let mut marker = String::new();
	for ch in prefix.chars() {
		if ch == '\t' {
			marker.push('\t');
		}
		else {
			marker.push(' ');
		}
	}
	let caret_count = if start < end {
		line_text[start..end].chars()
			.count()
			.max(1)
	}
	else {
		1
	};
	marker.push_str(&"^".repeat(caret_count));
	marker
}

fn bold(text: &str) -> String {
	if !supports_color() {
		return text.to_string();
	}
	format!("\x1b[1m{text}\x1b[0m")
}

static HIGHLIGHT_OVERRIDE: OnceLock<bool> = OnceLock::new();

static OUTPUT_FORMAT: OnceLock<OutputFormat> = OnceLock::new();

fn set_highlight_override(value: Option<bool>) {
	if let Some(enabled) = value {
		let _ = HIGHLIGHT_OVERRIDE.set(enabled);
	}
}

fn supports_color() -> bool {
	if let Some(enabled) = HIGHLIGHT_OVERRIDE.get().copied() {
		return enabled;
	}
	if let Ok(value) = std::env::var("NEATIFY_HIGHLIGHT") {
		if let Some(enabled) = parse_bool_env(&value) {
			return enabled;
		}
	}
	if std::env::var("NO_COLOR").is_ok() {
		return false;
	}
	if std::env::consts::OS == "windows" {
		return false;
	}
	if let Ok(term) = std::env::var("TERM") {
		if term == "dumb" {
			return false;
		}
	}
	true
}

fn env_flag_enabled(key: &str) -> bool {
	if let Ok(value) = std::env::var(key) {
		let trimmed = value.trim().to_lowercase();
		return matches!(trimmed.as_str(), "1" | "true" | "yes" | "on");
	}
	false
}

fn parse_bool_env(value: &str) -> Option<bool> {
	match value.trim()
		.to_ascii_lowercase()
		.as_str() {
		"1" | "true" | "yes" | "on" => Some(true),
		"0" | "false" | "no" | "off" => Some(false),
		_ => None,
	}
}

fn is_ignored(path: &Path, cwd: &Path, ignore_set: &GlobSet) -> bool {
	if ignore_set.is_match(path) {
		return true;
	}
	if let Ok(rel) = path.strip_prefix(cwd) {
		return ignore_set.is_match(rel);
	}
	false
}

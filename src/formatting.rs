use crate::{register_primitives, Context, ParseIssue, Range};
use libloading::Library;
use rhai::{Dynamic, Engine, Module, ModuleResolver, Scope, AST};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::SystemTime;

const PARSE_ERROR_PREFIX: &str = "parse error: ";

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
			cache.insert(
				key,
				CachedAst {
					mtime,
					ast: ast.clone()
				}
			);
			Ok(ast)
		}
	)
}

#[derive(Clone, Debug, Deserialize)]
pub struct TreesitterSpec {
	pub language: String,
	pub binary: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LanguageSpec {
	pub name: String,
	pub version: String,
	pub formatter: String,
	#[serde(rename = "fragment-marker")]
	pub fragment_marker: Option<String>,
	pub extensions: Vec<String>,
	pub treesitter: TreesitterSpec,
}

#[derive(Clone, Debug)]
pub enum OverrideValue {
	Int(i64),
	Bool(bool),
	Str(String),
}

#[derive(Clone, Copy, Debug)]
pub struct RangeSpec {
	pub start_row: usize,
	pub start_col: usize,
	pub end_row: usize,
	pub end_col: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct RangeOffsets {
	pub start: usize,
	pub end: usize,
}

pub fn resolve_entry_path(entry: &str, roots: &[PathBuf]) -> Option<PathBuf> {
	let entry_path = Path::new(entry);
	if entry_path.is_absolute() || entry.starts_with("./") || entry.starts_with("../") {
		return Some(entry_path.to_path_buf());
	}
	resolve_registry_path(entry, roots)
}

pub fn resolve_treesitter_binary(path: &str, roots: &[PathBuf]) -> Result<PathBuf, String> {
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

pub fn resolve_language_spec(language: &str, roots: &[PathBuf]) -> Result<LanguageSpec, String> {
	let language_path = if language.contains('/') {
		format!("{language}/language.toml")
	}
	else {
		format!("core/{language}/language.toml")
	};
	let spec_path = resolve_registry_path(&language_path, roots)
		.ok_or_else(|| format!("language spec not found: {language_path}"))?;
	let content = std::fs::read_to_string(&spec_path)
		.map_err(|e| format!("failed to read {}: {e}", spec_path.display()))?;
	let spec: LanguageSpec = toml::from_str(&content).map_err(|e| format!("invalid language spec {}: {e}", spec_path.display()))?;
	Ok(spec)
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

fn build_settings(
	overrides: &[(String, OverrideValue)],
	profile: bool) -> HashMap<String, Dynamic> {
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

pub fn format_source(
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
	let settings = build_settings(overrides, profile);
	format_source_with_settings(
		source,
		entry_path,
		binary_path,
		language,
		roots,
		&settings,
		debug,
		profile,
		test_group,
		strict
	)
}

pub(crate) fn format_source_with_settings(
	source: &str,
	entry_path: &Path,
	binary_path: &Path,
	language: &str,
	roots: &[PathBuf],
	settings: &HashMap<String, Dynamic>,
	debug: bool,
	profile: bool,
	test_group: bool,
	strict: bool) -> Result<String, String> {
	let ctx = Context::new(source.to_string());
	ctx.set_test_group(test_group);
	ctx.set_profile(profile);
	ctx.set_debug(debug);
	ctx.set_strict(strict);
	ctx.set_registry_roots(roots.to_vec());
	ctx.set_settings(settings.clone());
	let mut engine = Engine::new();
	register_primitives(&mut engine, ctx.clone());
	engine.set_max_expr_depths(128, 64);
	engine.set_max_call_levels(128);
	engine.set_module_resolver(RegistryModuleResolver::new(roots.to_vec()));
	let mut _libs = Vec::new();
	let lang = load_treesitter_language(binary_path, language, &mut _libs)?;
	ctx.register_language(language, lang);
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
	if profile {
		ctx.print_trace_report();
	}
	eval_result.map_err(|e| format!("script error: {e}"))?;
	Ok(ctx.source_text())
}

pub fn format_fragment(
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
	let settings = build_settings(overrides, profile);
	format_fragment_with_settings(
		source,
		range,
		marker_template,
		entry_path,
		binary_path,
		language,
		roots,
		&settings,
		debug,
		profile,
		test_group,
		strict
	)
}

pub(crate) fn format_fragment_with_settings(
	source: &str,
	range: &RangeOffsets,
	marker_template: &str,
	entry_path: &Path,
	binary_path: &Path,
	language: &str,
	roots: &[PathBuf],
	settings: &HashMap<String, Dynamic>,
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
	let formatted = format_source_with_settings(
		&with_markers,
		entry_path,
		binary_path,
		language,
		roots,
		settings,
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

pub fn range_offsets(source: &str, range: &RangeSpec) -> Result<RangeOffsets, String> {
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

pub(crate) fn format_fragment_optional(
	source: &str,
	range: &Range,
	language: &str,
	roots: &[PathBuf],
	settings: &HashMap<String, Dynamic>,
	debug: bool,
	profile: bool,
	test_group: bool,
	strict: bool) -> Option<String> {
	let spec = match resolve_language_spec(language, roots) {
		Ok(spec) => spec,
		Err(err) => {
			if debug {
				eprintln!("[neatify] fragment: failed to resolve {language} spec: {err}");
			}
			return None;
		}
	};
	let entry_path = match resolve_entry_path(&spec.formatter, roots) {
		Some(path) => path,
		None => {
			if debug {
				eprintln!(
					"[neatify] fragment: formatter not found for {language}: {}",
					spec.formatter
				);
			}
			return None;
		}
	};
	let binary_path = match resolve_treesitter_binary(&spec.treesitter.binary, roots) {
		Ok(path) => path,
		Err(err) => {
			if debug {
				eprintln!("[neatify] fragment: treesitter binary missing for {language}: {err}");
			}
			return None;
		}
	};
	if range.start >= range.end || range.end > source.len() {
		if debug {
			eprintln!(
				"[neatify] fragment: invalid range for {language}: {}..{} (len {})",
				range.start,
				range.end,
				source.len()
			);
		}
		return None;
	}
	let fragment = source[range.start..range.end].to_string();
	let formatted = match format_source_with_settings(
		&fragment,
		&entry_path,
		&binary_path,
		&spec.treesitter.language,
		roots,
		settings,
		debug,
		profile,
		test_group,
		strict
	) {
		Ok(out) => out,
		Err(err) => {
			if debug {
				eprintln!("[neatify] fragment: failed to format {language}: {err}");
			}
			return None;
		}
	};
	let trimmed = trim_fragment_output(&formatted);
	Some(trimmed)
}

fn trim_fragment_output(text: &str) -> String {
	let mut out = text.to_string();
	while !out.is_empty() {
		let last = out.chars()
			.last()
			.unwrap();
		if last == '\n' || last == '\r' || last == ' ' || last == '\t' {
			out.pop();
		}
		else {
			break;
		}
	}
	out
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
		if path == "base.rhai" {
			let fallback = "core/base.rhai";
			return resolve_registry_path(fallback, &self.roots);
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

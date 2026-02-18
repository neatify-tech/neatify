use crate::doc::{Doc, DocArena, DocId, LineKind};
use rhai::{Dynamic, INT};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
use tree_sitter::{
	Node,
	Parser,
	Query,
	QueryCursor,
	StreamingIterator,
	Tree,
	LANGUAGE_VERSION,
	MIN_COMPATIBLE_LANGUAGE_VERSION,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Range {
	pub start: usize,
	pub end: usize,
}

impl Range {
	pub fn new(start: usize, end: usize) -> Self {
		Self { start, end }
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
	pub row: usize,
	pub col: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseIssue {
	pub kind: String,
	pub is_missing: bool,
	pub range: Range,
	pub position: Position,
	pub end_position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Line {
	pub row: usize,
	pub start_offset: usize,
	pub end_offset: usize,
	pub indent: String,
	pub text: String,
}

#[derive(Debug)]
pub struct NodeRef {
	ctx: Context,
	range: Range,
	start_position: Position,
	end_position: Position,
	kind_id: u16,
	native_kind_id: u16,
	language: String,
	is_error: bool,
	is_missing: bool,
	doc_id: AtomicI64,
	token_len: OnceLock<usize>,
	children_cache: OnceLock<Vec<Arc<NodeRef>>>,
	parent_cache: OnceLock<Option<Weak<NodeRef>>>,
	next_sibling_cache: OnceLock<Option<Weak<NodeRef>>>,
	prev_sibling_cache: OnceLock<Option<Weak<NodeRef>>>,
	index_cache: OnceLock<Option<usize>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FindMode {
	First,
	Last,
	All,
}

#[derive(Clone, Debug)]
pub(crate) struct FindSpec {
	pub kind_id: Option<Vec<u16>>,
	pub mode: FindMode,
	pub exclude_kind_id: Vec<u16>,
	pub before_kind_id: Option<Vec<u16>>,
	pub after_kind_id: Option<Vec<u16>>,
	pub before_index: Option<usize>,
	pub after_index: Option<usize>,
}

#[derive(Clone, Debug)]
pub(crate) enum FindResult {
	None,
	Single(Arc<NodeRef>),
	Many(Vec<Arc<NodeRef>>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct NodeCacheKey {
	start: usize,
	end: usize,
	kind_id: u16,
	language: String,
}

impl NodeRef {
	fn build_children(&self) -> Vec<Arc<NodeRef>> {
		self.ctx
			.with_node(
				self,
				|node| {
					let mut result = Vec::new();
					let mut cursor = node.walk();
					if cursor.goto_first_child() {
						loop {
							let child = cursor.node();
							let child_ref = self.ctx.node_to_ref(child, &self.language);
							child_ref.set_index_cache(Some(result.len()));
							result.push(child_ref);
							if !cursor.goto_next_sibling() {
								break;
							}
						}
					}
					result
				}
			)
			.unwrap_or_default()
	}
	pub fn child(&self, idx: usize) -> Option<Arc<NodeRef>> {
		let children = self.children_cache.get_or_init(|| self.build_children());
		children.get(idx).cloned()
	}
	pub(crate) fn set_children_cache(&self, children: Vec<Arc<NodeRef>>) {
		let _ = self.children_cache.set(children);
	}
	pub(crate) fn set_parent_cache(&self, parent: Option<Weak<NodeRef>>) {
		let _ = self.parent_cache.set(parent);
	}
	pub(crate) fn set_next_sibling_cache(&self, next: Option<Weak<NodeRef>>) {
		let _ = self.next_sibling_cache.set(next);
	}
	pub(crate) fn set_prev_sibling_cache(&self, prev: Option<Weak<NodeRef>>) {
		let _ = self.prev_sibling_cache.set(prev);
	}
	pub(crate) fn set_index_cache(&self, index: Option<usize>) {
		let _ = self.index_cache.set(index);
	}
	pub(crate) fn child_kind_id(&self, idx: usize) -> Option<u16> {
		let children = self.children_cache.get_or_init(|| self.build_children());
		children.get(idx).map(|child| child.kind_id())
	}
	pub(crate) fn native_kind_id(&self) -> u16 {
		self.native_kind_id
	}
	pub(crate) fn child_range(&self, idx: usize) -> Option<Range> {
		let children = self.children_cache.get_or_init(|| self.build_children());
		children.get(idx).map(|child| child.byte_range())
	}
	pub(crate) fn child_doc_id(&self, idx: usize) -> Option<i64> {
		let children = self.children_cache.get_or_init(|| self.build_children());
		children.get(idx).map(|child| child.doc_id())
	}
	pub fn child_count(&self) -> usize {
		self.children_cache
			.get_or_init(|| self.build_children())
			.len()
	}
	fn cache_key(&self) -> NodeCacheKey {
		NodeCacheKey {
			start: self.range.start,
			end: self.range.end,
			kind_id: self.native_kind_id,
			language: self.language.clone()
		}
	}
	pub fn new(
		ctx: Context,
		range: Range,
		start_position: Position,
		end_position: Position,
		kind_id: u16,
		native_kind_id: u16,
		language: impl Into<String>,
		is_error: bool,
		is_missing: bool) -> Self {
		Self {
			ctx,
			range,
			start_position,
			end_position,
			kind_id,
			native_kind_id,
			language: language.into(),
			is_error,
			is_missing,
			doc_id: AtomicI64::new(-1),
			token_len: OnceLock::new(),
			children_cache: OnceLock::new(),
			parent_cache: OnceLock::new(),
			next_sibling_cache: OnceLock::new(),
			prev_sibling_cache: OnceLock::new(),
			index_cache: OnceLock::new()
		}
	}
	pub fn set_doc_id(&self, doc_id: i64) {
		self.doc_id.store(doc_id, Ordering::Relaxed);
	}
	pub fn doc_id(&self) -> i64 {
		self.doc_id.load(Ordering::Relaxed)
	}
	pub fn token_len(&self) -> usize {
		if let Some(value) = self.token_len.get() {
			return *value;
		}
		let value = self.compute_token_len();
		let _ = self.token_len.set(value);
		value
	}
	fn compute_token_len(&self) -> usize {
		self.ctx
			.with_node(
				self,
				|node| {
					let mut sum: usize = 0;
					let mut stack = vec![node];
					while let Some(current) = stack.pop() {
						if current.child_count() == 0 {
							sum += current.end_byte() - current.start_byte();
							continue;
						}
						let mut cursor = current.walk();
						if cursor.goto_first_child() {
							loop {
								stack.push(cursor.node());
								if !cursor.goto_next_sibling() {
									break;
								}
							}
						}
					}
					sum
				}
			)
			.unwrap_or(0)
	}
	pub(crate) fn set_token_len(&self, token_len: usize) {
		let _ = self.token_len.set(token_len);
	}
	pub fn text(&self) -> String {
		self.ctx.slice(&self.range)
	}
	pub fn kind_id(&self) -> u16 {
		self.kind_id
	}
	pub fn index(&self) -> Option<usize> {
		self.index_cache
			.get()
			.copied()
			.flatten()
	}
	// NOTE: Recursive search is intentionally deferred. We need to define how
	// before/after should behave across traversal depth without surprising users.
	pub(crate) fn find(&self, specs: &[FindSpec]) -> Vec<FindResult> {
		if specs.is_empty() {
			return Vec::new();
		}
		let mut results: Vec<FindResult> = vec![FindResult::None; specs.len()];
		let mut seen_after: Vec<bool> = specs.iter()
			.map(|spec| spec.after_kind_id.is_none())
			.collect();
		let mut seen_before: Vec<bool> = vec![false; specs.len()];
		let children = self.children();
		for (i, child) in children.iter().enumerate() {
			let kind_id = child.kind_id();
			let index = child.index().unwrap_or(i);
			for (spec_idx, spec) in specs.iter().enumerate() {
				let hit_after = spec.after_kind_id
					.as_ref()
					.is_some_and(|ids| ids.iter().any(|id| *id == kind_id));
				let hit_before = spec.before_kind_id
					.as_ref()
					.is_some_and(|ids| ids.iter().any(|id| *id == kind_id));
				if hit_after {
					seen_after[spec_idx] = true;
				}
				if hit_before {
					seen_before[spec_idx] = true;
				}
				if hit_after || hit_before {
					continue;
				}
				if !seen_after[spec_idx] || seen_before[spec_idx] {
					continue;
				}
				if let Some(after_index) = spec.after_index {
					if index <= after_index {
						continue;
					}
				}
				if let Some(before_index) = spec.before_index {
					if index >= before_index {
						continue;
					}
				}
				if !spec.exclude_kind_id.is_empty()
					&& spec.exclude_kind_id
						.iter()
						.any(|id| *id == kind_id) {
					continue;
				}
				if let Some(match_ids) = spec.kind_id.as_ref() {
					if !match_ids.iter().any(|id| *id == kind_id) {
						continue;
					}
				}
				match spec.mode {
					FindMode::First => {
						if let FindResult::None = results[spec_idx] {
							results[spec_idx] = FindResult::Single(Arc::clone(child));
						}
					}
					FindMode::Last => {
						results[spec_idx] = FindResult::Single(Arc::clone(child));
					}
					FindMode::All => match &mut results[spec_idx] {
						FindResult::Many(items) => {
							items.push(Arc::clone(child));
						}
						FindResult::Single(existing) => {
							let mut items = Vec::new();
							items.push(Arc::clone(existing));
							items.push(Arc::clone(child));
							results[spec_idx] = FindResult::Many(items);
						}
						FindResult::None => {
							results[spec_idx] = FindResult::Many(vec![Arc::clone(child)]);
						}
					},
				}
			}
		}
		results
	}
	pub fn ancestor(
		self: &Arc<NodeRef>,
		kinds: &[u16],
		furthest: bool,
		boundary: bool,
		include_self: bool,
		stop_kinds: Option<&[u16]>,
		continue_kinds: Option<&[u16]>) -> Option<Arc<NodeRef>> {
		if kinds.is_empty() {
			return None;
		}
		let mut current = if include_self {
			Some(Arc::clone(self))
		}
		else {
			self.parent()
		};
		let mut found: Option<Arc<NodeRef>> = None;
		let has_stop = stop_kinds.is_some();
		let has_continue = continue_kinds.is_some();
		while let Some(parent) = current {
			let kind_id = parent.kind_id();
			if kinds.iter().any(|k| *k == kind_id) {
				if !furthest {
					return Some(parent);
				}
				found = Some(Arc::clone(&parent));
			}
			let is_stop = has_stop
				&& stop_kinds.unwrap_or(&[])
					.iter()
					.any(|k| *k == kind_id);
			let is_continue_allowed = !has_continue
				|| continue_kinds.unwrap_or(&[])
					.iter()
					.any(|k| *k == kind_id);
			if is_stop || !is_continue_allowed {
				if boundary {
					if let Some(found_node) = found {
						return Some(found_node);
					}
					return Some(parent);
				}
				return found;
			}
			current = parent.parent();
		}
		found
	}
	pub fn start_position(&self) -> Position {
		self.start_position.clone()
	}
	pub fn end_position(&self) -> Position {
		self.end_position.clone()
	}
	pub fn byte_range(&self) -> Range {
		self.range.clone()
	}
	pub fn is_error(&self) -> bool {
		self.is_error
	}
	pub fn is_missing(&self) -> bool {
		self.is_missing
	}
	pub fn line(&self) -> Line {
		self.ctx.line_at(self.range.start)
	}
	pub fn parent(&self) -> Option<Arc<NodeRef>> {
		let cached = self.parent_cache
			.get_or_init(
				|| {
					self.ctx.record_cache_miss();
					self.ctx
						.with_node(
							self,
							|node| {
								node.parent().map(|parent| Arc::downgrade(&self.ctx.node_to_ref(parent, &self.language)))
							}
						)
						.unwrap_or(None)
				}
			);
		cached.as_ref().and_then(|weak| weak.upgrade())
	}
	pub fn children(&self) -> Vec<Arc<NodeRef>> {
		if self.children_cache
			.get()
			.is_none() {
			self.ctx.record_cache_miss();
		}
		self.children_cache
			.get_or_init(|| self.build_children())
			.clone()
	}
	pub fn next_sibling(&self) -> Option<Arc<NodeRef>> {
		let cached = self.next_sibling_cache
			.get_or_init(
				|| {
					self.ctx.record_cache_miss();
					self.ctx
						.with_node(
							self,
							|node| {
								node.next_sibling().map(
									|sibling| {
										Arc::downgrade(&self.ctx.node_to_ref(sibling, &self.language))
									}
								)
							}
						)
						.unwrap_or(None)
				}
			);
		cached.as_ref().and_then(|weak| weak.upgrade())
	}
	pub fn prev_sibling(&self) -> Option<Arc<NodeRef>> {
		let cached = self.prev_sibling_cache
			.get_or_init(
				|| {
					self.ctx.record_cache_miss();
					self.ctx
						.with_node(
							self,
							|node| {
								node.prev_sibling().map(
									|sibling| {
										Arc::downgrade(&self.ctx.node_to_ref(sibling, &self.language))
									}
								)
							}
						)
						.unwrap_or(None)
				}
			);
		cached.as_ref().and_then(|weak| weak.upgrade())
	}
}

#[derive(Clone, Debug)]
pub struct Match {
	captures: HashMap<String, Arc<NodeRef>>,
}

impl Match {
	pub fn new(captures: HashMap<String, Arc<NodeRef>>) -> Self {
		Self { captures }
	}
	pub fn captures(&self) -> HashMap<String, Arc<NodeRef>> {
		self.captures.clone()
	}
}

#[derive(Clone, Debug)]
pub struct LanguageRegistry {
	languages: HashMap<String, tree_sitter::Language>,
}

#[derive(Clone, Debug, Default)]
struct TraceTotals {
	count: u64,
	total_ns: u128,
}

#[derive(Clone, Copy, Debug)]
struct PrecacheStats {
	nodes: usize,
	total_ns: u128,
}

impl LanguageRegistry {
	pub fn new() -> Self {
		Self {
			languages: HashMap::new()
		}
	}
	pub fn register_language(&mut self, name: impl Into<String>, language: tree_sitter::Language) {
		self.languages.insert(name.into(), language);
	}
	pub fn get(&self, name: &str) -> Option<tree_sitter::Language> {
		self.languages
			.get(name)
			.cloned()
	}
}

#[derive(Clone, Debug)]
struct LanguageKindMap {
	canonical_by_id: Vec<u16>,
	named_ids_by_name: HashMap<String, Vec<u16>>,
}

#[derive(Clone, Debug)]
pub struct Context {
	source: Arc<Mutex<String>>,
	registry: Arc<Mutex<LanguageRegistry>>,
	trees: Arc<Mutex<HashMap<String, Tree>>>,
	queries: Arc<Mutex<HashMap<String, Arc<Query>>>>,
	cache: Arc<Mutex<HashMap<String, Dynamic>>>,
	node_refs: Arc<Mutex<HashMap<NodeCacheKey, Arc<NodeRef>>>>,
	node_doc_ids: Arc<Mutex<HashMap<NodeCacheKey, INT>>>,
	precache_postorder: Arc<Mutex<HashMap<String, Vec<Arc<NodeRef>>>>>,
	docs: Arc<Mutex<DocArena>>,
	registry_roots: Arc<Mutex<Vec<PathBuf>>>,
	settings: Arc<Mutex<HashMap<String, Dynamic>>>,
	kind_maps: Arc<Mutex<HashMap<String, LanguageKindMap>>>,
	test_group: Arc<AtomicBool>,
	profile: Arc<AtomicBool>,
	trace_profiles: Arc<Mutex<HashMap<String, TraceTotals>>>,
	precache_stats: Arc<Mutex<Option<PrecacheStats>>>,
	cache_miss_count: Arc<AtomicU64>,
	debug: Arc<AtomicBool>,
	strict: Arc<AtomicBool>,
}

impl Context {
	fn build_kind_map(lang: &tree_sitter::Language) -> LanguageKindMap {
		let count = lang.node_kind_count();
		let mut canonical_by_id = vec![0u16; count];
		let mut named_ids_by_name: HashMap<String, Vec<u16>> = HashMap::new();
		for id in 0..count {
			let id = id as u16;
			let name = lang.node_kind_for_id(id).unwrap_or("<unknown>");
			let named = lang.node_kind_is_named(id);
			let canonical = lang.id_for_node_kind(name, named);
			canonical_by_id[id as usize] = canonical;
			if named {
				named_ids_by_name.entry(name.to_string())
					.or_default()
					.push(id);
			}
		}
		LanguageKindMap {
			canonical_by_id,
			named_ids_by_name
		}
	}
	pub fn new(source: impl Into<String>) -> Self {
		Self {
			source: Arc::new(Mutex::new(source.into())),
			registry: Arc::new(Mutex::new(LanguageRegistry::new())),
			trees: Arc::new(Mutex::new(HashMap::new())),
			queries: Arc::new(Mutex::new(HashMap::new())),
			cache: Arc::new(Mutex::new(HashMap::new())),
			node_refs: Arc::new(Mutex::new(HashMap::new())),
			node_doc_ids: Arc::new(Mutex::new(HashMap::new())),
			precache_postorder: Arc::new(Mutex::new(HashMap::new())),
			docs: Arc::new(Mutex::new(DocArena::new())),
			registry_roots: Arc::new(Mutex::new(Vec::new())),
			settings: Arc::new(Mutex::new(HashMap::new())),
			kind_maps: Arc::new(Mutex::new(HashMap::new())),
			test_group: Arc::new(AtomicBool::new(false)),
			profile: Arc::new(AtomicBool::new(false)),
			trace_profiles: Arc::new(Mutex::new(HashMap::new())),
			precache_stats: Arc::new(Mutex::new(None)),
			cache_miss_count: Arc::new(AtomicU64::new(0)),
			debug: Arc::new(AtomicBool::new(false)),
			strict: Arc::new(AtomicBool::new(true))
		}
	}
	pub fn cache_get(&self, key: &str) -> Option<Dynamic> {
		let cache = self.cache
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		cache.get(key).cloned()
	}
	pub fn cache_set(&self, key: &str, value: Dynamic) {
		let mut cache = self.cache
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		cache.insert(key.to_string(), value);
	}
	pub fn clear_cache(&self) {
		let mut cache = self.cache
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		cache.clear();
	}
	pub fn clear_node_cache(&self) {
		let mut refs = self.node_refs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		refs.clear();
		let mut doc_ids = self.node_doc_ids
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		doc_ids.clear();
		let mut postorder = self.precache_postorder
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		postorder.clear();
		self.cache_miss_count.store(0, Ordering::Relaxed);
	}
	pub fn set_source(&self, source: impl Into<String>) {
		let mut stored = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		*stored = source.into();
		let mut trees = self.trees
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		trees.clear();
		self.clear_node_cache();
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		*docs = DocArena::new();
		self.reset_trace_profile();
		self.clear_cache();
	}
	pub fn set_registry_roots(&self, roots: Vec<PathBuf>) {
		let mut stored = self.registry_roots
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		*stored = roots;
	}
	pub fn registry_roots(&self) -> Vec<PathBuf> {
		let stored = self.registry_roots
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		stored.clone()
	}
	pub fn set_settings(&self, settings: HashMap<String, Dynamic>) {
		let mut stored = self.settings
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		*stored = settings;
	}
	pub fn settings(&self) -> HashMap<String, Dynamic> {
		let stored = self.settings
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		stored.clone()
	}
	pub fn set_test_group(&self, enabled: bool) {
		self.test_group.store(enabled, Ordering::Relaxed);
	}
	pub fn test_group_enabled(&self) -> bool {
		self.test_group.load(Ordering::Relaxed)
	}
	pub fn set_profile(&self, enabled: bool) {
		self.profile.store(enabled, Ordering::Relaxed);
	}
	pub fn profile_enabled(&self) -> bool {
		self.profile.load(Ordering::Relaxed)
	}
	pub fn record_trace(&self, name: &str, duration: Duration) {
		if !self.profile_enabled() {
			return;
		}
		let mut traces = self.trace_profiles
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		let entry = traces.entry(name.to_string()).or_default();
		entry.count += 1;
		entry.total_ns += duration.as_nanos();
	}
	pub fn reset_trace_profile(&self) {
		let mut traces = self.trace_profiles
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		traces.clear();
		let mut precache = self.precache_stats
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		*precache = None;
		self.cache_miss_count.store(0, Ordering::Relaxed);
	}
	pub fn record_cache_miss(&self) {
		if !self.profile_enabled() {
			return;
		}
		self.cache_miss_count.fetch_add(1, Ordering::Relaxed);
	}
	pub fn cache_miss_count(&self) -> u64 {
		if !self.profile_enabled() {
			return 0;
		}
		self.cache_miss_count.load(Ordering::Relaxed)
	}
	pub fn print_trace_report(&self) {
		if !self.profile_enabled() {
			return;
		}
		let mut traces = self.trace_profiles
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if traces.is_empty() {
			return;
		}
		let mut entries: Vec<(String, TraceTotals)> = traces.drain().collect();
		entries.sort_by(
			|a, b| b.1.total_ns.cmp(&a.1.total_ns).then_with(|| a.0.cmp(&b.0))
		);
		eprintln!("[neatify] trace profile");
		eprintln!("  ------------------------------------------------------------");
		eprintln!("  Hotspots (top 10 by total time)");
		for (idx, (name, totals)) in entries.into_iter()
			.take(10)
			.enumerate() {
			let total_ms = totals.total_ns as f64 / 1_000_000.0;
			let avg_ms = if totals.count > 0 {
				total_ms / totals.count as f64
			}
			else {
				0.0
			};
			eprintln!(
				"  {:>2}. {name} | total {:>9.3} ms | avg {:>8.3} ms | count {}",
				idx + 1,
				total_ms,
				avg_ms,
				totals.count
			);
		}
		eprintln!("  ------------------------------------------------------------");
	}
	pub fn precache_stats(&self) -> Option<(usize, u128)> {
		let precache = self.precache_stats
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		precache.map(|stats| (stats.nodes, stats.total_ns))
	}
	pub(crate) fn set_precache_postorder(&self, language: &str, nodes: Vec<Arc<NodeRef>>) {
		let mut precache = self.precache_postorder
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		precache.insert(language.to_string(), nodes);
	}
	pub(crate) fn precache_postorder(&self, language: &str) -> Option<Vec<Arc<NodeRef>>> {
		let precache = self.precache_postorder
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		precache.get(language).cloned()
	}
	pub(crate) fn canonical_kind_id(&self, language: &str, native_id: u16) -> u16 {
		let maps = self.kind_maps
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		let Some(map) = maps.get(language) else {
			return native_id;
		};
		if (native_id as usize) < map.canonical_by_id.len() {
			map.canonical_by_id[native_id as usize]
		}
		else {
			native_id
		}
	}
	pub(crate) fn named_kind_ids(&self, language: &str, name: &str) -> Option<Vec<u16>> {
		let maps = self.kind_maps
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		maps.get(language).and_then(
			|map| map.named_ids_by_name
				.get(name)
				.cloned()
		)
	}
	pub fn set_debug(&self, enabled: bool) {
		self.debug.store(enabled, Ordering::Relaxed);
	}
	pub fn debug_enabled(&self) -> bool {
		self.debug.load(Ordering::Relaxed)
	}
	pub fn set_strict(&self, enabled: bool) {
		self.strict.store(enabled, Ordering::Relaxed);
	}
	pub fn strict_enabled(&self) -> bool {
		self.strict.load(Ordering::Relaxed)
	}
	pub fn register_language(&self, name: impl Into<String>, language: tree_sitter::Language) {
		let name = name.into();
		let kind_map = Self::build_kind_map(&language);
		let mut registry = self.registry
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		registry.register_language(name.clone(), language);
		let mut maps = self.kind_maps
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		maps.insert(name, kind_map);
	}
	pub fn set_output(&self, text: &str) {
		let mut source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		*source = text.to_string();
	}
	pub fn source_len(&self) -> usize {
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		source.len()
	}
	pub fn line_at(&self, pos: usize) -> Line {
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		let bytes = source.as_bytes();
		let mut row = 0usize;
		for b in &bytes[..pos.min(bytes.len())] {
			if *b == b'\n' {
				row += 1;
			}
		}
		let mut start = pos.min(bytes.len());
		while start > 0 && bytes[start - 1] != b'\n' {
			start -= 1;
		}
		let mut end = pos.min(bytes.len());
		while end < bytes.len() && bytes[end] != b'\n' {
			end += 1;
		}
		let text = source[start..end].to_string();
		let indent = text.chars()
			.take_while(|c| *c == ' ' || *c == '\t')
			.collect::<String>();
		Line {
			row,
			start_offset: start,
			end_offset: end,
			indent,
			text
		}
	}
	pub fn slice(&self, range: &Range) -> String {
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if range.start >= range.end || range.end > source.len() {
			return String::new();
		}
		source[range.start..range.end].to_string()
	}
	pub fn source_text(&self) -> String {
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		source.clone()
	}
	pub fn position_at(&self, pos: usize) -> Position {
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		let bytes = source.as_bytes();
		let mut row = 0;
		let mut col = 0;
		let mut i = 0;
		let limit = pos.min(bytes.len());
		while i < limit {
			if bytes[i] == b'\n' {
				row += 1;
				col = 0;
			}
			else {
				col += 1;
			}
			i += 1;
		}
		Position { row, col }
	}
	pub fn range_around(&self, node: &NodeRef, before: i64, after: i64, same_line: bool) -> Range {
		let mut start = node.range
			.start
			.saturating_sub(before.max(0) as usize);
		let mut end = node.range
			.end
			.saturating_add(after.max(0) as usize);
		if same_line {
			let line = self.line_at(node.range.start);
			start = start.max(line.start_offset);
			end = end.min(line.end_offset);
		}
		Range { start, end }
	}
	pub fn parse_issues(&self, language: &str, limit: usize) -> Result<Vec<ParseIssue>, String> {
		let lang = self.lookup_language(language).ok_or_else(|| "unknown language".to_string())?;
		let tree = self.parse_tree(language, &lang)?;
		let root = tree.root_node();
		if !root.has_error() {
			return Ok(Vec::new());
		}
		let mut issues = Vec::new();
		let mut stack = vec![root];
		while let Some(node) = stack.pop() {
			if node.is_error() || node.is_missing() {
				let range = Range::new(node.start_byte(), node.end_byte());
				let position = self.position_at(range.start);
				let end_position = self.position_at(range.end);
				issues.push(
					ParseIssue {
						kind: lang.node_kind_for_id(node.kind_id())
							.unwrap_or("unknown")
							.to_string(),
						is_missing: node.is_missing(),
						range,
						position,
						end_position
					}
				);
				if limit > 0 && issues.len() >= limit {
					break;
				}
			}
			let mut cursor = node.walk();
			if cursor.goto_first_child() {
				loop {
					stack.push(cursor.node());
					if !cursor.goto_next_sibling() {
						break;
					}
				}
			}
		}
		Ok(issues)
	}
	pub fn query(&self, language: &str, query: &str, scope: Option<&Range>) -> Vec<Match> {
		let Some(lang) = self.lookup_language(language) else {
			return Vec::new();
		};
		let tree = match self.parse_tree(language, &lang) {
			Ok(tree) => tree,
			Err(_) => return Vec::new(),
		};
		let query = match self.cached_query(language, query, &lang) {
			Some(query) => query,
			None => return Vec::new(),
		};
		let mut cursor = QueryCursor::new();
		if let Some(scope) = scope {
			let source_len = self.source_len();
			let start = scope.start
				.min(scope.end)
				.min(source_len);
			let end = scope.end
				.max(scope.start)
				.min(source_len);
			if start < end {
				cursor.set_byte_range(start..end);
			}
		}
		let root = tree.root_node();
		let mut results = Vec::new();
		let capture_names = query.capture_names();
		let source_bytes = self.source_bytes();
		let mut matches = cursor.matches(&query, root, source_bytes.as_slice());
		while let Some(m) = matches.next() {
			let mut captures = HashMap::new();
			for capture in m.captures.iter() {
				let name = capture_names[capture.index as usize].to_string();
				let node = capture.node;
				let node_ref = self.node_to_ref(node, language);
				captures.insert(name, node_ref);
			}
			results.push(Match::new(captures));
		}
		results
	}
	pub fn query_ranges(&self, language: &str, query: &str) -> Vec<Range> {
		let matches = self.query(language, query, None);
		let mut ranges = Vec::new();
		for m in matches {
			for (_, node) in m.captures().into_iter() {
				ranges.push(node.byte_range());
			}
		}
		ranges.sort_by(|a, b| b.start.cmp(&a.start));
		ranges
	}
	pub fn root_node(&self, language: &str) -> Option<Arc<NodeRef>> {
		let lang = self.lookup_language(language)?;
		let tree = self.parse_tree(language, &lang).ok()?;
		let root = tree.root_node();
		Some(self.node_to_ref(root, language))
	}
	pub fn walk_postorder(&self, language: &str) -> Vec<Arc<NodeRef>> {
		let Some(lang) = self.lookup_language(language) else {
			return Vec::new();
		};
		let tree = match self.parse_tree(language, &lang) {
			Ok(tree) => tree,
			Err(_) => return Vec::new(),
		};
		let root = tree.root_node();
		let mut out = Vec::new();
		let mut stack = vec![(root, false)];
		while let Some((node, visited)) = stack.pop() {
			if visited {
				out.push(self.node_to_ref(node, language));
				continue;
			}
			stack.push((node, true));
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
					stack.push((child, false));
				}
			}
		}
		out
	}
	pub fn ancestors(&self, node_ref: &NodeRef) -> Vec<Arc<NodeRef>> {
		let mut out = Vec::new();
		let mut current = node_ref.parent();
		while let Some(node) = current {
			out.push(Arc::clone(&node));
			current = node.parent();
		}
		out
	}
	pub fn doc_clear(&self) {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.clear();
	}
	pub fn doc_text(&self, text: &str) -> DocId {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.push(Doc::Text(text.to_string()))
	}
	pub fn doc_range(&self, start: usize, end: usize) -> DocId {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.push(Doc::SourceRange { start, end })
	}
	pub fn doc_softline(&self) -> DocId {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.push(Doc::Line(LineKind::Soft))
	}
	pub fn doc_hardline(&self) -> DocId {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.push(Doc::Line(LineKind::Hard))
	}
	pub fn doc_concat(&self, docs_list: Vec<DocId>) -> DocId {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.push(Doc::Concat(docs_list))
	}
	pub fn doc_group(&self, doc: DocId) -> DocId {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.push(Doc::Group(doc))
	}
	pub fn doc_indent(&self, by: usize, doc: DocId) -> DocId {
		let mut docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.push(Doc::Indent { by, doc })
	}
	pub fn doc_render(&self, doc: DocId, width: usize) -> String {
		let docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.render(doc, width, &source)
	}
	pub fn doc_render_with_indent(
		&self,
		doc: DocId,
		width: usize,
		indent_style: crate::doc::IndentStyle,
		tab_width: usize) -> String {
		let docs = self.docs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		docs.render_with_indent(doc, width, &source, indent_style, tab_width)
	}
	// Clear cached ASTs after in-memory source edits.
	#[allow(dead_code)]
	fn clear_trees(&self) {
		let mut trees = self.trees
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		trees.clear();
	}
	pub(crate) fn cached_query(
		&self,
		language: &str,
		query: &str,
		lang: &tree_sitter::Language) -> Option<Arc<Query>> {
		let key = format!("{language}\0{query}");
		let mut queries = self.queries
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if let Some(existing) = queries.get(&key) {
			return Some(Arc::clone(existing));
		}
		let compiled = Arc::new(Query::new(lang, query).ok()?);
		queries.insert(key, Arc::clone(&compiled));
		Some(compiled)
	}
	fn lookup_language(&self, name: &str) -> Option<tree_sitter::Language> {
		let registry = self.registry
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		registry.get(name)
	}
	pub(crate) fn language_for(&self, name: &str) -> Option<tree_sitter::Language> {
		self.lookup_language(name)
	}
	fn precache_tree_internal(&self, tree: &Tree, language: &str) {
		let start = if self.profile_enabled() {
			Some(std::time::Instant::now())
		}
		else {
			None
		};
		let root = tree.root_node();
		let root_ref = self.node_to_ref(root, language);
		let mut stack = vec![(root, root_ref, false)];
		let mut node_count: usize = 0;
		let mut postorder: Vec<Arc<NodeRef>> = Vec::new();
		while let Some((node, node_ref, visited)) = stack.pop() {
			if visited {
				if node_ref.token_len
					.get()
					.is_none() {
					let token_len = if node.child_count() == 0 {
						node.end_byte() - node.start_byte()
					}
					else {
						if let Some(children) = node_ref.children_cache.get() {
							let mut sum: usize = 0;
							for child in children.iter() {
								sum += child.token_len();
							}
							sum
						}
						else {
							node_ref.compute_token_len()
						}
					};
					node_ref.set_token_len(token_len);
				}
				postorder.push(node_ref);
				continue;
			}
			node_count += 1;
			if node_ref.parent_cache
				.get()
				.is_none() {
				let parent = node.parent().map(|parent| Arc::downgrade(&self.node_to_ref(parent, language)));
				node_ref.set_parent_cache(parent);
			}
			let mut child_nodes = Vec::new();
			let mut cursor = node.walk();
			if cursor.goto_first_child() {
				loop {
					child_nodes.push(cursor.node());
					if !cursor.goto_next_sibling() {
						break;
					}
				}
			}
			if node_ref.children_cache
				.get()
				.is_none() {
				let mut children_refs = Vec::new();
				for child_node in child_nodes.iter() {
					children_refs.push(self.node_to_ref(*child_node, language));
				}
				if !children_refs.is_empty() {
					let parent_weak = Arc::downgrade(&node_ref);
					for i in 0..children_refs.len() {
						children_refs[i].set_parent_cache(Some(parent_weak.clone()));
						children_refs[i].set_index_cache(Some(i));
						if i + 1 < children_refs.len() {
							let next_weak = Arc::downgrade(&children_refs[i + 1]);
							children_refs[i].set_next_sibling_cache(Some(next_weak));
						}
						if i > 0 {
							let prev_weak = Arc::downgrade(&children_refs[i - 1]);
							children_refs[i].set_prev_sibling_cache(Some(prev_weak));
						}
					}
				}
				node_ref.set_children_cache(children_refs);
			}
			stack.push((node, Arc::clone(&node_ref), true));
			for child_node in child_nodes.into_iter().rev() {
				let child_ref = self.node_to_ref(child_node, language);
				stack.push((child_node, child_ref, false));
			}
		}
		self.set_precache_postorder(language, postorder);
		if let Some(start) = start {
			let elapsed = start.elapsed();
			self.record_trace("precache_tree", elapsed);
			let mut precache = self.precache_stats
				.lock()
				.unwrap_or_else(|e| e.into_inner());
			*precache = Some(
				PrecacheStats {
					nodes: node_count,
					total_ns: elapsed.as_nanos()
				}
			);
		}
	}
	fn parse_tree(&self, language: &str, lang: &tree_sitter::Language) -> Result<Tree, String> {
		let mut trees = self.trees
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if let Some(tree) = trees.get(language) {
			return Ok(tree.clone());
		}
		let mut parser = Parser::new();
		if let Err(err) = parser.set_language(lang) {
			eprintln!("[neatify] failed to set tree-sitter language '{language}': {err}");
			eprintln!(
			"[neatify] supported language versions: {MIN_COMPATIBLE_LANGUAGE_VERSION}..={LANGUAGE_VERSION}"
			);
			eprintln!(
			"[neatify] this usually means the parser .so was built with a different tree-sitter version"
			);
			return Err("failed to set language".to_string());
		}
		let source = self.source
			.lock()
			.map_err(|_| "lock failed".to_string())?;
		let tree = parser.parse(source.as_bytes(), None).ok_or_else(|| "failed to parse".to_string())?;
		trees.insert(language.to_string(), tree.clone());
		Ok(tree)
	}
	pub(crate) fn tree_for(&self, language: &str) -> Option<Tree> {
		let lang = self.lookup_language(language)?;
		self.parse_tree(language, &lang).ok()
	}
	pub fn precache_tree(&self, language: &str) -> Result<(), String> {
		let lang = self.lookup_language(language).ok_or_else(|| "unknown language".to_string())?;
		let tree = {
			let trees = self.trees
				.lock()
				.unwrap_or_else(|e| e.into_inner());
			trees.get(language).cloned()
		};
		let tree = match tree {
			Some(tree) => tree,
			None => self.parse_tree(language, &lang)?,
		};
		self.precache_tree_internal(&tree, language);
		Ok(())
	}
	pub(crate) fn source_bytes(&self) -> Vec<u8> {
		let source = self.source
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		source.as_bytes().to_vec()
	}
	fn with_node<F, R>(&self, node_ref: &NodeRef, f: F) -> Option<R>
	where F: FnOnce(Node) -> R {
		let lang = self.lookup_language(&node_ref.language)?;
		let tree = self.parse_tree(&node_ref.language, &lang).ok()?;
		let root = tree.root_node();
		let range = node_ref.range.clone();
		let candidate = root.descendant_for_byte_range(range.start, range.end)?;
		let node = if candidate.start_byte() == range.start
			&& candidate.end_byte() == range.end
			&& candidate.kind_id() == node_ref.native_kind_id() {
			candidate
		}
		else {
			Self::find_node_by_range_kind_id(root, &range, node_ref.native_kind_id())?
		};
		Some(f(node))
	}
	fn find_node_by_range_kind_id<'a>(
		root: Node<'a>,
		range: &Range,
		kind_id: u16) -> Option<Node<'a>> {
		let mut stack = vec![root];
		while let Some(node) = stack.pop() {
			if node.start_byte() == range.start
				&& node.end_byte() == range.end
				&& node.kind_id() == kind_id {
				return Some(node);
			}
			let mut child_cursor = node.walk();
			if child_cursor.goto_first_child() {
				loop {
					let child = child_cursor.node();
					stack.push(child);
					if !child_cursor.goto_next_sibling() {
						break;
					}
				}
			}
		}
		None
	}
	fn node_to_ref(&self, node: Node, language: &str) -> Arc<NodeRef> {
		let native_kind_id = node.kind_id();
		let canonical_kind_id = self.canonical_kind_id(language, native_kind_id);
		let key = NodeCacheKey {
			start: node.start_byte(),
			end: node.end_byte(),
			kind_id: native_kind_id,
			language: language.to_string()
		};
		let mut refs = self.node_refs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if let Some(existing) = refs.get(&key) {
			return Arc::clone(existing);
		}
		let range = Range::new(node.start_byte(), node.end_byte());
		let start_point = node.start_position();
		let end_point = node.end_position();
		let start_position = Position {
			row: start_point.row as usize,
			col: start_point.column as usize
		};
		let end_position = Position {
			row: end_point.row as usize,
			col: end_point.column as usize
		};
		let node_ref = Arc::new(
			NodeRef::new(
				self.clone(),
				range,
				start_position,
				end_position,
				canonical_kind_id,
				native_kind_id,
				language,
				node.is_error(),
				node.is_missing()
			)
		);
		if let Some(doc_id) = self.node_doc_ids
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.get(&key)
			.copied() {
			node_ref.set_doc_id(doc_id);
		}
		refs.insert(key, Arc::clone(&node_ref));
		node_ref
	}
	pub(crate) fn node_ref(&self, node: Node, language: &str) -> Arc<NodeRef> {
		self.node_to_ref(node, language)
	}
	pub(crate) fn set_node_doc_id(&self, node: &Node, language: &str, doc_id: INT) {
		let key = NodeCacheKey {
			start: node.start_byte(),
			end: node.end_byte(),
			kind_id: node.kind_id(),
			language: language.to_string()
		};
		let mut doc_ids = self.node_doc_ids
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		doc_ids.insert(key.clone(), doc_id);
		let refs = self.node_refs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if let Some(existing) = refs.get(&key) {
			existing.set_doc_id(doc_id);
		}
	}
	pub(crate) fn set_node_doc_id_ref(&self, node_ref: &NodeRef, doc_id: INT) {
		let key = node_ref.cache_key();
		let mut doc_ids = self.node_doc_ids
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		doc_ids.insert(key.clone(), doc_id);
		let refs = self.node_refs
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if let Some(existing) = refs.get(&key) {
			existing.set_doc_id(doc_id);
		}
	}
}

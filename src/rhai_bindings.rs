use crate::{formatting, Context, Line, Match, NodeRef, Position, Range};
use rhai::{Array, Dynamic, Engine, EvalAltResult, FnPtr, ImmutableString, Map, NativeCallContext, INT};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tree_sitter::{QueryCursor, StreamingIterator};

type RhaiResult <T> = Result<T, Box<EvalAltResult>>;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct NodeKey {
	start: usize,
	end: usize,
	kind_id: u16,
}

#[derive(Clone, Debug)]
struct TraceSpan {
	state: Option<Arc<Mutex<TraceSpanState>>>,
}

#[derive(Debug)]
struct TraceSpanState {
	ctx: Context,
	name: String,
	start: Instant,
	stopped: bool,
}

impl TraceSpan {
	fn start(ctx: Context, name: &str) -> Self {
		Self {
			state: Some(
				Arc::new(
					Mutex::new(
						TraceSpanState {
							ctx,
							name: name.to_string(),
							start: Instant::now(),
							stopped: false
						}
					)
				)
			)
		}
	}
	fn noop() -> Self {
		Self { state: None }
	}
	fn stop(&mut self) {
		let Some(state) = self.state.as_ref() else {
			return;
		};
		let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
		if state.stopped {
			return;
		}
		let elapsed = state.start.elapsed();
		state.stopped = true;
		state.ctx.record_trace(&state.name, elapsed);
	}
}
thread_local! {
	static ACTIVE_DOCS: RefCell<Option<Rc<RefCell<HashMap<NodeKey, INT>>>>> = RefCell::new(None);
}
impl NodeKey {
	fn from_node(node: &NodeRef) -> Self {
		let range = node.byte_range();
		Self {
			start: range.start,
			end: range.end,
			kind_id: node.kind_id()
		}
	}
	fn from_ts_node(node: &tree_sitter::Node) -> Self {
		Self {
			start: node.start_byte(),
			end: node.end_byte(),
			kind_id: node.kind_id()
		}
	}
}

fn last_index_of_str(haystack: &str, needle: &str) -> INT {
	if needle.is_empty() {
		return -1;
	}
	let h_chars: Vec<char> = haystack.chars().collect();
	let n_chars: Vec<char> = needle.chars().collect();
	if n_chars.len() > h_chars.len() {
		return -1;
	}
	let mut i = h_chars.len();
	while i >= n_chars.len() {
		let start = i - n_chars.len();
		let mut matched = true;
		let mut j = 0;
		while j < n_chars.len() {
			if h_chars[start + j] != n_chars[j] {
				matched = false;
				break;
			}
			j += 1;
		}
		if matched {
			return start as INT;
		}
		if i == 0 {
			break;
		}
		i -= 1;
	}
	-1
}

fn int_to_usize(value: INT, label: &str) -> RhaiResult<usize> {
	if value < 0 {
		return Err(format!("{label} must be >= 0").into());
	}
	Ok(value as usize)
}

fn matches_to_array(matches: Vec<Match>) -> Array {
	matches.into_iter().map(Dynamic::from).collect()
}

fn doc_from_string(ctx: &Context, text: &str) -> usize {
	let mut parts: Vec<usize> = Vec::new();
	let mut first = true;
	for line in text.split('\n') {
		if !first {
			parts.push(ctx.doc_hardline());
		}
		parts.push(ctx.doc_text(line));
		first = false;
	}
	if parts.is_empty() {
		return ctx.doc_text("");
	}
	ctx.doc_concat(parts)
}

fn collect_marked_captured(
	ctx: &Context,
	language: &str,
	lang: &tree_sitter::Language,
	root: tree_sitter::Node,
	source_bytes: &[u8],
	query_list: &[String]) -> (HashSet<NodeKey>, HashSet<NodeKey>) {
	let mut marked: HashSet<NodeKey> = HashSet::new();
	let mut captured: HashSet<NodeKey> = HashSet::new();
	for q in query_list.iter() {
		let Some(query) = ctx.cached_query(language, q, lang) else {
			continue;
		};
		let mut cursor = QueryCursor::new();
		let mut matches = cursor.matches(&query, root, source_bytes);
		while let Some(m) = matches.next() {
			for capture in m.captures.iter() {
				let node = capture.node;
				captured.insert(NodeKey::from_ts_node(&node));
				let mut current = Some(node);
				while let Some(n) = current {
					marked.insert(NodeKey::from_ts_node(&n));
					current = n.parent();
				}
			}
		}
	}
	marked.insert(NodeKey::from_ts_node(&root));
	(marked, captured)
}

fn collect_marked_captured_combined(
	ctx: &Context,
	language: &str,
	lang: &tree_sitter::Language,
	root: tree_sitter::Node,
	source_bytes: &[u8],
	query_list: &[String]) -> (HashSet<NodeKey>, HashSet<NodeKey>) {
	let mut marked: HashSet<NodeKey> = HashSet::new();
	let mut captured: HashSet<NodeKey> = HashSet::new();
	let combined = query_list.join("\n\n");
	if let Some(query) = ctx.cached_query(language, &combined, lang) {
		let mut cursor = QueryCursor::new();
		let mut matches = cursor.matches(&query, root, source_bytes);
		while let Some(m) = matches.next() {
			for capture in m.captures.iter() {
				let node = capture.node;
				captured.insert(NodeKey::from_ts_node(&node));
				let mut current = Some(node);
				while let Some(n) = current {
					marked.insert(NodeKey::from_ts_node(&n));
					current = n.parent();
				}
			}
		}
	}
	marked.insert(NodeKey::from_ts_node(&root));
	(marked, captured)
}

fn extract_node_types(query: &str) -> Vec<String> {
	let bytes = query.as_bytes();
	let mut out: Vec<String> = Vec::new();
	let mut i = 0;
	while i < bytes.len() {
		if bytes[i] != b'(' {
			i += 1;
			continue;
		}
		i += 1;
		while i < bytes.len() && bytes[i].is_ascii_whitespace() {
			i += 1;
		}
		if i >= bytes.len() {
			break;
		}
		let ch = bytes[i] as char;
		if ch == ')' || ch == '#' || ch == '@' {
			i += 1;
			continue;
		}
		let start = i;
		while i < bytes.len() {
			let c = bytes[i] as char;
			if c.is_ascii_alphanumeric() || c == '_' {
				i += 1;
				continue;
			}
			break;
		}
		if i > start {
			let name = &query[start..i];
			if name != "_" {
				out.push(name.to_string());
			}
		}
	}
	out
}

fn node_kind_tokens(name: &str) -> Vec<&str> {
	name.split('_').filter(|t| !t.is_empty()).collect()
}

fn validate_queries(
	language: &str,
	lang: &tree_sitter::Language,
	queries: &[String]) -> (Vec<String>, Vec<(String, Vec<(String, Vec<String>)>)>) {
	let mut kinds = HashSet::new();
	let mut kind_tokens: Vec<(String, Vec<String>)> = Vec::new();
	let count = lang.node_kind_count();
	for id in 0..count {
		if let Some(name) = lang.node_kind_for_id(id as u16) {
			kinds.insert(name.to_string());
			let tokens = node_kind_tokens(name).into_iter().map(|t| t.to_string()).collect::<Vec<_>>();
			kind_tokens.push((name.to_string(), tokens));
		}
	}
	let mut valid = Vec::new();
	let mut invalid: Vec<(String, Vec<(String, Vec<String>)>)> = Vec::new();
	for q in queries.iter() {
		let mut missing: Vec<(String, Vec<String>)> = Vec::new();
		let mut seen = HashSet::new();
		for name in extract_node_types(q).into_iter() {
			if !seen.insert(name.clone()) {
				continue;
			}
			if kinds.contains(&name) {
				continue;
			}
			let want_tokens = node_kind_tokens(&name).into_iter().map(|t| t.to_string()).collect::<Vec<_>>();
			let mut scored: Vec<(i64, String)> = Vec::new();
			for (kind, tokens) in kind_tokens.iter() {
				let mut score = 0;
				for t in want_tokens.iter() {
					if tokens.iter().any(|k| k == t) {
						score += 1;
					}
				}
				if score > 0 {
					scored.push((score, kind.clone()));
				}
			}
			scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
			let suggestions = scored.into_iter()
				.take(4)
				.map(|(_, s)| s)
				.collect();
			missing.push((name, suggestions));
		}
		if missing.is_empty() {
			valid.push(q.clone());
		}
		else {
			invalid.push((q.clone(), missing));
		}
	}
	if !invalid.is_empty() {
		eprintln!("[neatify] unsupported query node types ({language}) - script/binary mismatch");
		for (query, missing) in invalid.iter() {
			eprintln!("  Query: {query}");
			for (name, suggestions) in missing.iter() {
				if suggestions.is_empty() {
					eprintln!("    - {name}");
				}
				else {
					eprintln!("    - {name} (did you mean: {})", suggestions.join(", "));
				}
			}
		}
	}
	(valid, invalid)
}

pub fn register_primitives(engine: &mut Engine, ctx: Context) {
	engine.register_type_with_name::<Range>("Range");
	engine.register_type_with_name::<Line>("Line");
	engine.register_type_with_name::<Position>("Position");
	engine.register_type_with_name::<Arc<NodeRef>>("Node");
	engine.register_type_with_name::<Match>("Match");
	engine.register_type_with_name::<TraceSpan>("TraceSpan");
	engine.register_fn(
		"range",
		|start: INT, end: INT| -> RhaiResult<Range> {
			let start = int_to_usize(start, "start")?;
			let end = int_to_usize(end, "end")?;
			Ok(Range::new(start, end))
		}
	);
	let ctx_len = ctx.clone();
	engine.register_fn(
		"source_len",
		move || -> INT {
			ctx_len.source_len() as INT
		}
	);
	let ctx_cache_get = ctx.clone();
	engine.register_fn(
		"cache_get",
		move |key: &str| -> Dynamic {
			ctx_cache_get.cache_get(key).unwrap_or(Dynamic::UNIT)
		}
	);
	let ctx_cache_set = ctx.clone();
	engine.register_fn(
		"cache_set",
		move |key: &str, value: Dynamic| {
			ctx_cache_set.cache_set(key, value);
		}
	);
	engine.register_fn(
		"now_ms",
		|| -> INT {
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.map(|d| d.as_millis() as INT)
				.unwrap_or(0)
		}
	);
	let ctx_trace = ctx.clone();
	engine.register_fn(
		"trace",
		move |name: &str| -> TraceSpan {
			if ctx_trace.profile_enabled() {
				TraceSpan::start(ctx_trace.clone(), name)
			}
			else {
				TraceSpan::noop()
			}
		}
	);
	engine.register_fn(
		"end",
		|span: &mut TraceSpan| {
			span.stop();
		}
	);
	let ctx_set_output = ctx.clone();
	engine.register_fn(
		"set_output",
		move |text: &str| {
			ctx_set_output.set_output(text);
		}
	);
	let ctx_slice = ctx.clone();
	engine.register_fn(
		"slice",
		move |range: Range| -> String {
			ctx_slice.slice(&range)
		}
	);
	let ctx_line = ctx.clone();
	engine.register_fn(
		"line_at",
		move |pos: INT| -> RhaiResult<Line> {
			let pos = int_to_usize(pos, "pos")?;
			Ok(ctx_line.line_at(pos))
		}
	);
	let ctx_range_around = ctx.clone();
	engine.register_fn(
		"range_around",
		move |node: Arc<NodeRef>, before: INT, after: INT, same_line: bool| -> RhaiResult<Range> {
			let before = before as i64;
			let after = after as i64;
			Ok(ctx_range_around.range_around(&node, before, after, same_line))
		}
	);
	let ctx_query = ctx.clone();
	engine.register_fn(
		"query",
		move |language: &str, query: &str| -> Array {
			matches_to_array(ctx_query.query(language, query, None))
		}
	);
	let ctx_query_range = ctx.clone();
	engine.register_fn(
		"query",
		move |language: &str, query: &str, scope: Range| -> Array {
			matches_to_array(ctx_query_range.query(language, query, Some(&scope)))
		}
	);
	let ctx_query_node = ctx.clone();
	engine.register_fn(
		"query",
		move |language: &str, query: &str, scope: Arc<NodeRef>| -> Array {
			let scope_range = scope.byte_range();
			matches_to_array(ctx_query_node.query(language, query, Some(&scope_range)))
		}
	);
	let ctx_query_ranges = ctx.clone();
	engine.register_fn(
		"query_ranges",
		move |language: &str, query: &str| -> Array {
			let ranges = ctx_query_ranges.query_ranges(language, query);
			ranges.into_iter().map(Dynamic::from).collect()
		}
	);
	let ctx_root = ctx.clone();
	engine.register_fn(
		"root_node",
		move |language: &str| -> Option<Arc<NodeRef>> {
			ctx_root.root_node(language)
		}
	);
	let ctx_walk = ctx.clone();
	engine.register_fn(
		"walk",
		move |ctx_call: NativeCallContext, language: &str, queries: Array, rule: FnPtr| -> RhaiResult<INT> {
			let mut query_list = Vec::new();
			for item in queries {
				let q = item.into_string().map_err(|_| "walk query list expects strings")?;
				if !q.trim().is_empty() {
					query_list.push(q);
				}
			}
			if query_list.is_empty() {
				return Ok(0);
			}
			let Some(lang) = ctx_walk.language_for(language) else {
				return Ok(0);
			};
			let Some(tree) = ctx_walk.tree_for(language) else {
				return Ok(0);
			};
			let source_bytes = ctx_walk.source_bytes();
			let root = tree.root_node();
			let profile_enabled = ctx_walk.profile_enabled();
			let mut query_ms: i64 = 0;
			let mut match_count: i64 = 0;
			let mut capture_count: i64 = 0;
			let mut invalid_queries: i64 = 0;
			let mut used_combined = false;
			let (query_list, invalid) = validate_queries(language, &lang, &query_list);
			if !invalid.is_empty() {
				invalid_queries = invalid.len() as i64;
			}
			if query_list.is_empty() {
				return Ok(0);
			}
			let (marked, captured) = if profile_enabled {
				let query_start = Instant::now();
				let mut marked: HashSet<NodeKey> = HashSet::new();
				let mut captured: HashSet<NodeKey> = HashSet::new();
				if !ctx_walk.test_group_enabled() {
					let combined = query_list.join("\n\n");
					if let Some(query) = ctx_walk.cached_query(language, &combined, &lang) {
						used_combined = true;
						let mut cursor = QueryCursor::new();
						let mut matches = cursor.matches(&query, root, source_bytes.as_slice());
						while let Some(m) = matches.next() {
							match_count += 1;
							for capture in m.captures.iter() {
								capture_count += 1;
								let node = capture.node;
								captured.insert(NodeKey::from_ts_node(&node));
								let mut current = Some(node);
								while let Some(n) = current {
									marked.insert(NodeKey::from_ts_node(&n));
									current = n.parent();
								}
							}
						}
					}
					else {
						for q in query_list.iter() {
							let Some(query) = ctx_walk.cached_query(language, q, &lang) else {
								invalid_queries += 1;
								continue;
							};
							let mut cursor = QueryCursor::new();
							let mut matches = cursor.matches(&query, root, source_bytes.as_slice());
							while let Some(m) = matches.next() {
								match_count += 1;
								for capture in m.captures.iter() {
									capture_count += 1;
									let node = capture.node;
									captured.insert(NodeKey::from_ts_node(&node));
									let mut current = Some(node);
									while let Some(n) = current {
										marked.insert(NodeKey::from_ts_node(&n));
										current = n.parent();
									}
								}
							}
						}
					}
				}
				else {
					for q in query_list.iter() {
						let Some(query) = ctx_walk.cached_query(language, q, &lang) else {
							invalid_queries += 1;
							continue;
						};
						let mut cursor = QueryCursor::new();
						let mut matches = cursor.matches(&query, root, source_bytes.as_slice());
						while let Some(m) = matches.next() {
							match_count += 1;
							for capture in m.captures.iter() {
								capture_count += 1;
								let node = capture.node;
								captured.insert(NodeKey::from_ts_node(&node));
								let mut current = Some(node);
								while let Some(n) = current {
									marked.insert(NodeKey::from_ts_node(&n));
									current = n.parent();
								}
							}
						}
					}
				}
				marked.insert(NodeKey::from_ts_node(&root));
				query_ms = query_start.elapsed().as_millis() as i64;
				(marked, captured)
			}
			else if !ctx_walk.test_group_enabled() {
				collect_marked_captured_combined(
					&ctx_walk,
					language,
					&lang,
					root,
					source_bytes.as_slice(),
					&query_list
				)
			}
			else {
				collect_marked_captured(
					&ctx_walk,
					language,
					&lang,
					root,
					source_bytes.as_slice(),
					&query_list
				)
			};
			if ctx_walk.debug_enabled() {
				let mut stack = vec![root];
				while let Some(node) = stack.pop() {
					if node.is_error() || node.is_missing() {
						let kind_name = lang.node_kind_for_id(node.kind_id()).unwrap_or("unknown");
						eprintln!(
							"[neatify] error node in {language} at {}..{} ({kind_name})",
							node.start_byte(),
							node.end_byte()
						);
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
			}
			let traverse_start = Instant::now();
			let mut nodes: Vec<tree_sitter::Node> = Vec::new();
			let mut stack = vec![(root, false)];
			while let Some((node, visited)) = stack.pop() {
				let key = NodeKey::from_ts_node(&node);
				if !marked.contains(&key) {
					continue;
				}
				if visited {
					nodes.push(node);
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
						if marked.contains(&NodeKey::from_ts_node(&child)) {
							stack.push((child, false));
						}
					}
				}
			}
			if nodes.is_empty() {
				return Ok(0);
			}
			let traverse_ms = traverse_start.elapsed().as_millis() as i64;
			let build_start = Instant::now();
			let mut rule_calls: i64 = 0;
			let mut children_total: i64 = 0;
			let root_key = NodeKey::from_ts_node(nodes.last().unwrap());
			let mut root_doc_id: INT = 0;
			for node in nodes.iter() {
				let key = NodeKey::from_ts_node(node);
				let doc_id: INT = if captured.contains(&key) {
					rule_calls += 1;
					let node_ref = ctx_walk.node_ref(*node, language);
					if let Some(parent_node) = node.parent() {
						let parent_ref = ctx_walk.node_ref(parent_node, language);
						node_ref.set_parent_cache(Some(Arc::downgrade(&parent_ref)));
					}
					let mut children_refs = Vec::new();
					let mut cursor = node.walk();
					if cursor.goto_first_child() {
						loop {
							let child_node = cursor.node();
							children_refs.push(ctx_walk.node_ref(child_node, language));
							if !cursor.goto_next_sibling() {
								break;
							}
						}
					}
					if !children_refs.is_empty() {
						children_total += children_refs.len() as i64;
						for i in 0..children_refs.len() {
							let parent_weak = Arc::downgrade(&node_ref);
							children_refs[i].set_parent_cache(Some(parent_weak));
							if i + 1 < children_refs.len() {
								let next_weak = Arc::downgrade(&children_refs[i + 1]);
								children_refs[i].set_next_sibling_cache(Some(next_weak));
							}
							if i > 0 {
								let prev_weak = Arc::downgrade(&children_refs[i - 1]);
								children_refs[i].set_prev_sibling_cache(Some(prev_weak));
							}
						}
						node_ref.set_children_cache(children_refs);
					}
					if node_ref.is_error() || node_ref.is_missing() {
						if ctx_walk.debug_enabled() {
							let kind_name = lang.node_kind_for_id(node_ref.kind_id()).unwrap_or("unknown");
							eprintln!(
								"[neatify] error node in {language} at {}..{} ({kind_name})",
								node_ref.byte_range().start,
								node_ref.byte_range().end
							);
						}
						ctx_walk.doc_range(node.start_byte(), node.end_byte()) as INT
					}
					else {
						rule.call_within_context(&ctx_call, (node_ref.clone(), node_ref))
							.map_err(Box::<EvalAltResult>::from)?
					}
				}
				else {
					if (node.is_error() || node.is_missing()) && ctx_walk.debug_enabled() {
						let kind_name = lang.node_kind_for_id(node.kind_id()).unwrap_or("unknown");
						eprintln!(
							"[neatify] error node in {language} at {}..{} ({kind_name})",
							node.start_byte(),
							node.end_byte()
						);
					}
					let mut child_docs: Vec<usize> = Vec::new();
					let mut has_doc = false;
					let mut cursor = node.walk();
					let mut prev_end = node.start_byte();
					if cursor.goto_first_child() {
						loop {
							let child = cursor.node();
							let child_start = child.start_byte();
							let child_end = child.end_byte();
							if child_start > prev_end {
								child_docs.push(ctx_walk.doc_range(prev_end, child_start));
							}
							let child_ref = ctx_walk.node_ref(child, language);
							let child_doc = child_ref.doc_id();
							if child_doc >= 0 {
								child_docs.push(child_doc as usize);
								has_doc = true;
							}
							else {
								let raw = ctx_walk.doc_range(child_start, child_end);
								ctx_walk.set_node_doc_id(&child, language, raw as INT);
								child_docs.push(raw as usize);
							}
							prev_end = child_end;
							if !cursor.goto_next_sibling() {
								break;
							}
						}
					}
					if prev_end < node.end_byte() {
						child_docs.push(ctx_walk.doc_range(prev_end, node.end_byte()));
					}
					if has_doc {
						ctx_walk.doc_concat(child_docs) as INT
					}
					else {
						ctx_walk.doc_range(node.start_byte(), node.end_byte()) as INT
					}
				};
				ctx_walk.set_node_doc_id(node, language, doc_id);
				if key == root_key {
					root_doc_id = doc_id;
				}
			}
			let build_ms = build_start.elapsed().as_millis() as i64;
			if profile_enabled {
				let total_ms = query_ms + traverse_ms + build_ms;
				let mode = if ctx_walk.test_group_enabled() {
					"per-query"
				}
				else if used_combined {
					"combined"
				}
				else {
					"combined-fallback"
				};
				eprintln!("[neatify] walk profile ({language})");
				eprintln!("  ------------------------------------------------------------");
				if let Some((nodes, total_ns)) = ctx_walk.precache_stats() {
					let total_ms = total_ns as f64 / 1_000_000.0;
					let cache_miss = ctx_walk.cache_miss_count();
					eprintln!(
						"  Precache   : {:>9.3} ms | nodes {nodes} | cache_miss {cache_miss}",
						total_ms
					);
				}
				eprintln!("  Mode       : {mode}");
				eprintln!("  Time (ms)  : query {query_ms} | traverse {traverse_ms} | build {build_ms} | total {total_ms}");
				eprintln!("  Matches    : {match_count}   Captures: {capture_count}");
				eprintln!("  Marked     : {}   Captured: {}", marked.len(), captured.len());
				eprintln!("  Rule Calls : {rule_calls}   Children: {children_total}");
				if invalid_queries > 0 {
					eprintln!("  Invalid Qs : {invalid_queries}");
				}
				eprintln!("  ------------------------------------------------------------");
			}
			Ok(root_doc_id)
		}
	);
	engine.register_fn(
		"start",
		|range: &mut Range| -> INT {
			range.start as INT
		}
	);
	engine.register_fn(
		"end",
		|range: &mut Range| -> INT {
			range.end as INT
		}
	);
	engine.register_fn(
		"start_offset",
		|line: &mut Line| -> INT {
			line.start_offset as INT
		}
	);
	engine.register_fn(
		"end_offset",
		|line: &mut Line| -> INT {
			line.end_offset as INT
		}
	);
	engine.register_get(
		"row",
		|line: &mut Line| -> INT {
			line.row as INT
		}
	);
	engine.register_fn(
		"indent",
		|line: &mut Line| -> String {
			line.indent.clone()
		}
	);
	engine.register_fn(
		"text",
		|line: &mut Line| -> String {
			line.text.clone()
		}
	);
	engine.register_fn(
		"row",
		|pos: &mut Position| -> INT {
			pos.row as INT
		}
	);
	engine.register_fn(
		"col",
		|pos: &mut Position| -> INT {
			pos.col as INT
		}
	);
	engine.register_fn(
		"text",
		|node: &mut Arc<NodeRef>| -> String {
			node.text()
		}
	);
	engine.register_fn(
		"kind_id",
		|node: &mut Arc<NodeRef>| -> INT {
			node.kind_id() as INT
		}
	);
	engine.register_fn(
		"ancestor",
		|node: &mut Arc<NodeRef>, kinds: Array| -> RhaiResult<Dynamic> {
			let mut kind_ids: Vec<u16> = Vec::with_capacity(kinds.len());
			for item in kinds {
				let Some(id) = item.try_cast::<INT>() else {
					return Err("ancestor expects kind id integers".into());
				};
				if id < 0 || id > u16::MAX as INT {
					return Err("ancestor kind id out of range".into());
				}
				kind_ids.push(id as u16);
			}
			Ok(
				match node.ancestor(&kind_ids, false, false, false, None, None) {
					Some(parent) => Dynamic::from(parent),
					None => Dynamic::UNIT,
				}
			)
		}
	);
	engine.register_fn(
		"ancestor",
		|node: &mut Arc<NodeRef>, kinds: Array, opts: Map| -> RhaiResult<Dynamic> {
			let mut kind_ids: Vec<u16> = Vec::with_capacity(kinds.len());
			for item in kinds {
				let Some(id) = item.try_cast::<INT>() else {
					return Err("ancestor expects kind id integers".into());
				};
				if id < 0 || id > u16::MAX as INT {
					return Err("ancestor kind id out of range".into());
				}
				kind_ids.push(id as u16);
			}
			let mut furthest = false;
			let mut boundary = false;
			let mut include_self = false;
			let mut stop_kinds: Option<Vec<u16>> = None;
			let mut continue_kinds: Option<Vec<u16>> = None;
			if let Some(value) = opts.get("furthest") {
				furthest = value.clone().as_bool().map_err(|_| "ancestor opts.furthest expects boolean")?;
			}
			if let Some(value) = opts.get("boundary") {
				boundary = value.clone().as_bool().map_err(|_| "ancestor opts.boundary expects boolean")?;
			}
			if let Some(value) = opts.get("self") {
				include_self = value.clone().as_bool().map_err(|_| "ancestor opts.self expects boolean")?;
			}
			if let Some(value) = opts.get("stop") {
				let array = value.clone().try_cast::<Array>().ok_or("ancestor opts.stop expects array of kind ids")?;
				let mut kind_ids: Vec<u16> = Vec::with_capacity(array.len());
				for item in array {
					let Some(id) = item.try_cast::<INT>() else {
						return Err("ancestor opts.stop expects kind id integers".into());
					};
					if id < 0 || id > u16::MAX as INT {
						return Err("ancestor opts.stop kind id out of range".into());
					}
					kind_ids.push(id as u16);
				}
				if !kind_ids.is_empty() {
					stop_kinds = Some(kind_ids);
				}
			}
			if let Some(value) = opts.get("allow") {
				let array = value.clone().try_cast::<Array>().ok_or("ancestor opts.allow expects array of kind ids")?;
				let mut kind_ids: Vec<u16> = Vec::with_capacity(array.len());
				for item in array {
					let Some(id) = item.try_cast::<INT>() else {
						return Err("ancestor opts.allow expects kind id integers".into());
					};
					if id < 0 || id > u16::MAX as INT {
						return Err("ancestor opts.allow kind id out of range".into());
					}
					kind_ids.push(id as u16);
				}
				if !kind_ids.is_empty() {
					continue_kinds = Some(kind_ids);
				}
			}
			Ok(
				match node.ancestor(
					&kind_ids,
					furthest,
					boundary,
					include_self,
					stop_kinds.as_deref(),
					continue_kinds.as_deref()
				) {
					Some(parent) => Dynamic::from(parent),
					None => Dynamic::UNIT,
				}
			)
		}
	);
	// Deprecated: NodeRef child-view compatibility helpers.
	engine.register_get(
		"len",
		|node: &mut Arc<NodeRef>| -> INT {
			node.child_count() as INT
		}
	);
	engine.register_fn(
		"kind",
		|node: &mut Arc<NodeRef>, idx: INT| -> RhaiResult<INT> {
			let idx = int_to_usize(idx, "idx")?;
			Ok(node.child_kind_id(idx).map(|k| k as INT).unwrap_or(-1))
		}
	);
	engine.register_fn(
		"range",
		|node: &mut Arc<NodeRef>, idx: INT| -> RhaiResult<Range> {
			let idx = int_to_usize(idx, "idx")?;
			Ok(node.child_range(idx).unwrap_or_else(|| Range::new(0, 0)))
		}
	);
	engine.register_fn(
		"doc",
		|node: &mut Arc<NodeRef>, idx: INT| -> RhaiResult<Dynamic> {
			let idx = int_to_usize(idx, "idx")?;
			let id = node.child_doc_id(idx).unwrap_or(-1);
			if id < 0 {
				return Ok(Dynamic::UNIT);
			}
			Ok(Dynamic::from_int(id))
		}
	);
	engine.register_fn(
		"child",
		|node: &mut Arc<NodeRef>, idx: INT| -> RhaiResult<Dynamic> {
			let idx = int_to_usize(idx, "idx")?;
			Ok(
				match node.child(idx) {
					Some(child) => Dynamic::from(child),
					None => Dynamic::UNIT,
				}
			)
		}
	);
	engine.register_fn(
		"child_count",
		|node: &mut Arc<NodeRef>| -> INT {
			node.child_count() as INT
		}
	);
	engine.register_fn(
		"doc_id",
		|node: &mut Arc<NodeRef>| -> RhaiResult<Dynamic> {
			let id = node.doc_id();
			if id < 0 {
				return Ok(Dynamic::UNIT);
			}
			Ok(Dynamic::from_int(id))
		}
	);
	engine.register_fn(
		"set_doc_id",
		|node: &mut Arc<NodeRef>, doc_id: INT| -> RhaiResult<Dynamic> {
			node.set_doc_id(doc_id as i64);
			Ok(Dynamic::UNIT)
		}
	);
	engine.register_fn(
		"start_position",
		|node: &mut Arc<NodeRef>| -> Position {
			node.start_position()
		}
	);
	engine.register_fn(
		"end_position",
		|node: &mut Arc<NodeRef>| -> Position {
			node.end_position()
		}
	);
	engine.register_fn(
		"range",
		|node: &mut Arc<NodeRef>| -> Range {
			node.byte_range()
		}
	);
	// Deprecated: use node.range() instead.
	engine.register_fn(
		"byte_range",
		|node: &mut Arc<NodeRef>| -> Range {
			node.byte_range()
		}
	);
	engine.register_fn(
		"line",
		|node: &mut Arc<NodeRef>| -> Line {
			node.line()
		}
	);
	engine.register_fn(
		"parent",
		|node: &mut Arc<NodeRef>| -> Dynamic {
			match node.parent() {
				Some(n) => Dynamic::from(n),
				None => Dynamic::UNIT,
			}
		}
	);
	engine.register_fn(
		"children",
		|node: &mut Arc<NodeRef>| -> Array {
			node.children()
				.into_iter()
				.map(Dynamic::from)
				.collect()
		}
	);
	engine.register_fn(
		"next_sibling",
		|node: &mut Arc<NodeRef>| -> Option<Arc<NodeRef>> {
			node.next_sibling()
		}
	);
	engine.register_fn(
		"prev_sibling",
		|node: &mut Arc<NodeRef>| -> Option<Arc<NodeRef>> {
			node.prev_sibling()
		}
	);
	let ctx_doc_reset = ctx.clone();
	engine.register_fn(
		"doc_reset",
		move || {
			ctx_doc_reset.doc_clear();
		}
	);
	let ctx_doc_text = ctx.clone();
	engine.register_fn(
		"doc_text",
		move |text: &str| -> INT {
			ctx_doc_text.doc_text(text) as INT
		}
	);
	let ctx_doc_range = ctx.clone();
	engine.register_fn(
		"doc_range",
		move |start: INT, end: INT| -> RhaiResult<INT> {
			let start = int_to_usize(start, "start")?;
			let end = int_to_usize(end, "end")?;
			Ok(ctx_doc_range.doc_range(start, end) as INT)
		}
	);
	let ctx_doc_node = ctx.clone();
	engine.register_fn(
		"doc_node",
		move |node: &mut Arc<NodeRef>| -> INT {
			let range = node.byte_range();
			ctx_doc_node.doc_range(range.start, range.end) as INT
		}
	);
	engine.register_fn(
		"doc_for_node",
		move |node: &mut Arc<NodeRef>| -> RhaiResult<Dynamic> {
			let id = node.doc_id();
			if id < 0 {
				return Ok(Dynamic::UNIT);
			}
			Ok(Dynamic::from_int(id))
		}
	);
	let ctx_doc_softline = ctx.clone();
	engine.register_fn(
		"doc_softline",
		move || -> INT {
			ctx_doc_softline.doc_softline() as INT
		}
	);
	let ctx_doc_hardline = ctx.clone();
	engine.register_fn(
		"doc_hardline",
		move || -> INT {
			ctx_doc_hardline.doc_hardline() as INT
		}
	);
	let ctx_doc_concat = ctx.clone();
	engine.register_fn(
		"doc_concat",
		move |docs: Array| -> RhaiResult<INT> {
			let mut list = Vec::new();
			for d in docs {
				let value = d.as_int().map_err(|_| "doc_concat expects int doc ids")?;
				list.push(value as usize);
			}
			Ok(ctx_doc_concat.doc_concat(list) as INT)
		}
	);
	let ctx_doc_group = ctx.clone();
	engine.register_fn(
		"doc_group",
		move |doc: INT| -> RhaiResult<INT> {
			let id = int_to_usize(doc, "doc")?;
			Ok(ctx_doc_group.doc_group(id) as INT)
		}
	);
	let ctx_doc_indent = ctx.clone();
	engine.register_fn(
		"doc_indent",
		move |by: INT, doc: INT| -> RhaiResult<INT> {
			let by = int_to_usize(by, "by")?;
			let id = int_to_usize(doc, "doc")?;
			Ok(ctx_doc_indent.doc_indent(by, id) as INT)
		}
	);
	let ctx_doc_render = ctx.clone();
	engine.register_fn(
		"doc_render",
		move |doc: INT, width: INT| -> RhaiResult<String> {
			let doc = int_to_usize(doc, "doc")?;
			let width = int_to_usize(width, "width")?;
			Ok(ctx_doc_render.doc_render(doc, width))
		}
	);
	let ctx_kind_id = ctx.clone();
	engine.register_fn(
		"kind_id",
		move |language: &str, name: &str, named: bool| -> RhaiResult<INT> {
			let Some(lang) = ctx_kind_id.language_for(language) else {
				return Ok(-1);
			};
			Ok(lang.id_for_node_kind(name, named) as INT)
		}
	);
	let ctx_kind_ids = ctx.clone();
	engine.register_fn(
		"kind_ids",
		move |language: &str, names: Array, named_flags: Array| -> RhaiResult<Array> {
			let Some(lang) = ctx_kind_ids.language_for(language) else {
				return Ok(Array::new());
			};
			if names.len() != named_flags.len() {
				return Err("kind_ids expects names and flags same length".into());
			}
			let mut out = Array::new();
			for i in 0..names.len() {
				let name = names[i].clone().into_string().map_err(|_| "kind_ids expects string names")?;
				let named = named_flags[i].clone().as_bool().map_err(|_| "kind_ids expects boolean flags")?;
				let id = lang.id_for_node_kind(&name, named) as INT;
				out.push(Dynamic::from_int(id));
			}
			Ok(out)
		}
	);
	let ctx_kind_ids_map = ctx.clone();
	engine.register_fn(
		"kind_ids_map",
		move |language: &str, names: Array, named_flags: Array| -> RhaiResult<Map> {
			let Some(lang) = ctx_kind_ids_map.language_for(language) else {
				return Ok(Map::new());
			};
			if names.len() != named_flags.len() {
				return Err("kind_ids_map expects names and flags same length".into());
			}
			let mut out = Map::new();
			for i in 0..names.len() {
				let name = names[i].clone().into_string().map_err(|_| "kind_ids_map expects string names")?;
				let named = named_flags[i].clone().as_bool().map_err(|_| "kind_ids_map expects boolean flags")?;
				let id = lang.id_for_node_kind(&name, named) as INT;
				out.insert(name.into(), Dynamic::from_int(id));
			}
			Ok(out)
		}
	);
	let ctx_doc_render_indent = ctx.clone();
	engine.register_fn(
		"doc_render_with_indent",
		move |doc: INT, width: INT, indent_style: &str, tab_width: INT| -> RhaiResult<String> {
			let doc = int_to_usize(doc, "doc")?;
			let width = int_to_usize(width, "width")?;
			let tab_width = int_to_usize(tab_width, "tab_width")?.max(1);
			let style = if indent_style == "tabs" {
				crate::doc::IndentStyle::Tabs
			}
			else {
				crate::doc::IndentStyle::Spaces
			};
			Ok(ctx_doc_render_indent.doc_render_with_indent(doc, width, style, tab_width))
		}
	);
	let ctx_format_fragment = ctx.clone();
	engine.register_fn(
		"format_fragment_doc",
		move |language: &str, range: Range| -> RhaiResult<Dynamic> {
			let source = ctx_format_fragment.source_text();
			let roots = ctx_format_fragment.registry_roots();
			let settings = ctx_format_fragment.settings();
			let formatted = formatting::format_fragment_optional(
				&source,
				&range,
				language,
				&roots,
				&settings,
				false,
				ctx_format_fragment.profile_enabled(),
				ctx_format_fragment.test_group_enabled(),
				ctx_format_fragment.strict_enabled()
			);
			if let Some(text) = formatted {
				let doc_id = doc_from_string(&ctx_format_fragment, &text);
				Ok(Dynamic::from_int(doc_id as INT))
			}
			else {
				Ok(Dynamic::UNIT)
			}
		}
	);
	engine.register_fn(
		"captures",
		|m: &mut Match| -> Map {
			let mut map = Map::new();
			for (k, v) in m.captures().into_iter() {
				map.insert(k.into(), Dynamic::from(v));
			}
			map
		}
	);
	engine.register_fn(
		"trim",
		|s: &mut ImmutableString| -> String {
			s.trim().to_string()
		}
	);
	engine.register_fn(
		"trim_start",
		|s: &mut ImmutableString| -> String {
			s.trim_start().to_string()
		}
	);
	engine.register_fn(
		"trim_end",
		|s: &mut ImmutableString| -> String {
			s.trim_end().to_string()
		}
	);
	engine.register_fn(
		"len",
		|s: &mut ImmutableString| -> INT {
			s.chars().count() as INT
		}
	);
	engine.register_fn(
		"is_empty",
		|s: &mut ImmutableString| -> bool {
			s.is_empty()
		}
	);
	engine.register_fn(
		"last_index_of",
		|s: &mut ImmutableString, needle: &str| -> INT {
			last_index_of_str(s, needle)
		}
	);
}

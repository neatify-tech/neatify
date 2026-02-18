use crate::api::{FindMode, FindResult, FindSpec};
use crate::{formatting, Context, Line, Match, NodeRef, Position, Range};
use rhai::{
    Array, Dynamic, Engine, EvalAltResult, FnPtr, ImmutableString, Map, NativeCallContext, INT,
};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tree_sitter::{QueryCursor, StreamingIterator};

type RhaiResult<T> = Result<T, Box<EvalAltResult>>;

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
            state: Some(Arc::new(Mutex::new(TraceSpanState {
                ctx,
                name: name.to_string(),
                start: Instant::now(),
                stopped: false,
            }))),
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
            kind_id: node.kind_id(),
        }
    }
    fn from_ts_node(node: &tree_sitter::Node) -> Self {
        Self {
            start: node.start_byte(),
            end: node.end_byte(),
            kind_id: node.kind_id(),
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

fn parse_find_mode(value: &str) -> RhaiResult<FindMode> {
    match value {
        "first" => Ok(FindMode::First),
        "last" => Ok(FindMode::Last),
        "all" => Ok(FindMode::All),
        _ => Err("find mode must be 'first', 'last', or 'all'".into()),
    }
}

fn parse_kind_id(value: Dynamic, label: &str) -> RhaiResult<u16> {
    let kind_id = value
        .try_cast::<INT>()
        .ok_or_else(|| format!("{label} expects integer"))?;
    if kind_id < 0 || kind_id > u16::MAX as INT {
        return Err(format!("{label} out of range").into());
    }
    Ok(kind_id as u16)
}

fn parse_kind_id_list(value: Dynamic, label: &str) -> RhaiResult<Vec<u16>> {
    if let Some(kind_id) = value.clone().try_cast::<INT>() {
        return Ok(vec![parse_kind_id(Dynamic::from_int(kind_id), label)?]);
    }
    let list = value
        .clone()
        .try_cast::<Array>()
        .ok_or_else(|| format!("{label} expects integer or array"))?;
    let mut out = Vec::with_capacity(list.len());
    for item in list {
        out.push(parse_kind_id(item, label)?);
    }
    Ok(out)
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
    query_list: &[String],
) -> (HashSet<NodeKey>, HashSet<NodeKey>) {
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
    query_list: &[String],
) -> (HashSet<NodeKey>, HashSet<NodeKey>) {
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

fn parse_simple_kind_query(query: &str) -> Option<String> {
    let bytes = query.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'(' {
        return None;
    }
    i += 1;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
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
    if i == start {
        return None;
    }
    let name = &query[start..i];
    if name == "_" {
        return None;
    }
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b')' {
        return None;
    }
    i += 1;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i < bytes.len() {
        if bytes[i] != b'@' {
            return None;
        }
        i += 1;
        let cap_start = i;
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_alphanumeric() || c == '_' {
                i += 1;
                continue;
            }
            break;
        }
        if i == cap_start {
            return None;
        }
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i < bytes.len() {
            return None;
        }
    }
    Some(name.to_string())
}

fn simple_kind_ids(
    ctx: &Context,
    language: &str,
    lang: &tree_sitter::Language,
    queries: &[String],
) -> Option<HashSet<u16>> {
    let mut kind_ids: HashSet<u16> = HashSet::new();
    for query in queries.iter() {
        let Some(name) = parse_simple_kind_query(query) else {
            return None;
        };
        let canonical = lang.id_for_node_kind(&name, true);
        if lang.node_kind_is_supertype(canonical) {
            return None;
        }
        let ids = ctx
            .named_kind_ids(language, &name)
            .unwrap_or_else(|| vec![canonical]);
        for id in ids {
            kind_ids.insert(id);
        }
    }
    if kind_ids.is_empty() {
        None
    } else {
        Some(kind_ids)
    }
}

fn node_kind_tokens(name: &str) -> Vec<&str> {
    name.split('_').filter(|t| !t.is_empty()).collect()
}

fn gray_text(text: &str) -> String {
    if std::env::var("NO_COLOR").is_ok() {
        return text.to_string();
    }
    if let Ok(term) = std::env::var("TERM") {
        if term == "dumb" {
            return text.to_string();
        }
    }
    format!("\x1b[90m{text}\x1b[0m")
}

fn validate_queries(
    language: &str,
    lang: &tree_sitter::Language,
    queries: &[String],
) -> (Vec<String>, Vec<(String, Vec<(String, Vec<String>)>)>) {
    let mut kinds = HashSet::new();
    let mut kind_tokens: Vec<(String, Vec<String>)> = Vec::new();
    let count = lang.node_kind_count();
    for id in 0..count {
        if let Some(name) = lang.node_kind_for_id(id as u16) {
            kinds.insert(name.to_string());
            let tokens = node_kind_tokens(name)
                .into_iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>();
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
            let want_tokens = node_kind_tokens(&name)
                .into_iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>();
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
            let suggestions = scored.into_iter().take(4).map(|(_, s)| s).collect();
            missing.push((name, suggestions));
        }
        if missing.is_empty() {
            valid.push(q.clone());
        } else {
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
                } else {
                    eprintln!("    - {name} (did you mean: {})", suggestions.join(", "));
                }
            }
        }
    }
    (valid, invalid)
}

fn capture_kind_counts_sorted(
    counts: &HashMap<u16, i64>,
    lang: &tree_sitter::Language,
) -> Vec<(u16, String, i64)> {
    let mut items: Vec<(u16, String, i64)> = counts
        .iter()
        .map(|(id, count)| {
            let name = lang.node_kind_for_id(*id).unwrap_or("unknown");
            (*id, name.to_string(), *count)
        })
        .collect();
    items.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.1.cmp(&b.1)));
    items
}

fn walk_impl(
    ctx_walk: &Context,
    ctx_call: NativeCallContext,
    language: &str,
    queries: Array,
    rule: FnPtr,
    rewrite: bool,
) -> RhaiResult<INT> {
    let mut query_list = Vec::new();
    let mut kind_id_list: Vec<u16> = Vec::new();
    let mut has_strings = false;
    let mut has_ints = false;
    for item in queries {
        if item.is::<INT>() {
            if has_strings {
                return Err("walk list expects all strings or all ints".into());
            }
            has_ints = true;
            let value = item.as_int().unwrap_or(0);
            let value = int_to_usize(value, "kind_id")?;
            if value > u16::MAX as usize {
                return Err("walk kind_id exceeds u16".into());
            }
            kind_id_list.push(value as u16);
        } else if item.is::<ImmutableString>() || item.is::<String>() {
            if has_ints {
                return Err("walk list expects all strings or all ints".into());
            }
            has_strings = true;
            let q = item
                .into_string()
                .map_err(|_| "walk query list expects strings")?;
            if !q.trim().is_empty() {
                query_list.push(q);
            }
        } else {
            return Err("walk list expects strings or ints".into());
        }
    }
    if has_strings && query_list.is_empty() {
        return Ok(0);
    }
    if has_ints && kind_id_list.is_empty() {
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
    let mut node_ref_ns: u128 = 0;
    let mut node_ref_calls: i64 = 0;
    let mut kind_id_set: Option<HashSet<u16>> = if has_ints {
        let mut expanded: HashSet<u16> = HashSet::new();
        for id in kind_id_list.iter().copied() {
            if let Some(name) = lang.node_kind_for_id(id) {
                if lang.node_kind_is_named(id) {
                    if let Some(ids) = ctx_walk.named_kind_ids(language, name) {
                        for entry in ids {
                            expanded.insert(entry);
                        }
                        continue;
                    }
                }
            }
            expanded.insert(id);
        }
        Some(expanded)
    } else {
        None
    };
    let mut rewrite_used = false;
    if has_strings {
        let (validated, invalid) = validate_queries(language, &lang, &query_list);
        query_list = validated;
        if !invalid.is_empty() {
            invalid_queries = invalid.len() as i64;
        }
        if query_list.is_empty() {
            return Ok(0);
        }
        if rewrite && kind_id_set.is_none() && !ctx_walk.test_group_enabled() {
            if let Some(kind_ids) = simple_kind_ids(ctx_walk, language, &lang, &query_list) {
                kind_id_set = Some(kind_ids);
                rewrite_used = true;
            }
        }
    }
    if let Some(kind_ids) = kind_id_set {
        if let Some(nodes) = ctx_walk.precache_postorder(language) {
            if nodes.is_empty() {
                return Ok(0);
            }
            let mut capture_kind_counts: Option<HashMap<u16, i64>> = if profile_enabled {
                Some(HashMap::new())
            } else {
                None
            };
            let rewrite_kind_names: Option<Vec<(String, Vec<u16>)>> =
                if profile_enabled && rewrite_used {
                    let mut seen = HashSet::new();
                    let mut names: Vec<(String, Vec<u16>)> = Vec::new();
                    for query in query_list.iter() {
                        let Some(name) = parse_simple_kind_query(query) else {
                            continue;
                        };
                        if !seen.insert(name.clone()) {
                            continue;
                        }
                        let ids = ctx_walk
                            .named_kind_ids(language, &name)
                            .unwrap_or_else(|| vec![lang.id_for_node_kind(&name, true)]);
                        names.push((name, ids));
                    }
                    names.sort_by(|a, b| a.0.cmp(&b.0));
                    Some(names)
                } else {
                    None
                };
            let mut captured: HashSet<NodeKey> = HashSet::new();
            let mut marked: HashSet<NodeKey> = HashSet::new();
            for node_ref in nodes.iter() {
                if kind_ids.contains(&node_ref.native_kind_id()) {
                    let key = NodeKey::from_node(node_ref);
                    captured.insert(key.clone());
                    if let Some(counts) = capture_kind_counts.as_mut() {
                        let entry = counts.entry(node_ref.native_kind_id()).or_insert(0);
                        *entry += 1;
                    }
                    let mut current = Some(node_ref.clone());
                    while let Some(node) = current {
                        marked.insert(NodeKey::from_node(&node));
                        current = node.parent();
                    }
                }
            }
            if let Some(root_ref) = nodes.last() {
                marked.insert(NodeKey::from_node(root_ref));
            }
            let traverse_start = Instant::now();
            let traverse_ms = traverse_start.elapsed().as_millis() as i64;
            let build_start = Instant::now();
            let mut rule_calls: i64 = 0;
            let mut children_total: i64 = 0;
            let mut root_doc_id: INT = 0;
            capture_count = captured.len() as i64;
            let root_key = nodes.last().map(|node| NodeKey::from_node(node.as_ref()));
            for node_ref in nodes.iter() {
                let key = NodeKey::from_node(node_ref);
                if !marked.contains(&key) {
                    continue;
                }
                let doc_id: INT = if captured.contains(&key) {
                    rule_calls += 1;
                    let captured_children = node_ref.children();
                    if !captured_children.is_empty() {
                        children_total += captured_children.len() as i64;
                    }
                    if (node_ref.is_error() || node_ref.is_missing()) && ctx_walk.debug_enabled() {
                        let kind_name = lang
                            .node_kind_for_id(node_ref.kind_id())
                            .unwrap_or("unknown");
                        let range = node_ref.byte_range();
                        eprintln!(
                            "[neatify] error node in {language} at {}..{} ({kind_name})",
                            range.start, range.end
                        );
                        ctx_walk.doc_range(range.start, range.end) as INT
                    } else {
                        rule.call_within_context(&ctx_call, (node_ref.clone(),))
                            .map_err(Box::<EvalAltResult>::from)?
                    }
                } else {
                    if (node_ref.is_error() || node_ref.is_missing()) && ctx_walk.debug_enabled() {
                        let kind_name = lang
                            .node_kind_for_id(node_ref.kind_id())
                            .unwrap_or("unknown");
                        let range = node_ref.byte_range();
                        eprintln!(
                            "[neatify] error node in {language} at {}..{} ({kind_name})",
                            range.start, range.end
                        );
                    }
                    let mut child_docs: Vec<usize> = Vec::new();
                    let mut has_doc = false;
                    let range = node_ref.byte_range();
                    let mut prev_end = range.start;
                    let children = node_ref.children();
                    if !children.is_empty() {
                        for child_ref in children.iter() {
                            let child_range = child_ref.byte_range();
                            let child_start = child_range.start;
                            let child_end = child_range.end;
                            if child_start > prev_end {
                                child_docs.push(ctx_walk.doc_range(prev_end, child_start));
                            }
                            let child_doc = child_ref.doc_id();
                            if child_doc >= 0 {
                                child_docs.push(child_doc as usize);
                                has_doc = true;
                            } else {
                                let raw = ctx_walk.doc_range(child_start, child_end);
                                ctx_walk.set_node_doc_id_ref(child_ref, raw as INT);
                                child_docs.push(raw as usize);
                            }
                            prev_end = child_end;
                        }
                    }
                    if prev_end < range.end {
                        child_docs.push(ctx_walk.doc_range(prev_end, range.end));
                    }
                    if has_doc {
                        ctx_walk.doc_concat(child_docs) as INT
                    } else {
                        ctx_walk.doc_range(range.start, range.end) as INT
                    }
                };
                ctx_walk.set_node_doc_id_ref(node_ref, doc_id);
                if root_key.as_ref() == Some(&key) {
                    root_doc_id = doc_id;
                }
            }
            let build_ms = build_start.elapsed().as_millis() as i64;
            if profile_enabled {
                let total_ms = query_ms + traverse_ms + build_ms;
                let marked_count = marked.len();
                let node_ref_ms = node_ref_ns as f64 / 1_000_000.0;
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
                eprintln!("  Mode       : kind-ids");
                eprintln!("  Time (ms)  : query {query_ms} | traverse {traverse_ms} | build {build_ms} | total {total_ms}");
                eprintln!("  Matches    : {match_count}   Captures: {capture_count}");
                eprintln!("  Marked     : {marked_count}   Captured: {capture_count}");
                eprintln!("  Rule Calls : {rule_calls}   Children: {children_total}");
                eprintln!(
                    "  NodeRef    : {:>9.3} ms | calls {node_ref_calls}",
                    node_ref_ms
                );
                if let Some(counts) = capture_kind_counts.as_ref() {
                    let mut count_by_name: HashMap<String, i64> = HashMap::new();
                    let mut ids_by_name: HashMap<String, Vec<u16>> = HashMap::new();
                    for (id, count) in counts.iter() {
                        let name = lang.node_kind_for_id(*id).unwrap_or("unknown").to_string();
                        *count_by_name.entry(name.clone()).or_insert(0) += *count;
                        ids_by_name.entry(name).or_default().push(*id);
                    }
                    if let Some(names) = rewrite_kind_names.as_ref() {
                        let mut captured: Vec<String> = Vec::new();
                        let mut uncaptured: Vec<String> = Vec::new();
                        for (name, _) in names.iter() {
                            let count = count_by_name.get(name).copied().unwrap_or(0);
                            if count > 0 {
                                captured.push(name.clone());
                            } else {
                                uncaptured.push(name.clone());
                            }
                        }
                        captured.sort();
                        uncaptured.sort();
                        eprintln!("  Captured Kinds:");
                        for name in captured.iter() {
                            let count = count_by_name.get(name).copied().unwrap_or(0);
                            let mut ids = ids_by_name.get(name).cloned().unwrap_or_default();
                            ids.sort();
                            let id_text = gray_text(
                                &ids.iter()
                                    .map(|id| format!("#{id}"))
                                    .collect::<Vec<_>>()
                                    .join(","),
                            );
                            eprintln!("    {name} ({id_text}): {count}");
                        }
                        if !uncaptured.is_empty() {
                            eprintln!("  Uncaptured Kinds:");
                            for name in uncaptured.iter() {
                                let ids = names
                                    .iter()
                                    .find(|(n, _)| n == name)
                                    .map(|(_, ids)| ids.clone())
                                    .unwrap_or_default();
                                let mut ids = ids;
                                ids.sort();
                                let id_text = gray_text(
                                    &ids.iter()
                                        .map(|id| format!("#{id}"))
                                        .collect::<Vec<_>>()
                                        .join(","),
                                );
                                eprintln!("    {name} ({id_text}): 0");
                            }
                        }
                    } else if !counts.is_empty() {
                        eprintln!("  Captured Kinds:");
                        let mut names: Vec<String> = count_by_name.keys().cloned().collect();
                        names.sort();
                        for name in names {
                            let count = count_by_name.get(&name).copied().unwrap_or(0);
                            let mut ids = ids_by_name.get(&name).cloned().unwrap_or_default();
                            ids.sort();
                            let id_text = gray_text(
                                &ids.iter()
                                    .map(|id| format!("#{id}"))
                                    .collect::<Vec<_>>()
                                    .join(","),
                            );
                            eprintln!("    {name} ({id_text}): {count}");
                        }
                    }
                }
                if invalid_queries > 0 {
                    eprintln!("  Invalid Qs : {invalid_queries}");
                }
                eprintln!("  ------------------------------------------------------------");
            }
            return Ok(root_doc_id);
        }
        return Ok(0);
    }
    let mut capture_kind_counts: Option<HashMap<u16, i64>> = if profile_enabled {
        Some(HashMap::new())
    } else {
        None
    };
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
                        if let Some(counts) = capture_kind_counts.as_mut() {
                            let entry = counts.entry(node.kind_id()).or_insert(0);
                            *entry += 1;
                        }
                        captured.insert(NodeKey::from_ts_node(&node));
                        let mut current = Some(node);
                        while let Some(n) = current {
                            marked.insert(NodeKey::from_ts_node(&n));
                            current = n.parent();
                        }
                    }
                }
            } else {
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
                            if let Some(counts) = capture_kind_counts.as_mut() {
                                let entry = counts.entry(node.kind_id()).or_insert(0);
                                *entry += 1;
                            }
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
        } else {
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
                        if let Some(counts) = capture_kind_counts.as_mut() {
                            let entry = counts.entry(node.kind_id()).or_insert(0);
                            *entry += 1;
                        }
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
    } else if !ctx_walk.test_group_enabled() {
        collect_marked_captured_combined(
            ctx_walk,
            language,
            &lang,
            root,
            source_bytes.as_slice(),
            &query_list,
        )
    } else {
        collect_marked_captured(
            ctx_walk,
            language,
            &lang,
            root,
            source_bytes.as_slice(),
            &query_list,
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
            let node_ref = if profile_enabled {
                let start = Instant::now();
                let node_ref = ctx_walk.node_ref(*node, language);
                node_ref_ns += start.elapsed().as_nanos();
                node_ref_calls += 1;
                node_ref
            } else {
                ctx_walk.node_ref(*node, language)
            };
            if let Some(parent_node) = node.parent() {
                let parent_ref = if profile_enabled {
                    let start = Instant::now();
                    let parent_ref = ctx_walk.node_ref(parent_node, language);
                    node_ref_ns += start.elapsed().as_nanos();
                    node_ref_calls += 1;
                    parent_ref
                } else {
                    ctx_walk.node_ref(parent_node, language)
                };
                node_ref.set_parent_cache(Some(Arc::downgrade(&parent_ref)));
            }
            let mut children_refs = Vec::new();
            let mut cursor = node.walk();
            if cursor.goto_first_child() {
                loop {
                    let child_node = cursor.node();
                    let child_ref = if profile_enabled {
                        let start = Instant::now();
                        let child_ref = ctx_walk.node_ref(child_node, language);
                        node_ref_ns += start.elapsed().as_nanos();
                        node_ref_calls += 1;
                        child_ref
                    } else {
                        ctx_walk.node_ref(child_node, language)
                    };
                    children_refs.push(child_ref);
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
                    let kind_name = lang
                        .node_kind_for_id(node_ref.kind_id())
                        .unwrap_or("unknown");
                    eprintln!(
                        "[neatify] error node in {language} at {}..{} ({kind_name})",
                        node_ref.byte_range().start,
                        node_ref.byte_range().end
                    );
                }
                ctx_walk.doc_range(node.start_byte(), node.end_byte()) as INT
            } else {
                rule.call_within_context(&ctx_call, (node_ref.clone(),))
                    .map_err(Box::<EvalAltResult>::from)?
            }
        } else {
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
                    let child_ref = if profile_enabled {
                        let start = Instant::now();
                        let child_ref = ctx_walk.node_ref(child, language);
                        node_ref_ns += start.elapsed().as_nanos();
                        node_ref_calls += 1;
                        child_ref
                    } else {
                        ctx_walk.node_ref(child, language)
                    };
                    let child_doc = child_ref.doc_id();
                    if child_doc >= 0 {
                        child_docs.push(child_doc as usize);
                        has_doc = true;
                    } else {
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
            } else {
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
        } else if used_combined {
            "combined"
        } else {
            "combined-fallback"
        };
        let node_ref_ms = node_ref_ns as f64 / 1_000_000.0;
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
        eprintln!(
            "  Marked     : {}   Captured: {}",
            marked.len(),
            captured.len()
        );
        eprintln!("  Rule Calls : {rule_calls}   Children: {children_total}");
        eprintln!(
            "  NodeRef    : {:>9.3} ms | calls {node_ref_calls}",
            node_ref_ms
        );
        if let Some(counts) = capture_kind_counts.as_ref() {
            if !counts.is_empty() {
                eprintln!("  Captured Kinds:");
                for (id, name, count) in capture_kind_counts_sorted(counts, &lang) {
                    let id_text = gray_text(&format!("#{id}"));
                    eprintln!("    {name} ({id_text}): {count}");
                }
            }
        }
        if invalid_queries > 0 {
            eprintln!("  Invalid Qs : {invalid_queries}");
        }
        eprintln!("  ------------------------------------------------------------");
    }
    Ok(root_doc_id)
}

pub fn register_primitives(engine: &mut Engine, ctx: Context) {
    engine.register_type_with_name::<Range>("Range");
    engine.register_type_with_name::<Line>("Line");
    engine.register_type_with_name::<Position>("Position");
    engine.register_type_with_name::<Arc<NodeRef>>("Node");
    engine.register_type_with_name::<Match>("Match");
    engine.register_type_with_name::<TraceSpan>("TraceSpan");
    engine.register_fn("range", |start: INT, end: INT| -> RhaiResult<Range> {
        let start = int_to_usize(start, "start")?;
        let end = int_to_usize(end, "end")?;
        Ok(Range::new(start, end))
    });
    let ctx_len = ctx.clone();
    engine.register_fn("source_len", move || -> INT { ctx_len.source_len() as INT });
    let ctx_cache_get = ctx.clone();
    engine.register_fn("cache_get", move |key: &str| -> Dynamic {
        ctx_cache_get.cache_get(key).unwrap_or(Dynamic::UNIT)
    });
    let ctx_cache_set = ctx.clone();
    engine.register_fn("cache_set", move |key: &str, value: Dynamic| {
        ctx_cache_set.cache_set(key, value);
    });
    engine.register_fn("now_ms", || -> INT {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as INT)
            .unwrap_or(0)
    });
    let ctx_trace = ctx.clone();
    engine.register_fn("trace", move |name: &str| -> TraceSpan {
        if ctx_trace.profile_enabled() {
            TraceSpan::start(ctx_trace.clone(), name)
        } else {
            TraceSpan::noop()
        }
    });
    engine.register_fn("end", |span: &mut TraceSpan| {
        span.stop();
    });
    let ctx_set_output = ctx.clone();
    engine.register_fn("set_output", move |text: &str| {
        ctx_set_output.set_output(text);
    });
    let ctx_slice = ctx.clone();
    engine.register_fn("slice", move |range: Range| -> String {
        ctx_slice.slice(&range)
    });
    let ctx_line = ctx.clone();
    engine.register_fn("line_at", move |pos: INT| -> RhaiResult<Line> {
        let pos = int_to_usize(pos, "pos")?;
        Ok(ctx_line.line_at(pos))
    });
    let ctx_range_around = ctx.clone();
    engine.register_fn(
        "range_around",
        move |node: Arc<NodeRef>, before: INT, after: INT, same_line: bool| -> RhaiResult<Range> {
            let before = before as i64;
            let after = after as i64;
            Ok(ctx_range_around.range_around(&node, before, after, same_line))
        },
    );
    let ctx_query = ctx.clone();
    engine.register_fn("query", move |language: &str, query: &str| -> Array {
        matches_to_array(ctx_query.query(language, query, None))
    });
    let ctx_query_range = ctx.clone();
    engine.register_fn(
        "query",
        move |language: &str, query: &str, scope: Range| -> Array {
            matches_to_array(ctx_query_range.query(language, query, Some(&scope)))
        },
    );
    let ctx_query_node = ctx.clone();
    engine.register_fn(
        "query",
        move |language: &str, query: &str, scope: Arc<NodeRef>| -> Array {
            let scope_range = scope.byte_range();
            matches_to_array(ctx_query_node.query(language, query, Some(&scope_range)))
        },
    );
    let ctx_query_ranges = ctx.clone();
    engine.register_fn(
        "query_ranges",
        move |language: &str, query: &str| -> Array {
            let ranges = ctx_query_ranges.query_ranges(language, query);
            ranges.into_iter().map(Dynamic::from).collect()
        },
    );
    let ctx_root = ctx.clone();
    engine.register_fn("root_node", move |language: &str| -> Option<Arc<NodeRef>> {
        ctx_root.root_node(language)
    });
    let ctx_walk = ctx.clone();
    engine.register_fn(
        "walk",
        move |ctx_call: NativeCallContext,
              language: &str,
              queries: Array,
              rule: FnPtr|
              -> RhaiResult<INT> {
            walk_impl(&ctx_walk, ctx_call, language, queries, rule, true)
        },
    );
    let ctx_walk_opts = ctx.clone();
    engine.register_fn(
        "walk",
        move |ctx_call: NativeCallContext,
              language: &str,
              queries: Array,
              rule: FnPtr,
              opts: Map|
              -> RhaiResult<INT> {
            let mut rewrite = true;
            if let Some(value) = opts.get("rewrite") {
                rewrite = value
                    .clone()
                    .as_bool()
                    .map_err(|_| "walk opts.rewrite expects boolean")?;
            }
            walk_impl(&ctx_walk_opts, ctx_call, language, queries, rule, rewrite)
        },
    );
    engine.register_fn("start", |range: &mut Range| -> INT { range.start as INT });
    engine.register_fn("end", |range: &mut Range| -> INT { range.end as INT });
    engine.register_fn("start_offset", |line: &mut Line| -> INT {
        line.start_offset as INT
    });
    engine.register_fn("end_offset", |line: &mut Line| -> INT {
        line.end_offset as INT
    });
    engine.register_get("row", |line: &mut Line| -> INT { line.row as INT });
    engine.register_fn("indent", |line: &mut Line| -> String {
        line.indent.clone()
    });
    engine.register_fn("text", |line: &mut Line| -> String { line.text.clone() });
    engine.register_fn("row", |pos: &mut Position| -> INT { pos.row as INT });
    engine.register_fn("col", |pos: &mut Position| -> INT { pos.col as INT });
    engine.register_fn("text", |node: &mut Arc<NodeRef>| -> String { node.text() });
    engine.register_fn("kind_id", |node: &mut Arc<NodeRef>| -> INT {
        node.kind_id() as INT
    });
    engine.register_fn("token_len", |node: &mut Arc<NodeRef>| -> INT {
        node.token_len() as INT
    });
    engine.register_fn("index", |node: &mut Arc<NodeRef>| -> Dynamic {
        match node.index() {
            Some(index) => Dynamic::from_int(index as INT),
            None => Dynamic::UNIT,
        }
    });
    engine.register_fn(
        "find",
        |node: &mut Arc<NodeRef>, specs: Array| -> RhaiResult<Array> {
            let mut parsed: Vec<FindSpec> = Vec::with_capacity(specs.len());
            for spec in specs {
                let spec_map = spec
                    .try_cast::<Map>()
                    .ok_or("find expects an array of maps")?;
                if spec_map.contains_key("recursive") {
                    return Err("find recursive is not supported yet".into());
                }
                let kind_id = if let Some(kind_value) = spec_map.get("kind_id") {
                    Some(parse_kind_id_list(kind_value.clone(), "find kind_id")?)
                } else {
                    None
                };
                let mut mode = FindMode::First;
                if let Some(mode_value) = spec_map.get("mode") {
                    let mode_text = mode_value
                        .clone()
                        .into_string()
                        .map_err(|_| "find mode expects string")?;
                    let mode_text = mode_text.trim().to_lowercase();
                    mode = parse_find_mode(&mode_text)?;
                }
                let exclude_kind_id = if let Some(exclude_value) = spec_map.get("exclude_kind_id") {
                    parse_kind_id_list(exclude_value.clone(), "find exclude_kind_id")?
                } else {
                    Vec::new()
                };
                let before_kind_id = if let Some(value) = spec_map.get("before_kind_id") {
                    Some(parse_kind_id_list(value.clone(), "find before_kind_id")?)
                } else {
                    None
                };
                let after_kind_id = if let Some(value) = spec_map.get("after_kind_id") {
                    Some(parse_kind_id_list(value.clone(), "find after_kind_id")?)
                } else {
                    None
                };
                let mut before_index: Option<usize> = None;
                if let Some(before_value) = spec_map.get("before_index") {
                    let before_int = before_value
                        .clone()
                        .try_cast::<INT>()
                        .ok_or("find before_index expects integer")?;
                    before_index = Some(int_to_usize(before_int, "before_index")?);
                }
                let mut after_index: Option<usize> = None;
                if let Some(after_value) = spec_map.get("after_index") {
                    let after_int = after_value
                        .clone()
                        .try_cast::<INT>()
                        .ok_or("find after_index expects integer")?;
                    after_index = Some(int_to_usize(after_int, "after_index")?);
                }
                parsed.push(FindSpec {
                    kind_id,
                    mode,
                    exclude_kind_id,
                    before_kind_id,
                    after_kind_id,
                    before_index,
                    after_index,
                });
            }
            let results = node.find(&parsed);
            let mut out = Array::with_capacity(results.len());
            for result in results.into_iter() {
                match result {
                    FindResult::None => out.push(Dynamic::UNIT),
                    FindResult::Single(node) => out.push(Dynamic::from(node)),
                    FindResult::Many(nodes) => {
                        let array: Array = nodes.into_iter().map(Dynamic::from).collect();
                        out.push(Dynamic::from(array));
                    }
                }
            }
            Ok(out)
        },
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
                },
            )
        },
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
                furthest = value
                    .clone()
                    .as_bool()
                    .map_err(|_| "ancestor opts.furthest expects boolean")?;
            }
            if let Some(value) = opts.get("boundary") {
                boundary = value
                    .clone()
                    .as_bool()
                    .map_err(|_| "ancestor opts.boundary expects boolean")?;
            }
            if let Some(value) = opts.get("self") {
                include_self = value
                    .clone()
                    .as_bool()
                    .map_err(|_| "ancestor opts.self expects boolean")?;
            }
            if let Some(value) = opts.get("stop") {
                let array = value
                    .clone()
                    .try_cast::<Array>()
                    .ok_or("ancestor opts.stop expects array of kind ids")?;
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
                let array = value
                    .clone()
                    .try_cast::<Array>()
                    .ok_or("ancestor opts.allow expects array of kind ids")?;
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
                    continue_kinds.as_deref(),
                ) {
                    Some(parent) => Dynamic::from(parent),
                    None => Dynamic::UNIT,
                },
            )
        },
    );
    // Deprecated: NodeRef child-view compatibility helpers.
    engine.register_get("len", |node: &mut Arc<NodeRef>| -> INT {
        node.child_count() as INT
    });
    engine.register_fn(
        "kind",
        |node: &mut Arc<NodeRef>, idx: INT| -> RhaiResult<INT> {
            let idx = int_to_usize(idx, "idx")?;
            Ok(node.child_kind_id(idx).map(|k| k as INT).unwrap_or(-1))
        },
    );
    engine.register_fn(
        "range",
        |node: &mut Arc<NodeRef>, idx: INT| -> RhaiResult<Range> {
            let idx = int_to_usize(idx, "idx")?;
            Ok(node.child_range(idx).unwrap_or_else(|| Range::new(0, 0)))
        },
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
        },
    );
    engine.register_fn(
        "child",
        |node: &mut Arc<NodeRef>, idx: INT| -> RhaiResult<Dynamic> {
            let idx = int_to_usize(idx, "idx")?;
            Ok(match node.child(idx) {
                Some(child) => Dynamic::from(child),
                None => Dynamic::UNIT,
            })
        },
    );
    engine.register_fn("child_count", |node: &mut Arc<NodeRef>| -> INT {
        node.child_count() as INT
    });
    engine.register_fn("doc_id", |node: &mut Arc<NodeRef>| -> RhaiResult<Dynamic> {
        let id = node.doc_id();
        if id < 0 {
            return Ok(Dynamic::UNIT);
        }
        Ok(Dynamic::from_int(id))
    });
    engine.register_fn(
        "set_doc_id",
        |node: &mut Arc<NodeRef>, doc_id: INT| -> RhaiResult<Dynamic> {
            node.set_doc_id(doc_id as i64);
            Ok(Dynamic::UNIT)
        },
    );
    engine.register_fn("start_position", |node: &mut Arc<NodeRef>| -> Position {
        node.start_position()
    });
    engine.register_fn("end_position", |node: &mut Arc<NodeRef>| -> Position {
        node.end_position()
    });
    engine.register_fn("range", |node: &mut Arc<NodeRef>| -> Range {
        node.byte_range()
    });
    // Deprecated: use node.range() instead.
    engine.register_fn("byte_range", |node: &mut Arc<NodeRef>| -> Range {
        node.byte_range()
    });
    engine.register_fn("line", |node: &mut Arc<NodeRef>| -> Line { node.line() });
    engine.register_fn("parent", |node: &mut Arc<NodeRef>| -> Dynamic {
        match node.parent() {
            Some(n) => Dynamic::from(n),
            None => Dynamic::UNIT,
        }
    });
    engine.register_fn("children", |node: &mut Arc<NodeRef>| -> Array {
        node.children().into_iter().map(Dynamic::from).collect()
    });
    engine.register_fn(
        "next_sibling",
        |node: &mut Arc<NodeRef>| -> Option<Arc<NodeRef>> { node.next_sibling() },
    );
    engine.register_fn(
        "prev_sibling",
        |node: &mut Arc<NodeRef>| -> Option<Arc<NodeRef>> { node.prev_sibling() },
    );
    let ctx_doc_reset = ctx.clone();
    engine.register_fn("doc_reset", move || {
        ctx_doc_reset.doc_clear();
    });
    let ctx_doc_text = ctx.clone();
    engine.register_fn("doc_text", move |text: &str| -> INT {
        ctx_doc_text.doc_text(text) as INT
    });
    let ctx_doc_range = ctx.clone();
    engine.register_fn(
        "doc_range",
        move |start: INT, end: INT| -> RhaiResult<INT> {
            let start = int_to_usize(start, "start")?;
            let end = int_to_usize(end, "end")?;
            Ok(ctx_doc_range.doc_range(start, end) as INT)
        },
    );
    let ctx_doc_node = ctx.clone();
    engine.register_fn("doc_node", move |node: &mut Arc<NodeRef>| -> INT {
        let range = node.byte_range();
        ctx_doc_node.doc_range(range.start, range.end) as INT
    });
    engine.register_fn(
        "doc_for_node",
        move |node: &mut Arc<NodeRef>| -> RhaiResult<Dynamic> {
            let id = node.doc_id();
            if id < 0 {
                return Ok(Dynamic::UNIT);
            }
            Ok(Dynamic::from_int(id))
        },
    );
    let ctx_doc_softline = ctx.clone();
    engine.register_fn("doc_softline", move || -> INT {
        ctx_doc_softline.doc_softline() as INT
    });
    let ctx_doc_hardline = ctx.clone();
    engine.register_fn("doc_hardline", move || -> INT {
        ctx_doc_hardline.doc_hardline() as INT
    });
    let ctx_doc_concat = ctx.clone();
    engine.register_fn("doc_concat", move |docs: Array| -> RhaiResult<INT> {
        let mut list = Vec::new();
        for d in docs {
            let value = d.as_int().map_err(|_| "doc_concat expects int doc ids")?;
            list.push(value as usize);
        }
        Ok(ctx_doc_concat.doc_concat(list) as INT)
    });
    let ctx_doc_group = ctx.clone();
    engine.register_fn("doc_group", move |doc: INT| -> RhaiResult<INT> {
        let id = int_to_usize(doc, "doc")?;
        Ok(ctx_doc_group.doc_group(id) as INT)
    });
    let ctx_doc_indent = ctx.clone();
    engine.register_fn("doc_indent", move |by: INT, doc: INT| -> RhaiResult<INT> {
        let by = int_to_usize(by, "by")?;
        let id = int_to_usize(doc, "doc")?;
        Ok(ctx_doc_indent.doc_indent(by, id) as INT)
    });
    let ctx_doc_render = ctx.clone();
    engine.register_fn(
        "doc_render",
        move |doc: INT, width: INT| -> RhaiResult<String> {
            let doc = int_to_usize(doc, "doc")?;
            let width = int_to_usize(width, "width")?;
            Ok(ctx_doc_render.doc_render(doc, width))
        },
    );
    let ctx_kind_id = ctx.clone();
    engine.register_fn(
        "kind_id",
        move |language: &str, name: &str, named: bool| -> RhaiResult<INT> {
            let Some(lang) = ctx_kind_id.language_for(language) else {
                return Ok(-1);
            };
            Ok(lang.id_for_node_kind(name, named) as INT)
        },
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
                let name = names[i]
                    .clone()
                    .into_string()
                    .map_err(|_| "kind_ids expects string names")?;
                let named = named_flags[i]
                    .clone()
                    .as_bool()
                    .map_err(|_| "kind_ids expects boolean flags")?;
                let id = lang.id_for_node_kind(&name, named) as INT;
                out.push(Dynamic::from_int(id));
            }
            Ok(out)
        },
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
                let name = names[i]
                    .clone()
                    .into_string()
                    .map_err(|_| "kind_ids_map expects string names")?;
                let named = named_flags[i]
                    .clone()
                    .as_bool()
                    .map_err(|_| "kind_ids_map expects boolean flags")?;
                let id = lang.id_for_node_kind(&name, named) as INT;
                out.insert(name.into(), Dynamic::from_int(id));
            }
            Ok(out)
        },
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
            } else {
                crate::doc::IndentStyle::Spaces
            };
            Ok(ctx_doc_render_indent.doc_render_with_indent(doc, width, style, tab_width))
        },
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
                ctx_format_fragment.strict_enabled(),
            );
            if let Some(text) = formatted {
                let doc_id = doc_from_string(&ctx_format_fragment, &text);
                Ok(Dynamic::from_int(doc_id as INT))
            } else {
                Ok(Dynamic::UNIT)
            }
        },
    );
    engine.register_fn("captures", |m: &mut Match| -> Map {
        let mut map = Map::new();
        for (k, v) in m.captures().into_iter() {
            map.insert(k.into(), Dynamic::from(v));
        }
        map
    });
    engine.register_fn("trim", |s: &mut ImmutableString| -> String {
        s.trim().to_string()
    });
    engine.register_fn("trim_start", |s: &mut ImmutableString| -> String {
        s.trim_start().to_string()
    });
    engine.register_fn("trim_end", |s: &mut ImmutableString| -> String {
        s.trim_end().to_string()
    });
    engine.register_fn("len", |s: &mut ImmutableString| -> INT {
        s.chars().count() as INT
    });
    engine.register_fn("is_empty", |s: &mut ImmutableString| -> bool {
        s.is_empty()
    });
    engine.register_fn(
        "last_index_of",
        |s: &mut ImmutableString, needle: &str| -> INT { last_index_of_str(s, needle) },
    );
}

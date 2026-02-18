# Neatify

In the age of AI-generated code, it’s easier than ever to be a prolific polyglot. When you’re jumping between five different languages a day, the challenge isn't the syntax—it's maintaining consistency across fragmented ecosystems.

Most formatters are either limited to a handful of languages or are so opinionated that you’re stuck with their "one true way."

**Neatify** was built to solve this. It is a universal, scriptable formatting and linting engine designed for a world where code moves fast.

## Why Neatify?

* **Tree-sitter Powered:** If a Tree-sitter grammar exists for a language, Neatify can format and lint it.
* **Scriptable (Rhai):** Formatting rules aren't hidden in a JSON file; they are live scripts. You have total control over the "opinion" of the formatter.
* **Repository First:** Switch between entirely different style repositories seamlessly. Use the community standard, or fork one to create your own "flavor."
* **AI-Native Workflow:** I built this project using Rust and Rhai—languages I was unfamiliar with—by coordinating with AI. Because the formatting logic is scripted in Rhai, you can easily use an LLM to "describe" a formatting style and have it generate the Neatify script for you.

# Scriptable formatting

For those who want to build their own formatters, here is a small example of what such a script might look like:

```rust
fn doc_if_expression(node) {
	// find specific parts of the AST
	let hits = node.find([
		#{ kind_id: kinds::block },
		#{ kind_id: kinds::else_clause },
		#{ exclude_kind_id: kinds::kw_if, before_kind_id: kinds::block }
	]);
	// with no "mode" defined for the queries, the default is "first" which means the result contains a singular node
	// the results are returned in the order of the queries
	let block_node = hits[0];
	let else_node = hits[1];
	let cond_node = hits[2];
	if block_node == () || cond_node == () {
		return doc_text(text(node));
	}
	let block_doc = doc_for_node_or_range(block_node);
	let cond_doc = doc_for_node_or_range(cond_node);
	if else_node != () {
		let else_doc = doc_for_node_or_range(else_node);
		return doc_concat_list([
			doc_text("if "),
			cond_doc,
			doc_text(" "),
			block_doc,
			doc_hardline(),
			else_doc
		]);
	}
	doc_concat_list([doc_text("if "), cond_doc, doc_text(" "), block_doc])
}
```

# Status & Roadmap

Neatify is currently in Beta. It is a "self-hosted" project: the formatters for Rust and Rhai were built and refined by formatting Neatify's own source code.

⚠️ Important: Neatify is still in active development. While you can use --stdout to preview changes without modifying files, the default behavior is to modify source code in place. At this stage, I strongly recommend a "Commit-First" workflow so you can easily rollback changes if an edge case is handled unexpectedly.

Roadmap:

- **Coverage**: Gradually increasing edge-case coverage for the current language suite (the "long tail" of formatting).
- **Performance:** I've already spent quite a bit of time finetuning performance but I believe there are still some gains to be made.
- **LSP:** there is some initial support for LSP but it needs to be fleshed out
- **Linting:** Expanding the logic library to include more out-of-the-box linting rules beyond basic formatting.

# Getting Started

Typical usage (no custom repos):

```bash
neatify
```

On first run -if you haven't added an explicit repository- Neatify adds the core repository to `~/.neatify/config.toml` and syncs it automatically. After that, running `neatify` formats all supported files in the current directory (recursive), based on file extensions.

See all options:

```bash
neatify --help
```

## Format a Single Language

```bash
neatify java
neatify typescript
```

You can also pass globs or explicit files:

```bash
neatify "src/**/*.ts" "web/**/*.html"
```

## Ignore Files

Neatify respects `.gitignore` by default and also reads `.neatifyignore` (same syntax). You can add additional ignore patterns to `~/.neatify/config.toml`.

# Repositories

Repositories are listed in `~/.neatify/config.toml` in priority order (top to bottom). The first repo that contains a language spec wins, with fallback to later repos.

Add a remote repo:

```bash
neatify --repository-add myrepo https://example.com/repository
```

Add a local repo (no syncing):

```bash
neatify --repository-add local /home/alex/formatter-registry
```

List repos:

```bash
neatify --repository-list
```

Promote/demote priority:

```bash
neatify --repository-promote myrepo
neatify --repository-demote core
```

Sync remote repos:

```bash
neatify --sync
```

Sync tests as well:

```bash
neatify --sync-tests
```

## Config File

Example `~/.neatify/config.toml`:

```toml
[[repositories]]
name = "core"
path = "/home/alex/.neatify/repository/core"
url = "https://example.com/neatify/core"
version = "v5.0.0"

[[repositories]]
name = "local"
path = "/home/alex/formatter-registry"

ignore = [
  "**/node_modules/**",
  "**/dist/**"
]
```

Local repositories do not have a `url` and are never polled during `--sync`.

# Testing

The formatters are tested with a list of testcases where we have an "in" and "out" file.

Run tests for all locally available languages:

```bash
neatify --test
```

Run tests for a single language:

```bash
neatify --test java
```

Tests are only run if they exist locally. Use `--sync-tests` to download them for remote repositories.

# Build a grammar (Linux example)

```bash
git clone --depth 1 https://github.com/tree-sitter/tree-sitter-java
cd tree-sitter-java
tree-sitter generate
cc -fPIC -shared -o java.so src/parser.c src/scanner.c -I src
```

The tree-sitter CLI version used for generating parser.c: `0.26.3`.

General rebuild steps:

1) Clone the grammar repo (source and tag/commit are listed in each language's `language.toml`).
2) Run `tree-sitter generate` (CLI 0.26.3).
3) Build the shared library:
   - If the grammar has a C scanner (`scanner.c`), include it in the compile.
   - If the grammar has a C++ scanner (`scanner.cc`), compile `parser.c` with `cc`, `scanner.cc` with `c++`, then link both objects into a shared library.
4) Place the binary under:
   `repository/core/treesitter/<os>/<arch>/<language>.{ext}`

Where `{ext}` is:

- Linux: `so`
- macOS: `dylib`
- Windows: `dll`

The `{os}` and `{arch}` folder names come from Rust's `std::env::consts::OS` and `std::env::consts::ARCH`, so they must match exactly. Common values:

- `os`: `linux`, `macos`, `windows`
- `arch`: `x86_64`, `aarch64`, `arm`, `i686`

## Rhai API (Rust bindings)

Node API
- `text(node)`: get a Node source text.
- `kind_id(node)`: get a Node kind id.
- `token_len(node)`: get total token byte length (no whitespace).
- `index(node)`: get cached child index (or unit).
- `find(node, specs)`: find child nodes by specs.
- `ancestor(node, kinds)`: find the nearest ancestor with matching kinds.
- `ancestor(node, kinds, opts)`: ancestor search with options (`furthest`, `boundary`, `self`, `stop`, `allow`).
- `len(node)`: child count (deprecated child-view alias).
- `kind(node, idx)`: child kind id at index.
- `range(node, idx)`: child range at index.
- `doc(node, idx)`: child doc id at index.
- `child(node, idx)`: child node at index.
- `child_count(node)`: number of children.
- `doc_id(node)`: get a Node doc id (or unit).
- `set_doc_id(node, id)`: set a Node doc id.
- `start_position(node)`: get start Position for a Node.
- `end_position(node)`: get end Position for a Node.
- `range(node)`: get a Node byte range.
- `byte_range(node)`: get a Node byte range (deprecated alias).
- `line(node)`: get the Line for a Node start.
- `parent(node)`: get a Node parent (or unit).
- `children(node)`: get Node children array.
- `next_sibling(node)`: get next sibling (or unit).
- `prev_sibling(node)`: get previous sibling (or unit).

Doc API
- `doc_reset()`: clear the doc store.
- `doc_text(text)`: create a text doc.
- `doc_range(start, end)`: create a doc from a source range.
- `doc_node(node)`: create a doc from a Node range.
- `doc_for_node(node)`: fetch a cached doc id (or unit).
- `doc_softline()`: create a soft line break doc.
- `doc_hardline()`: create a hard line break doc.
- `doc_concat(docs)`: concatenate docs.
- `doc_group(doc)`: group a doc.
- `doc_indent(by, doc)`: indent a doc.
- `doc_render(doc, width)`: render a doc to text.
- `doc_render_with_indent(doc, width, indent_style, tab_width)`: render with indent style.

Query & walk
- `query(language, query)`: run a tree-sitter query.
- `query(language, query, scope: Range)`: run a scoped query by range.
- `query(language, query, scope: Node)`: run a scoped query by node.
- `query_ranges(language, query)`: return match ranges for a query.
- `root_node(language)`: get the root Node for a language.
- `walk(language, queries, rule)`: walk the AST with a rule callback.
- `walk(language, queries, rule, opts)`: walk with options (`rewrite`).
- `captures(match)`: return capture map for a query match.

Range, line, and position
- `range(start, end)`: create a byte range.
- `start(range)`: get a range start offset.
- `end(range)`: get a range end offset.
- `line_at(pos)`: return the Line at a byte position.
- `range_around(node, before, after, same_line)`: expand a range around a node.
- `start_offset(line)`: get a Line start offset.
- `end_offset(line)`: get a Line end offset.
- `row(line)`: get a Line row index.
- `indent(line)`: get a Line leading whitespace.
- `text(line)`: get a Line text.
- `row(position)`: get a Position row.
- `col(position)`: get a Position column.

Language and formatting helpers
- `kind_id(language, name, named)`: look up a kind id.
- `kind_ids(language, names, named_flags)`: look up multiple kind ids.
- `kind_ids_map(language, names, named_flags)`: map kind names to ids.
- `format_fragment_doc(language, range)`: format an embedded fragment into a doc.

Context and utilities
- `source_len()`: total source length in bytes.
- `cache_get(key)`: read a cached value (or unit if missing).
- `cache_set(key, value)`: store a cached value.
- `now_ms()`: current time in milliseconds since epoch.
- `trace(name)`: start a profiling span and return a TraceSpan.
- `end(span)`: stop a TraceSpan.
- `set_output(text)`: override the formatter output string.
- `slice(range)`: extract source text for a range.

String helpers
- `trim(s)`: trim both ends of a string.
- `trim_start(s)`: trim leading whitespace.
- `trim_end(s)`: trim trailing whitespace.
- `len(s)`: string length in characters.
- `is_empty(s)`: string empty check.
- `last_index_of(s, needle)`: last index of a substring.

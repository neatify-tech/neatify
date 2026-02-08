# Neatify

In the age of AI generated code, it is much easier to be a prolific polyglot then ever before because its rarely about knowing the actual syntax anymore.
That means however, keeping everything formatted consistently has become more of a challenge. And I do believe a clean format helps with human understanding.

This project has a number of goals:

- support as many syntaxes as possible: it is based on tree-sitter so anything supported there can be formatted
- support as many formatting styles as possible: instead of reducing formatting choices to a limited number of configuration options, it is entirely scriptable, allowing from small variations to full blown rewrites without touching the core. You can install multiple repositories that offer entirely different formatting results or if you're adventurous enough: write your own.

In the spirit of this new age, I created this project in a language I never used (rust) and a language I hadn't even heard of (rhai).
As you can imagine, the codebase is pretty much all AI generated though with heavy coordination, targetted manual intervention and no small amount of frustration.

## Steps

A formatting round starts by parsing the document into an AST. By default the parse is strict which means error or missing will abort further formatting, though this can be circumvented by --lax.

Once we have an AST, depending on the repository used and the language, a formatter is picked.
The formatter chooses the nodes its interested in through tree-sitter queries. These nodes are captured. Any ancestor nodes not explicitly captured are set as marked.

Rust will walk depth first over the captured nodes and will use rhai scripts to build pretty printer docs that will eventually be formatted into the new result.

Neatify also comes with an optional linting stage, by default we report error/missing problems in the AST but dedicated lint scripts can be added as well. Use --lint to run it (no formatting then), use --inspect if you want more details (though a bit more overhead) and --lax if you want to do an internal linting stage even if the document contains an error or has missing pieces.

## Getting Started

Typical usage (no custom repos):

```bash
neatify
```

See all options:

```bash
neatify --help
```

On first run, Neatify adds the core repository to `~/.neatify/config.toml` and syncs it automatically. After that, running `neatify` formats all supported files in the current directory (recursive), based on file extensions.

File-level detection only. If a formatter (like HTML) embeds other languages, it can dispatch internally.

### Format a Single Language

```bash
neatify java
neatify typescript
```

You can also pass globs or explicit files:

```bash
neatify "src/**/*.ts" "web/**/*.html"
```

### Ignore Files

Neatify respects `.gitignore` by default and also reads `.neatifyignore` (same syntax). You can add additional ignore patterns to `~/.neatify/config.toml`.

## Repositories

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

### Config File

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

## Testing

Run tests for all locally available languages:

```bash
neatify --test
```

Run tests for a single language:

```bash
neatify --test java
```

Tests are only run if they exist locally. Use `--sync-tests` to download them for remote repositories.

## Technical Details

### Sync Behavior

- On first run, Neatify adds the core repository to `~/.neatify/config.toml` and runs a sync.
- `--sync` only polls repositories with a `url` set. Local repos are never polled.
- Updates are prompted per repository (versioned manifests when available).

### Registry Hosting

Repositories are served as static registries (e.g., GitHub Pages) with a manifest at the root:

- `manifest.toml` (preferred) or `manifest.json`
- Each file entry includes path, sha256, and size

Tree-sitter binaries are stored alongside the repository under (in this repo, the directory is `registry/`):

```
repository/core/treesitter/<os>/<arch>/<language>.{ext}
```

### Manifest Generation

Generate a manifest for the local cache root:

```bash
neatify --generate-manifest
```

Generate a manifest for a named repository or a direct path:

```bash
neatify --manifest-generate core
neatify --manifest-generate /path/to/repo
neatify --manifest-generate file:/path/to/repo
```

Notes:

- `--manifest-generate` refuses repositories with a `url` (externally managed).
- If no repository name matches, Neatify treats the argument as a path.
- Manifest entries are sorted alphabetically to keep diffs small.

### LSP Mode

Neatify can run as an LSP server over stdio:

```bash
neatify java --lsp
```

Supports document formatting and range formatting only.

### Tree-sitter Binaries

Neatify uses Tree-sitter grammars as native shared libraries. These must be built per OS/arch.

Versions:

- Tree-sitter runtime: `0.26.5` (Rust crate)
- Tree-sitter CLI for generating parser.c: `0.26.3`

#### Build a grammar (Linux example)

```bash
git clone --depth 1 https://github.com/tree-sitter/tree-sitter-java
cd tree-sitter-java
tree-sitter generate
cc -fPIC -shared -o java.so src/parser.c src/scanner.c -I src
```

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

### Formatter Guarantees

- Formatting is non-semantic: it preserves tokens and only changes whitespace/layout.
- Embedded formatting (HTML -> CSS/JS) is handled by the HTML formatter, not by global detection.
- Parse error handling: when Tree-sitter produces error or missing nodes, Neatify preserves the raw text for those ranges. With `--debug` enabled, it logs the affected ranges.

### Formatter Internals (Brief)

- Tree-sitter parses the AST.
- Rhai formatter scripts build a document tree.
- The document tree is rendered to final output.

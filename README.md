# Neatify

In the age of AI-generated code, it’s easier than ever to be a prolific polyglot. When you’re jumping between five different languages a day, the challenge isn't the syntax—it's maintaining consistency across fragmented ecosystems.

Most formatters are either limited to a handful of languages or are so opinionated that you’re stuck with their "one true way."

**Neatify** was built to solve this. It is a universal, scriptable formatting and linting engine designed for a world where code moves fast.

### Why Neatify?

* **Tree-sitter Powered:** If a Tree-sitter grammar exists for a language, Neatify can format and lint it.
* **Scriptable (Rhai):** Formatting rules aren't hidden in a JSON file; they are live scripts. You have total control over the "opinion" of the formatter.
* **Repository First:** Switch between entirely different style repositories seamlessly. Use the community standard, or fork one to create your own "flavor."
* **AI-Native Workflow:** I built this project using Rust and Rhai—languages I was unfamiliar with—by coordinating with AI. Because the formatting logic is scripted in Rhai, you can easily use an LLM to "describe" a formatting style and have it generate the Neatify script for you.

## Getting Started

Typical usage (no custom repos):

```bash
neatify
```

On first run -if you haven't added an explicit repository- Neatify adds the core repository to `~/.neatify/config.toml` and syncs it automatically. After that, running `neatify` formats all supported files in the current directory (recursive), based on file extensions.

See all options:

```bash
neatify --help
```

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

#### Build a grammar (Linux example)

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

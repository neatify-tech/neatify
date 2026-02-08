mod api;
mod cache;
mod doc;
mod formatting;
mod manifest;
mod rhai_bindings;

pub use api::{Context, LanguageRegistry, Line, Match, NodeRef, ParseIssue, Position, Range};
pub use cache::RegistryCache;
pub use doc::{Doc, DocArena, DocId, LineKind};
pub use formatting::{
	format_fragment,
	format_source,
	range_offsets,
	resolve_entry_path,
	resolve_language_spec,
	resolve_treesitter_binary,
	LanguageSpec,
	OverrideValue,
	RangeOffsets,
	RangeSpec,
	TreesitterSpec,
};
pub use manifest::{
	parse_manifest,
	parse_manifest_auto,
	read_manifest_from_path,
	write_manifest_toml,
	Manifest,
	ManifestEntry,
	ManifestFormat,
};
pub use rhai_bindings::register_primitives;

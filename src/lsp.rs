use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tower_lsp::jsonrpc::{Error as LspError, Result as LspResult};
use tower_lsp::lsp_types::{
	InitializeParams,
	InitializeResult,
	MessageType,
	OneOf,
	Position,
	Range,
	ServerCapabilities,
	TextDocumentSyncCapability,
	TextDocumentSyncKind,
	TextEdit,
};
use tower_lsp::{Client, LanguageServer, LspService, Server};
use crate::{
	format_fragment,
	format_source,
	range_offsets,
	resolve_entry_path,
	resolve_treesitter_binary,
	OverrideValue,
	RangeSpec,
	LanguageSpec,
};

pub fn run_lsp(
	spec: LanguageSpec,
	roots: Vec<PathBuf>,
	overrides: Vec<(String, OverrideValue)>,
	debug: bool,
	test_group: bool,
	strict: bool) -> Result<(), String> {
	let entry_path = resolve_entry_path(&spec.formatter, &roots)
		.ok_or_else(|| format!("formatter script not found: {}", spec.formatter))?;
	let binary_path = resolve_treesitter_binary(&spec.treesitter.binary, &roots)?;
	let marker = spec.fragment_marker.clone();
	let language = spec.treesitter
		.language
		.clone();
	let backend = move |client: Client| Backend::new(
		client,
		entry_path.clone(),
		binary_path.clone(),
		language.clone(),
		roots.clone(),
		overrides.clone(),
		marker.clone(),
		debug,
		test_group,
		strict
	);
	let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
	rt.block_on(
		async {
			let (service, socket) = LspService::new(backend);
			Server::new(tokio::io::stdin(), tokio::io::stdout(), socket).serve(service).await;
		}
	);
	Ok(())
}

struct Backend {
	client: Client,
	documents: Arc<Mutex<HashMap<String, String>>>,
	entry_path: PathBuf,
	binary_path: PathBuf,
	language: String,
	roots: Vec<PathBuf>,
	overrides: Vec<(String, OverrideValue)>,
	fragment_marker: Option<String>,
	debug: bool,
	test_group: bool,
	strict: bool,
}

impl Backend {
	fn new(
		client: Client,
		entry_path: PathBuf,
		binary_path: PathBuf,
		language: String,
		roots: Vec<PathBuf>,
		overrides: Vec<(String, OverrideValue)>,
		fragment_marker: Option<String>,
		debug: bool,
		test_group: bool,
		strict: bool) -> Self {
		Self {
			client,
			documents: Arc::new(Mutex::new(HashMap::new())),
			entry_path,
			binary_path,
			language,
			roots,
			overrides,
			fragment_marker,
			debug,
			test_group,
			strict
		}
	}
	fn get_text(&self, uri: &str) -> Result<String, LspError> {
		let docs = self.documents
			.lock()
			.map_err(|_| LspError::internal_error())?;
		docs.get(uri)
			.cloned()
			.ok_or_else(|| LspError::invalid_params("document not open"))
	}
	fn format_full(&self, source: &str) -> Result<String, LspError> {
		format_source(
			source,
			&self.entry_path,
			&self.binary_path,
			&self.language,
			&self.roots,
			&self.overrides,
			self.debug,
			false,
			self.test_group,
			self.strict
		)
			.map_err(|_| LspError::internal_error())
	}
	fn format_range(&self, source: &str, range: &RangeSpec) -> Result<String, LspError> {
		let marker = self.fragment_marker
			.as_ref()
			.ok_or_else(|| LspError::internal_error())?;
		let offsets = range_offsets(source, range).map_err(|msg| LspError::invalid_params(msg))?;
		format_fragment(
			source,
			&offsets,
			marker,
			&self.entry_path,
			&self.binary_path,
			&self.language,
			&self.roots,
			&self.overrides,
			self.debug,
			false,
			self.test_group,
			self.strict
		)
			.map_err(|_| LspError::internal_error())
	}
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
	async fn initialize(&self, _: InitializeParams) -> LspResult<InitializeResult> {
		Ok(
			InitializeResult {
				capabilities: ServerCapabilities {
					text_document_sync: Some(
						TextDocumentSyncCapability::Kind(
							TextDocumentSyncKind::FULL
						)
					),
					document_formatting_provider: Some(OneOf::Left(true)),
					document_range_formatting_provider: Some(OneOf::Left(true)),
					..ServerCapabilities::default()
				},
				server_info: None
			}
		)
	}
	async fn initialized(&self, _: tower_lsp::lsp_types::InitializedParams) {
		self.client.log_message(MessageType::INFO, "neatify LSP initialized").await;
	}
	async fn shutdown(&self) -> LspResult<()> {
		Ok(())
	}
	async fn did_open(&self, params: tower_lsp::lsp_types::DidOpenTextDocumentParams) {
		let uri = params.text_document
			.uri
			.to_string();
		let mut docs = match self.documents.lock() {
			Ok(docs) => docs,
			Err(err) => err.into_inner(),
		};
		docs.insert(uri, params.text_document.text);
	}
	async fn did_change(
		&self,
		params: tower_lsp::lsp_types::DidChangeTextDocumentParams) {
		let uri = params.text_document
			.uri
			.to_string();
		let mut docs = match self.documents.lock() {
			Ok(docs) => docs,
			Err(err) => err.into_inner(),
		};
		if let Some(change) = params.content_changes
			.into_iter()
			.last() {
			docs.insert(uri, change.text);
		}
	}
	async fn did_close(&self, params: tower_lsp::lsp_types::DidCloseTextDocumentParams) {
		let uri = params.text_document
			.uri
			.to_string();
		let mut docs = match self.documents.lock() {
			Ok(docs) => docs,
			Err(err) => err.into_inner(),
		};
		docs.remove(&uri);
	}
	async fn formatting(
		&self,
		params: tower_lsp::lsp_types::DocumentFormattingParams) -> LspResult<Option<Vec<TextEdit>>> {
		let uri = params.text_document
			.uri
			.to_string();
		let text = self.get_text(&uri)?;
		let formatted = self.format_full(&text)?;
		if formatted == text {
			return Ok(Some(Vec::new()));
		}
		let range = full_document_range(&text);
		Ok(
			Some(
				vec![TextEdit {
					range,
					new_text: formatted,
				}]
			)
		)
	}
	async fn range_formatting(
		&self,
		params: tower_lsp::lsp_types::DocumentRangeFormattingParams) -> LspResult<Option<Vec<TextEdit>>> {
		let uri = params.text_document
			.uri
			.to_string();
		let text = self.get_text(&uri)?;
		let range = params.range;
		let range_spec = range_to_spec(&range)?;
		let fragment = self.format_range(&text, &range_spec)?;
		Ok(
			Some(
				vec![TextEdit {
					range,
					new_text: fragment,
				}]
			)
		)
	}
}

fn range_to_spec(range: &Range) -> Result<RangeSpec, LspError> {
	let start_row = usize::try_from(range.start.line).map_err(|_| LspError::invalid_params("range start line overflow"))?;
	let start_col = usize::try_from(range.start.character)
		.map_err(|_| LspError::invalid_params("range start column overflow"))?;
	let end_row = usize::try_from(range.end.line).map_err(|_| LspError::invalid_params("range end line overflow"))?;
	let end_col = usize::try_from(range.end.character)
		.map_err(|_| LspError::invalid_params("range end column overflow"))?;
	if start_row > end_row || (start_row == end_row && start_col > end_col) {
		return Err(LspError::invalid_params("range start after end"));
	}
	Ok(
		RangeSpec {
			start_row,
			start_col,
			end_row,
			end_col
		}
	)
}

fn full_document_range(text: &str) -> Range {
	let mut line: u32 = 0;
	let mut col: u32 = 0;
	for ch in text.chars() {
		if ch == '\n' {
			line += 1;
			col = 0;
		}
		else {
			col += ch.len_utf16() as u32;
		}
	}
	Range::new(Position::new(0, 0), Position::new(line, col))
}

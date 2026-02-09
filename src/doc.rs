#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineKind {
	Soft,
	Hard,
}

#[derive(Clone, Debug)]
pub enum Doc {
	Text(String),
	SourceRange {
		start: usize,
		end: usize,
	},
	Line(LineKind),
	Concat(Vec<DocId>),
	Group(DocId),
	Indent {
		by: usize,
		doc: DocId,
	},
}

pub type DocId = usize;

#[derive(Clone, Debug, Default)]
pub struct DocArena {
	docs: Vec<Doc>,
}

impl DocArena {
	pub fn new() -> Self {
		Self { docs: Vec::new() }
	}
	pub fn clear(&mut self) {
		self.docs.clear();
	}
	pub fn push(&mut self, doc: Doc) -> DocId {
		let id = self.docs.len();
		self.docs.push(doc);
		id
	}
	pub fn render(&self, doc_id: DocId, width: usize, source: &str) -> String {
		let printer = DocPrinter::new(self, width, source, IndentStyle::Tabs, 4);
		printer.render(doc_id)
	}
	pub fn render_with_indent(
		&self,
		doc_id: DocId,
		width: usize,
		source: &str,
		indent_style: IndentStyle,
		tab_width: usize) -> String {
		let printer = DocPrinter::new(self, width, source, indent_style, tab_width);
		printer.render(doc_id)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PrintMode {
	Flat,
	Break,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndentStyle {
	Tabs,
	Spaces,
}

struct DocPrinter <'a> {
	arena: &'a DocArena,
	width: usize,
	col: usize,
	out: String,
	source: &'a str,
	indent_style: IndentStyle,
	tab_width: usize,
}

impl <'a> DocPrinter<'a> {
	fn new(
		arena: &'a DocArena,
		width: usize,
		source: &'a str,
		indent_style: IndentStyle,
		tab_width: usize) -> Self {
		Self {
			arena,
			width,
			col: 0,
			out: String::new(),
			source,
			indent_style,
			tab_width
		}
	}
	fn render(mut self, doc_id: DocId) -> String {
		let mut stack = vec![(doc_id, 0usize, PrintMode::Break)];
		while let Some((id, indent, mode)) = stack.pop() {
			if id >= self.arena
				.docs
				.len() {
				continue;
			}
			match &self.arena.docs[id] {
				Doc::Text(text) => {
					self.out.push_str(text);
					self.col += text.chars().count();
				}
				Doc::SourceRange { start, end } => {
					if *start < *end && *end <= self.source.len() {
						let slice = &self.source[*start.. * end];
						self.out.push_str(slice);
						self.col += slice.chars().count();
					}
				}
				Doc::Line(kind) => match kind {
					LineKind::Soft => match mode {
						PrintMode::Flat => {
							self.out.push(' ');
							self.col += 1;
						}
						PrintMode::Break => self.newline(indent),
					},
					LineKind::Hard => self.newline(indent),
				},
				Doc::Concat(list) => {
					for child in list.iter().rev() {
						stack.push((*child, indent, mode));
					}
				}
				Doc::Group(child) => {
					let fits = self.fits(*child, indent, self.width.saturating_sub(self.col));
					let next_mode = if fits {
						PrintMode::Flat
					}
					else {
						PrintMode::Break
					};
					stack.push((*child, indent, next_mode));
				}
				Doc::Indent { by, doc } => {
					stack.push((*doc, indent + *by, mode));
				}
			}
		}
		self.out
	}
	fn newline(&mut self, indent: usize) {
		self.out.push('\n');
		if indent > 0 {
			match self.indent_style {
				IndentStyle::Spaces => {
					self.out.push_str(&" ".repeat(indent));
				}
				IndentStyle::Tabs => {
					let tab_width = self.tab_width.max(1);
					let tabs = indent / tab_width;
					let spaces = indent % tab_width;
					if tabs > 0 {
						self.out.push_str(&"\t".repeat(tabs));
					}
					if spaces > 0 {
						self.out.push_str(&" ".repeat(spaces));
					}
				}
			}
		}
		self.col = indent;
	}
	fn fits(&self, doc_id: DocId, indent: usize, mut remaining: usize) -> bool {
		let mut stack = vec![(doc_id, indent, PrintMode::Flat)];
		while let Some((id, indent, mode)) = stack.pop() {
			if id >= self.arena
				.docs
				.len() {
				continue;
			}
			match &self.arena.docs[id] {
				Doc::Text(text) => {
					let len = text.chars().count();
					if len > remaining {
						return false;
					}
					remaining -= len;
				}
				Doc::SourceRange { start, end } => {
					if *start < *end && *end <= self.source.len() {
						let slice = &self.source[*start.. * end];
						let len = slice.chars().count();
						if len > remaining {
							return false;
						}
						remaining -= len;
					}
				}
				Doc::Line(kind) => match kind {
					LineKind::Hard => return false,
					LineKind::Soft => match mode {
						PrintMode::Flat => {
							if remaining == 0 {
								return false;
							}
							remaining -= 1;
						}
						PrintMode::Break => {
							remaining = remaining.max(indent);
						}
					},
				},
				Doc::Concat(list) => {
					for child in list.iter().rev() {
						stack.push((*child, indent, mode));
					}
				}
				Doc::Group(child) => {
					stack.push((*child, indent, mode));
				}
				Doc::Indent { by, doc } => {
					stack.push((*doc, indent + *by, mode));
				}
			}
		}
		true
	}
}

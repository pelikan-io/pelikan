//! Shared SVG-emission helpers and the visual language common to all
//! generated diagrams: one palette (d3 schemePastel1), one type scale
//! (14/16/17/20), monospace for literal identifiers, italic for external
//! elements, orthogonal arrows with heavier strokes for process-boundary
//! (wire) edges.

pub const FONT: &str = "Helvetica, Arial, sans-serif";
pub const MONO: &str = "SFMono-Regular, Menlo, Consolas, monospace";

pub const QUEUE_FILL: &str = "#F2F2F2";
pub const PANEL_FILL: &str = "#FAFAFA";
pub const PANEL_BORDER: &str = "#9E9E9E";
pub const EDGE: &str = "#4D4D4D";
pub const EDGE_SIGNAL: &str = "#999999";

// d3 schemePastel1, assigned by layer
pub const FILL_CORE: &str = "#CCEBC5";
pub const FILL_PROTOCOL: &str = "#FBB4AE";
pub const FILL_STORAGE: &str = "#B3CDE3";
pub const FILL_FOUNDATION: &str = "#F2F2F2";
pub const FILL_EXTERNAL: &str = "#FFFFFF";

pub const ARROW_DEFS: &str = concat!(
    "<defs><marker id=\"a\" viewBox=\"0 0 10 10\" refX=\"9\" refY=\"5\" ",
    "markerWidth=\"6\" markerHeight=\"6\" orient=\"auto-start-reverse\">",
    "<path d=\"M 0 0 L 10 5 L 0 10 z\" fill=\"#4D4D4D\"/></marker></defs>"
);

pub struct Rect {
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
    pub fill: String,
    pub stroke: String,
    pub sw: f64,
    pub rx: f64,
    pub dashed: bool,
    pub opacity: f64,
}

pub fn rect(x: f64, y: f64, w: f64, h: f64, fill: &str) -> Rect {
    Rect {
        x,
        y,
        w,
        h,
        fill: fill.to_string(),
        stroke: EDGE.to_string(),
        sw: 1.5,
        rx: 0.0,
        dashed: false,
        opacity: 1.0,
    }
}

impl Rect {
    pub fn stroke(mut self, s: &str) -> Self {
        self.stroke = s.to_string();
        self
    }
    pub fn sw(mut self, v: f64) -> Self {
        self.sw = v;
        self
    }
    pub fn rx(mut self, v: f64) -> Self {
        self.rx = v;
        self
    }
    pub fn dashed(mut self) -> Self {
        self.dashed = true;
        self
    }
    pub fn opacity(mut self, v: f64) -> Self {
        self.opacity = v;
        self
    }
    pub fn build(&self) -> String {
        let op = if self.opacity < 1.0 {
            format!(" fill-opacity=\"{:.2}\"", self.opacity)
        } else {
            String::new()
        };
        let dash = if self.dashed {
            " stroke-dasharray=\"6,4\""
        } else {
            ""
        };
        format!(
            "<rect x=\"{:.0}\" y=\"{:.0}\" width=\"{:.0}\" height=\"{:.0}\" \
             fill=\"{}\" stroke=\"{}\" stroke-width=\"{}\" rx=\"{}\"{}{}/>",
            self.x, self.y, self.w, self.h, self.fill, self.stroke, self.sw, self.rx, op, dash
        )
    }
}

pub struct Text {
    pub x: f64,
    pub y: f64,
    pub s: String,
    pub size: u32,
    pub weight: &'static str,
    pub fill: String,
    pub italic: bool,
    pub mono: bool,
    pub anchor: &'static str,
}

pub fn text(x: f64, y: f64, s: &str) -> Text {
    Text {
        x,
        y,
        s: s.to_string(),
        size: 14,
        weight: "normal",
        fill: "#000".to_string(),
        italic: false,
        mono: false,
        anchor: "middle",
    }
}

impl Text {
    pub fn size(mut self, v: u32) -> Self {
        self.size = v;
        self
    }
    pub fn bold(mut self) -> Self {
        self.weight = "bold";
        self
    }
    pub fn fill(mut self, s: &str) -> Self {
        self.fill = s.to_string();
        self
    }
    pub fn italic(mut self) -> Self {
        self.italic = true;
        self
    }
    pub fn mono(mut self) -> Self {
        self.mono = true;
        self
    }
    pub fn start(mut self) -> Self {
        self.anchor = "start";
        self
    }
    pub fn end(mut self) -> Self {
        self.anchor = "end";
        self
    }
    pub fn build(&self) -> String {
        let style = if self.italic {
            " font-style=\"italic\""
        } else {
            ""
        };
        let fam = if self.mono { MONO } else { FONT };
        format!(
            "<text x=\"{:.0}\" y=\"{:.0}\" dy=\"0.35em\" font-family=\"{}\" \
             font-size=\"{}\" font-weight=\"{}\" fill=\"{}\"{} \
             text-anchor=\"{}\">{}</text>",
            self.x, self.y, fam, self.size, self.weight, self.fill, style, self.anchor, self.s
        )
    }
}

pub struct Ortho<'a> {
    pub points: &'a [(f64, f64)],
    pub signal: bool,
    pub both: bool,
    pub network: bool,
}

pub fn ortho(points: &[(f64, f64)]) -> Ortho<'_> {
    Ortho {
        points,
        signal: false,
        both: false,
        network: false,
    }
}

impl Ortho<'_> {
    pub fn signal(mut self) -> Self {
        self.signal = true;
        self
    }
    pub fn both(mut self) -> Self {
        self.both = true;
        self
    }
    pub fn network(mut self) -> Self {
        self.network = true;
        self
    }
    pub fn build(&self) -> String {
        let stroke = if self.signal { EDGE_SIGNAL } else { EDGE };
        let dash = if self.signal {
            " stroke-dasharray=\"5,4\""
        } else {
            ""
        };
        let ms = if self.both {
            " marker-start=\"url(#a)\""
        } else {
            ""
        };
        let sw = if self.network { 2.4 } else { 1.4 };
        let mut d = format!("M {:.0} {:.0}", self.points[0].0, self.points[0].1);
        for (x, y) in &self.points[1..] {
            d.push_str(&format!(" L {x:.0} {y:.0}"));
        }
        format!(
            "<path d=\"{d}\" fill=\"none\" stroke=\"{stroke}\" \
             stroke-width=\"{sw}\"{dash} marker-end=\"url(#a)\"{ms}/>"
        )
    }
}

/// Semantic type scale for a chart. Every text element plays one of three
/// roles — no ad-hoc font sizes:
pub struct TypeScale {
    /// panel and band titles
    pub h1: u32,
    /// element names: crates, threads, stages, lane headers, externals
    pub h2: u32,
    /// everything else: chips, edge/queue/sub labels, mini-tables
    pub body: u32,
}

/// One ramp for every chart: the canvases are reconciled to equal width
/// (2280), so a single scale renders text at the same effective size across
/// the whole set, inline and at full size.
pub const TYPE_SCALE: TypeScale = TypeScale {
    h1: 30,
    h2: 26,
    body: 22,
};

/// Estimated rendered width of a label at an explicit font size.
pub fn label_w_at(s: &str, size: f64) -> f64 {
    s.len() as f64 * size * 0.55 + 16.0
}

pub fn svg_document(w: f64, h: f64, generator: &str, parts: &[String]) -> String {
    format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w:.0}\" height=\"{h:.0}\" \
         viewBox=\"0 0 {w:.0} {h:.0}\">\n\
         <!-- generated by {generator}, do not edit -->\n\
         <rect width=\"100%\" height=\"100%\" fill=\"white\"/>\n{}\n</svg>\n",
        parts.join("\n")
    )
}

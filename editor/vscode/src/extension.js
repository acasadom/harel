// VSCode client for the harel DSL language server + the live statechart preview.
//
// Syntax highlighting works with no server (grammar + language-configuration).
// This client additionally launches the Python diagnostics server over stdio
// (`<pythonPath> -m harel.lsp`, or `harel.serverCommand` verbatim) and wires
// its diagnostics into the editor. Disable via `harel.enableLanguageServer`.
//
// The "Open Statechart Preview" command opens a webview that renders the active
// `.stm` as a Mermaid diagram, re-rendering as you type. The render itself comes
// from the language server (custom `harel/render` request), so the preview needs
// the server enabled; the diagram is drawn by the vendored `media/mermaid.min.js`.

const { workspace, window, commands, ViewColumn, Uri } = require("vscode");
const fs = require("fs");
const path = require("path");

let client;

// when pythonPath is left at the default, prefer a virtualenv in the workspace
// (harel is developed with uv, so `.venv/bin/python` exists) — avoids any
// per-user setting and avoids `uv run` writing to stdout (which corrupts LSP).
function detectWorkspacePython() {
  const rels = [".venv/bin/python3", ".venv/bin/python", ".venv/Scripts/python.exe"];
  for (const folder of workspace.workspaceFolders || []) {
    for (const rel of rels) {
      const p = path.join(folder.uri.fsPath, rel);
      if (fs.existsSync(p)) return p;
    }
  }
  return null;
}

function expandVars(s) {
  const folder = workspace.workspaceFolders?.[0]?.uri.fsPath ?? "";
  return s.replace(/\$\{workspaceFolder\}/g, folder);
}

function resolveServerCommand() {
  const cfg = workspace.getConfiguration("harel");
  const override = cfg.get("serverCommand") || [];
  if (Array.isArray(override) && override.length > 0) {
    return { command: expandVars(override[0]), args: override.slice(1).map(expandVars) };
  }
  let python = cfg.get("pythonPath") || "python";
  if (python === "python") {
    python = detectWorkspacePython() || python;
  } else {
    python = expandVars(python);
  }
  return { command: python, args: ["-m", "harel.lsp"] };
}

function nonce() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let s = "";
  for (let i = 0; i < 32; i++) s += chars.charAt(Math.floor(Math.random() * chars.length));
  return s;
}

// --- live statechart preview --------------------------------------------------

const preview = {
  panel: null,
  uri: null, // the .stm document the panel is currently tracking
  target: null, // the chosen machine/fragment/submachine to render (null = let the server pick)
  timer: null,
  context: null,

  isStm(doc) {
    return doc && doc.languageId === "stm";
  },

  open() {
    const editor = window.activeTextEditor;
    if (!this.isStm(editor && editor.document)) {
      window.showInformationMessage("harel: open a .stm file to preview it.");
      return;
    }
    if (editor.document.uri.toString() !== this.uri) this.target = null; // new doc → reset picker
    this.uri = editor.document.uri.toString();
    if (!this.panel) {
      this.panel = window.createWebviewPanel(
        "harelPreview",
        "Statechart Preview",
        { viewColumn: ViewColumn.Beside, preserveFocus: true },
        { enableScripts: true, retainContextWhenHidden: true }
      );
      this.panel.webview.html = this.html(this.panel.webview);
      this.panel.onDidDispose(() => {
        this.panel = null;
        this.uri = null;
      });
      // render once the webview signals it is ready; re-render on a target pick
      this.panel.webview.onDidReceiveMessage((m) => {
        if (!m) return;
        if (m.type === "ready") this.render();
        else if (m.type === "select") {
          this.target = m.target;
          this.render();
        }
      });
    } else {
      this.panel.reveal(ViewColumn.Beside, true);
      this.render();
    }
  },

  // re-render (debounced) when the tracked document changes
  onChange(doc) {
    if (!this.panel || !doc || doc.uri.toString() !== this.uri) return;
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.render(), 300);
  },

  // follow the active editor when it is another .stm file
  onActiveEditor(editor) {
    if (!this.panel || !this.isStm(editor && editor.document)) return;
    const uri = editor.document.uri.toString();
    if (uri !== this.uri) {
      this.uri = uri;
      this.target = null; // following a different .stm → reset the picker
      this.render();
    }
  },

  async render() {
    if (!this.panel || !this.uri) return;
    if (!client) {
      this.panel.webview.postMessage({
        type: "render",
        error: "the language server is required for the preview (harel.enableLanguageServer).",
      });
      return;
    }
    try {
      const res = await client.sendRequest("harel/render", { uri: this.uri, machine: this.target });
      this.panel.webview.postMessage({ type: "render", ...res });
    } catch (e) {
      this.panel.webview.postMessage({ type: "render", error: String(e) });
    }
  },

  html(webview) {
    const media = Uri.joinPath(this.context.extensionUri, "media");
    const mermaidUri = webview.asWebviewUri(Uri.joinPath(media, "mermaid.min.js"));
    const previewUri = webview.asWebviewUri(Uri.joinPath(media, "preview.js"));
    const n = nonce();
    const csp =
      `default-src 'none'; img-src ${webview.cspSource} data:; ` +
      `style-src ${webview.cspSource} 'unsafe-inline'; ` +
      `script-src 'nonce-${n}' 'unsafe-eval';`;
    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta http-equiv="Content-Security-Policy" content="${csp}" />
<style>
  html, body { margin: 0; height: 100%; overflow: hidden; }
  #status {
    display: none; position: absolute; top: 8px; left: 8px; right: 88px; z-index: 2;
    padding: 6px 8px; border-radius: 4px; white-space: pre-wrap;
    font-family: var(--vscode-editor-font-family, monospace);
    background: var(--vscode-inputValidation-errorBackground, #5a1d1d);
    border: 1px solid var(--vscode-inputValidation-errorBorder, #be1100);
    color: var(--vscode-foreground);
  }
  #note {
    display: none; position: absolute; bottom: 8px; left: 8px; z-index: 2;
    padding: 4px 8px; border-radius: 3px; font-size: 11px; opacity: 0.9;
    font-family: var(--vscode-font-family);
    background: var(--vscode-editorWidget-background, #252526);
    border: 1px solid var(--vscode-editorWidget-border, #454545);
    color: var(--vscode-descriptionForeground, #ccc);
  }
  #graph { position: absolute; inset: 0; overflow: hidden; cursor: grab; }
  #graph.panning { cursor: grabbing; }
  #graph svg { transform-origin: 0 0; }
  #zoom {
    position: absolute; top: 8px; right: 8px; z-index: 3;
    display: flex; gap: 4px; align-items: center;
    font-family: var(--vscode-font-family); font-size: 11px;
  }
  #zoom button {
    width: 24px; height: 22px; cursor: pointer; border-radius: 3px;
    border: 1px solid var(--vscode-button-border, transparent);
    background: var(--vscode-button-secondaryBackground, #3a3d41);
    color: var(--vscode-button-secondaryForeground, #fff);
  }
  #zoom button:hover { background: var(--vscode-button-secondaryHoverBackground, #45494e); }
  #zoom #pct { min-width: 38px; text-align: center; color: var(--vscode-foreground); opacity: 0.8; }
  #target {
    display: none; max-width: 220px; height: 22px; margin-right: 6px; border-radius: 3px;
    font-family: var(--vscode-font-family); font-size: 11px;
    background: var(--vscode-dropdown-background, #3c3c3c);
    color: var(--vscode-dropdown-foreground, #f0f0f0);
    border: 1px solid var(--vscode-dropdown-border, #3c3c3c);
  }
</style>
</head>
<body>
  <div id="status"></div>
  <div id="zoom">
    <select id="target" title="Preview target"></select>
    <button id="zoom-out" title="Zoom out">&minus;</button>
    <span id="pct">100%</span>
    <button id="zoom-in" title="Zoom in">+</button>
    <button id="zoom-fit" title="Fit to window">&#9974;</button>
  </div>
  <div id="graph"></div>
  <div id="note"></div>
  <script nonce="${n}" src="${mermaidUri}"></script>
  <script nonce="${n}" src="${previewUri}"></script>
</body>
</html>`;
  },
};

function activate(context) {
  preview.context = context;
  // register the preview command + listeners always, so the palette/title entry
  // exists even in highlighting-only mode (render() reports if the server is off).
  context.subscriptions.push(
    commands.registerCommand("harel.showPreview", () => preview.open()),
    workspace.onDidChangeTextDocument((e) => preview.onChange(e.document)),
    window.onDidChangeActiveTextEditor((ed) => preview.onActiveEditor(ed))
  );

  const cfg = workspace.getConfiguration("harel");
  if (cfg.get("enableLanguageServer") === false) {
    return; // highlighting + (server-less) preview command only
  }

  // loaded lazily so the extension still provides syntax highlighting even when
  // `npm install` has not run yet (no vscode-languageclient on disk)
  let LanguageClient, TransportKind;
  try {
    ({ LanguageClient, TransportKind } = require("vscode-languageclient/node"));
  } catch (e) {
    window.showInformationMessage(
      "harel: syntax highlighting active. For live diagnostics + preview run `bun install` in editor/vscode."
    );
    return;
  }

  const { command, args } = resolveServerCommand();
  const serverOptions = {
    run: { command, args, transport: TransportKind.stdio },
    debug: { command, args, transport: TransportKind.stdio },
  };
  const clientOptions = {
    documentSelector: [{ scheme: "file", language: "stm" }],
    synchronize: { fileEvents: workspace.createFileSystemWatcher("**/*.stm") },
  };

  client = new LanguageClient("harel", "harel DSL", serverOptions, clientOptions);
  client.start().catch((err) => {
    window.showWarningMessage(
      `harel: could not start the language server (${command}). ` +
        `Set harel.pythonPath to a venv with harel[lsp] installed. ${err}`
    );
  });
}

function deactivate() {
  return client ? client.stop() : undefined;
}

module.exports = { activate, deactivate };

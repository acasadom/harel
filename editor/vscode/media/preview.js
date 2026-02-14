// Webview side of the statechart preview. Receives `{type:"render", mermaid|error}`
// messages from the extension and draws the diagram with the vendored mermaid.js.
// A parse/build error keeps the last good diagram on screen and shows a banner.
// Adds pan (drag) + zoom (wheel / buttons) on the rendered SVG, since large
// machines would otherwise be squeezed to fit the panel width and be unreadable.
/* global mermaid, acquireVsCodeApi */
(function () {
  const vscode = acquireVsCodeApi();
  const status = document.getElementById("status");
  const graph = document.getElementById("graph");
  const note = document.getElementById("note");
  const pct = document.getElementById("pct");
  const targetSel = document.getElementById("target");
  let counter = 0;
  let targetKey = ""; // the current option set, to avoid rebuilding (and losing focus) mid-typing

  targetSel.addEventListener("change", () => {
    vscode.postMessage({ type: "select", target: targetSel.value });
  });

  function syncTargets(targets, current) {
    if (!targets || targets.length < 2) {
      targetSel.style.display = "none";
      targetKey = "";
      return;
    }
    const key = targets.map((t) => t.kind + ":" + t.name).join("|");
    if (key !== targetKey) {
      targetKey = key;
      targetSel.innerHTML = "";
      for (const t of targets) {
        const opt = document.createElement("option");
        opt.value = t.name;
        opt.textContent = t.kind === "machine" ? t.name : t.name + "  (" + t.kind + ")";
        targetSel.appendChild(opt);
      }
    }
    if (current) targetSel.value = current;
    targetSel.style.display = "inline-block";
  }

  // pan/zoom transform state (applied to the current <svg>)
  let svg = null;
  let scale = 1;
  let tx = 0;
  let ty = 0;
  let interacted = false; // once the user zooms/pans, stop auto-fitting on re-render
  let lastMachine = null;

  function theme() {
    const c = document.body.className || "";
    if (c.indexOf("vscode-high-contrast") !== -1) return "dark";
    return c.indexOf("vscode-dark") !== -1 ? "dark" : "default";
  }

  mermaid.initialize({ startOnLoad: false, theme: theme(), securityLevel: "loose" });

  function apply() {
    if (svg) svg.style.transform = "translate(" + tx + "px," + ty + "px) scale(" + scale + ")";
    pct.textContent = Math.round(scale * 100) + "%";
  }

  function svgSize() {
    const vb = svg && svg.viewBox && svg.viewBox.baseVal;
    if (vb && vb.width && vb.height) return { w: vb.width, h: vb.height };
    const b = svg ? svg.getBBox() : { width: 1, height: 1 };
    return { w: b.width || 1, h: b.height || 1 };
  }

  function fit() {
    if (!svg) return;
    const rect = graph.getBoundingClientRect();
    // layout not ready yet (panel just opened) → show at the top-left and retry
    if (rect.width < 2 || rect.height < 2) {
      scale = 1;
      tx = 0;
      ty = 0;
      apply();
      requestAnimationFrame(fit);
      return;
    }
    const { w, h } = svgSize();
    const pad = 24;
    scale = Math.min((rect.width - pad) / w, (rect.height - pad) / h);
    if (!isFinite(scale) || scale <= 0) scale = 1;
    tx = (rect.width - w * scale) / 2;
    ty = (rect.height - h * scale) / 2;
    interacted = false;
    apply();
  }

  function zoomAt(cx, cy, factor) {
    const next = Math.min(8, Math.max(0.05, scale * factor));
    tx = cx - (cx - tx) * (next / scale);
    ty = cy - (cy - ty) * (next / scale);
    scale = next;
    interacted = true;
    apply();
  }

  function center() {
    const rect = graph.getBoundingClientRect();
    return { x: rect.width / 2, y: rect.height / 2 };
  }

  // --- interactions ---
  graph.addEventListener(
    "wheel",
    (e) => {
      if (!svg) return;
      e.preventDefault();
      const rect = graph.getBoundingClientRect();
      zoomAt(e.clientX - rect.left, e.clientY - rect.top, e.deltaY < 0 ? 1.1 : 1 / 1.1);
    },
    { passive: false }
  );

  let dragging = false;
  let sx = 0;
  let sy = 0;
  let ox = 0;
  let oy = 0;
  graph.addEventListener("mousedown", (e) => {
    if (!svg) return;
    dragging = true;
    sx = e.clientX;
    sy = e.clientY;
    ox = tx;
    oy = ty;
    graph.classList.add("panning");
  });
  window.addEventListener("mousemove", (e) => {
    if (!dragging) return;
    tx = ox + (e.clientX - sx);
    ty = oy + (e.clientY - sy);
    interacted = true;
    apply();
  });
  window.addEventListener("mouseup", () => {
    dragging = false;
    graph.classList.remove("panning");
  });
  graph.addEventListener("dblclick", () => fit());
  window.addEventListener("resize", () => {
    if (!interacted) fit();
  });

  document.getElementById("zoom-in").addEventListener("click", () => {
    const c = center();
    zoomAt(c.x, c.y, 1.2);
  });
  document.getElementById("zoom-out").addEventListener("click", () => {
    const c = center();
    zoomAt(c.x, c.y, 1 / 1.2);
  });
  document.getElementById("zoom-fit").addEventListener("click", () => fit());

  function showError(text) {
    status.textContent = text;
    status.style.display = "block";
  }

  async function draw(code, machine) {
    const { svg: markup } = await mermaid.render("stm-graph-" + ++counter, code);
    graph.innerHTML = markup;
    svg = graph.querySelector("svg");
    if (svg) {
      const { w, h } = svgSize();
      svg.style.maxWidth = "none";
      svg.setAttribute("width", w);
      svg.setAttribute("height", h);
      svg.style.transformOrigin = "0 0";
    }
    // auto-fit a fresh machine; preserve the user's view while editing the same one
    if (machine !== lastMachine) interacted = false;
    lastMachine = machine;
    if (!interacted) fit();
    else apply();
  }

  window.addEventListener("message", async (ev) => {
    const msg = ev.data;
    if (!msg || msg.type !== "render") return;
    syncTargets(msg.targets, msg.machine);
    if (msg.error) {
      showError(msg.machine ? msg.machine + ": " + msg.error : msg.error);
      return; // keep whatever is currently drawn
    }
    if (!msg.mermaid) return;
    try {
      await draw(msg.mermaid, msg.machine);
      status.style.display = "none";
      if (msg.note) {
        note.textContent = msg.note;
        note.style.display = "block";
      } else {
        note.style.display = "none";
      }
    } catch (e) {
      showError("render error: " + (e && e.message ? e.message : String(e)));
    }
  });

  // tell the extension we are ready for the first render
  vscode.postMessage({ type: "ready" });
})();

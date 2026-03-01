const $ = (id) => document.getElementById(id);

function setText(id, msg) {
  const el = $(id);
  if (!el) return;
  el.textContent = msg || "";
}

function setDisabled(id, disabled) {
  const el = $(id);
  if (!el) return;
  el.disabled = !!disabled;
}

function fillField(id, value) {
  const el = $(id);
  if (!el) return;
  el.value = value || "";
}

function setConf(id, value, label = "Confidence") {
  const el = $(id);
  if (!el) return;
  if (value === null || value === undefined || value === "") {
    el.textContent = "";
    return;
  }
  const pct = Math.max(0, Math.min(100, Math.round(Number(value) || 0)));
  el.textContent = `${label}: ${pct}%`;
}

function getStep1() {
  return {
    organisation_name: ($("org").value || "").trim(),
    employee_forename: ($("forename").value || "").trim(),
    employee_surname: ($("surname_user").value || "").trim(),
  };
}

function statusLabel(status) {
  if (!status) return "";
  if (status === "queued") return "Queued";
  if (status === "running") return "Running…";
  if (status === "clear") return "✅ Clear";
  if (status === "not_on_update_service") return "⚠ Not on Update Service";
  if (status === "needs_review") return "❌ Needs Review";
  if (status === "portal_unavailable") return "⛔ Portal Unavailable";
  return status;
}

function badgeClass(status) {
  return `badge ${status || ""}`.trim();
}

function buildBadge(status) {
  const span = document.createElement("span");
  span.className = badgeClass(status);
  span.textContent = statusLabel(status);
  return span;
}

function ensureModeUI() {
  const mode = $("modeBulk").checked ? "bulk" : "single";
  $("singleWrap").classList.toggle("hidden", mode !== "single");
  $("bulkWrap").classList.toggle("hidden", mode !== "bulk");
}

$("modeSingle")?.addEventListener("change", ensureModeUI);
$("modeBulk")?.addEventListener("change", ensureModeUI);
ensureModeUI();

// -------------------------
// Bulk file handling (append + drag/drop, max 20)
// -------------------------
let bulkFiles = [];
let lastExtractedItems = [];

function escapeHtml(s){
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/\'/g,"&#39;");
}

function isSupportedBulkFile(f){
  const name=(f?.name||"").toLowerCase();
  return name.endsWith(".pdf")||name.endsWith(".png")||name.endsWith(".jpg")||name.endsWith(".jpeg")||name.endsWith(".csv")||name.endsWith(".xlsx");
}

function renderChips(){
  const wrap=$("bulkChips");
  if(!wrap) return;
  wrap.innerHTML="";
  bulkFiles.forEach((f, idx)=>{
    const chip=document.createElement("span");
    chip.className="chip";
    chip.innerHTML=`<span title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>`;
    const btn=document.createElement("button");
    btn.type="button";
    btn.title="Remove";
    btn.textContent="×";
    btn.addEventListener("click", ()=>{ bulkFiles.splice(idx,1); renderChips(); renderChips();
updateBulkCount(); });
    chip.appendChild(btn);
    wrap.appendChild(chip);
  });
}

function updateBulkCount() {
  setText("bulkCount", `${bulkFiles.length}/20 files selected`);
}

function appendFiles(files) {
  const arr = Array.from(files || []);
  let rejected = 0;
  for (const f of arr) {
    if (!isSupportedBulkFile(f)) { rejected++; continue; }
    if (bulkFiles.length >= 20) break;
    bulkFiles.push(f);
  }
  renderChips();
  updateBulkCount();
  if (rejected) setText("extractBulkStatus", `Skipped ${rejected} unsupported file(s).`);
}

$("btnAddMore")?.addEventListener("click", () => {
  $("files").click();
});

$("btnClearAll")?.addEventListener("click", () => {
  bulkFiles = [];
  lastExtractedItems = [];
  renderChips();
  updateBulkCount();
  const tbody = $("bulkTbody");
  if (tbody) tbody.innerHTML = "";
  const rtb = $("resultsTbody");
  if (rtb) rtb.innerHTML = "";
  setText("extractBulkStatus", "");
  setText("runBulkStatus", "");
  setText("zipNotice", "");
  const zip = $("zipLink");
  if (zip) zip.classList.add("hidden");
});

$("files")?.addEventListener("change", (e) => {
  appendFiles(e.target.files);
  // reset input so selecting same file again still triggers
  e.target.value = "";
});

const dz = $("dropZone");
if (dz) {
  dz.addEventListener("dragover", (e) => {
    e.preventDefault();
    dz.classList.add("dragover");
  });
  dz.addEventListener("dragleave", () => dz.classList.remove("dragover"));
  dz.addEventListener("drop", (e) => {
    e.preventDefault();
    dz.classList.remove("dragover");
    appendFiles(e.dataTransfer.files);
  });
}

updateBulkCount();

// -------------------------
// Extract (single)
// -------------------------
$("btnExtract")?.addEventListener("click", async () => {
  const file = $("file").files?.[0];
  if (!file) {
    setText("extractStatus", "Please choose a file first.");
    return;
  }
  setText("extractStatus", "Extracting…");
  setDisabled("btnExtract", true);

  const fd = new FormData();
  fd.append("files", file);

  try {
    const resp = await fetch("/dbs/extract", { method: "POST", body: fd });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data?.detail || "Extraction failed");

    const item = (data.items || [])[0] || {};
    fillField("certificate_number", item.certificate_number);
    fillField("surname_extracted", item.surname);
    fillField("dob_day", item.dob_day);
    fillField("dob_month", item.dob_month);
    fillField("dob_year", item.dob_year);

    setConf("conf_cert", item.confidence?.certificate_number, "Confidence");
    setConf("conf_surname", item.confidence?.surname, "Confidence");
    setConf("conf_dob", item.confidence?.dob, "Confidence");
    setConf("conf_overall", item.confidence?.overall, "Overall Confidence");

    setText("extractStatus", "Done.");
  } catch (err) {
    setText("extractStatus", err?.message || "Extraction failed.");
  } finally {
    setDisabled("btnExtract", false);
  }
});

// -------------------------
// Run (single)
// -------------------------
$("btnRun")?.addEventListener("click", async () => {
  setText("runStatus", "");
  setDisabled("btnRun", true);

  const step1 = getStep1();
  const payload = {
    ...step1,
    certificate_number: ($("certificate_number").value || "").trim(),
    surname_extracted: ($("surname_extracted").value || "").trim(),
    surname_user: ($("surname_user").value || "").trim(),
    dob_day: ($("dob_day").value || "").trim(),
    dob_month: ($("dob_month").value || "").trim(),
    dob_year: ($("dob_year").value || "").trim(),
  };

  try {
    const resp = await fetch("/dbs/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data?.detail || "Run failed");

    if (data.status === "portal_unavailable") {
      setText("runStatus", data.message || "DBS portal unavailable (maintenance). Try later.");
      return;
    }

    const link = $("singleDownload");
    if (link && data.pdf_url) {
      link.href = data.pdf_url;
      link.classList.remove("hidden");
      link.textContent = "Download PDF";
    }
    setText("runStatus", statusLabel(data.status));
  } catch (err) {
    setText("runStatus", err?.message || "Run failed.");
  } finally {
    setDisabled("btnRun", false);
  }
});

// -------------------------
// Extract (bulk)
// -------------------------
function renderBulkTable(items) {
  const tbody = $("bulkTbody");
  tbody.innerHTML = "";
  (items || []).forEach((it, idx) => {
    const tr = document.createElement("tr");
    tr.dataset.row = String(idx + 1);

    const conf = it.confidence || {};
    const overall = conf.overall ?? "";

    tr.innerHTML = `
      <td><button type="button" class="btnTiny danger btnRemoveRow" data-idx="${idx}">Remove</button></td>
      <td>${idx + 1}</td>
      <td class="muted">${escapeHtml(it.original_filename || "")}</td>
      <td><input class="cell cert" value="${it.certificate_number || ""}"></td>
      <td><input class="cell surname" value="${it.surname || ""}"></td>
      <td class="dob">
        <div class="grid3">
          <input class="cell dd" value="${it.dob_day || ""}" placeholder="DD">
          <input class="cell mm" value="${it.dob_month || ""}" placeholder="MM">
          <input class="cell yy" value="${it.dob_year || ""}" placeholder="YYYY">
        </div>
      </td>
      <td class="muted">
        Cert ${Math.round(conf.certificate_number || 0)}% ·
        Surname ${Math.round(conf.surname || 0)}% ·
        DOB ${Math.round(conf.dob || 0)}% ·
        Overall ${Math.round(overall || 0)}%
      </td>
      <td class="statusCell"></td>
      <td class="dlCell"></td>
    `;
    tbody.appendChild(tr);
  });

// Remove row in extracted table
document.addEventListener("click", (e) => {
  const btn = e.target.closest?.(".btnRemoveRow");
  if (!btn) return;
  const tr = btn.closest("tr");
  if (tr) tr.remove();
  // re-number rows
  Array.from(document.querySelectorAll("#bulkTbody tr")).forEach((row, i) => {
    row.dataset.row = String(i + 1);
    const numCell = row.querySelector("td:nth-child(2)");
    if (numCell) numCell.textContent = String(i + 1);
  });
});
}

$("btnExtractBulk")?.addEventListener("click", async () => {
  if (!bulkFiles.length) {
    setText("extractBulkStatus", "Please add files first.");
    return;
  }
  setText("extractBulkStatus", "Extracting…");
  setDisabled("btnExtractBulk", true);

  const fd = new FormData();
  bulkFiles.slice(0, 20).forEach((f) => fd.append("files", f));

  try {
    const resp = await fetch("/dbs/extract", { method: "POST", body: fd });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data?.detail || "Bulk extraction failed");
    lastExtractedItems = data.items || [];
    renderBulkTable(lastExtractedItems);
    setText("extractBulkStatus", data.notice ? data.notice : "Done.");
    setText("zipNotice", "");
  } catch (err) {
    setText("extractBulkStatus", err?.message || "Bulk extraction failed.");
  } finally {
    setDisabled("btnExtractBulk", false);
  }
});

// -------------------------
// Run (bulk) + live updates via polling
// -------------------------
let pollTimer = null;

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

function collectBulkItems() {
  const rows = Array.from(document.querySelectorAll("#bulkTbody tr"));
  return rows.map((tr) => {
    const cert = tr.querySelector("input.cert")?.value || "";
    const surname = tr.querySelector("input.surname")?.value || "";
    const dd = tr.querySelector("input.dd")?.value || "";
    const mm = tr.querySelector("input.mm")?.value || "";
    const yy = tr.querySelector("input.yy")?.value || "";
    return {
      certificate_number: cert.trim(),
      surname: surname.trim(),
      dob_day: dd.trim(),
      dob_month: mm.trim(),
      dob_year: yy.trim(),
    };
  });
}

function updateBulkUIFromStatus(data) {
  const running = data.running || {};
  const done = running.done || 0;
  const total = running.total || 0;

  setText("runBulkStatus", total ? `Running ${done}/${total}` : "");

  const rows = data.rows || [];
  rows.forEach((r, idx) => {
    const tr = document.querySelector(`#bulkTbody tr[data-row="${idx + 1}"]`);
    if (!tr) return;
    const statusCell = tr.querySelector(".statusCell");
    const dlCell = tr.querySelector(".dlCell");

    statusCell.innerHTML = "";
    statusCell.appendChild(buildBadge(r.status));

    if (r.status === "running" || r.status === "queued") {
      dlCell.innerHTML = "";
      return;
    }

    if (r.status === "portal_unavailable") {
      dlCell.innerHTML = `<span class="muted">No output (portal unavailable)</span>`;
      // Show user-friendly message under status
      const msg = document.createElement("div");
      msg.className = "submsg";
      msg.textContent = "DBS portal unavailable (maintenance). Try later.";
      statusCell.appendChild(msg);
      return;
    }

    if (r.pdf_url) {
      dlCell.innerHTML = `<a class="btnSmall" href="${r.pdf_url}">Download</a>`;
    } else {
      dlCell.innerHTML = `<span class="muted">No output</span>`;
    }
  });

  const zipBtn = $("zipLink");
  if ($("zipNotice")) setText("zipNotice", data.message || "");
  if (zipBtn) {
    if (data.zip_ready && data.zip_url) {
      zipBtn.href = data.zip_url;
      zipBtn.classList.remove("hidden");
      zipBtn.textContent = "Download ZIP";
    } else {
      zipBtn.classList.add("hidden");
    }
  }

  if (data.state === "done") {
    stopPolling();
    setDisabled("btnRunBulk", false);
    setText("runBulkStatus", `Completed ${done}/${total}`);
  }
}

$("btnRunBulk")?.addEventListener("click", async () => {
  stopPolling();
  setText("runBulkStatus", "");
  setDisabled("btnRunBulk", true);

  const step1 = getStep1();
  const items = collectBulkItems();

  try {
    const resp = await fetch("/dbs/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...step1, items }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data?.detail || "Bulk run failed");

    // init status UI
    updateBulkUIFromStatus({ rows: data.rows || [], running: { done: 0, total: (data.rows || []).length }, state: "running" });

    // poll status
    pollTimer = setInterval(async () => {
      try {
        const r = await fetch(data.status_url);
        const st = await r.json();
        if (!r.ok) throw new Error(st?.detail || "Status failed");
        updateBulkUIFromStatus(st);
      } catch (e) {
        // keep trying, but show minimal signal
        setText("runBulkStatus", "Updating…");
      }
    }, 1000);
  } catch (err) {
    setText("runBulkStatus", err?.message || "Bulk run failed.");
    setDisabled("btnRunBulk", false);
  }
});

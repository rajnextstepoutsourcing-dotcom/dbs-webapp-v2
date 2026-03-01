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


// -------------------------
// Premium bulk state (must be declared before first use)
// -------------------------
let bulkFiles = [];
let lastExtractedItems = [];

function updateBulkCount() {
  setText("bulkCount", `${bulkFiles.length}/100 files selected`);
  // Update rows-ready counter if present
  const rowEl = document.getElementById("bulkRowCount");
  if (rowEl) {
    const rows = document.querySelectorAll("#bulkList .bulkRow").length;
    rowEl.textContent = `${rows}/100 rows ready`;
  }
}

function applySavedTheme(){
  try{
    const saved = localStorage.getItem("dbs_theme") || "trust";
    document.documentElement.dataset.theme = saved;
  }catch(e){}
}

function initThemeToggle(){
  try{
    const btnTrust = $("themeTrust");
    const btnModern = $("themeModern");
    const apply = (t)=>{
      document.documentElement.dataset.theme = t;
      localStorage.setItem("dbs_theme", t);
      if (btnTrust) btnTrust.classList.toggle("chipActive", t==="trust");
      if (btnModern) btnModern.classList.toggle("chipActive", t==="modern");
    };
    if (btnTrust) btnTrust.addEventListener("click", ()=>apply("trust"));
    if (btnModern) btnModern.addEventListener("click", ()=>apply("modern"));
    applySavedTheme();
    apply(document.documentElement.dataset.theme || "trust");
  }catch(e){}
}


function downloadBlob(blob, filename){
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename || "download";
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(()=>URL.revokeObjectURL(url), 5000);
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
  if (status === "clear") return "Clear";
  if (status === "not_on_update_service") return "Not on Update Service";
  if (status === "needs_review") return "Needs Review";
  if (status === "portal_unavailable") return "Portal unavailable";
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

function confClass(pct){
  const p = Number(pct||0);
  if (p >= 90) return "confHigh";
  if (p >= 75) return "confMed";
  return "confLow";
}

function confHint(pct){
  const p = Math.max(0, Math.min(100, Math.round(Number(pct||0))));
  return `<span class="confDot ${confClass(p)}" aria-hidden="true"></span>${p}%`;
}

function ensureModeUI() {
  const mode = $("modeBulk").checked ? "bulk" : "single";
  $("singleWrap").classList.toggle("hidden", mode !== "single");
  $("bulkWrap").classList.toggle("hidden", mode !== "bulk");
}

function bindModeHandlers(){
  $("modeSingle")?.addEventListener("change", ensureModeUI);
  $("modeBulk")?.addEventListener("change", ensureModeUI);
}

// -------------------------
// Bulk file handling (append + drag/drop, max 100)
// -------------------------

function escapeHtml(s){
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/\'/g,"&#39;");
}

function isSupportedBulkFile(f){
  const name=(f?.name||"").toLowerCase();
  return name.endsWith(".pdf")||name.endsWith(".png")||name.endsWith(".jpg")||name.endsWith(".jpeg")||
         name.endsWith(".webp")||name.endsWith(".csv")||name.endsWith(".xlsx")||name.endsWith(".docx");
}

function renderChips(){
  const wrap=$("bulkChips");
  if(!wrap) return;
  wrap.innerHTML="";
  bulkFiles.forEach((f, idx)=>{
    const chip=document.createElement("span");
    chip.className="chipFile";
    chip.innerHTML=`<span title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>`;
    const btn=document.createElement("button");
    btn.type="button";
    btn.title="Remove";
    btn.textContent="×";
    btn.addEventListener("click", ()=>{ bulkFiles.splice(idx,1); renderChips(); updateBulkCount(); });
    chip.appendChild(btn);
    wrap.appendChild(chip);
  });
}


function appendFiles(files) {
  const arr = Array.from(files || []);
  let rejected = 0;
  for (const f of arr) {
    if (!isSupportedBulkFile(f)) { rejected++; continue; }
    if (bulkFiles.length >= 100) break;
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
  const list = $("bulkList");
  if (list) list.innerHTML = "";
  setText("extractBulkStatus", "");
  setText("runBulkStatus", "");
  setText("zipNotice", "");
  const zip = $("btnDlZip");
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
  const list = $("bulkList");
  if (!list) return;
  list.innerHTML = "";

  (items || []).forEach((it, idx) => {
    const row = document.createElement("div");
    row.className = "bulkRow";
    row.dataset.row = String(idx + 1);
    row.dataset.originalFilename = it.original_filename || "";
    row.dataset.forename = it.forename || "";
    row.dataset.issueDay = it.issue_day || "";
    row.dataset.issueMonth = it.issue_month || "";
    row.dataset.issueYear = it.issue_year || "";

    const conf = it.confidence || {};
    const overall = conf.overall ?? "";

    row.innerHTML = `
      <div class="bulkRowTop">
        <div class="bulkRowLeft">
          <button type="button" class="iconBtn btnRemoveRow" data-idx="${idx}" aria-label="Remove row" title="Remove">×</button>
          <div class="bulkIndex">#${idx + 1}</div>
        </div>

        <div class="bulkFields">
          <div class="fieldBlock">
            <div class="fieldLabel">Source</div>
            <div class="bulkSource">${escapeHtml(it.original_filename || "")}</div>
          </div>

          <div class="fieldBlock">
            <div class="fieldLabel">Certificate No</div>
            <input class="cell cert" value="${it.certificate_number || ""}">
            <div class="fieldHint">${confHint(conf.certificate_number || 0)}</div>
          </div>

          <div class="fieldBlock">
            <div class="fieldLabel">Surname</div>
            <input class="cell surname" value="${it.surname || ""}">
            <div class="fieldHint">${confHint(conf.surname || 0)}</div>
          </div>

          <div class="fieldBlock">
            <div class="fieldLabel">DOB</div>
            <div class="fieldRow">
              <input class="cell dd" value="${it.dob_day || ""}" placeholder="DD">
              <input class="cell mm" value="${it.dob_month || ""}" placeholder="MM">
              <input class="cell yy" value="${it.dob_year || ""}" placeholder="YYYY">
            </div>
            <div class="fieldHint">${confHint(conf.dob || 0)}</div>
          </div>
        </div>

        <div class="bulkActions">
          <div class="statusWrap"><span class="statusCell"></span></div>
          <div class="dlWrap"><span class="dlCell muted">Not run yet</span></div>
        </div>
      </div>

      <div class="bulkMeta">
        <div class="confLine">Overall confidence: <span class="confDot ${confClass(overall||0)}" aria-hidden="true"></span>${Math.round(overall || 0)}%</div>
      </div>
    `;
    list.appendChild(row);
  });

  updateBulkCount();
}

// Remove row in extracted list (cards)
document.addEventListener("click", (e) => {
  const btn = e.target.closest?.(".btnRemoveRow");
  if (!btn) return;
  const row = btn.closest(".bulkRow");
  if (row) row.remove();

  // re-number rows
  Array.from(document.querySelectorAll("#bulkList .bulkRow")).forEach((r, i) => {
    r.dataset.row = String(i + 1);
    const idxEl = r.querySelector(".bulkIndex");
    if (idxEl) idxEl.textContent = `#${i + 1}`;
  });

  lastExtractedItems = collectBulkItems();
  updateBulkCount();
});

$("btnExtractBulk")?.addEventListener("click", async () => {
  if (!bulkFiles.length) {
    setText("extractBulkStatus", "Please add files first.");
    return;
  }
  setText("extractBulkStatus", "Extracting…");
  setDisabled("btnExtractBulk", true);

  const fd = new FormData();
  bulkFiles.slice(0, 100).forEach((f) => fd.append("files", f));

  try {
    const resp = await fetch("/dbs/extract", { method: "POST", body: fd });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data?.detail || "Bulk extraction failed");
    lastExtractedItems = data.items || [];
    renderBulkTable(lastExtractedItems);
    updateBulkCount();
    setText("extractBulkStatus", data.notice ? data.notice : "Done.");
    setText("zipNotice", "");
  } catch (err) {
    setText("extractBulkStatus", err?.message || "Bulk extraction failed.");
  } finally {
    setDisabled("btnExtractBulk", false);
  }
});


async function exportExtract(fmt){
  if(!lastExtractedItems?.length){
    setText("extractBulkStatus","Nothing to export. Please Extract first.");
    return;
  }
  const resp = await fetch("/dbs/export/extract", {
    method:"POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({format: fmt, items: lastExtractedItems})
  });
  if(!resp.ok){
    const data = await resp.json().catch(()=>({}));
    throw new Error(data?.detail || "Export failed");
  }
  const blob = await resp.blob();
  downloadBlob(blob, fmt==="csv" ? "extract.csv" : "extract.xlsx");
}

async function exportResults(fmt){
  // We store last job payload in window._lastJobPayload from poll updates
  const payload = window._lastJobPayload;
  if(!payload?.rows?.length){
    setText("runBulkStatus","Nothing to export. Please Run first.");
    return;
  }
  const resp = await fetch("/dbs/export/results", {
    method:"POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({format: fmt, checked_date: payload.checked_date || "", rows: payload.rows || []})
  });
  if(!resp.ok){
    const data = await resp.json().catch(()=>({}));
    throw new Error(data?.detail || "Export failed");
  }
  const blob = await resp.blob();
  downloadBlob(blob, fmt==="csv" ? "results.csv" : "results.xlsx");
}

$("btnDlExtractXlsx")?.addEventListener("click", async ()=>{
  try{ await exportExtract("xlsx"); } catch(e){ setText("extractBulkStatus", e?.message || "Export failed."); }
});
$("btnDlExtractCsv")?.addEventListener("click", async ()=>{
  try{ await exportExtract("csv"); } catch(e){ setText("extractBulkStatus", e?.message || "Export failed."); }
});
$("btnDlResultsXlsx")?.addEventListener("click", async ()=>{
  try{ await exportResults("xlsx"); } catch(e){ setText("runBulkStatus", e?.message || "Export failed."); }
});
$("btnDlResultsCsv")?.addEventListener("click", async ()=>{
  try{ await exportResults("csv"); } catch(e){ setText("runBulkStatus", e?.message || "Export failed."); }
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
  const rows = Array.from(document.querySelectorAll("#bulkList .bulkRow"));
  return rows.map((row) => {
    const cert = row.querySelector("input.cert")?.value || "";
    const surname = row.querySelector("input.surname")?.value || "";
    const dd = row.querySelector("input.dd")?.value || "";
    const mm = row.querySelector("input.mm")?.value || "";
    const yy = row.querySelector("input.yy")?.value || "";
    return {
      original_filename: row.dataset.originalFilename || "",
      forename: row.dataset.forename || "",
      issue_day: row.dataset.issueDay || "",
      issue_month: row.dataset.issueMonth || "",
      issue_year: row.dataset.issueYear || "",
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
    const tr = document.querySelector(`#bulkList .bulkRow[data-row="${idx + 1}"]`);
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
      dlCell.innerHTML = `<span class="muted">DBS portal unavailable (maintenance). Try later.</span>`;
      return;
    }

    if (r.pdf_url) {
      dlCell.innerHTML = `<a class="btnSmall downloadBtn" href="${r.pdf_url}">⬇ Download PDF</a>`;
    } else {
      dlCell.innerHTML = `<span class="muted">No output</span>`;
    }
  });

  const zipBtn = $("btnDlZip") || $("btnDlZip");
  if ($("zipNotice")) setText("zipNotice", data.message || "");
  if (zipBtn) {
    if (data.zip_ready && data.zip_url) {
      zipBtn.href = data.zip_url;
      zipBtn.classList.remove("hidden");
      zipBtn.textContent = "Download All PDFs (ZIP)";
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


document.addEventListener("DOMContentLoaded", () => {
  applySavedTheme();
  initThemeToggle();
  bindModeHandlers();
  ensureModeUI();
  updateBulkCount();
});

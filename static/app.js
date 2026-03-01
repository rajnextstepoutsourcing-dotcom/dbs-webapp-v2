
const $ = (id) => document.getElementById(id);

function setStatus(el, msg) {
  if (!el) return;
  el.textContent = msg || "";
}

function fillField(id, value) {
  const el = $(id);
  if (!el) return;
  el.value = value || "";
}

function setConf(id, value) {
  const el = $(id);
  if (!el) return;
  if (value === null || value === undefined || value === "") {
    el.textContent = "";
    return;
  }
  const pct = Math.max(0, Math.min(100, Math.round(Number(value) || 0)));
  // Neutral naming (no AI mention)
  el.textContent = `Verification: ${pct}%`;
}

function getStep1() {
  return {
    organisation_name: ($("org").value || "").trim(),
    employee_forename: ($("forename").value || "").trim(),
    employee_surname: ($("surname_user").value || "").trim(),
  };
}

function parseFilenameFromDisposition(disposition) {
  if (!disposition) return "";
  // Content-Disposition: attachment; filename="ABC.pdf"
  const m = /filename\*?=(?:UTF-8''|")?([^\";]+)"?/i.exec(disposition);
  if (!m) return "";
  try {
    return decodeURIComponent(m[1]);
  } catch {
    return m[1];
  }
}

async function downloadBlobWithFilename(resp) {
  const blob = await resp.blob();
  const disposition = resp.headers.get("Content-Disposition") || "";
  const filename = parseFilenameFromDisposition(disposition) || "download.pdf";
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);
}

function switchMode(mode) {
  const singleWrap = $("singleWrap");
  const bulkWrap = $("bulkWrap");
  if (mode === "bulk") {
    singleWrap.classList.add("hidden");
    bulkWrap.classList.remove("hidden");
  } else {
    bulkWrap.classList.add("hidden");
    singleWrap.classList.remove("hidden");
  }
}

// ---- Mode UI ----
$("modeSingle").addEventListener("change", () => switchMode("single"));
$("modeBulk").addEventListener("change", () => switchMode("bulk"));

$("files").addEventListener("change", () => {
  const n = $("files").files ? $("files").files.length : 0;
  if (n > 20) {
    setStatus($("bulkCount"), "Max 20 files. Please select 20 or fewer.");
  } else {
    setStatus($("bulkCount"), `${n}/20 selected`);
  }
});

// ---- Single extract ----
$("btnExtract").addEventListener("click", async () => {
  const f = $("file").files && $("file").files[0];
  if (!f) {
    setStatus($("extractStatus"), "Please choose a file.");
    return;
  }
  setStatus($("extractStatus"), "Extracting…");
  try {
    const fd = new FormData();
    fd.append("files", f);

    const resp = await fetch("/dbs/extract", { method: "POST", body: fd });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || "Extract failed.");
    }
    const data = await resp.json();
    const item = (data.items && data.items[0]) || {};

    fillField("certificate_number", item.certificate_number);
    fillField("surname_extracted", item.surname);
    fillField("dob_day", item.dob_day);
    fillField("dob_month", item.dob_month);
    fillField("dob_year", item.dob_year);
    fillField("issue_day", item.issue_day);
    fillField("issue_month", item.issue_month);
    fillField("issue_year", item.issue_year);

    setConf("conf_cert", item.confidence?.certificate_number);
    setConf("conf_surname", item.confidence?.surname);
    setConf("conf_dob", item.verification_score ?? item.confidence?.dob);
    setConf("conf_issue", item.confidence?.issue_date);

    setStatus($("extractStatus"), "Done. Review/edit fields if needed.");
  } catch (e) {
    setStatus($("extractStatus"), String(e.message || e));
  }
});

// ---- Single run ----
$("btnRun").addEventListener("click", async () => {
  const step1 = getStep1();
  const payload = {
    ...step1,
    certificate_number: ($("certificate_number").value || "").trim(),
    surname_user: ($("surname_user").value || "").trim(),
    surname_extracted: ($("surname_extracted").value || "").trim(),
    dob_day: ($("dob_day").value || "").trim(),
    dob_month: ($("dob_month").value || "").trim(),
    dob_year: ($("dob_year").value || "").trim(),
  };

  setStatus($("runStatus"), "Running…");
  try {
    const resp = await fetch("/dbs/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail?.message || err.detail || "Run failed.");
    }

    await downloadBlobWithFilename(resp);
    setStatus($("runStatus"), "Downloaded.");
  } catch (e) {
    setStatus($("runStatus"), String(e.message || e));
  }
});

// ---- Bulk extract ----
function renderBulkTable(items) {
  const tbody = $("bulkTbody");
  tbody.innerHTML = "";
  items.forEach((it, idx) => {
    const i = idx + 1;
    const tr = document.createElement("tr");
    tr.dataset.row = String(i);

    tr.innerHTML = `
      <td>${i}</td>
      <td><input class="cell" data-k="certificate_number" value="${(it.certificate_number || "").replaceAll('"','&quot;')}"></td>
      <td><input class="cell" data-k="surname" value="${(it.surname || "").replaceAll('"','&quot;')}"></td>
      <td>
        <div class="grid3">
          <input class="cell" data-k="dob_day" placeholder="DD" value="${(it.dob_day || "").replaceAll('"','&quot;')}">
          <input class="cell" data-k="dob_month" placeholder="MM" value="${(it.dob_month || "").replaceAll('"','&quot;')}">
          <input class="cell" data-k="dob_year" placeholder="YYYY" value="${(it.dob_year || "").replaceAll('"','&quot;')}">
        </div>
      </td>
      <td>
        <div class="grid3">
          <input class="cell" data-k="issue_day" placeholder="DD" value="${(it.issue_day || "").replaceAll('"','&quot;')}">
          <input class="cell" data-k="issue_month" placeholder="MM" value="${(it.issue_month || "").replaceAll('"','&quot;')}">
          <input class="cell" data-k="issue_year" placeholder="YYYY" value="${(it.issue_year || "").replaceAll('"','&quot;')}">
        </div>
      </td>
      <td><span class="pill">${Math.round(Number(it.verification_score || 0))}%</span></td>
    `;
    tbody.appendChild(tr);
  });
}

function collectBulkItems() {
  const tbody = $("bulkTbody");
  const rows = Array.from(tbody.querySelectorAll("tr"));
  return rows.map((tr) => {
    const obj = {};
    const inputs = Array.from(tr.querySelectorAll("input.cell"));
    for (const inp of inputs) {
      const k = inp.dataset.k;
      obj[k] = (inp.value || "").trim();
    }
    return obj;
  });
}

$("btnExtractBulk").addEventListener("click", async () => {
  const filesEl = $("files");
  const fileList = filesEl.files ? Array.from(filesEl.files) : [];
  if (fileList.length === 0) {
    setStatus($("extractBulkStatus"), "Please choose files.");
    return;
  }
  if (fileList.length > 20) {
    setStatus($("extractBulkStatus"), "Max 20 files.");
    return;
  }

  setStatus($("extractBulkStatus"), "Extracting…");
  $("bulkResults").classList.add("hidden");
  try {
    const fd = new FormData();
    fileList.forEach((f) => fd.append("files", f));
    const resp = await fetch("/dbs/extract", { method: "POST", body: fd });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || "Extract failed.");
    }
    const data = await resp.json();
    const items = data.items || [];
    renderBulkTable(items);
    setStatus($("extractBulkStatus"), `Done. Review/edit ${items.length} rows.`);
  } catch (e) {
    setStatus($("extractBulkStatus"), String(e.message || e));
  }
});

// ---- Bulk run ----
function renderBulkResults(payload) {
  const { zip_url, zip_name, results } = payload;
  const zipLink = $("zipLink");
  if (zip_url) {
    zipLink.href = zip_url;
    zipLink.textContent = `Download ZIP (${zip_name || "DBS_Checks.zip"})`;
    zipLink.classList.remove("hidden");
  } else {
    zipLink.classList.add("hidden");
  }

  const tbody = $("resultsTbody");
  tbody.innerHTML = "";
  (results || []).forEach((r) => {
    const tr = document.createElement("tr");
    const status = r.ok ? "✅" : "❌";
    const dl = r.ok && r.pdf_url ? `<a class="btnLink" href="${r.pdf_url}">Download</a>` : "";
    tr.innerHTML = `
      <td>${r.row || ""}</td>
      <td>${status}</td>
      <td class="mono">${(r.filename || "").replaceAll("<","&lt;")}</td>
      <td>${dl}</td>
    `;
    tbody.appendChild(tr);
  });

  $("bulkResults").classList.remove("hidden");
}

$("btnRunBulk").addEventListener("click", async () => {
  const step1 = getStep1();
  const items = collectBulkItems();
  if (items.length === 0) {
    setStatus($("runBulkStatus"), "No extracted rows.");
    return;
  }

  const download_mode = $("bulkDownloadMode").value || "zip";
  const payload = {
    ...step1,
    items,
    download_mode,
  };

  setStatus($("runBulkStatus"), "Running… (this can take a few minutes)");
  try {
    const resp = await fetch("/dbs/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail?.message || err.detail || "Run failed.");
    }
    const data = await resp.json();
    renderBulkResults(data);
    setStatus($("runBulkStatus"), "Done.");
    if (download_mode === "zip" && data.zip_url) {
      // Auto-start single download (ZIP is safe)
      window.location.href = data.zip_url;
    }
  } catch (e) {
    setStatus($("runBulkStatus"), String(e.message || e));
  }
});

// init
switchMode("single");

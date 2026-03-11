const DEFAULT_HEADERS = [
  "Job Title",
  "Company",
  "Location",
  "Employment Type",
  "Seniority",
  "Salary Range",
  "Applicants",
  "Posted",
  "Apply Link",
  "Company URL",
  "Job Summary",
  "AI Fit",
  "Resume Doc",
  "Fit Score",
  "Recommendation",
  "Decision",
  "Reasoning",
  "Missing Skills",
  "Candidate Highlights",
  "Resume Focus",
  "Resume Summary",
  "Resume Path",
  "Cover Letter Path",
  "Email Intro Path",
  "Source Site",
  "Search Title",
  "Search Country",
  "Run ID",
  "Searched At"
];

function doGet() {
  return ContentService.createTextOutput(
    JSON.stringify({ ok: true, message: "Apps Script sink is live" })
  ).setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  const payload = JSON.parse(e.postData.contents || "{}");
  const spreadsheetId = payload.spreadsheetId;
  const worksheetName = payload.worksheet || "Jobs";
  const headers = payload.headers || DEFAULT_HEADERS;
  const rows = payload.rows || [];

  if (!spreadsheetId) {
    return ContentService.createTextOutput(
      JSON.stringify({ ok: false, error: "spreadsheetId is required" })
    ).setMimeType(ContentService.MimeType.JSON);
  }

  const spreadsheet = SpreadsheetApp.openById(spreadsheetId);
  const sheet = getOrCreateSheet_(spreadsheet, worksheetName);
  ensureHeaders_(sheet, headers);

  if (rows.length > 0) {
    const values = rows.map((row) => headers.map((header) => row[header] || ""));
    sheet.getRange(sheet.getLastRow() + 1, 1, values.length, headers.length).setValues(values);
  }

  return ContentService.createTextOutput(
    JSON.stringify({ ok: true, appended: rows.length })
  ).setMimeType(ContentService.MimeType.JSON);
}

function getOrCreateSheet_(spreadsheet, worksheetName) {
  const existing = spreadsheet.getSheetByName(worksheetName);
  if (existing) {
    return existing;
  }
  return spreadsheet.insertSheet(worksheetName);
}

function ensureHeaders_(sheet, headers) {
  const firstRow = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
  const hasHeaders = firstRow.some((value) => value !== "");
  if (!hasHeaders) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return;
  }

  const existingHeaders = firstRow.filter((value) => value !== "");
  const missingHeaders = headers.filter((header) => !existingHeaders.includes(header));
  if (missingHeaders.length > 0) {
    const mergedHeaders = existingHeaders.concat(missingHeaders);
    sheet.getRange(1, 1, 1, mergedHeaders.length).setValues([mergedHeaders]);
  }
}

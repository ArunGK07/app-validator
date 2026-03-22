import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import { formatTaskArtifactName } from '../workspace-config.mjs';
import { buildValidationChecklist, enrichValidationResult, VALIDATION_CHECKLIST_CATALOG } from './checklist.mjs';

export const VALIDATOR_NAMES = {
  promptStructure: 'PromptStructureValidator',
  plsqlProgram: 'PLSQLProgramValidator',
  complexityTableCount: 'ComplexityTableCountValidator',
  namingStandard: 'NamingStandardValidator',
  artifactAlignment: 'ArtifactAlignmentValidator',
  master: 'MasterValidator',
};

export function buildValidationResult({
  taskId,
  turnId = null,
  validator,
  item,
  ruleId,
  status,
  expected = null,
  present = null,
  update = null,
  sourceFile = null,
  line = null,
}) {
  const normalizedStatus = String(status || '').toUpperCase();
  if (!['PASS', 'FAIL'].includes(normalizedStatus)) {
    throw new Error(`Unsupported validation status: ${status}`);
  }

  return {
    taskId: String(taskId),
    turnId: Number.isInteger(turnId) ? turnId : null,
    validator,
    item,
    ruleId,
    status: normalizedStatus,
    expected,
    present,
    update,
    sourceFile,
    line: Number.isInteger(line) ? line : null,
  };
}

export function createPass(validatorName, taskId, turnId, item, ruleId, sourceFile = null) {
  return buildValidationResult({
    taskId,
    turnId,
    validator: validatorName,
    item,
    ruleId,
    status: 'PASS',
    sourceFile,
  });
}

export function createFail(
  validatorName,
  taskId,
  turnId,
  item,
  ruleId,
  {
    expected,
    present,
    update,
    sourceFile = null,
    line = null,
  },
) {
  return buildValidationResult({
    taskId,
    turnId,
    validator: validatorName,
    item,
    ruleId,
    status: 'FAIL',
    expected,
    present,
    update,
    sourceFile,
    line,
  });
}

export function summarizeResults(results) {
  let total = 0;
  let passed = 0;
  let failed = 0;
  const taskFailures = new Map();

  for (const result of results) {
    total += 1;
    if (result.status === 'PASS') {
      passed += 1;
    } else {
      failed += 1;
    }

    const existing = taskFailures.get(result.taskId) ?? false;
    taskFailures.set(result.taskId, existing || result.status === 'FAIL');
  }

  const tasksTotal = taskFailures.size;
  const tasksFailed = [...taskFailures.values()].filter(Boolean).length;

  return {
    total,
    passed,
    failed,
    tasksTotal,
    tasksPassed: tasksTotal - tasksFailed,
    tasksFailed,
  };
}

export function buildReportNames(taskId) {
  return {
    master: `master_validator_task_${taskId}.json`,
    promptStructure: `promptstructure_task_${taskId}.json`,
    plsqlCombined: `plsqlcombined_task_${taskId}.json`,
    namingStandard: `namingstandard_task_${taskId}.json`,
    artifactAlignment: `artifactalignment_task_${taskId}.json`,
    fileIndex: `files/index_task_${taskId}.json`,
  };
}

export async function ensureValidationDir(taskDir) {
  const validationDir = join(taskDir, '_validation');
  await mkdir(validationDir, { recursive: true });
  return validationDir;
}

export async function writeValidationReport(reportPath, validatorName, results, extra = null) {
  const enrichedResults = results.map((result) => enrichValidationResult(result));
  const payload = {
    validator: validatorName,
    generatedAt: new Date().toISOString(),
    summary: summarizeResults(enrichedResults),
    results: enrichedResults,
  };

  if (extra && typeof extra === 'object') {
    payload.extra = extra;
  }

  await mkdir(dirname(reportPath), { recursive: true });
  await writeFile(reportPath, JSON.stringify(payload, null, 2), 'utf8');
  return payload;
}

export async function writeMasterValidationReport(reportPath, taskId, validatorReports) {
  const generatedAt = new Date().toISOString();
  const enrichedValidatorReports = validatorReports.map((entry) => ({
    ...entry,
    results: entry.results.map((result) => enrichValidationResult(result)),
  }));
  const allResults = enrichedValidatorReports.flatMap((entry) => entry.results);
  const itemsSummary = summarizeResults(allResults);
  const validatorsPassed = enrichedValidatorReports.filter((entry) => entry.summary.failed === 0).length;
  const validatorsFailed = enrichedValidatorReports.length - validatorsPassed;
  const checklist = buildValidationChecklist(enrichedValidatorReports);
  const fileArtifacts = await writeFileValidationArtifacts(dirname(reportPath), taskId, allResults, checklist, generatedAt);

  const payload = {
    validator: VALIDATOR_NAMES.master,
    generatedAt,
    taskId: String(taskId),
    summary: {
      validatorsRun: validatorReports.length,
      validatorsPassed,
      validatorsFailed,
      itemsTotal: itemsSummary.total,
      itemsPassed: itemsSummary.passed,
      itemsFailed: itemsSummary.failed,
      taskFailHits: itemsSummary.tasksFailed,
      tasksTotal: itemsSummary.tasksTotal,
      tasksPassed: itemsSummary.tasksPassed,
      tasksFailed: itemsSummary.tasksFailed,
    },
    validators: enrichedValidatorReports.map((entry) => ({
      validator: entry.validator,
      summary: entry.summary,
      success: entry.summary.failed === 0,
      reportFile: entry.reportFile,
    })),
    checklistCatalog: VALIDATION_CHECKLIST_CATALOG,
    checklist,
    results: allResults,
    fileReports: fileArtifacts.files,
  };

  await mkdir(dirname(reportPath), { recursive: true });
  await writeFile(reportPath, JSON.stringify(payload, null, 2), 'utf8');
  return {
    ...payload,
    fileIndex: fileArtifacts.indexFile,
  };
}

export function metadataBool(value, defaultValue = false) {
  if (value === null || value === undefined || value === '') {
    return defaultValue;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'y'].includes(normalized)) {
      return true;
    }
    if (['false', '0', 'no', 'n', ''].includes(normalized)) {
      return false;
    }
  }

  if (typeof value === 'number') {
    return Boolean(value);
  }

  return defaultValue;
}

export function parseReasoningTypes(metadata) {
  const raw = metadata?.required_reasoning_types ?? metadata?.target_reasoning_types ?? [];
  if (Array.isArray(raw)) {
    return raw.map((item) => String(item).trim()).filter(Boolean);
  }

  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return parsed.map((item) => String(item).trim()).filter(Boolean);
      }
    } catch {
      return raw
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean);
    }
  }

  return [];
}

export function findLineNumber(text, matchStart) {
  return text.slice(0, matchStart).split(/\r?\n/).length;
}

export function normalizeComplexity(rawValue) {
  const text = String(rawValue ?? '').trim().toLowerCase();
  return text === 'complex' ? 'advanced' : text;
}

export async function loadTaskMetadata(taskDir, taskId) {
  const fileName = formatTaskArtifactName('metadata_file', { taskId });
  const metadataFile = join(taskDir, fileName);

  try {
    const content = await readFile(metadataFile, 'utf8');
    const parsed = JSON.parse(content);

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      return {
        metadata: null,
        metadataFile,
        errors: [
          createFail(VALIDATOR_NAMES.promptStructure, taskId, null, 'Metadata', 'invalid_shape', {
            expected: 'metadata payload must be a JSON object',
            present: `${fileName} contains ${Array.isArray(parsed) ? 'array' : typeof parsed}`,
            update: 'rewrite the metadata file as a JSON object',
            sourceFile: fileName,
          }),
        ],
      };
    }

    return {
      metadata: parsed,
      metadataFile,
      errors: [],
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown metadata error';

    return {
      metadata: null,
      metadataFile,
      errors: [
        createFail(VALIDATOR_NAMES.promptStructure, taskId, null, 'Metadata', 'invalid_or_missing_metadata', {
          expected: `metadata file ${fileName} must exist and contain valid JSON`,
          present: message,
          update: 'fetch task output to regenerate metadata, then rerun validation',
          sourceFile: fileName,
        }),
      ],
    };
  }
}

export async function loadTurnTextArtifact(taskDir, templateKey, taskId, turnNumber) {
  const fileName = formatTaskArtifactName(templateKey, { taskId, turnNumber });
  const filePath = join(taskDir, fileName);

  try {
    const text = (await readFile(filePath, 'utf8')).trim();
    return {
      text,
      fileName,
      filePath,
    };
  } catch {
    return {
      text: '',
      fileName,
      filePath,
    };
  }
}

export function formatLogText(masterReport, validatorReports) {
  const lines = [];

  lines.push(`Validation run completed at ${new Date().toISOString()}`);
  lines.push(`Task: ${masterReport.taskId}`);
  lines.push(
    `Validators: ${masterReport.summary.validatorsRun} total, ${masterReport.summary.validatorsPassed} passed, ${masterReport.summary.validatorsFailed} failed`,
  );
  lines.push(
    `Checks: ${masterReport.summary.itemsTotal} total, ${masterReport.summary.itemsPassed} passed, ${masterReport.summary.itemsFailed} failed`,
  );
  lines.push('');

  for (const report of validatorReports) {
    lines.push(`${report.validator}: ${report.summary.failed === 0 ? 'PASS' : 'FAIL'}`);
    lines.push(`  report: ${report.reportFile}`);
    lines.push(`  checks: ${report.summary.total} total, ${report.summary.passed} passed, ${report.summary.failed} failed`);

    for (const result of report.results.filter((entry) => entry.status === 'FAIL')) {
      const turnLabel = result.turnId === null ? 'task' : `turn ${result.turnId}`;
      lines.push(`  - ${turnLabel} | ${result.item}`);
      if (result.sourceFile) {
        lines.push(`    source: ${result.sourceFile}${result.line ? ` line ${result.line}` : ''}`);
      }
      if (result.expected) {
        lines.push(`    expected: ${result.expected}`);
      }
      if (result.present) {
        lines.push(`    present: ${result.present}`);
      }
      if (result.update) {
        lines.push(`    update: ${result.update}`);
      }
    }

    lines.push('');
  }

  return lines.join('\n');
}

async function writeFileValidationArtifacts(validationDir, taskId, results, checklist, generatedAt) {
  const filesDir = join(validationDir, 'files');
  await mkdir(filesDir, { recursive: true });

  const resultsByFile = new Map();
  const checklistByFile = new Map();

  for (const row of results) {
    if (!row.sourceFile) {
      continue;
    }

    const existing = resultsByFile.get(row.sourceFile) ?? [];
    existing.push(row);
    resultsByFile.set(row.sourceFile, existing);
  }

  for (const row of checklist) {
    if (!row.sourceFile) {
      continue;
    }

    const existing = checklistByFile.get(row.sourceFile) ?? [];
    existing.push(row);
    checklistByFile.set(row.sourceFile, existing);
  }

  const sourceFiles = [...new Set([...resultsByFile.keys(), ...checklistByFile.keys()])].sort((left, right) =>
    left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' }),
  );
  const files = [];

  for (const sourceFile of sourceFiles) {
    const fileResults = [...(resultsByFile.get(sourceFile) ?? [])].sort(compareValidationRows);
    const fileChecklist = [...(checklistByFile.get(sourceFile) ?? [])].sort(compareChecklistRows);
    const summary = summarizeResults(fileResults);
    const validators = [...new Set(fileResults.map((row) => row.validator))].map((validator) => {
      const validatorResults = fileResults.filter((row) => row.validator === validator);
      const validatorSummary = summarizeResults(validatorResults);
      return {
        validator,
        summary: validatorSummary,
        success: validatorSummary.failed === 0,
      };
    });
    const turnIds = [...new Set(fileResults.map((row) => row.turnId).filter((value) => Number.isInteger(value)))];
    const baseName = buildFileValidationBaseName(sourceFile);
    const reportFile = `_validation/files/${baseName}.json`;
    const logFile = `_validation/files/${baseName}.log`;
    const reportPayload = {
      validator: 'FileValidationReport',
      generatedAt,
      taskId: String(taskId),
      sourceFile,
      turnId: turnIds.length === 1 ? turnIds[0] : null,
      summary,
      validators,
      checklist: fileChecklist,
      results: fileResults,
    };

    await writeFile(join(filesDir, `${baseName}.json`), JSON.stringify(reportPayload, null, 2), 'utf8');
    await writeFile(join(filesDir, `${baseName}.log`), formatFileLogText(reportPayload), 'utf8');

    files.push({
      sourceFile,
      turnId: reportPayload.turnId,
      reportFile,
      logFile,
      summary,
      validators,
    });
  }

  const indexFile = `_validation/files/index_task_${taskId}.json`;
  const indexPayload = {
    validator: 'FileValidationIndex',
    generatedAt,
    taskId: String(taskId),
    files,
  };
  await writeFile(join(filesDir, `index_task_${taskId}.json`), JSON.stringify(indexPayload, null, 2), 'utf8');

  return {
    indexFile,
    files,
  };
}

function compareValidationRows(left, right) {
  return compareMaybeNumber(left.line, right.line)
    || compareStrings(left.validator, right.validator)
    || compareStrings(left.item, right.item)
    || compareStrings(left.ruleId, right.ruleId);
}

function compareChecklistRows(left, right) {
  return compareMaybeNumber(left.line, right.line)
    || compareStrings(left.validator, right.validator)
    || compareStrings(left.item, right.item)
    || compareStrings(left.ruleId ?? '', right.ruleId ?? '');
}

function compareMaybeNumber(left, right) {
  const normalizedLeft = Number.isInteger(left) ? left : Number.MAX_SAFE_INTEGER;
  const normalizedRight = Number.isInteger(right) ? right : Number.MAX_SAFE_INTEGER;
  return normalizedLeft - normalizedRight;
}

function compareStrings(left, right) {
  return String(left ?? '').localeCompare(String(right ?? ''), undefined, { numeric: true, sensitivity: 'base' });
}

function buildFileValidationBaseName(sourceFile) {
  const normalized = String(sourceFile ?? '').replace(/\\/g, '/');
  const sourceBaseName = normalized.split('/').pop() ?? normalized;
  const slug = sourceBaseName
    .replace(/\.[^.]+$/, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    || 'file';
  const hash = createHash('sha1').update(normalized).digest('hex').slice(0, 8);
  return `${slug}__${hash}`;
}

function formatFileLogText(report) {
  const lines = [];

  lines.push(`Validation file report generated at ${report.generatedAt}`);
  lines.push(`Task: ${report.taskId}`);
  lines.push(`Source: ${report.sourceFile}`);
  if (report.turnId !== null) {
    lines.push(`Turn: ${report.turnId}`);
  }
  lines.push(`Checks: ${report.summary.total} total, ${report.summary.passed} passed, ${report.summary.failed} failed`);
  lines.push('');

  const failed = report.results.filter((row) => row.status === 'FAIL');
  const passed = report.results.filter((row) => row.status === 'PASS');

  if (failed.length) {
    lines.push('Failures:');
    appendFileLogRows(lines, failed);
    lines.push('');
  }

  if (passed.length) {
    lines.push('Passes:');
    appendFileLogRows(lines, passed);
  }

  return lines.join('\n');
}

function appendFileLogRows(lines, rows) {
  for (const row of rows) {
    lines.push(`- ${row.validator} | ${row.item} | ${row.status}`);
    if (row.sourceFile) {
      lines.push(`  source: ${row.sourceFile}${row.line ? ` line ${row.line}` : ''}`);
    }
    if (row.expected) {
      lines.push(`  expected: ${row.expected}`);
    }
    if (row.present) {
      lines.push(`  present: ${row.present}`);
    }
    if (row.update) {
      lines.push(`  update: ${row.update}`);
    }
  }
}

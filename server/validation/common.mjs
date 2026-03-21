import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

import { formatTaskArtifactName } from '../workspace-config.mjs';

export const VALIDATOR_NAMES = {
  promptStructure: 'PromptStructureValidator',
  plsqlProgram: 'PLSQLProgramValidator',
  complexityTableCount: 'ComplexityTableCountValidator',
  namingStandard: 'NamingStandardValidator',
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
  };
}

export async function ensureValidationDir(taskDir) {
  const validationDir = join(taskDir, '_validation');
  await mkdir(validationDir, { recursive: true });
  return validationDir;
}

export async function writeValidationReport(reportPath, validatorName, results, extra = null) {
  const payload = {
    validator: validatorName,
    generatedAt: new Date().toISOString(),
    summary: summarizeResults(results),
    results,
  };

  if (extra && typeof extra === 'object') {
    payload.extra = extra;
  }

  await mkdir(dirname(reportPath), { recursive: true });
  await writeFile(reportPath, JSON.stringify(payload, null, 2), 'utf8');
  return payload;
}

export async function writeMasterValidationReport(reportPath, taskId, validatorReports) {
  const allResults = validatorReports.flatMap((entry) => entry.results);
  const itemsSummary = summarizeResults(allResults);
  const validatorsPassed = validatorReports.filter((entry) => entry.summary.failed === 0).length;
  const validatorsFailed = validatorReports.length - validatorsPassed;

  const payload = {
    validator: VALIDATOR_NAMES.master,
    generatedAt: new Date().toISOString(),
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
    validators: validatorReports.map((entry) => ({
      validator: entry.validator,
      summary: entry.summary,
      success: entry.summary.failed === 0,
      reportFile: entry.reportFile,
    })),
    results: allResults,
  };

  await mkdir(dirname(reportPath), { recursive: true });
  await writeFile(reportPath, JSON.stringify(payload, null, 2), 'utf8');
  return payload;
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

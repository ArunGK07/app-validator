import { access, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { formatTaskArtifactName } from '../workspace-config.mjs';

const TURN_FIELD_MAPPINGS = [
  { sourceKey: 'user_request', targetField: 'user', templateKey: 'turn_user_file', formatter: formatTextField },
  { sourceKey: 'tables', targetField: 'tables', templateKey: 'turn_tables_file', formatter: formatListField },
  { sourceKey: 'columns', targetField: 'columns', templateKey: 'turn_columns_file', formatter: formatListField },
  { sourceKey: 'reference_answer', targetField: 'referenceAnswer', templateKey: 'turn_reference_answer_file', formatter: formatTextField },
  { sourceKey: 'test_cases', targetField: 'testCases', templateKey: 'turn_test_cases_file', formatter: formatTestCasesField },
  { sourceKey: 'reasoning_types', targetField: 'reasoningTypes', templateKey: 'turn_reasoning_types_file', formatter: formatListField },
  { sourceKey: 'plsql_constructs', targetField: 'plSqlConstructs', templateKey: 'turn_plsql_constructs_file', formatter: formatListField },
];

export async function runNativeImportJson(taskId, taskDir, logFilePath) {
  const startedAt = new Date();
  const logLines = [`Import JSON started for task ${taskId}`];
  const artifacts = [];

  try {
    const sourceJson = await findTaskImportJson(taskId, taskDir);
    if (!sourceJson) {
      throw new Error(`Missing import source file ${taskId}_hil.json for task ${taskId}.`);
    }

    const parsed = JSON.parse(await readFile(sourceJson.path, 'utf8'));
    const turns = Array.isArray(parsed?.turns) ? parsed.turns : [];
    const turnValueMap = new Map();

    logLines.push(`Source JSON: ${sourceJson.name}`);
    logLines.push(`Turns detected: ${turns.length}`);

    for (const [index, turn] of turns.entries()) {
      const turnNumberRaw = Number.parseInt(String(turn?.turn_number ?? index + 1), 10);
      const turnNumber = Number.isInteger(turnNumberRaw) && turnNumberRaw > 0 ? turnNumberRaw : index + 1;
      const values = {};

      for (const mapping of TURN_FIELD_MAPPINGS) {
        if (!(mapping.sourceKey in (turn ?? {}))) {
          continue;
        }
        const rendered = mapping.formatter(turn[mapping.sourceKey]);
        if (!rendered) {
          continue;
        }

        const filePath = join(taskDir, formatTaskArtifactName(mapping.templateKey, { taskId, turnNumber }));
        await writeFile(filePath, rendered, 'utf8');
        artifacts.push(filePath);
        values[mapping.targetField] = rendered;
        logLines.push(`Turn ${turnNumber}: wrote ${mapping.targetField} -> ${filePath}`);
      }

      if (Object.keys(values).length) {
        turnValueMap.set(turnNumber, values);
      }
    }

    const publishContextPath = join(taskDir, formatTaskArtifactName('publish_context_file', { taskId }));
    try {
      await syncPublishContext(publishContextPath, turnValueMap);
      artifacts.push(publishContextPath);
      logLines.push(`Publish context updated: ${publishContextPath}`);
    } catch (error) {
      logLines.push(`Publish context update skipped: ${error instanceof Error ? error.message : 'unknown error'}`);
    }

    await writeFile(logFilePath, `${logLines.join('\n')}\n`, 'utf8');
    return buildResult(true, startedAt, taskId, taskDir, logFilePath, [...new Set(artifacts)], {
      sourceJson: sourceJson.name,
      turnsImported: turnValueMap.size,
      artifactsWritten: [...new Set(artifacts)].length,
    });
  } catch (error) {
    logLines.push(`Import failed: ${error instanceof Error ? error.message : 'unknown error'}`);
    await writeFile(logFilePath, `${logLines.join('\n')}\n`, 'utf8');
    return buildResult(false, startedAt, taskId, taskDir, logFilePath, [...new Set(artifacts)], {
      message: error instanceof Error ? error.message : 'Import failed',
    });
  }
}

async function findTaskImportJson(taskId, taskDir) {
  const fileName = `${taskId}_hil.json`;
  const candidates = [
    join(taskDir, fileName),
    join(taskDir, 'to_submit', fileName),
  ];

  for (const fullPath of candidates) {
    try {
      await access(fullPath);
      const parsed = JSON.parse(await readFile(fullPath, 'utf8'));

      if (!isValidImportJson(parsed)) {
        throw new Error(`Import source file ${fileName} exists at ${fullPath} but does not contain a valid turns payload.`);
      }

      return { path: fullPath, name: fileName };
    } catch (error) {
      if (error && error.code === 'ENOENT') {
        continue;
      }
      if (error instanceof SyntaxError || error instanceof Error) {
        throw error;
      }
    }
  }

  return null;
}

function isValidImportJson(value) {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const turns = value.turns;
  if (!Array.isArray(turns) || !turns.length) {
    return false;
  }
  return turns.some((turn) => turn && typeof turn === 'object' && (
    'user_request' in turn ||
    'tables' in turn ||
    'columns' in turn ||
    'reference_answer' in turn ||
    'test_cases' in turn ||
    'reasoning_types' in turn ||
    'plsql_constructs' in turn
  ));
}

function formatTextField(value) {
  const text = typeof value === 'string' ? value : value == null ? '' : String(value);
  const trimmed = text.replace(/\r\n/g, '\n');
  return trimmed ? `${trimmed.trimEnd()}\n` : '';
}

function formatListField(value) {
  const normalized = normalizeList(value);
  if (!normalized.length) {
    return '';
  }
  return `${normalized.join(',\n')}\n`;
}

function formatTestCasesField(value) {
  if (typeof value === 'string') {
    return formatTextField(value);
  }

  if (!Array.isArray(value)) {
    return '';
  }

  const blocks = value
    .map((entry, index) => formatSingleTestCase(entry, index + 1))
    .filter(Boolean);

  if (!blocks.length) {
    return '';
  }

  return `${blocks.join('\n\n')}\n`;
}

function formatSingleTestCase(entry, fallbackNumber) {
  if (!entry || typeof entry !== 'object') {
    return '';
  }

  const number = Number.parseInt(String(entry.test_case_number ?? fallbackNumber), 10);
  const testCaseNumber = Number.isInteger(number) && number > 0 ? number : fallbackNumber;
  const instructions = asText(entry.test_case_code ?? entry.execution_instructions ?? entry.instructions);
  const result = asText(entry.execution_result ?? entry.result ?? entry.expected_output);

  const lines = [
    `Test Case ${testCaseNumber}:`,
    'execution_instructions:',
    instructions,
  ];

  if (result) {
    lines.push('execution_result:');
    lines.push(result);
  }

  return lines.join('\n').trimEnd();
}

function asText(value) {
  if (typeof value === 'string') {
    return value.replace(/\r\n/g, '\n').trimEnd();
  }
  if (value == null) {
    return '';
  }
  return String(value).trimEnd();
}

function normalizeList(value) {
  if (Array.isArray(value)) {
    return value
      .map((item) => String(item ?? '').trim())
      .filter(Boolean);
  }
  if (typeof value === 'string') {
    return value
      .split(/\r?\n|,/)
      .map((item) => item.trim())
      .filter(Boolean);
  }
  return [];
}

async function syncPublishContext(publishContextPath, turnValueMap) {
  if (!turnValueMap.size) {
    return;
  }

  const payload = JSON.parse(await readFile(publishContextPath, 'utf8'));
  const turns = Array.isArray(payload?.turns) ? payload.turns : [];

  for (const turn of turns) {
    const turnNumber = Number.parseInt(String(turn?.turnNumber ?? ''), 10);
    if (!turnValueMap.has(turnNumber)) {
      continue;
    }

    const updates = turnValueMap.get(turnNumber);
    if (!turn.promptEvaluationFeedback || typeof turn.promptEvaluationFeedback !== 'object') {
      turn.promptEvaluationFeedback = { promptTurnEvaluation: [] };
    }
    if (!Array.isArray(turn.promptEvaluationFeedback.promptTurnEvaluation)) {
      turn.promptEvaluationFeedback.promptTurnEvaluation = [];
    }

    const evaluations = turn.promptEvaluationFeedback.promptTurnEvaluation;
    const normalizedNameMap = new Map(evaluations
      .map((entry) => [normalizeName(entry?.name), entry])
      .filter(([key]) => key));

    for (const [field, value] of Object.entries(updates)) {
      upsertEvaluationEntry(evaluations, normalizedNameMap, field, value);
    }

    if (typeof updates.tables === 'string') {
      upsertEvaluationEntry(evaluations, normalizedNameMap, 'tablesRequired', updates.tables);
    }
    if (typeof updates.columns === 'string') {
      upsertEvaluationEntry(evaluations, normalizedNameMap, 'columnsRequired', updates.columns);
    }
  }

  await writeFile(publishContextPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

function upsertEvaluationEntry(evaluations, normalizedNameMap, name, value) {
  const key = normalizeName(name);
  const existing = normalizedNameMap.get(key);
  if (existing && typeof existing === 'object') {
    existing.value = value;
    return;
  }
  const next = { name, value };
  evaluations.push(next);
  normalizedNameMap.set(key, next);
}

function normalizeName(value) {
  return String(value ?? '').toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function buildResult(success, startedAt, taskId, taskDir, logFilePath, artifacts, summary) {
  const finishedAt = new Date();
  return {
    action: 'import-json',
    taskId: String(taskId),
    success,
    exitCode: success ? 0 : 1,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    scriptPath: '',
    workingDirectory: taskDir,
    command: ['native-import-json'],
    logFile: logFilePath,
    stdoutTail: '',
    stderrTail: success ? '' : String(summary?.message ?? ''),
    artifacts,
    summary,
  };
}

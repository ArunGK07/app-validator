import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { resolveAuthorizationHeader } from '../auth-header.mjs';
import { REQUIRED_PUBLISH_FIELDS } from '../generation/reference-data.mjs';
import { formatTaskArtifactName } from '../workspace-config.mjs';

const FIELD_TEMPLATE_KEYS = {
  user: 'turn_user_file',
  tables: 'turn_tables_file',
  columns: 'turn_columns_file',
  referenceAnswer: 'turn_reference_answer_file',
  testCases: 'turn_test_cases_file',
  reasoningTypes: 'turn_reasoning_types_file',
  plSqlConstructs: 'turn_plsql_constructs_file',
};

const FIELD_ALIASES = {
  user: ['user'],
  tables: ['tables', 'tablesRequired'],
  columns: ['columns', 'columnsRequired'],
  referenceAnswer: ['referenceAnswer'],
  testCases: ['testCases'],
  reasoningTypes: ['reasoningTypes'],
  plSqlConstructs: ['plSqlConstructs'],
};

const GRAPHQL_URL = 'https://rlhf-api.turing.com/graphql';
const GRAPHQL_MUTATION = `mutation ReviewPromptTurn($promptTurnId: ID!, $input: ReviewPromptTurnInput!) {
  reviewPromptTurn(promptTurnId: $promptTurnId, review: $input) { id __typename }
}`;
const DEFAULT_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 250;

export async function runNativePublish(taskId, taskDir, logFilePath, config, dependencies = {}) {
  const startedAt = new Date();
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const retryAttempts = resolveRetryAttempts(config, dependencies);
  const existingOutputPath = join(taskDir, formatTaskArtifactName('existing_output_file', { taskId }));
  const existingOutput = JSON.parse(await readFile(existingOutputPath, 'utf8'));
  const referer = String(existingOutput?.colabLink ?? '').trim();
  if (!referer) {
    throw new Error('Missing colabLink in existing output JSON');
  }

  const turnNumbers = await detectAvailableTurns(taskDir, taskId, existingOutput);
  const logLines = [];
  let successCount = 0;

  for (const turnNumber of turnNumbers) {
    try {
      const payload = await buildReviewPayload(taskId, taskDir, existingOutput, turnNumber);
      const publishResult = await submitReviewPayload(fetchImpl, payload, referer, config.cookie, retryAttempts);
      successCount += 1;
      logLines.push(
        publishResult.attempts > 1
          ? `Turn ${turnNumber}: prompt turn review saved successfully after ${publishResult.attempts} attempts`
          : `Turn ${turnNumber}: prompt turn review saved successfully`,
      );
    } catch (error) {
      logLines.push(`Turn ${turnNumber}: ${error instanceof Error ? error.message : 'publish failed'}`);
    }
  }

  await writeFile(logFilePath, `${logLines.join('\n')}\n`, 'utf8');
  const finishedAt = new Date();
  const success = successCount === turnNumbers.length && turnNumbers.length > 0;
  return {
    action: 'publish',
    taskId: String(taskId),
    success,
    exitCode: success ? 0 : 1,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    scriptPath: '',
    workingDirectory: taskDir,
    command: ['native-publish'],
    logFile: logFilePath,
    stdoutTail: '',
    stderrTail: success ? '' : logLines.filter((line) => /failed|Missing/i.test(line)).join('\n'),
    artifacts: [],
    summary: {
      turnsDiscovered: turnNumbers.length,
      turnsPublished: successCount,
      turnsFailed: turnNumbers.length - successCount,
    },
  };
}

function resolveRetryAttempts(config, dependencies) {
  const rawValue = dependencies.retryAttempts ?? config.publishRetryAttempts ?? DEFAULT_RETRY_ATTEMPTS;
  const parsed = Number.parseInt(String(rawValue ?? ''), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : DEFAULT_RETRY_ATTEMPTS;
}

async function submitReviewPayload(fetchImpl, payload, referer, cookie, retryAttempts) {
  const authorizationHeader = resolveAuthorizationHeader({
    authorizationHeader: process.env.AUTHORIZATION,
    cookie,
  });
  let lastError = null;

  for (let attempt = 1; attempt <= retryAttempts; attempt += 1) {
    try {
      const response = await fetchImpl(GRAPHQL_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Cookie: cookie,
          ...(authorizationHeader ? { Authorization: authorizationHeader } : {}),
          referer,
        },
        body: JSON.stringify(payload),
      });
      const body = await readResponseBody(response);

      if (!response.ok) {
        const detail = formatPublishFailureDetail(body) || response.statusText || 'request failed';
        throw createPublishError(`publish failed with ${response.status}: ${detail}`, response.status >= 500 || response.status === 429);
      }

      if (Array.isArray(body?.errors) && body.errors.length) {
        throw createPublishError(`publish failed with GraphQL errors: ${formatGraphqlErrors(body.errors)}`, false);
      }

      return { attempts: attempt, body };
    } catch (error) {
      lastError = error;
      if (attempt >= retryAttempts || !isRetryablePublishError(error)) {
        break;
      }
      await delay(RETRY_DELAY_MS * attempt);
    }
  }

  throw normalizePublishError(lastError, retryAttempts);
}

async function readResponseBody(response) {
  if (typeof response?.text === 'function') {
    const rawText = await response.text();
    if (!rawText) {
      return null;
    }
    try {
      return JSON.parse(rawText);
    } catch {
      return rawText;
    }
  }

  if (typeof response?.json === 'function') {
    return response.json();
  }

  return null;
}

function formatPublishFailureDetail(body) {
  if (!body) {
    return '';
  }

  if (typeof body === 'string') {
    return body.trim();
  }

  if (Array.isArray(body?.errors) && body.errors.length) {
    return formatGraphqlErrors(body.errors);
  }

  if (typeof body?.message === 'string') {
    return body.message.trim();
  }

  return '';
}

function formatGraphqlErrors(errors) {
  return errors
    .map((entry) => {
      if (typeof entry?.message === 'string' && entry.message.trim()) {
        return entry.message.trim();
      }
      return JSON.stringify(entry);
    })
    .join(' | ');
}

function createPublishError(message, retryable) {
  return Object.assign(new Error(message), { retryable });
}

function isRetryablePublishError(error) {
  return Boolean(error && typeof error === 'object' && 'retryable' in error
    ? error.retryable
    : error instanceof TypeError || /fetch failed/i.test(String(error instanceof Error ? error.message : error)));
}

function normalizePublishError(error, retryAttempts) {
  if (error instanceof Error) {
    if (retryAttempts > 1 && isRetryablePublishError(error) && !/after \d+ attempts/i.test(error.message)) {
      return new Error(`${error.message} after ${retryAttempts} attempts`);
    }
    return error;
  }

  const message = String(error ?? 'publish failed');
  return new Error(retryAttempts > 1 ? `${message} after ${retryAttempts} attempts` : message);
}

function delay(durationMs) {
  return new Promise((resolve) => setTimeout(resolve, durationMs));
}

async function detectAvailableTurns(taskDir, taskId, existingOutput) {
  const turns = Array.isArray(existingOutput?.response?.data?.prompt?.promptTurns) ? existingOutput.response.data.prompt.promptTurns : [];
  const discovered = [];
  for (let turnNumber = 1; turnNumber <= turns.length; turnNumber += 1) {
    try {
      await loadTurnFileValues(taskDir, taskId, turnNumber);
      discovered.push(turnNumber);
    } catch {}
  }
  if (!discovered.length) {
    throw new Error(`No complete turn files found for task ${taskId}`);
  }
  return discovered;
}

async function buildReviewPayload(taskId, taskDir, existingOutput, turnNumber) {
  const promptTurn = getPromptTurn(existingOutput, turnNumber);
  const promptTurnId = String(promptTurn?.id ?? '').trim();
  if (!promptTurnId) {
    throw new Error(`Missing prompt turn id for turn ${turnNumber}`);
  }
  const fileValues = await loadTurnFileValues(taskDir, taskId, turnNumber);
  const feedback = promptTurn?.promptEvaluationFeedback;
  if (!feedback || typeof feedback !== 'object') {
    throw new Error(`Missing promptEvaluationFeedback for turn ${turnNumber}`);
  }

  const input = {
    preferenceSignal: promptTurn.preferenceSignal,
    promptEvaluationFeedback: structuredClone(feedback),
    unratable: Boolean(promptTurn.unratable),
  };
  input.promptEvaluationFeedback.promptTurnEvaluation = buildPromptTurnEvaluation(promptTurn, fileValues);

  return {
    operationName: 'ReviewPromptTurn',
    variables: { promptTurnId, input },
    query: GRAPHQL_MUTATION,
  };
}

function getPromptTurn(existingOutput, turnNumber) {
  const turns = existingOutput?.response?.data?.prompt?.promptTurns;
  if (!Array.isArray(turns)) {
    throw new Error('Missing promptTurns in existing output JSON');
  }
  return turns.find((turn, index) => (Number.isInteger(turn?.promptIndex) ? turn.promptIndex + 1 : index + 1) === turnNumber);
}

async function loadTurnFileValues(taskDir, taskId, turnNumber) {
  const values = {};
  for (const field of REQUIRED_PUBLISH_FIELDS) {
    const path = join(taskDir, formatTaskArtifactName(FIELD_TEMPLATE_KEYS[field], { taskId, turnNumber }));
    values[field] = await readFile(path, 'utf8');
  }
  return values;
}

function buildPromptTurnEvaluation(promptTurn, fileValues) {
  const existingItems = Array.isArray(promptTurn?.promptEvaluationFeedback?.promptTurnEvaluation)
    ? promptTurn.promptEvaluationFeedback.promptTurnEvaluation
    : [];
  const updated = [];
  const seen = new Set();

  for (const item of existingItems) {
    const name = typeof item?.name === 'string' ? item.name.trim() : '';
    if (!name) continue;
    seen.add(normalizeFieldName(name));
    const canonical = canonicalizeFieldName(name);
    updated.push({ name, value: canonical ? fileValues[canonical] : String(item?.value ?? '') });
  }

  for (const field of REQUIRED_PUBLISH_FIELDS) {
    const aliases = FIELD_ALIASES[field].map((alias) => normalizeFieldName(alias));
    if (aliases.some((alias) => seen.has(alias))) continue;
    updated.push({ name: field, value: fileValues[field] });
  }

  return updated;
}

function canonicalizeFieldName(name) {
  const normalized = normalizeFieldName(name);
  return Object.keys(FIELD_ALIASES).find((field) => FIELD_ALIASES[field].some((alias) => normalizeFieldName(alias) === normalized)) ?? null;
}

function normalizeFieldName(name) {
  return String(name ?? '').toLowerCase().replace(/[^a-z0-9]+/g, '');
}

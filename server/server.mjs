import express from 'express';
import { readFileSync, existsSync } from 'node:fs';
import { resolve } from 'node:path';

import { createLogger, isBackendDebugEnabled, summarizePayload } from './logger.mjs';
import {
  editConversation,
  fetchBatches,
  fetchConversation,
  fetchConversationReviewDetail,
  fetchConversations,
  fetchRawConversations,
  fetchTeamMembers,
  getProxyHealth,
  listTaskOutputFiles,
  listTaskOutputTasks,
  readRuntimeConfig,
  readTaskOutputFile,
  writeTaskOutputFile,
} from './turing-api.mjs';
import { fetchTaskOutputArtifacts } from './task-output-fetcher.mjs';
import { runTaskWorkflowAction } from './task-workflows.mjs';
import { warmSchemaCache } from './schema-warmup.mjs';

loadLocalEnvFile('.env.local');

const app = express();
const port = Number(process.env.PORT ?? 3000);
const host = process.env.HOST ?? '127.0.0.1';
const logger = createLogger('server');

app.use(express.json({ limit: '10mb' }));
app.use((request, response, next) => {
  const startedAt = Date.now();

  if (isBackendDebugEnabled()) {
    logger.debug('Incoming request', {
      method: request.method,
      path: request.path,
      query: request.query,
      body: summarizePayload(request.body),
    });
  }

  response.on('finish', () => {
    logger.info('Request completed', {
      method: request.method,
      path: request.path,
      statusCode: response.statusCode,
      durationMs: Date.now() - startedAt,
    });
  });

  next();
});

app.get('/api/health', (_request, response) => {
  response.json(getProxyHealth(readRuntimeConfig()));
});

app.get('/api/team-members', async (_request, response) => {
  try {
    const members = await fetchTeamMembers(readRuntimeConfig());
    response.json(members);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/batches', async (_request, response) => {
  try {
    const batches = await fetchBatches(readRuntimeConfig());
    response.json(batches);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/conversations', async (request, response) => {
  const filters = {
    userId: asQueryString(request.query.userId),
    taskId: asQueryString(request.query.taskId),
    status: asQueryString(request.query.status),
    batchId: asQueryString(request.query.batchId),
  };

  try {
    const rows = await fetchConversations(filters, readRuntimeConfig());
    response.json(rows);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/conversations/raw', async (request, response) => {
  try {
    const payload = await fetchRawConversations(request.query, readRuntimeConfig());
    response.json(payload);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/task-output/tasks', async (request, response) => {
  try {
    const rows = await listTaskOutputTasks(readRuntimeConfig(), {
      taskId: asQueryString(request.query.taskId),
    });
    response.json(rows);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/conversations/:taskId', async (request, response) => {
  try {
    const row = await fetchConversation(asPathString(request.params.taskId), readRuntimeConfig());

    if (!row) {
      response.status(404).json({ message: `Conversation ${asPathString(request.params.taskId)} was not found.` });
      return;
    }

    response.json(row);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/conversations/:taskId/review', async (request, response) => {
  try {
    const detail = await fetchConversationReviewDetail(asPathString(request.params.taskId), readRuntimeConfig());
    response.json(detail);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.post('/api/conversations/:taskId/edit', async (request, response) => {
  try {
    const reason = typeof request.body?.reason === 'string' ? request.body.reason : 'Fixing client feedback';
    const payload = await editConversation(asPathString(request.params.taskId), readRuntimeConfig(), { reason });
    response.json(payload);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/reports/:taskId', async (request, response) => {
  try {
    const report = await listTaskOutputFiles(asPathString(request.params.taskId), readRuntimeConfig());
    response.json(report);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.get('/api/reports/:taskId/file', async (request, response) => {
  try {
    const file = await readTaskOutputFile(
      asPathString(request.params.taskId),
      asQueryString(request.query.name),
      readRuntimeConfig(),
    );
    response.json(file);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.put('/api/reports/:taskId/file', async (request, response) => {
  try {
    const file = await writeTaskOutputFile(
      asPathString(request.params.taskId),
      asQueryString(request.query.name),
      typeof request.body?.content === 'string' ? request.body.content : '',
      readRuntimeConfig(),
    );
    response.json(file);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.post('/api/tasks/:taskId/fetch-output', async (request, response) => {
  try {
    const result = await fetchTaskOutputArtifacts(
      {
        taskId: asPathString(request.params.taskId),
        promptId: asQueryString(request.body?.promptId),
        collabLink: asQueryString(request.body?.collabLink),
        metadata: isRecord(request.body?.metadata) ? request.body.metadata : null,
      },
      readRuntimeConfig(),
    );
    response.json(result);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.post('/api/tasks/:taskId/actions/:action', async (request, response) => {
  try {
    const action = asPathString(request.params.action);
    const taskId = asPathString(request.params.taskId);
    const config = readRuntimeConfig();

    const result = await runTaskWorkflowAction(action, taskId, config, request.body ?? {});
    response.json(result);
  } catch (error) {
    sendProxyError(response, error);
  }
});

app.use((error, _request, response, _next) => {
  const statusCode =
    typeof error === 'object' && error !== null && 'status' in error && typeof error.status === 'number'
      ? error.status
      : typeof error === 'object' && error !== null && 'statusCode' in error && typeof error.statusCode === 'number'
        ? error.statusCode
        : 500;

  const type = typeof error === 'object' && error !== null && 'type' in error ? String(error.type) : '';
  const message =
    type === 'entity.too.large'
      ? 'Saved file is too large for the current request limit.'
      : error instanceof Error
        ? error.message
        : 'Unexpected proxy error.';

  logger.error('Unhandled request error', { statusCode, type, message });
  response.status(statusCode).json({ message });
});

const server = app.listen(port, host, () => {
  console.log(`Proxy backend listening on http://${host}:${port}`);
  logger.debug('Backend logger configured', {
    debugBackend: isBackendDebugEnabled(),
    logLevel: process.env.BACKEND_LOG_LEVEL ?? 'warn',
  });

  void warmSchemaCache(readRuntimeConfig()).catch((error) => {
    const message = error instanceof Error ? error.message : 'Schema warmup crashed unexpectedly.';
    logger.error('Schema warmup crashed unexpectedly', { message });
  });
});

server.on('error', (error) => {
  logger.error('Proxy backend failed to start.', error);
  process.exit(1);
});

function sendProxyError(response, error) {
  const statusCode =
    typeof error === 'object' && error !== null && 'statusCode' in error && typeof error.statusCode === 'number'
      ? error.statusCode
      : 500;

  const message = error instanceof Error ? error.message : 'Unexpected proxy error.';
  // Log client errors (4xx) at a lower level to avoid noisy server error logs
  if (statusCode >= 500) {
    logger.error('Proxy request failed', { statusCode, message });
  } else if (statusCode >= 400) {
    logger.warn('Proxy request returned client error', { statusCode, message });
  } else {
    logger.info('Proxy request failed', { statusCode, message });
  }

  response.status(statusCode).json({ message });
}



function isCompletedConversationStatus(row) {
  const status = typeof row?.status === 'string' ? row.status.trim().toLowerCase() : '';
  const businessStatus = typeof row?.businessStatus === 'string' ? row.businessStatus.trim().toLowerCase() : '';
  return status === 'completed' || businessStatus === 'completed';
}

function asQueryString(value) {
  if (Array.isArray(value)) {
    return value[0] ?? '';
  }

  return typeof value === 'string' ? value : '';
}

function asPathString(value) {
  return typeof value === 'string' ? value : '';
}

function isRecord(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function loadLocalEnvFile(filename) {
  const filePath = resolve(process.cwd(), filename);

  if (!existsSync(filePath)) {
    return;
  }

  const source = readFileSync(filePath, 'utf8');

  for (const line of source.split(/\r?\n/)) {
    const trimmed = line.trim();

    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }

    const separatorIndex = trimmed.indexOf('=');

    if (separatorIndex === -1) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    const rawValue = trimmed.slice(separatorIndex + 1).trim();

    if (!process.env[key]) {
      process.env[key] = stripWrappingQuotes(rawValue);
    }
  }
}

function stripWrappingQuotes(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }

  return value;
}


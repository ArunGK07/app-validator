import { mkdir, readdir, unlink } from 'node:fs/promises';
import { join, resolve as pathResolve, relative as pathRelative } from 'node:path';

import { createLogger } from './logger.mjs';
import { runNativeValidation } from './validation/engine.mjs';
import { runNativeGenerateOutputs } from './generation/engine.mjs';
import { runNativePublish } from './publish/engine.mjs';

const DEFAULT_TRAINER_PROJECT_DIR = 'D:\\Turing\\Projects\\workspace\\llm-trainer-project';
const DEFAULT_VALIDATION_REPORTS_DIR = pathResolve(DEFAULT_TRAINER_PROJECT_DIR, 'tmp', 'validation_reports');
const LOGS_DIR_NAME = '_logs';
const logger = createLogger('task-workflows');

const ACTION_DEFINITIONS = {
  validate: {
    logPrefix: 'validate',
    beforeRun: clearTaskLogFiles,
    runNative: runNativeValidateAction,
  },
  'generate-outputs': {
    logPrefix: 'generate-outputs',
    runNative: runNativeGenerateOutputsAction,
  },
  publish: {
    logPrefix: 'publish',
    runNative: runNativePublishAction,
  },
};

export function extendRuntimeConfigWithWorkflowDefaults(config, env = process.env) {
  return {
    ...config,
    trainerProjectDir: env.LLM_TRAINER_PROJECT_DIR ?? DEFAULT_TRAINER_PROJECT_DIR,
    trainerValidationReportsDir: env.VALIDATION_REPORTS_DIR ?? DEFAULT_VALIDATION_REPORTS_DIR,
  };
}

export async function runTaskWorkflowAction(action, taskId, config, options = {}) {
  const definition = ACTION_DEFINITIONS[action];

  if (!definition) {
    throw withStatus(new Error(`Unknown task workflow action: ${action}`), 404);
  }

  if (!config.cookie) {
    throw withStatus(new Error('Missing TURING_COOKIE. Add it to .env.local before running task workflows.'), 500);
  }

  const normalizedTaskId = asTaskId(taskId);
  const taskDir = resolveTaskOutputFolder(normalizedTaskId, config);
  const logsDir = await ensureTaskLogsFolder(taskDir);
  const startedAt = new Date();
  const timestamp = formatTimestampForName(startedAt);
  const logFilePath = join(logsDir, `${definition.logPrefix}-${timestamp}.log`);
  const context = {
    action,
    taskId: normalizedTaskId,
    taskDir,
    logsDir,
    logFilePath,
  };

  if (typeof definition.beforeRun === 'function') {
    await definition.beforeRun(context, config);
  }

  if (typeof definition.runNative !== 'function') {
    throw withStatus(new Error(`Workflow action ${action} has no native Node/MJS implementation.`), 500);
  }

  logger.info('Starting native task workflow action', {
    action,
    taskId: normalizedTaskId,
    logFilePath,
  });

  return definition.runNative(context, config, options);
}

export function buildTaskWorkflowActionPaths(taskId) {
  return {
    masterReport: `_validation/master_validator_task_${taskId}.json`,
    promptStructureReport: `_validation/promptstructure_task_${taskId}.json`,
    plsqlCombinedReport: `_validation/plsqlcombined_task_${taskId}.json`,
    namingStandardReport: `_validation/namingstandard_task_${taskId}.json`,
  };
}

async function runNativeValidateAction(context, _config, options = {}) {
  return runNativeValidation(context.taskId, context.taskDir, context.logFilePath, options.validationDependencies ?? {});
}

async function runNativeGenerateOutputsAction(context, config, options = {}) {
  return runNativeGenerateOutputs(
    context.taskId,
    context.taskDir,
    context.logFilePath,
    config,
    options.generateDependencies ?? {},
  );
}

async function runNativePublishAction(context, config, options = {}) {
  return runNativePublish(
    context.taskId,
    context.taskDir,
    context.logFilePath,
    config,
    options.publishDependencies ?? {},
  );
}

async function clearTaskLogFiles(context) {
  const entries = await readdir(context.logsDir, { withFileTypes: true });

  await Promise.all(
    entries
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.log'))
      .map((entry) => unlink(join(context.logsDir, entry.name)).catch(() => undefined)),
  );
}

async function ensureTaskLogsFolder(taskDir) {
  const logsDir = join(taskDir, LOGS_DIR_NAME);
  await mkdir(logsDir, { recursive: true });
  return logsDir;
}

function resolveTaskOutputFolder(taskId, config) {
  if (!/^[a-z0-9_-]+$/i.test(taskId)) {
    throw withStatus(new Error(`Task ${taskId} is not a valid task folder name.`), 400);
  }

  const rootPath = pathResolve(config.taskOutputDir);
  const folderPath = pathResolve(rootPath, taskId);
  const relativePath = pathRelative(rootPath, folderPath);

  if (relativePath.startsWith('..') || pathResolve(rootPath, relativePath) !== folderPath) {
    throw withStatus(new Error(`Task ${taskId} is outside the configured output directory.`), 400);
  }

  return folderPath;
}

function asTaskId(value) {
  const taskId = String(value ?? '').trim();

  if (!taskId) {
    throw withStatus(new Error('Task id is required.'), 400);
  }

  return taskId;
}

function formatTimestampForName(date) {
  return date.toISOString().replace(/[:.]/g, '-');
}

function withStatus(error, statusCode) {
  return Object.assign(error, { statusCode });
}

import { appendFile, mkdir, readdir, unlink, writeFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { join, resolve as pathResolve, relative as pathRelative, dirname as pathDirname } from 'node:path';

const execFileAsync = promisify(execFile);

import { createLogger } from './logger.mjs';
import { runNativeValidation } from './validation/engine.mjs';
import { runNativeGenerateOutputs, runNativeGenerateArtifacts, runNativeExecuteTestCases } from './generation/engine.mjs';
import { runNativeImportJson } from './generation/import-json.mjs';
import { runNativePublish } from './publish/engine.mjs';

const DEFAULT_TRAINER_PROJECT_DIR = 'D:\\Turing\\Projects\\workspace\\llm-trainer-project';
const DEFAULT_VALIDATION_REPORTS_DIR = pathResolve(DEFAULT_TRAINER_PROJECT_DIR, 'tmp', 'validation_reports');
const LOGS_DIR_NAME = '_logs';
const logger = createLogger('task-workflows');

const ACTION_DEFINITIONS = {
  validate: {
    command: ['native-validation'],
    logPrefix: 'validate',
    runNative: runNativeValidateAction,
  },
  'generate-outputs': {
    command: ['native-generate-outputs'],
    logPrefix: 'generate-outputs',
    runNative: runNativeGenerateOutputsAction,
  },
  'generate-artifacts': {
    command: ['native-generate-artifacts'],
    logPrefix: 'generate-artifacts',
    runNative: runNativeGenerateArtifactsAction,
  },
  'execute-tests': {
    command: ['native-execute-tests'],
    logPrefix: 'execute-tests',
    runNative: runNativeExecuteTestCasesAction,
  },
  'import-json': {
    command: ['native-import-json'],
    logPrefix: 'import-json',
    runNative: runNativeImportJsonAction,
  },
  publish: {
    command: ['native-publish'],
    logPrefix: 'publish',
    runNative: runNativePublishAction,
  },
};

function normalizeTaskWorkflowActionName(action) {
  return typeof action === 'string' ? action.trim().toLowerCase().replace(/_/g, '-') : '';
}

export function extendRuntimeConfigWithWorkflowDefaults(config, env = process.env) {
  return {
    ...config,
    trainerProjectDir: env.LLM_TRAINER_PROJECT_DIR ?? DEFAULT_TRAINER_PROJECT_DIR,
    trainerValidationReportsDir: env.VALIDATION_REPORTS_DIR ?? DEFAULT_VALIDATION_REPORTS_DIR,
  };
}

export async function runTaskWorkflowAction(action, taskId, config, options = {}) {
  const normalizedAction = normalizeTaskWorkflowActionName(action);
  const definition = ACTION_DEFINITIONS[action] ?? ACTION_DEFINITIONS[normalizedAction];

  if (!definition) {
    throw withStatus(
      new Error(`Unknown task workflow action: ${action}${normalizedAction && normalizedAction !== action ? ` (normalized: ${normalizedAction})` : ''}`),
      404,
    );
  }

  if (!config.cookie) {
    throw withStatus(new Error('Missing TURING_COOKIE. Add it to .env.local before running task workflows.'), 500);
  }

  const normalizedTaskId = asTaskId(taskId);
  const taskDir = resolveTaskOutputFolder(normalizedTaskId, config);
  const logsDir = await ensureTaskLogsFolder(taskDir);
  await clearTaskLogFiles({ logsDir });
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

  await writeWorkflowStartLog(context, definition.command, startedAt);

  try {
    return await definition.runNative(context, config, options);
  } catch (error) {
    logger.error('Native task workflow action failed', {
      action,
      taskId: normalizedTaskId,
      logFilePath,
      message: error instanceof Error ? error.message : String(error),
    });

    await appendWorkflowFailureLog(logFilePath, error);
    return buildWorkflowFailureResult(action, normalizedTaskId, taskDir, logFilePath, definition.command, startedAt, error);
  }
}

export function buildTaskWorkflowActionPaths(taskId) {
  return {
    masterReport: `_validation/master_validator_task_${taskId}.json`,
    promptStructureReport: `_validation/promptstructure_task_${taskId}.json`,
    plsqlCombinedReport: `_validation/plsqlcombined_task_${taskId}.json`,
    namingStandardReport: `_validation/namingstandard_task_${taskId}.json`,
    artifactAlignmentReport: `_validation/artifactalignment_task_${taskId}.json`,
    fileIndexReport: `_validation/files/index_task_${taskId}.json`,
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

async function runNativeGenerateArtifactsAction(context, config, options = {}) {
  const gitResult = options.autoCommit
    ? await commitTaskToGit(context.taskId, context.taskDir, `Snapshot before generate-artifacts for task ${context.taskId}`)
    : null;
  const result = await runNativeGenerateArtifacts(
    context.taskId,
    context.taskDir,
    context.logFilePath,
    config,
    options.generateDependencies ?? {},
  );
  return gitResult ? { ...result, gitCommit: gitResult } : result;
}

async function runNativeExecuteTestCasesAction(context, config, options = {}) {
  const gitResult = options.autoCommit
    ? await commitTaskToGit(context.taskId, context.taskDir, `Snapshot before execute-tests for task ${context.taskId}`)
    : null;
  const result = await runNativeExecuteTestCases(
    context.taskId,
    context.taskDir,
    context.logFilePath,
    config,
    options.generateDependencies ?? {},
  );
  return gitResult ? { ...result, gitCommit: gitResult } : result;
}

async function runNativeImportJsonAction(context, _config, _options = {}) {
  return runNativeImportJson(
    context.taskId,
    context.taskDir,
    context.logFilePath,
  );
}

async function runNativePublishAction(context, config, options = {}) {
  const result = await runNativePublish(
    context.taskId,
    context.taskDir,
    context.logFilePath,
    config,
    options.publishDependencies ?? {},
  );
  if (options.autoCommit && result.success) {
    const gitResult = await commitTaskToGit(context.taskId, context.taskDir, `Published task ${context.taskId}`);
    return { ...result, gitCommit: gitResult };
  }
  return result;
}

async function commitTaskToGit(taskId, taskDir, message) {
  const repoRoot = pathDirname(taskDir);
  try {
    await execFileAsync('git', ['add', taskDir], { cwd: repoRoot });
    await execFileAsync('git', ['commit', '-m', message], { cwd: repoRoot });
    logger.info(`[git] Committed: ${message}`);
    return { committed: true, message };
  } catch (error) {
    const reason = error?.stderr?.trim() || error?.message || String(error);
    // 'nothing to commit' is exit code 1 — not a real error
    const nothingToCommit = reason.includes('nothing to commit') || reason.includes('nothing added to commit');
    logger.warn(`[git] Commit skipped for task ${taskId}: ${reason}`);
    return { committed: false, reason: nothingToCommit ? 'Nothing to commit' : reason };
  }
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

async function writeWorkflowStartLog(context, command, startedAt) {
  await writeFile(
    context.logFilePath,
    [
      `Workflow action started: ${context.action}`,
      `Task: ${context.taskId}`,
      `Started At: ${startedAt.toISOString()}`,
      `Command: ${command.join(' ')}`,
      `Working Directory: ${context.taskDir}`,
      '',
    ].join('\n'),
    'utf8',
  );
}

async function appendWorkflowFailureLog(logFilePath, error) {
  const details = error instanceof Error ? error.stack || error.message : String(error);

  await appendFile(
    logFilePath,
    [
      'Workflow action failed unexpectedly.',
      `Finished At: ${new Date().toISOString()}`,
      details,
      '',
    ].join('\n'),
    'utf8',
  );
}

function buildWorkflowFailureResult(action, taskId, taskDir, logFilePath, command, startedAt, error) {
  const finishedAt = new Date();
  const message = error instanceof Error ? error.message : String(error);

  return {
    action,
    taskId,
    success: false,
    exitCode: 1,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    scriptPath: '',
    workingDirectory: taskDir,
    command,
    logFile: logFilePath,
    stdoutTail: '',
    stderrTail: message,
    artifacts: [],
  };
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
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hour = String(date.getHours()).padStart(2, '0');
  const minute = String(date.getMinutes()).padStart(2, '0');
  return `${year}${month}${day}_${hour}${minute}`;
}

function withStatus(error, statusCode) {
  return Object.assign(error, { statusCode });
}

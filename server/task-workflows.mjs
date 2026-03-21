import { mkdir, readdir, stat, unlink, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { basename, dirname, join, resolve as pathResolve, relative as pathRelative } from 'node:path';

import { createLogger } from './logger.mjs';
import { runNativeValidation } from './validation/engine.mjs';
import { runNativeGenerateOutputs } from './generation/engine.mjs';
import { runNativePublish } from './publish/engine.mjs';

const DEFAULT_TRAINER_PROJECT_DIR = 'D:\\Turing\\Projects\\workspace\\llm-trainer-project';
const DEFAULT_PYTHON_EXECUTABLE = 'python';
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
    pythonExecutable: env.PYTHON_EXECUTABLE ?? DEFAULT_PYTHON_EXECUTABLE,
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
  const scriptPath = Array.isArray(definition.scriptSegments)
    ? pathResolve(config.trainerProjectDir, ...definition.scriptSegments)
    : '';
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

  if (typeof definition.runNative === 'function') {
    logger.info('Starting native task workflow action', {
      action,
      taskId: normalizedTaskId,
      logFilePath,
    });

    return definition.runNative(context, config, options);
  }

  const command = [
    config.pythonExecutable,
    scriptPath,
    ...definition.buildArgs(normalizedTaskId, config, context).map((value) => String(value)),
  ];

  logger.info('Starting task workflow action', {
    action,
    taskId: normalizedTaskId,
    command,
    trainerProjectDir: config.trainerProjectDir,
    logFilePath,
  });

  const runner = options.spawnProcess ?? spawnProcess;
  const runResult = await runner({
    command,
    cwd: config.trainerProjectDir,
    logFilePath,
    env: {
      ...process.env,
      PYTHONUTF8: '1',
      PYTHONIOENCODING: 'utf-8',
    },
  });

  const finishedAt = new Date();
  const artifacts =
    typeof definition.collectArtifacts === 'function'
      ? await definition.collectArtifacts(normalizedTaskId, config, context)
      : [];

  logger.info('Completed task workflow action', {
    action,
    taskId: normalizedTaskId,
    exitCode: runResult.exitCode,
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    logFilePath,
    artifactCount: artifacts.length,
  });

  return {
    action,
    taskId: normalizedTaskId,
    success: runResult.exitCode === 0,
    exitCode: runResult.exitCode,
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    scriptPath,
    workingDirectory: config.trainerProjectDir,
    command,
    logFile: logFilePath,
    stdoutTail: runResult.stdoutTail,
    stderrTail: runResult.stderrTail,
    artifacts,
  };
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

async function collectValidationArtifacts(taskId, config) {
  const names = buildTaskWorkflowActionPaths(taskId);
  const artifacts = [];

  for (const fileName of Object.values(names)) {
    const filePath = join(config.trainerValidationReportsDir, fileName);

    if (await fileExists(filePath)) {
      artifacts.push(filePath);
    }
  }

  return artifacts;
}

async function collectGeneratedOutputArtifacts(taskId, config) {
  try {
    const taskDir = resolveTaskOutputFolder(taskId, config);
    const entries = await readdir(taskDir, { withFileTypes: true });

    return entries
      .filter((entry) => entry.isFile())
      .map((entry) => join(taskDir, entry.name))
      .sort((left, right) => left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' }));
  } catch {
    return [];
  }
}

async function spawnProcess({ command, cwd, logFilePath, env }) {
  await mkdir(dirname(logFilePath), { recursive: true }).catch(() => undefined);

  return new Promise((resolve, reject) => {
    const child = spawn(command[0], command.slice(1), {
      cwd,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });

    let stdoutTail = '';
    let stderrTail = '';
    const writeChunks = [];

    child.stdout.on('data', (chunk) => {
      const text = chunk.toString();
      writeChunks.push(text);
      stdoutTail = appendTail(stdoutTail, text);
    });

    child.stderr.on('data', (chunk) => {
      const text = chunk.toString();
      writeChunks.push(text);
      stderrTail = appendTail(stderrTail, text);
    });

    child.on('error', (error) => {
      reject(withStatus(new Error(`Failed to launch workflow command: ${error.message}`), 500));
    });

    child.on('close', async (exitCode) => {
      const combined = writeChunks.join('');

      try {
        await writeFile(logFilePath, combined, 'utf8');
      } catch (error) {
        reject(withStatus(new Error(`Failed to write workflow log file: ${error.message}`), 500));
        return;
      }

      resolve({
        exitCode: Number(exitCode ?? 1),
        stdoutTail,
        stderrTail,
      });
    });
  });
}

function appendTail(current, nextChunk, maxLength = 8000) {
  const combined = `${current}${nextChunk}`;
  return combined.length > maxLength ? combined.slice(-maxLength) : combined;
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

async function fileExists(filePath) {
  try {
    const metadata = await stat(filePath);
    return metadata.isFile();
  } catch {
    return false;
  }
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

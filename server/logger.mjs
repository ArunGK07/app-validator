const LEVELS = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const DEFAULT_LEVEL = 'warn';

export function createLogger(scope) {
  return {
    debug: (message, details) => log('debug', scope, message, details),
    info: (message, details) => log('info', scope, message, details),
    warn: (message, details) => log('warn', scope, message, details),
    error: (message, details) => log('error', scope, message, details),
  };
}

export function isBackendDebugEnabled(env = process.env) {
  return String(env.DEBUG_BACKEND ?? '').trim().toLowerCase() === 'true';
}

export function sanitizeHeaders(headers = {}) {
  const result = {};

  for (const [key, value] of Object.entries(headers)) {
    result[key] = /cookie|authorization/i.test(key) ? '[redacted]' : value;
  }

  return result;
}

export function summarizePayload(value) {
  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === 'string') {
    return value.length > 300 ? `${value.slice(0, 300)}...` : value;
  }

  if (typeof value !== 'object') {
    return value;
  }

  try {
    const serialized = JSON.stringify(value);
    return serialized.length > 600 ? `${serialized.slice(0, 600)}...` : value;
  } catch {
    return '[unserializable]';
  }
}

function log(level, scope, message, details) {
  if (!shouldLog(level)) {
    return;
  }

  const timestamp = new Date().toISOString();
  const suffix = details === undefined ? '' : ` ${formatDetails(details)}`;
  const line = `[${timestamp}] [${level.toUpperCase()}] [${scope}] ${message}${suffix}`;

  if (level === 'error') {
    console.error(line);
    return;
  }

  if (level === 'warn') {
    console.warn(line);
    return;
  }

  console.log(line);
}

function shouldLog(level) {
  const configured = String(process.env.BACKEND_LOG_LEVEL ?? DEFAULT_LEVEL).trim().toLowerCase();
  const currentLevel = LEVELS[configured] ?? LEVELS[DEFAULT_LEVEL];
  const targetLevel = LEVELS[level] ?? LEVELS.info;

  if (level === 'debug' && !isBackendDebugEnabled()) {
    return false;
  }

  return targetLevel >= currentLevel;
}

function formatDetails(details) {
  if (details instanceof Error) {
    return details.stack || details.message;
  }

  try {
    return JSON.stringify(details);
  } catch {
    return String(details);
  }
}

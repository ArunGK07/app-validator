import oracledb from 'oracledb';

import { getConnectionParamsForTask, resolveTaskRouting } from '../schema-db-config.mjs';
import { VALIDATOR_NAMES, createFail, createPass, loadTurnTextArtifact } from './common.mjs';

const DISALLOWED_NAMES = new Set(['temp', 'temp1', 'test', 'test123', 'x', 'x1', 'y', 'y1', 'data', 'proc1', 'my_package']);
const VARIABLE_PREFIXES = ['lv_', 'gv_', 'lv_tmp_', 'rec_', 't_', 'o_'];
const PARAM_PREFIX = {
  'FORMAL IN': 'p_',
  'FORMAL OUT': 'out_',
  'FORMAL IN OUT': 'io_',
};
const PROGRAM_PREFIX_RULES = {
  PACKAGE: 'pkg_',
  PROCEDURE: 'sp_',
  FUNCTION: 'sf_',
  TRIGGER: 'trg_',
};
const CREATE_OBJ_RE = /\bCREATE\s+(?:OR\s+REPLACE\s+)?(PACKAGE(?:\s+BODY)?|TYPE(?:\s+BODY)?|TRIGGER|PROCEDURE|FUNCTION)\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)/i;
const TMP_PREFIX = 'TMP_VALIDATE_';

function normalizeObjectName(name) {
  return String(name ?? '').split('.').pop()?.replaceAll('"', '').toUpperCase() ?? '';
}

function normalizeObjectType(value) {
  const token = String(value ?? '').trim().toUpperCase();
  if (token.startsWith('PACKAGE')) {
    return 'PACKAGE';
  }
  if (token.startsWith('TYPE')) {
    return 'TYPE';
  }
  return token;
}

function splitSqlBlocks(sql) {
  const blocks = [];
  const current = [];

  for (const line of sql.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (trimmed === '/') {
      const block = current.join('\n').trim();
      if (block) {
        blocks.push(block);
      }
      current.length = 0;
      continue;
    }
    if (/^(set|prompt|spool|show\s+errors|exit)\b/i.test(trimmed)) {
      continue;
    }
    current.push(line);
  }

  const tail = current.join('\n').trim();
  if (tail) {
    blocks.push(tail);
  }

  return blocks;
}

function isPureAnonymous(sql) {
  const blocks = splitSqlBlocks(sql).map((block) => block.trim()).filter(Boolean);
  if (!blocks.length) {
    return false;
  }
  return blocks.every((block) => /^DECLARE\b/i.test(block) || /^BEGIN\b/i.test(block));
}

function tempObjectName(filePath) {
  const source = filePath.replace(/[^A-Z0-9_]/gi, '_').toUpperCase();
  const digest = Buffer.from(filePath).toString('hex').slice(0, 8).toUpperCase();
  const available = 30 - TMP_PREFIX.length - 1 - digest.length;
  return `${TMP_PREFIX}${source.slice(0, Math.max(0, available))}_${digest}`;
}

function wrapAnonymousBlock(sql, name) {
  const trimmed = sql.trim();

  if (/^DECLARE\b/i.test(trimmed)) {
    return `CREATE OR REPLACE PROCEDURE ${name} ${trimmed.replace(/^DECLARE\b/i, 'IS')}`;
  }

  if (/^BEGIN\b/i.test(trimmed)) {
    return `CREATE OR REPLACE PROCEDURE ${name} IS\n${trimmed}`;
  }

  return `CREATE OR REPLACE PROCEDURE ${name} IS\nBEGIN\n${trimmed}\nEND;`;
}

function extractCreatedObjects(sql) {
  return splitSqlBlocks(sql)
    .map((block) => block.match(CREATE_OBJ_RE))
    .filter(Boolean)
    .map((match) => [normalizeObjectName(match[2]), normalizeObjectType(match[1])]);
}

function createExecutor(connection) {
  if (connection && typeof connection.execute === 'function') {
    return connection;
  }

  if (connection && typeof connection.cursor === 'function') {
    return connection.cursor();
  }

  throw new Error('Unsupported Oracle connection object');
}

async function executeSqlBlocks(cursor, sql) {
  for (const block of splitSqlBlocks(sql)) {
    if (!block.trim()) {
      continue;
    }
    await cursor.execute(block.trim());
  }
}

async function compileFile(cursor, sql, syntheticName) {
  const createdObjects = extractCreatedObjects(sql);
  let compiledSql = sql;
  let primaryObject = createdObjects[0] ?? [syntheticName, null];

  if (!createdObjects.length && isPureAnonymous(sql)) {
    compiledSql = wrapAnonymousBlock(sql, syntheticName);
    primaryObject = [syntheticName, null];
  }

  await executeSqlBlocks(cursor, compiledSql);

  if (createdObjects.length) {
    for (const [name] of createdObjects) {
      const result = await cursor.execute(
        `SELECT type, line, position, text FROM user_errors WHERE name = :name ORDER BY sequence`,
        { name },
      );
      if (result.rows?.length) {
        const preview = result.rows
          .slice(0, 5)
          .map((row) => `${row[0]} L${row[1]}:${row[2]} ${row[3]}`)
          .join('; ');
        throw new Error(`Compile errors in ${name}: ${preview}`);
      }
    }
  }

  return primaryObject;
}

function checkPrefixes(name, prefixes) {
  const lowered = name.toLowerCase();
  return prefixes.some((prefix) => lowered.startsWith(prefix));
}

function validateIdentifier(name, type) {
  const lowered = name.toLowerCase();
  const issues = [];

  if (DISALLOWED_NAMES.has(lowered)) {
    issues.push('Disallowed identifier');
  }

  if (type === 'VARIABLE' && !checkPrefixes(name, VARIABLE_PREFIXES)) {
    issues.push('Variable must start with lv_/gv_/lv_tmp_/rec_/t_/o_');
  } else if (type === 'CURSOR' && !lowered.startsWith('cur_')) {
    issues.push('Cursor must start with cur_');
  } else if (type === 'CONSTANT' && !lowered.startsWith('co_')) {
    issues.push('Constant must start with co_');
  } else if (type === 'EXCEPTION' && !lowered.startsWith('exp_')) {
    issues.push('Exception must start with exp_');
  } else if (type === 'REF CURSOR' && !lowered.startsWith('ref_cur_')) {
    issues.push('Ref cursor must start with ref_cur_');
  } else if (type === 'RECORD TYPE' && !/^rec_.*_type$/.test(lowered)) {
    issues.push('Record type must match rec_<name>_type');
  } else if (type === 'COLLECTION TYPE' && !/^t_.*_type$/.test(lowered)) {
    issues.push('Collection type must match t_<name>_type');
  } else if (type === 'TYPE' && !(lowered.endsWith('_type') || lowered.startsWith('ct_'))) {
    issues.push('User-defined type must end with _type or start with ct_');
  } else if (type === 'RECORD ITERATOR' && lowered !== 'rec' && !lowered.startsWith('rec_')) {
    issues.push('Record iterator must start with rec_');
  } else if (PARAM_PREFIX[type] && !lowered.startsWith(PARAM_PREFIX[type])) {
    issues.push(`${type} parameter must start with ${PARAM_PREFIX[type]}`);
  } else if (PROGRAM_PREFIX_RULES[type] && !lowered.startsWith(PROGRAM_PREFIX_RULES[type])) {
    issues.push(`${type} must start with ${PROGRAM_PREFIX_RULES[type]}`);
  }

  return issues;
}

function suggestRename(name, type, message) {
  const lowered = name.toLowerCase();
  if (PARAM_PREFIX[type] && !lowered.startsWith(PARAM_PREFIX[type])) {
    return `${PARAM_PREFIX[type]}${lowered.replace(/^_+/, '')}`;
  }
  if (type === 'VARIABLE' && !checkPrefixes(name, VARIABLE_PREFIXES)) return `lv_${lowered}`;
  if (type === 'CURSOR' && !lowered.startsWith('cur_')) return `cur_${lowered}`;
  if (type === 'CONSTANT' && !lowered.startsWith('co_')) return `co_${lowered}`;
  if (type === 'EXCEPTION' && !lowered.startsWith('exp_')) return `exp_${lowered}`;
  if (type === 'REF CURSOR' && !lowered.startsWith('ref_cur_')) return `ref_cur_${lowered}`;
  if (type === 'PROCEDURE' && !lowered.startsWith('sp_')) return `sp_${lowered}`;
  if (type === 'FUNCTION' && !lowered.startsWith('sf_')) return `sf_${lowered}`;
  if (message.includes('Disallowed identifier')) return 'Use a meaningful business name with the required prefix';
  return 'rename the identifier to satisfy the naming standard';
}

export async function runNamingStandardValidator(taskId, taskDir, metadata, dependencies = {}) {
  const validatorName = VALIDATOR_NAMES.namingStandard;
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);
  if (!Number.isInteger(numTurns) || numTurns <= 0) {
    return [createFail(validatorName, taskId, null, 'Metadata', 'invalid_num_turns', {
      expected: 'metadata must contain a positive integer `num_turns`',
      present: `found \`num_turns=${metadata?.num_turns ?? null}\``,
      update: 'set `num_turns` to the actual number of turns for the task',
    })];
  }

  const connect = dependencies.connect ?? (async (taskMetadata) => {
    const params = getConnectionParamsForTask(taskMetadata);
    return oracledb.getConnection({
      user: params.user,
      password: params.password,
      connectString: `${params.host}:${params.port}/${params.service}`,
      privilege: String(params.mode ?? '').toUpperCase() === 'SYSDBA' ? oracledb.SYSDBA : undefined,
    });
  });

  const results = [];
  const routing = resolveTaskRouting(metadata);
  let connection = null;
  let executor = null;

  try {
    connection = await connect(metadata);
    executor = createExecutor(connection);
    await executor.execute(`ALTER SESSION SET CURRENT_SCHEMA = ${routing.schemaName}`);
    await executor.execute(`ALTER SESSION SET PLSCOPE_SETTINGS='IDENTIFIERS:ALL'`);

    for (let turnNumber = 1; turnNumber <= numTurns; turnNumber += 1) {
      const artifact = await loadTurnTextArtifact(taskDir, 'turn_reference_answer_file', taskId, turnNumber);
      if (!artifact.text) {
        results.push(createFail(validatorName, taskId, turnNumber, 'Reference Answer Artifact', 'missing_artifact', {
          expected: `reference answer file for turn ${turnNumber} must exist`,
          present: `no reference-answer file found for turn ${turnNumber} in ${taskDir}`,
          update: 'create the per-turn reference answer artifact before running naming validation',
          sourceFile: artifact.fileName,
        }));
        continue;
      }

      try {
        const [objectName, objectType] = await compileFile(executor, artifact.text, tempObjectName(artifact.fileName));
        const identifierResult = await executor.execute(
          `SELECT name, type, usage, line, col, usage_id, usage_context_id
           FROM user_identifiers
           WHERE object_name = :objectName AND usage = 'DECLARATION'`,
          { objectName },
        );
        const iteratorResult = await executor.execute(
          `SELECT DISTINCT usage_id FROM user_identifiers
           WHERE object_name = :objectName AND type = 'RECORD ITERATOR' AND usage = 'DECLARATION'`,
          { objectName },
        );
        const iteratorIds = new Set((iteratorResult.rows ?? []).map((row) => row[0]));
        const violations = [];

        if (PROGRAM_PREFIX_RULES[objectType] && !objectName.toLowerCase().startsWith(PROGRAM_PREFIX_RULES[objectType])) {
          violations.push({
            type: objectType,
            name: objectName,
            line: 1,
            col: 1,
            message: `${objectType} must start with ${PROGRAM_PREFIX_RULES[objectType]}`,
            recommended: `Rename object to start with ${PROGRAM_PREFIX_RULES[objectType]}`,
          });
        }

        for (const row of identifierResult.rows ?? []) {
          const [name, type, , line, col, , usageContextId] = row;
          if (iteratorIds.has(usageContextId)) {
            continue;
          }
          for (const message of validateIdentifier(name, type)) {
            violations.push({
              type,
              name,
              line,
              col,
              message,
              recommended: suggestRename(name, type, message),
            });
          }
        }

        if (violations.length) {
          for (const violation of violations) {
            results.push(createFail(validatorName, taskId, turnNumber, `${violation.type} ${violation.name}`, 'naming_violation', {
              expected: violation.message,
              present: `${violation.type} \`${violation.name}\` violates the naming rule at line ${violation.line}:${violation.col}`,
              update: violation.recommended,
              sourceFile: artifact.fileName,
              line: violation.line,
            }));
          }
        } else {
          results.push(createPass(validatorName, taskId, turnNumber, 'Naming Standard', 'compliant', artifact.fileName));
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown naming validation error';
        results.push(createFail(validatorName, taskId, turnNumber, 'Compilation', 'compile_or_execution_error', {
          expected: 'PL/SQL should compile successfully before naming inspection',
          present: `SQL compile error: ${message}`,
          update: 'repair the compilation error, then rerun naming validation',
          sourceFile: artifact.fileName,
        }));
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown database connection error';
    return [createFail(validatorName, taskId, null, 'Connection', 'connection_error', {
      expected: 'database connection must succeed before naming validation can run',
      present: `failed to connect as schema \`${routing.schemaName || 'UNKNOWN'}\`: ${message}`,
      update: 'fix the schema credentials/connection settings, then rerun validation',
    })];
  } finally {
    if (executor && executor !== connection && typeof executor.close === 'function') {
      await executor.close().catch(() => undefined);
    }
    if (connection) {
      await connection.close().catch(() => undefined);
    }
  }

  return results;
}



import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import oracledb from 'oracledb';

import { getConnectionParamsForTask } from '../schema-db-config.mjs';
import { formatTaskArtifactName } from '../workspace-config.mjs';

const TESTCASE_BLOCK_RE = new RegExp(
  '^Test Case\\s+(?<number>\\d+):\\s*\\n(?:(?!^\\s*execution_instructions:\\s*$).*\\n)*?^\\s*execution_instructions:\\s*\\n(?<instructions>.*?)^\\s*execution_result:[ \\t]*(?:\\n)?(?<result>.*?)(?=^\\s*Test Case\\s+\\d+:\\s*|$)',
  'gms',
);

export async function refreshTaskTestCases(taskId, taskDir, metadata, _config, dependencies = {}) {
  const updatedFiles = [];
  const logLines = [];
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);
  if (!Number.isInteger(numTurns) || numTurns <= 0) {
    return { updatedFiles, logLines };
  }

  const connectionFactory = dependencies.connectionFactory ?? defaultConnectionFactory;
  const compileReference = dependencies.compileReference ?? defaultCompileReference;
  const executeInstructions = dependencies.executeInstructions ?? defaultExecuteInstructions;

  for (let turnNumber = 1; turnNumber <= numTurns; turnNumber += 1) {
    const testcasePath = join(taskDir, formatTaskArtifactName('turn_test_cases_file', { taskId, turnNumber }));
    const referencePath = join(taskDir, formatTaskArtifactName('turn_reference_answer_file', { taskId, turnNumber }));
    let original = '';
    try {
      original = await readFile(testcasePath, 'utf8');
    } catch {
      continue;
    }

    const connection = await connectionFactory(metadata);
    try {
      const referenceText = await readFile(referencePath, 'utf8').catch(() => '');
      if (referenceText.trim()) {
        await compileReference(referenceText, connection);
      }

      let replaced = false;
      const updated = await replaceAsync(original, TESTCASE_BLOCK_RE, async (fullMatch, ...args) => {
        const groups = args.at(-1);
        const result = await executeInstructions(groups.instructions, connection);
        replaced = true;
        return fullMatch.replace(/(^\s*execution_result:[ \t]*(?:\n)?)([\s\S]*?)$/m, (_, prefix) => `${prefix}${result.trimEnd()}\n`);
      });

      if (replaced && updated !== original) {
        await writeFile(testcasePath, updated, 'utf8');
        updatedFiles.push(testcasePath);
      }
      logLines.push(`Turn ${turnNumber}: refreshed testcase output`);
    } finally {
      if (typeof connection?.close === 'function') {
        await connection.close();
      }
    }
  }

  return { updatedFiles, logLines };
}

async function defaultConnectionFactory(metadata) {
  const params = getConnectionParamsForTask(metadata);
  return oracledb.getConnection({
    user: params.user,
    password: params.password,
    connectString: `${params.host}:${params.port}/${params.service}`,
    privilege: String(params.mode ?? '').toUpperCase() === 'SYSDBA' ? oracledb.SYSDBA : undefined,
  });
}

async function defaultCompileReference(referenceText, connection) {
  const cursor = connection.cursor();
  try {
    for (const statement of splitSqlStatements(referenceText)) {
      await cursor.execute(statement);
    }
    await connection.commit();
  } finally {
    await cursor.close();
  }
}

async function defaultExecuteInstructions(instructions, connection) {
  const cursor = connection.cursor();
  const output = [];
  try {
    await cursor.execute('BEGIN DBMS_OUTPUT.ENABLE(NULL); END;');
    for (const statement of splitSqlStatements(instructions)) {
      await cursor.execute(statement);
      if (cursor.description) {
        const rows = await cursor.getRows(500);
        output.push(...rows.map((row) => row.map((value) => String(value ?? 'NULL')).join(' | ')));
      }
      output.push(...(await drainDbmsOutput(cursor)));
    }
    await connection.commit();
  } finally {
    await cursor.close();
  }
  return output.join('\n').trimEnd();
}

async function drainDbmsOutput(cursor) {
  const lines = [];
  while (true) {
    const result = await cursor.execute('BEGIN DBMS_OUTPUT.GET_LINE(:line, :status); END;', {
      line: { dir: oracledb.BIND_OUT, type: oracledb.STRING, maxSize: 32767 },
      status: { dir: oracledb.BIND_OUT, type: oracledb.NUMBER },
    });
    if (Number(result.outBinds.status) !== 0) {
      break;
    }
    lines.push(String(result.outBinds.line ?? ''));
  }
  return lines;
}

function splitSqlStatements(source) {
  const statements = [];
  let buffer = [];
  let inBlock = false;
  const flush = () => {
    const text = buffer.join('\n').trim();
    if (text) {
      statements.push(text.replace(/;\s*$/u, ''));
    }
    buffer = [];
    inBlock = false;
  };

  for (const line of String(source).replace(/\r/g, '').split('\n')) {
    const trimmed = line.trim();
    if (!buffer.length && /^(CREATE(\s+OR\s+REPLACE)?\s+(PROCEDURE|FUNCTION|PACKAGE|TRIGGER|TYPE)|DECLARE|BEGIN)\b/i.test(trimmed)) {
      inBlock = true;
    }
    if (trimmed === '/' && inBlock) {
      flush();
      continue;
    }
    buffer.push(line);
    if (!inBlock && /;\s*$/u.test(trimmed)) {
      flush();
    }
  }
  flush();
  return statements;
}

async function replaceAsync(input, regex, replacer) {
  const matches = [...input.matchAll(regex)];
  if (!matches.length) {
    return input;
  }

  let result = '';
  let cursor = 0;
  for (const match of matches) {
    result += input.slice(cursor, match.index);
    result += await replacer(...match);
    cursor = match.index + match[0].length;
  }
  result += input.slice(cursor);
  return result;
}

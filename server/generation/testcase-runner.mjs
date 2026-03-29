import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import oracledb from 'oracledb';

import { getConnectionParamsForTask } from '../schema-db-config.mjs';
import { formatTaskArtifactName } from '../workspace-config.mjs';

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
        try {
          await compileReference(referenceText, connection);
        } catch (error) {
          throw new Error(
            `Turn ${turnNumber}: reference compile failed in ${formatTaskArtifactName('turn_reference_answer_file', { taskId, turnNumber })}: ${formatErrorMessage(error)}`,
            { cause: error },
          );
        }
      }

      const blocks = parseTestCaseBlocks(original);
      let replaced = false;
      let updated = '';
      let cursor = 0;

      for (const block of blocks) {
        let result = '';
        try {
          result = await executeInstructions(block.instructions, connection);
        } catch (error) {
          throw new Error(
            `Turn ${turnNumber} Test Case ${block.number}: execution failed: ${formatErrorMessage(error)}`,
            { cause: error },
          );
        }
        replaced = true;
        updated += original.slice(cursor, block.resultStart);
        updated += `\n${result.trimEnd()}\n`;
        cursor = block.resultEnd;
      }

      if (replaced) {
        updated += original.slice(cursor);
      } else {
        updated = original;
      }

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

function createExecutor(connection) {
  if (connection && typeof connection.execute === 'function') {
    return connection;
  }

  if (connection && typeof connection.cursor === 'function') {
    return connection.cursor();
  }

  throw new Error('Unsupported Oracle connection object');
}

async function closeExecutor(executor, connection) {
  if (executor !== connection && typeof executor?.close === 'function') {
    await executor.close();
  }
}

async function defaultCompileReference(referenceText, connection) {
  const executor = createExecutor(connection);
  try {
    const statements = splitSqlStatements(referenceText);
    for (const [index, statement] of statements.entries()) {
      try {
        await executor.execute(statement);
      } catch (error) {
        throw new Error(`statement ${index + 1} failed near "${previewSql(statement)}": ${formatErrorMessage(error)}`, { cause: error });
      }
    }
    await connection.commit();
  } finally {
    await closeExecutor(executor, connection);
  }
}

async function defaultExecuteInstructions(instructions, connection) {
  const executor = createExecutor(connection);
  const output = [];
  try {
    await executor.execute('BEGIN DBMS_OUTPUT.ENABLE(NULL); END;');
    const statements = splitSqlStatements(instructions);
    for (const [index, statement] of statements.entries()) {
      let result;
      try {
        result = await executor.execute(statement);
      } catch (error) {
        throw new Error(`statement ${index + 1} failed near "${previewSql(statement)}": ${formatErrorMessage(error)}`, { cause: error });
      }
      const rows = await extractRows(executor, result);
      output.push(...rows.map((row) => normalizeRow(row).map((value) => String(value ?? 'NULL')).join(' | ')));
      output.push(...(await drainDbmsOutput(executor)));
    }
    await connection.commit();
  } finally {
    await closeExecutor(executor, connection);
  }
  return output.join('\n').trimEnd();
}

async function extractRows(executor, result) {
  if (Array.isArray(result?.rows)) {
    return result.rows;
  }

  if (result?.resultSet && typeof result.resultSet.getRows === 'function') {
    try {
      return await result.resultSet.getRows(500);
    } finally {
      if (typeof result.resultSet.close === 'function') {
        await result.resultSet.close();
      }
    }
  }

  if (typeof executor?.getRows === 'function') {
    return executor.getRows(500);
  }

  return [];
}

function normalizeRow(row) {
  return Array.isArray(row) ? row : [row];
}

function previewSql(statement) {
  return String(statement).replace(/\s+/g, ' ').trim().slice(0, 120);
}

function formatErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

async function drainDbmsOutput(executor) {
  const lines = [];
  while (true) {
    const result = await executor.execute('BEGIN DBMS_OUTPUT.GET_LINE(:line, :status); END;', {
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
  const isSqlPlusDirective = (line) => /^(set|prompt|spool|show\s+errors|exit)\b/i.test(line);
  const isIgnorableLeadingLine = (line) => {
    const trimmed = line.trim();
    return !trimmed || /^--/.test(trimmed) || /^\/\*/.test(trimmed) || /^\*/.test(trimmed) || /\*\/$/.test(trimmed);
  };
  const blockStartRe = /^(CREATE(\s+OR\s+REPLACE)?\s+(PROCEDURE|FUNCTION|PACKAGE|TRIGGER|TYPE)|DECLARE|BEGIN)\b/i;
  const execRe = /^(?:exec|execute)\s+([\s\S]+?)\s*;?\s*$/i;
  const flush = () => {
    const text = buffer.join('\n').trim();
    if (text) {
      statements.push(inBlock ? text : text.replace(/;\s*$/u, ''));
    }
    buffer = [];
    inBlock = false;
  };

  for (const line of String(source).replace(/\r/g, '').split('\n')) {
    const trimmed = line.trim();
    if (!inBlock && isSqlPlusDirective(trimmed)) {
      continue;
    }
    if (!inBlock && !buffer.length && !trimmed) {
      continue;
    }
    const execMatch = !inBlock ? trimmed.match(execRe) : null;
    if (execMatch) {
      const invocation = execMatch[1].replace(/;\s*$/u, '').trim();
      if (invocation) {
        statements.push(`BEGIN ${invocation}; END;`);
      }
      continue;
    }
    if ((!buffer.length || buffer.every((entry) => isIgnorableLeadingLine(entry))) && blockStartRe.test(trimmed)) {
      inBlock = true;
    }
    if (trimmed === '/') {
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

function parseTestCaseBlocks(source) {
  const normalized = String(source).replace(/\r/g, '');
  const blockStarts = [...normalized.matchAll(/^Test Case\s+(?<number>\d+):\s*$/gm)];
  if (!blockStarts.length) {
    return [];
  }

  const blocks = [];

  for (let index = 0; index < blockStarts.length; index += 1) {
    const match = blockStarts[index];
    const blockStart = match.index ?? 0;
    const blockEnd = blockStarts[index + 1]?.index ?? normalized.length;
    const blockText = normalized.slice(blockStart, blockEnd);
    const lines = blockText.split('\n');
    let offset = 0;
    let instructionsLine = -1;
    let instructionsInline = '';
    let resultLine = -1;
    let resultPrefixLength = 0;

    for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
      const line = lines[lineIndex];
      const instructionsMatch = line.match(/^(\s*execution_instructions:\s*)(.*)$/i);
      if (instructionsMatch) {
        instructionsLine = lineIndex;
        instructionsInline = instructionsMatch[2] ?? '';
      }
      const resultMatch = line.match(/^(\s*execution_result:\s*)(.*)$/i);
      if (resultMatch) {
        resultLine = lineIndex;
        resultPrefixLength = resultMatch[1].length;
        break;
      }
    }

    if (instructionsLine === -1 || resultLine === -1 || resultLine < instructionsLine) {
      continue;
    }

    const lineOffsets = [];
    offset = 0;
    for (const line of lines) {
      lineOffsets.push(offset);
      offset += line.length + 1;
    }

    const instructionParts = [];
    if (instructionsInline.trim()) {
      instructionParts.push(instructionsInline);
    }
    for (let lineIndex = instructionsLine + 1; lineIndex < resultLine; lineIndex += 1) {
      instructionParts.push(lines[lineIndex]);
    }

    const resultLineStart = blockStart + lineOffsets[resultLine];
    blocks.push({
      number: match.groups?.number ?? '',
      instructions: instructionParts.join('\n').trimEnd(),
      resultStart: resultLineStart + resultPrefixLength,
      resultEnd: blockEnd,
    });
  }

  return blocks;
}





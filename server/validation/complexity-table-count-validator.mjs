import { VALIDATOR_NAMES, createFail, createPass, normalizeComplexity, loadTurnTextArtifact } from './common.mjs';

const TABLE_TOKEN_RE = /\b([A-Za-z0-9_$#"]+)\.([A-Za-z0-9_$#"]+)\b/g;

function countTablesInText(content) {
  if (!content.trim() || content.toUpperCase().includes('[NO TABLES FOUND]')) {
    return 0;
  }

  const seen = new Set();
  for (const match of content.matchAll(TABLE_TOKEN_RE)) {
    const schemaName = match[1].replaceAll('"', '').toUpperCase();
    const tableName = match[2].replaceAll('"', '').toUpperCase();
    seen.add(`${schemaName}.${tableName}`);
  }
  return seen.size;
}

function validateCountForComplexity(complexity, count) {
  if (complexity === 'simple') {
    return { valid: count >= 2, expectedText: 'at least 2 tables' };
  }
  if (complexity === 'intermediate') {
    return { valid: count >= 3 && count <= 4, expectedText: '3-4 tables' };
  }
  if (complexity === 'advanced') {
    return { valid: count >= 4, expectedText: 'at least 4 tables' };
  }
  return { valid: false, expectedText: 'supported values are simple, intermediate, advanced' };
}

export async function runComplexityTableCountValidator(taskId, taskDir, metadata) {
  const validatorName = VALIDATOR_NAMES.complexityTableCount;
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);

  if (!Number.isInteger(numTurns) || numTurns <= 0) {
    return [
      createFail(validatorName, taskId, null, 'Metadata', 'invalid_num_turns', {
        expected: 'metadata must contain a positive integer `num_turns`',
        present: `found \`num_turns=${metadata?.num_turns ?? null}\``,
        update: 'set `num_turns` to the actual number of turns for the task',
      }),
    ];
  }

  const complexity = normalizeComplexity(metadata?.complexity);
  if (!['simple', 'intermediate', 'advanced'].includes(complexity)) {
    return [
      createFail(validatorName, taskId, null, 'Complexity', 'unsupported_complexity', {
        expected: 'complexity must be one of simple, intermediate, advanced',
        present: `found \`complexity=${metadata?.complexity ?? null}\``,
        update: 'correct the metadata complexity or update the validator rules explicitly',
      }),
    ];
  }

  const results = [];

  for (let turnNumber = 1; turnNumber <= numTurns; turnNumber += 1) {
    const artifact = await loadTurnTextArtifact(taskDir, 'turn_tables_file', taskId, turnNumber);
    if (!artifact.text) {
      results.push(
        createFail(validatorName, taskId, turnNumber, 'Table Artifact', 'missing_artifact', {
          expected: `table artifact for turn ${turnNumber} must exist`,
          present: `\`${artifact.fileName}\` not found in ${taskDir}`,
          update: 'run the tables extraction step before running complexity validation',
          sourceFile: artifact.fileName,
        }),
      );
      continue;
    }

    results.push(createPass(validatorName, taskId, turnNumber, 'Table Artifact', 'artifact_present', artifact.fileName));

    if (turnNumber !== numTurns) {
      continue;
    }

    const tableCount = countTablesInText(artifact.text);
    const { valid, expectedText } = validateCountForComplexity(complexity, tableCount);

    if (valid) {
      results.push(createPass(validatorName, taskId, turnNumber, 'Complexity Table Count', 'count_aligned', artifact.fileName));
    } else {
      results.push(
        createFail(validatorName, taskId, turnNumber, 'Complexity Table Count', 'count_mismatch', {
          expected: `complexity \`${complexity}\` requires ${expectedText}`,
          present: `${tableCount} distinct tables found in \`${artifact.fileName}\``,
          update: 'revise the prompt/output table usage or correct the task complexity metadata',
          sourceFile: artifact.fileName,
        }),
      );
    }
  }

  return results;
}

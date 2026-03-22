import {
  VALIDATOR_NAMES,
  createFail,
  createPass,
  findLineNumber,
  loadTurnTextArtifact,
  metadataBool,
  parseReasoningTypes,
} from './common.mjs';

const SQLERRM_RE = /\bSQLERRM\b/gi;
const EXCEPTION_BLOCK_RE = /\bEXCEPTION\b[\s\S]{0,100}?\bWHEN\b/i;
const RCA_REASONING_TYPE = 'root cause analysis';
const INLINE_EXECUTION_REASONING_TYPES = new Set(['inline execution']);
const MULTI_TURN_QUERY_TYPE = 'multi-turn';
const INLINE_EXECUTION_PATTERNS = [
  [/\bEXISTS\s*\(/i, 'EXISTS subquery'],
  [/\bTABLE\s*\(/i, 'TABLE(...) inline source'],
  [/\bFROM\s*\(\s*SELECT\b/i, 'inline view via FROM (SELECT ...)'],
  [/\b(?:IN|NOT\s+IN|ANY|ALL)\s*\(\s*SELECT\b/i, 'nested subquery'],
  [/\b(?:=|<>|!=|>|<|>=|<=)\s*\(\s*SELECT\b/i, 'scalar subquery comparison'],
  [/\bPRAGMA\s+AUTONOMOUS_TRANSACTION\b/i, 'pragma autonomous transaction'],
];
const PROGRAM_UNIT_CREATE_RE = /\bCREATE\s+(?:OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION|PACKAGE(?:\s+BODY)?)\b/i;
const TYPE_CREATE_RE = /\bCREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+(\w+)\s+/gi;
const PROMPT_REQUIREMENTS_SECTION_RE = /^\s*Requirements?\s*:\s*$/i;
const PROMPT_SECTION_END_RE = /^\s*(?:Parameters?|Output|Sorting\s+Order|Exception\s+Handling)\s*:/i;
const PROMPT_OBJECT_HEADER_RE = /^\s*(?:Procedure|Function|Package|Trigger|Object)\s+Name\s*:\s*$/i;
const PROMPT_ANON_BLOCK_HEADER_RE = /^\s*Anonymous\s+Block\s*:\s*$/i;
const CLASSIC_RCA_RE =
  /\bEXCEPTION\b[\s\S]{0,600}?\bWHEN\s+(?:NO_DATA_FOUND|TOO_MANY_ROWS|ZERO_DIVIDE|VALUE_ERROR|INVALID_NUMBER|DUP_VAL_ON_INDEX|ROWTYPE_MISMATCH|PROGRAM_ERROR|STORAGE_ERROR|LOGIN_DENIED|NOT_LOGGED_ON|ACCESS_INTO_NULL|COLLECTION_IS_NULL|SUBSCRIPT_BEYOND_COUNT|SUBSCRIPT_OUTSIDE_LIMIT|CASE_NOT_FOUND|INVALID_CURSOR|CURSOR_ALREADY_OPEN|SELF_IS_NULL|TIMEOUT_ON_RESOURCE|TRANSACTION_BACKED_OUT|SYS_INVALID_ROWID|INVALID_TRANSACTION|SERIALIZABLE_TRANSACTION|DEADLOCK_DETECTED|OTHERS)\b[\s\S]{0,250}?(?:DBMS_OUTPUT\s*\.\s*PUT_LINE|RAISE_APPLICATION_ERROR|RETURN\b|:=|NULL\s*;)/i;
const ANTICIPATORY_RCA_GUARD_RE =
  /(?:\bIF\b[\s\S]{0,220}?(?:<>|!=|>=|<=|>|<)\s*0\b[\s\S]{0,120}?\bTHEN\b|\bIF\b[\s\S]{0,220}?\bIS\s+(?:NOT\s+)?NULL\b[\s\S]{0,120}?\bTHEN\b|\bIF\b[\s\S]{0,220}?\b(?:EXISTS|COUNT|LENGTH|TRIM|NVL|COALESCE)\s*\([\s\S]{0,120}?\bTHEN\b)/i;
const ANTICIPATORY_RCA_MESSAGE_RE =
  /\bDBMS_OUTPUT\s*\.\s*PUT_LINE\s*\([\s\S]{0,200}?(?:root\s+cause|issue|prevent|avoid|skip|invalid|missing|no\s+data|zero|divide|diagnos|warning|potential)/i;
const RCA_ANNOTATION_RE =
  /(?:--\s*RCA(?:\s+Note|\s+Summary|\s+Enhancement)?\s*:|\bDBMS_OUTPUT\s*\.\s*PUT_LINE\s*\([\s\S]{0,220}?(?:RCA\s+Note|RCA\s+Summary|RCA\s+Enhancement|Root\s+Cause|Issue\s+identified|Solution\s*:)|--\s*(?:Issue\s+identified|Root\s+Cause|Solution)\s*:)/i;
const REASONING_TYPE_EVIDENCE = {
  aggregation: [/\b(?:COUNT|SUM|AVG|MIN|MAX|LISTAGG|MEDIAN|STDDEV|VARIANCE)\s*\(/i, 'use an aggregate function such as COUNT(), SUM(), AVG(), MIN(), or MAX()'],
  collections: [/(?:\bTYPE\s+\w+\s+IS\s+(?:TABLE|VARRAY|NESTED\s+TABLE)\b|\bINDEX\s+BY\s+(?:PLS_INTEGER|BINARY_INTEGER|VARCHAR2)\b|\b\.(?:EXTEND|TRIM|DELETE|COUNT|FIRST|LAST|NEXT|PRIOR)\s*[\(;])/i, 'declare and use a PL/SQL collection (associative array, nested table, or VARRAY)'],
  'control flow': [/\b(?:IF\s+\w|CASE\s+WHEN|CASE\b[\s\S]{0,10}?\bWHEN\b)/i, 'use conditional control flow with IF/THEN/ELSE or CASE/WHEN'],
  cursors: [/(?:\bCURSOR\s+\w+\b|\bFOR\s+\w+\s+IN\s+(?:\w+|\(\s*SELECT)\b|\bOPEN\s+\w+\b|\bFETCH\s+\w+\s+INTO\b)/i, 'declare and use an explicit cursor (CURSOR ... IS / FOR rec IN cursor LOOP / OPEN-FETCH-CLOSE)'],
  'data manipulation': [/\b(?:INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM|MERGE\s+INTO)/i, 'include a DML statement: INSERT INTO, UPDATE, DELETE FROM, or MERGE INTO'],
  'data retrieval': [/\bSELECT\b[\s\S]{0,200}?\bFROM\b/i, 'include a SELECT ... FROM query to retrieve data'],
  debugging: [/\bDBMS_OUTPUT\s*\.\s*PUT_LINE\s*\(/i, 'add DBMS_OUTPUT.PUT_LINE() calls to emit diagnostic output'],
  encapsulation: [/\bCREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\b/i, 'use a PACKAGE to encapsulate related procedures, functions, and variables'],
  'event-driven logic': [/\bCREATE\s+(?:OR\s+REPLACE\s+)?TRIGGER\b/i, 'create a TRIGGER that fires on a database event (INSERT/UPDATE/DELETE)'],
  'exception handling': [/\bEXCEPTION\b[\s\S]{0,100}?\bWHEN\b/i, 'add an EXCEPTION block with WHEN handlers to manage runtime errors'],
  'inline execution': [/(?:\bEXISTS\s*\(|\bTABLE\s*\(|\bFROM\s*\(\s*SELECT\b|\b(?:IN|NOT\s+IN|ANY|ALL)\s*\(\s*SELECT\b|\b(?:=|<>|!=|>|<|>=|<=)\s*\(\s*SELECT\b|\bPRAGMA\s+AUTONOMOUS_TRANSACTION\b)/i, 'add inline execution logic: EXISTS(...), IN (SELECT ...), FROM (SELECT ...), scalar subquery, or TABLE(...)'],
  iterative: [/\b(?:FOR\s+\w+\s+IN\b|WHILE\b[\s\S]{0,80}?LOOP\b|(?:^|\s)LOOP\b)/i, 'add an iterative construct: FOR loop, WHILE loop, or basic LOOP ... END LOOP'],
  'memory & type': [/(?:\b\w+\s+\w+%(?:TYPE|ROWTYPE)\b|\bCREATE\s+(?:OR\s+REPLACE\s+)?TYPE\b)/i, 'use %TYPE / %ROWTYPE anchored declarations or CREATE TYPE for memory-safe type binding'],
  'object-oriented design': [/(?:\bCREATE\s+(?:OR\s+REPLACE\s+)?TYPE\s+\w+\s+AS\s+OBJECT\b|\bMEMBER\s+(?:FUNCTION|PROCEDURE)\b)/i, 'define an object type using CREATE TYPE ... AS OBJECT with MEMBER FUNCTION or MEMBER PROCEDURE'],
  'root cause analysis': [/(?:\bEXCEPTION\b[\s\S]{0,600}?\bWHEN\s+(?:NO_DATA_FOUND|TOO_MANY_ROWS|ZERO_DIVIDE|VALUE_ERROR|INVALID_NUMBER|DUP_VAL_ON_INDEX|OTHERS)\b[\s\S]{0,250}?(?:DBMS_OUTPUT\s*\.\s*PUT_LINE|RAISE_APPLICATION_ERROR|RETURN\b|:=|NULL\s*;)|--\s*RCA(?:\s+Note|\s+Summary|\s+Enhancement)?\s*:|--\s*(?:Issue\s+identified|Root\s+Cause|Solution)\s*:)/i, 'add exception-driven root cause analysis: handle a named Oracle exception and include an RCA annotation comment'],
  'transaction management': [/\b(?:COMMIT|ROLLBACK|SAVEPOINT|SET\s+TRANSACTION)\b/i, 'include explicit transaction control with COMMIT, ROLLBACK, or SAVEPOINT'],
  validation: [/(?:\bIF\b[\s\S]{0,300}?\bTHEN\b|\bRAISE(?:_APPLICATION_ERROR)?\b|\b\w+\s+EXCEPTION\b)/i, 'add input validation using IF checks, user-defined exceptions, or RAISE_APPLICATION_ERROR'],
};

function hasLeadingAnonymousBlock(codeText) {
  let stripped = codeText.trimStart();
  while (stripped) {
    if (stripped.startsWith('--')) {
      const newline = stripped.indexOf('\n');
      if (newline === -1) {
        return false;
      }
      stripped = stripped.slice(newline + 1).trimStart();
      continue;
    }
    if (stripped.startsWith('/*')) {
      const end = stripped.indexOf('*/');
      if (end === -1) {
        return false;
      }
      stripped = stripped.slice(end + 2).trimStart();
      continue;
    }
    break;
  }

  const normalized = stripped.toUpperCase();
  return normalized.startsWith('DECLARE') || normalized.startsWith('BEGIN');
}

function requiresRootCauseAnalysis(metadata) {
  return parseReasoningTypes(metadata).some((entry) => entry.toLowerCase() === RCA_REASONING_TYPE);
}

function requiresInlineExecution(metadata) {
  if (metadataBool(metadata?.required_inline_execution)) {
    return true;
  }
  return parseReasoningTypes(metadata).some((entry) => INLINE_EXECUTION_REASONING_TYPES.has(entry.toLowerCase()));
}

function isMultiTurnTask(metadata) {
  const queryType = String(metadata?.query_type ?? '').trim().toLowerCase();
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);
  return queryType === MULTI_TURN_QUERY_TYPE || numTurns > 1;
}

function extractRequiredObjectsFromPrompt(promptText) {
  const lines = promptText.split(/\r?\n/);
  const startIndex = lines.findIndex((line) => PROMPT_REQUIREMENTS_SECTION_RE.test(line));
  if (startIndex === -1) {
    return [];
  }

  let endIndex = lines.length;
  for (let index = startIndex + 1; index < lines.length; index += 1) {
    if (PROMPT_SECTION_END_RE.test(lines[index])) {
      endIndex = index;
      break;
    }
  }

  const body = lines.slice(startIndex + 1, endIndex).map((line) => line.trim()).filter(Boolean);
  const entries = [];

  for (let cursor = 0; cursor < body.length; cursor += 1) {
    const label = body[cursor];
    if (PROMPT_ANON_BLOCK_HEADER_RE.test(label)) {
      entries.push(['ANONYMOUS BLOCK', 'ANONYMOUS BLOCK']);
      continue;
    }

    if (!PROMPT_OBJECT_HEADER_RE.test(label)) {
      continue;
    }

    const normalizedLabel = label.toLowerCase();
    const objectType =
      normalizedLabel.includes('procedure') ? 'PROCEDURE' :
      normalizedLabel.includes('function') ? 'FUNCTION' :
      normalizedLabel.includes('package') ? 'PACKAGE' :
      normalizedLabel.includes('trigger') ? 'TRIGGER' :
      normalizedLabel.includes('object') ? 'OBJECT' :
      null;

    if (!objectType) {
      continue;
    }

    while (cursor + 1 < body.length) {
      const value = body[cursor + 1].trim();
      if (!value) {
        cursor += 1;
        continue;
      }
      if (PROMPT_OBJECT_HEADER_RE.test(value) || PROMPT_ANON_BLOCK_HEADER_RE.test(value)) {
        break;
      }
      entries.push([objectType, value.toUpperCase()]);
      cursor += 1;
    }
  }

  return entries;
}

function objectDefinedInCode(objectType, objectName, allCode) {
  const escaped = objectName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  let pattern = null;
  if (['PROCEDURE', 'FUNCTION'].includes(objectType)) {
    pattern = new RegExp(`\\b(?:PROCEDURE|FUNCTION)\\s+${escaped}\\b`, 'i');
  } else if (objectType === 'PACKAGE') {
    pattern = new RegExp(`\\bPACKAGE\\s+(?!BODY\\s+)${escaped}\\b`, 'i');
  } else if (objectType === 'TRIGGER') {
    pattern = new RegExp(`\\bTRIGGER\\s+${escaped}\\b`, 'i');
  } else if (objectType === 'OBJECT') {
    pattern = new RegExp(`\\bTYPE\\s+${escaped}\\b`, 'i');
  }
  return pattern ? pattern.test(allCode) : true;
}

function detectInlineExecution(codeText) {
  for (const [pattern] of INLINE_EXECUTION_PATTERNS) {
    const match = pattern.exec(codeText);
    if (match) {
      return { found: true, line: findLineNumber(codeText, match.index) };
    }
  }
  return { found: false, line: null };
}

function validateTurnCode(taskId, turnNumber, codeText, sourceName) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  const results = [createPass(validatorName, taskId, turnNumber, 'Reference Answer Artifact', 'artifact_present', sourceName)];

  if (!EXCEPTION_BLOCK_RE.test(codeText)) {
    results.push(createFail(validatorName, taskId, turnNumber, 'Exception Handling', 'missing_exception_block', {
      expected: 'every turn must contain an EXCEPTION block with at least one WHEN handler',
      present: `no EXCEPTION...WHEN block found in ${sourceName}`,
      update: 'add an EXCEPTION block with appropriate WHEN handlers to the PL/SQL program',
      sourceFile: sourceName,
    }));
  } else {
    results.push(createPass(validatorName, taskId, turnNumber, 'Exception Handling', 'exception_block_present', sourceName));
  }

  const sqlerrmMatches = [...codeText.matchAll(SQLERRM_RE)];
  if (sqlerrmMatches.length) {
    const lines = sqlerrmMatches.map((match) => findLineNumber(codeText, match.index ?? 0));
    results.push(createFail(validatorName, taskId, turnNumber, 'SQLERRM Usage', 'disallowed_sqlerrm', {
      expected: 'stable fixed error text without SQLERRM',
      present: `SQLERRM found at line(s) ${lines.join(', ')} in ${sourceName}`,
      update: 'replace SQLERRM with a rubric-safe generic message',
      sourceFile: sourceName,
      line: lines[0],
    }));
  } else {
    results.push(createPass(validatorName, taskId, turnNumber, 'SQLERRM Usage', 'not_present', sourceName));
  }

  return results;
}

const AUXILIARY_ARTIFACTS = [
  {
    templateKey: 'turn_columns_file',
    item: 'Columns Artifact',
    missingUpdate: 'run the columns extraction step before running validation',
  },
  {
    templateKey: 'turn_test_cases_file',
    item: 'Test Cases Artifact',
    missingUpdate: 'run the test-case generation step before running validation',
  },
  {
    templateKey: 'turn_reasoning_types_file',
    item: 'Reasoning Types Artifact',
    missingUpdate: 'run the reasoning-types generation step before running validation',
  },
  {
    templateKey: 'turn_plsql_constructs_file',
    item: 'PL/SQL Constructs Artifact',
    missingUpdate: 'run the PL/SQL constructs generation step before running validation',
  },
];

async function validateAuxiliaryArtifacts(taskId, taskDir, turnNumber) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  const results = [];

  for (const artifactSpec of AUXILIARY_ARTIFACTS) {
    const artifact = await loadTurnTextArtifact(taskDir, artifactSpec.templateKey, taskId, turnNumber);
    if (!artifact.text) {
      results.push(createFail(validatorName, taskId, turnNumber, artifactSpec.item, 'missing_artifact', {
        expected: `${artifactSpec.item.toLowerCase()} for turn ${turnNumber} must exist`,
        present: `\`${artifact.fileName}\` not found in ${taskDir}`,
        update: artifactSpec.missingUpdate,
        sourceFile: artifact.fileName,
      }));
      continue;
    }

    results.push(createPass(validatorName, taskId, turnNumber, artifactSpec.item, 'artifact_present', artifact.fileName));
  }

  return results;
}
function validateRequiredConstructs(taskId, turnPayloads, metadata) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  const results = [];
  const requiredNamed = metadataBool(metadata?.required_procs_funcs_pkgs);
  const requiredAnonymousBlock = metadataBool(metadata?.required_anonymous_block);

  let programUnitMatch = null;
  let anonymousTurn = null;

  for (const payload of turnPayloads) {
    if (!programUnitMatch) {
      const match = PROGRAM_UNIT_CREATE_RE.exec(payload.codeText);
      if (match) {
        programUnitMatch = { turnNumber: payload.turnNumber, sourceName: payload.sourceName, objectType: match[1].toUpperCase() };
      }
    }
    if (!anonymousTurn && hasLeadingAnonymousBlock(payload.codeText)) {
      anonymousTurn = { turnNumber: payload.turnNumber, sourceName: payload.sourceName };
    }
  }

  if (requiredNamed) {
    if (!programUnitMatch) {
      results.push(createFail(validatorName, taskId, null, 'Program Unit Creation', 'missing_required_program_unit_creation', {
        expected: 'at least one CREATE PROCEDURE/FUNCTION/PACKAGE statement when required_procs_funcs_pkgs=true',
        present: 'no CREATE PROCEDURE/FUNCTION/PACKAGE statement found in any turn',
        update: 'add the required named program unit to one of the PL/SQL turns',
      }));
    } else {
      results.push(createPass(validatorName, taskId, programUnitMatch.turnNumber, 'Program Unit Creation', `required_program_unit_present_${programUnitMatch.objectType.toLowerCase()}`, programUnitMatch.sourceName));
    }
  }

  if (requiredAnonymousBlock) {
    if (!anonymousTurn) {
      results.push(createFail(validatorName, taskId, null, 'Anonymous Block', 'missing_required_anonymous_block', {
        expected: 'at least one turn must begin with an anonymous DECLARE/BEGIN block when required_anonymous_block=true',
        present: 'no anonymous block turn found in the PL/SQL artifacts',
        update: 'add an anonymous block in the required turn scope',
      }));
    } else {
      results.push(createPass(validatorName, taskId, anonymousTurn.turnNumber, 'Anonymous Block', 'required_anonymous_block_present', anonymousTurn.sourceName));
    }
  }

  return results;
}

function validateRootCauseAnalysis(taskId, turnPayloads, metadata) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  if (!requiresRootCauseAnalysis(metadata)) {
    return [createPass(validatorName, taskId, null, 'Root Cause Analysis', 'not_required')];
  }

  const combinedCode = turnPayloads.map((entry) => entry.codeText).filter(Boolean).join('\n\n');
  if (!combinedCode) {
    return [createFail(validatorName, taskId, null, 'Root Cause Analysis', 'missing_evidence', {
      expected: 'RCA evidence must exist because metadata requires `Root Cause Analysis`',
      present: 'no PL/SQL turn content was available to validate RCA',
      update: 'generate the per-turn reference answer files and include the required RCA behavior',
    })];
  }

  if (isMultiTurnTask(metadata)) {
    const hasClassic = CLASSIC_RCA_RE.test(combinedCode);
    const hasAnnotation = RCA_ANNOTATION_RE.test(combinedCode);
    if (hasClassic && hasAnnotation) {
      return [createPass(validatorName, taskId, null, 'Root Cause Analysis', 'multi_turn_evidence_present')];
    }

    const present = [];
    if (!hasClassic) {
      present.push('no exception-driven RCA evidence with recovery behavior');
    }
    if (!hasAnnotation) {
      present.push('no RCA annotation (-- RCA Note:, Root Cause:, Issue identified:) found');
    }

    return [createFail(validatorName, taskId, null, 'Root Cause Analysis', hasClassic ? 'missing_rca_annotation' : 'missing_multi_turn_rca', {
      expected: 'at least one turn must show classic RCA through exception handling AND include a clear RCA annotation or diagnostic message',
      present: present.join('; '),
      update: hasClassic
        ? "add '-- RCA Note: ...' or DBMS_OUTPUT containing 'Root Cause' / 'Issue identified'"
        : 'add an EXCEPTION block handling a concrete failure such as NO_DATA_FOUND or ZERO_DIVIDE with an RCA annotation',
    })];
  }

  const firstTurn = turnPayloads[0];
  const hasGuard = ANTICIPATORY_RCA_GUARD_RE.test(combinedCode);
  const hasMessage = ANTICIPATORY_RCA_MESSAGE_RE.test(combinedCode);
  const hasAnnotation = RCA_ANNOTATION_RE.test(combinedCode);

  if (hasGuard && (hasMessage || hasAnnotation)) {
    return [createPass(validatorName, taskId, firstTurn?.turnNumber ?? null, 'Root Cause Analysis', 'single_turn_evidence_present', firstTurn?.sourceName ?? null)];
  }

  const present = [];
  if (!hasGuard) {
    present.push('preventive null/zero/invalid-state guard not found');
  }
  if (!(hasMessage || hasAnnotation)) {
    present.push('clear RCA explanation not found in DBMS_OUTPUT or approved RCA note');
  }

  return [createFail(validatorName, taskId, firstTurn?.turnNumber ?? null, 'Root Cause Analysis', 'missing_single_turn_rca', {
    expected: 'preventive guard logic and a clear RCA explanation must both be present',
    present: present.join('; '),
    update: 'add IF-based defensive logic and emit a clear RCA message such as an RCA Note, RCA Summary, Issue identified, or Solution',
    sourceFile: firstTurn?.sourceName ?? null,
  })];
}

function validateInlineExecution(taskId, turnPayloads, metadata) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  if (!requiresInlineExecution(metadata)) {
    return [createPass(validatorName, taskId, null, 'Inline Execution', 'not_required')];
  }

  for (const payload of turnPayloads) {
    const detected = detectInlineExecution(payload.codeText);
    if (detected.found) {
      return [createPass(validatorName, taskId, null, 'Inline Execution', 'evidence_present', payload.sourceName)];
    }
  }

  const searchedTurns = turnPayloads.length ? turnPayloads.map((entry) => `Turn ${entry.turnNumber}`).join(', ') : 'none';
  return [createFail(validatorName, taskId, null, 'Inline Execution', 'missing_inline_execution', {
    expected: 'at least one turn must contain nested subquery, inline view, correlated subquery, or equivalent inline execution evidence',
    present: `no EXISTS, inline view \`FROM (SELECT ...)\`, nested subquery, scalar subquery comparison, TABLE(...), or PRAGMA AUTONOMOUS_TRANSACTION found across ${searchedTurns}`,
    update: 'add real inline execution logic such as EXISTS(...), IN (SELECT ...), FROM (SELECT ...), or another nested/correlated query pattern',
    sourceFile: turnPayloads[0]?.sourceName ?? null,
  })];
}

function validateReasoningTypeCoverage(taskId, turnPayloads, metadata) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  const requiredTypes = parseReasoningTypes(metadata);
  if (!requiredTypes.length) {
    return [];
  }

  const combinedCode = turnPayloads.map((entry) => entry.codeText).filter(Boolean).join('\n\n');
  const results = [];

  for (const rawType of requiredTypes) {
    const entry = REASONING_TYPE_EVIDENCE[rawType.trim().toLowerCase()];
    if (!entry) {
      continue;
    }
    const [pattern, fixHint] = entry;
    if (pattern.test(combinedCode)) {
      results.push(createPass(validatorName, taskId, null, `Reasoning Type: ${rawType}`, 'evidence_present'));
    } else {
      results.push(createFail(validatorName, taskId, null, `Reasoning Type: ${rawType}`, 'missing_reasoning_type_evidence', {
        expected: `'${rawType}' is required by target_reasoning_types and must be evidenced in the program`,
        present: `no evidence of '${rawType}' found across all turns`,
        update: fixHint,
      }));
    }
  }

  return results;
}

function validateTypeUsage(taskId, turnPayloads) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  const allCode = turnPayloads.map((entry) => entry.codeText).join('\n');
  const results = [];
  for (const match of allCode.matchAll(TYPE_CREATE_RE)) {
    const typeName = match[1];
    const remaining = `${allCode.slice(0, match.index)}${allCode.slice((match.index ?? 0) + match[0].length)}`;
    if (remaining.toUpperCase().includes(typeName.toUpperCase())) {
      continue;
    }
    const owner = turnPayloads.find((entry) => entry.codeText.toUpperCase().includes(match[0].toUpperCase()));
    results.push(createFail(validatorName, taskId, owner?.turnNumber ?? null, 'Type Usage', 'unused_type_created', {
      expected: `Created type '${typeName}' must be used in the program`,
      present: `CREATE TYPE ${typeName} found but not used elsewhere`,
      update: 'Either use the type in variable declarations, collections, or remove the CREATE TYPE if not needed',
      sourceFile: owner?.sourceName ?? null,
    }));
  }
  return results;
}

function validatePromptObjectCoverage(taskId, turnPayloads, turnPrompts) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  if (!turnPrompts.length) {
    return [];
  }

  const allCode = turnPayloads.map((entry) => entry.codeText).join('\n\n');
  const results = [];
  for (const promptEntry of turnPrompts) {
    const required = extractRequiredObjectsFromPrompt(promptEntry.promptText).filter(([objectType]) => objectType !== 'ANONYMOUS BLOCK');
    if (!required.length) {
      results.push(createPass(validatorName, taskId, promptEntry.turnNumber, 'Prompt Object Coverage', 'no_named_requirements', promptEntry.sourceName));
      continue;
    }
    const missing = required.filter(([objectType, objectName]) => !objectDefinedInCode(objectType, objectName, allCode)).map(([objectType, objectName]) => `${objectType} ${objectName}`);
    if (missing.length) {
      results.push(createFail(validatorName, taskId, promptEntry.turnNumber, 'Prompt Object Coverage', 'missing_required_objects', {
        expected: 'every named object in the prompt Requirements section must be created or declared in the reference answer',
        present: `required object(s) not found in any turn's code: ${missing.join(', ')}`,
        update: "add CREATE or inline declaration for each missing object in the appropriate turn's reference answer",
        sourceFile: promptEntry.sourceName,
      }));
    } else {
      results.push(createPass(validatorName, taskId, promptEntry.turnNumber, 'Prompt Object Coverage', 'all_objects_implemented', promptEntry.sourceName));
    }
  }
  return results;
}

export async function runPlsqlProgramValidator(taskId, taskDir, metadata) {
  const validatorName = VALIDATOR_NAMES.plsqlProgram;
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);
  if (!Number.isInteger(numTurns) || numTurns <= 0) {
    return [createFail(validatorName, taskId, null, 'Metadata', 'invalid_num_turns', {
      expected: 'metadata must contain a positive integer `num_turns`',
      present: `found \`num_turns=${metadata?.num_turns ?? null}\``,
      update: 'set `num_turns` to the actual number of turns for the task',
    })];
  }

  const results = [];
  const turnPayloads = [];
  const turnPrompts = [];

  for (let turnNumber = 1; turnNumber <= numTurns; turnNumber += 1) {
    results.push(...await validateAuxiliaryArtifacts(taskId, taskDir, turnNumber));

    const codeArtifact = await loadTurnTextArtifact(taskDir, 'turn_reference_answer_file', taskId, turnNumber);
    if (!codeArtifact.text) {
      results.push(createFail(validatorName, taskId, turnNumber, 'Reference Answer Artifact', 'missing_artifact', {
        expected: `reference answer file for turn ${turnNumber} must exist`,
        present: `no PL/SQL reference-answer file found for turn ${turnNumber} in ${taskDir}`,
        update: 'create the per-turn reference answer artifact before running validation',
        sourceFile: codeArtifact.fileName,
      }));
      continue;
    }

    turnPayloads.push({ turnNumber, sourceName: codeArtifact.fileName, codeText: codeArtifact.text });
    results.push(...validateTurnCode(taskId, turnNumber, codeArtifact.text, codeArtifact.fileName));

    const promptArtifact = await loadTurnTextArtifact(taskDir, 'turn_user_file', taskId, turnNumber);
    if (promptArtifact.text) {
      turnPrompts.push({ turnNumber, sourceName: promptArtifact.fileName, promptText: promptArtifact.text });
    }
  }

  results.push(...validateRequiredConstructs(taskId, turnPayloads, metadata));
  results.push(...validateInlineExecution(taskId, turnPayloads, metadata));
  results.push(...validateRootCauseAnalysis(taskId, turnPayloads, metadata));
  results.push(...validateReasoningTypeCoverage(taskId, turnPayloads, metadata));
  results.push(...validateTypeUsage(taskId, turnPayloads));
  results.push(...validatePromptObjectCoverage(taskId, turnPayloads, turnPrompts));

  return results;
}




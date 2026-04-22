import {
  VALIDATOR_NAMES,
  createFail,
  createPass,
  findLineNumber,
  loadTurnTextArtifact,
} from './common.mjs';

const REQUIREMENT_HEADERS = new Map([
  ['PROCEDURE NAME:', 'PROCEDURE'],
  ['FUNCTION NAME:', 'FUNCTION'],
  ['PACKAGE NAME:', 'PACKAGE'],
  ['TRIGGER NAME:', 'TRIGGER'],
  ['OBJECT NAME:', 'OBJECT'],
]);
const SECTION_HEADERS = /^(Requirements|Parameters|Output|Sorting Order|Exception Handling)\s*:/i;
const TOP_LEVEL_UNIT_RE = /\bCREATE\s+(?:OR\s+REPLACE\s+)?(PACKAGE(?!\s+BODY\b)|TRIGGER|TYPE(?!\s+BODY\b)|PROCEDURE|FUNCTION)\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)/gi;
const PACKAGE_ROUTINE_RE = /\b(PROCEDURE|FUNCTION)\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)\s*(?=\(|;|IS\b|AS\b)/gi;
const PLACEHOLDER_TOKEN_RE = /(\[[^\]]+\]|<[^>]+>)/g;

export async function runArtifactAlignmentValidator(taskId, taskDir, metadata) {
  const validatorName = VALIDATOR_NAMES.artifactAlignment;
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);
  if (!Number.isInteger(numTurns) || numTurns <= 0) {
    return [createFail(validatorName, taskId, null, 'Metadata', 'invalid_num_turns', {
      expected: 'metadata must contain a positive integer `num_turns`',
      present: `found \`num_turns=${metadata?.num_turns ?? null}\``,
      update: 'set `num_turns` to the actual number of turns for the task',
    })];
  }

  const results = [];

  for (let turnNumber = 1; turnNumber <= numTurns; turnNumber += 1) {
    const promptArtifact = await loadTurnTextArtifact(taskDir, 'turn_user_file', taskId, turnNumber);
    const codeArtifact = await loadTurnTextArtifact(taskDir, 'turn_reference_answer_file', taskId, turnNumber);
    const testcaseArtifact = await loadTurnTextArtifact(taskDir, 'turn_test_cases_file', taskId, turnNumber);

    if (!promptArtifact.text) {
      results.push(createFail(validatorName, taskId, turnNumber, 'Prompt Input Artifact', 'missing_prompt_artifact', {
        expected: `prompt input file for turn ${turnNumber} must exist`,
        present: `\`${promptArtifact.fileName}\` not found in ${taskDir}`,
        update: 'fetch or regenerate the prompt artifact before running validation',
        sourceFile: promptArtifact.fileName,
      }));
      continue;
    }
    results.push(createPass(validatorName, taskId, turnNumber, 'Prompt Input Artifact', 'prompt_artifact_present', promptArtifact.fileName));

    if (!codeArtifact.text) {
      results.push(createFail(validatorName, taskId, turnNumber, 'Reference Answer Artifact', 'missing_reference_artifact', {
        expected: `reference answer file for turn ${turnNumber} must exist`,
        present: `\`${codeArtifact.fileName}\` not found in ${taskDir}`,
        update: 'generate the reference answer before running alignment validation',
        sourceFile: codeArtifact.fileName,
      }));
      continue;
    }
    results.push(createPass(validatorName, taskId, turnNumber, 'Reference Answer Artifact', 'reference_artifact_present', codeArtifact.fileName));

    if (!testcaseArtifact.text) {
      results.push(createFail(validatorName, taskId, turnNumber, 'Test Cases Artifact', 'missing_testcase_artifact', {
        expected: `test cases file for turn ${turnNumber} must exist`,
        present: `\`${testcaseArtifact.fileName}\` not found in ${taskDir}`,
        update: 'generate or refresh the testcase artifact before running alignment validation',
        sourceFile: testcaseArtifact.fileName,
      }));
      continue;
    }
    results.push(createPass(validatorName, taskId, turnNumber, 'Test Cases Artifact', 'testcase_artifact_present', testcaseArtifact.fileName));

    const promptShape = parsePromptShape(promptArtifact.text);
    const testcaseBlocks = parseTestCaseBlocks(testcaseArtifact.text);
    const implementedUnits = extractImplementedUnits(codeArtifact.text);

    for (const requirement of promptShape.requirements) {
      const normalizedName = normalizeIdentifier(requirement.name);
      const implemented = implementedUnits.find((entry) => normalizeIdentifier(entry.name) === normalizedName);
      const implementationItem = `Required Program Implementation: ${requirement.type} ${requirement.name}`;

      if (!implemented) {
        results.push(createFail(validatorName, taskId, turnNumber, implementationItem, 'missing_program_implementation', {
          expected: `prompt requirement ${requirement.type} ${requirement.name} must be implemented in the reference answer`,
          present: `${requirement.type} ${requirement.name} is not defined in ${codeArtifact.fileName}`,
          update: `implement ${requirement.type.toLowerCase()} ${requirement.name} in the reference answer`,
          sourceFile: codeArtifact.fileName,
        }));
      } else {
        results.push(createPass(validatorName, taskId, turnNumber, implementationItem, 'program_implemented', codeArtifact.fileName));
      }

      const testcaseProgramItem = `Testcase Program Coverage: ${requirement.type} ${requirement.name}`;
      const testcaseUsage = findProgramCoverageInTestcases(testcaseBlocks, requirement.name);
      if (!testcaseUsage) {
        results.push(createFail(validatorName, taskId, turnNumber, testcaseProgramItem, 'missing_testcase_program_coverage', {
          expected: `testcase execution instructions must reference ${requirement.name}`,
          present: `${requirement.name} was not referenced by any testcase execution instructions in ${testcaseArtifact.fileName}`,
          update: `call ${requirement.name} explicitly from at least one testcase execution block`,
          sourceFile: testcaseArtifact.fileName,
        }));
      } else {
        results.push(createPass(validatorName, taskId, turnNumber, testcaseProgramItem, 'testcase_program_covered', testcaseArtifact.fileName));
      }
    }

    for (const literal of promptShape.outputLiterals) {
      results.push(...buildLiteralCoverageResults({
        validatorName,
        taskId,
        turnNumber,
        literal,
        itemPrefix: 'Output Literal',
        codeText: codeArtifact.text,
        codeFile: codeArtifact.fileName,
        testcaseFile: testcaseArtifact.fileName,
        testcaseBlocks,
        codeRuleId: 'output_literal_in_code',
        codeMissingRuleId: 'missing_output_literal_in_code',
        testcaseRuleId: 'output_literal_in_testcase',
        testcaseMissingRuleId: 'missing_output_literal_in_testcase',
        codeExpectation: 'explicit literal output from the prompt must appear in the program output logic',
        testcaseExpectation: 'explicit literal output from the prompt must appear in at least one testcase execution result',
      }));
    }

    for (const literal of promptShape.exceptionMessages) {
      results.push(...buildLiteralCoverageResults({
        validatorName,
        taskId,
        turnNumber,
        literal,
        itemPrefix: 'Exception Message',
        codeText: codeArtifact.text,
        codeFile: codeArtifact.fileName,
        testcaseFile: testcaseArtifact.fileName,
        testcaseBlocks,
        codeRuleId: 'exception_message_in_code',
        codeMissingRuleId: 'missing_exception_message_in_code',
        testcaseRuleId: 'exception_message_in_testcase',
        testcaseMissingRuleId: 'missing_exception_message_in_testcase',
        codeExpectation: 'explicit exception message from the prompt must appear in program exception handling',
        testcaseExpectation: 'explicit exception message from the prompt must appear in at least one testcase execution result',
      }));
    }

    // MANDATORY: Validate ALL DBMS_OUTPUT.PUT_LINE statements in BEGIN/EXCEPTION blocks are covered by testcases
    const dbmsOutputStatements = extractAllDbmsOutputStatements(codeArtifact.text);
    for (const stmt of dbmsOutputStatements) {
      const item = `DBMS_OUTPUT Coverage: ${stmt.text}`;
      const foundInTestcase = testcaseBlocks.some((block) => {
        const normalizedResult = String(block.result ?? '').toUpperCase();
        const normalizedOutput = String(stmt.normalizedOutput).toUpperCase();
        return normalizedResult.includes(normalizedOutput);
      });

      if (foundInTestcase) {
        results.push(createPass(validatorName, taskId, turnNumber, item, 'dbms_output_covered', testcaseArtifact.fileName));
      } else {
        results.push(createFail(validatorName, taskId, turnNumber, item, 'dbms_output_not_covered', {
          expected: `DBMS_OUTPUT.PUT_LINE '${stmt.text}' must be verified in testcase execution result`,
          present: `No testcase execution result contains '${stmt.text}'`,
          update: `Add a testcase that triggers the '${stmt.text}' output path and verify it in execution_result`,
          sourceFile: testcaseArtifact.fileName,
          line: stmt.line,
        }));
      }
    }
  }

  return results;
}

function parsePromptShape(promptText) {
  const lines = promptText.split(/\r?\n/);
  const sections = new Map();

  lines.forEach((line, index) => {
    const match = line.trim().match(SECTION_HEADERS);
    if (match) {
      sections.set(match[1].toLowerCase().replace(/\s+/g, '_'), index);
    }
  });

  return {
    requirements: parseRequirementEntries(lines, sections),
    outputLiterals: extractPromptLiterals(lines, sections, 'output'),
    exceptionMessages: extractPromptLiterals(lines, sections, 'exception_handling', { extractMessage: true }),
  };
}

function parseRequirementEntries(lines, sections) {
  const start = sections.get('requirements');
  if (start === undefined) {
    return [];
  }

  const stop = findSectionStop(sections, start, lines.length);
  const entries = [];

  for (let lineIndex = start + 1; lineIndex < stop; lineIndex += 1) {
    const trimmed = lines[lineIndex].trim();
    if (!trimmed) {
      continue;
    }

    const normalizedHeader = trimmed.toUpperCase().replace(/\s+/g, ' ');
    const type = REQUIREMENT_HEADERS.get(normalizedHeader);
    if (!type) {
      continue;
    }

    let valueLine = null;
    for (let cursor = lineIndex + 1; cursor < stop; cursor += 1) {
      const value = lines[cursor].trim();
      if (!value) {
        continue;
      }
      if (value.endsWith(':')) {
        break;
      }
      valueLine = { name: value, line: cursor + 1 };
      lineIndex = cursor;
      break;
    }

    if (valueLine) {
      entries.push({
        type,
        name: valueLine.name,
        line: valueLine.line,
      });
    }
  }

  return entries;
}

function extractPromptLiterals(lines, sections, sectionKey, options = {}) {
  const start = sections.get(sectionKey);
  if (start === undefined) {
    return [];
  }

  const stop = findSectionStop(sections, start, lines.length);
  const values = [];

  for (let lineIndex = start + 1; lineIndex < stop; lineIndex += 1) {
    const trimmed = lines[lineIndex].trim();
    if (!trimmed || isPromptGroupHeader(trimmed)) {
      continue;
    }

    if (options.extractMessage) {
      values.push(...deriveExceptionLiteralEntries(trimmed, lineIndex + 1));
      continue;
    }

    values.push(...deriveOutputLiteralEntries(trimmed, lineIndex + 1));
  }

  return dedupeLiterals(values);
}

function deriveOutputLiteralEntries(line, lineNumber) {
  const rawLine = line.trim();
  const headerValueMatch = /^([A-Za-z][A-Za-z0-9 _-]{0,60}:)\s+(.+)$/.exec(rawLine);
  const candidate = headerValueMatch && !looksLikeInstructionalOutput(rawLine)
    ? headerValueMatch[2].trim()
    : rawLine;
  if (!candidate) {
    return [];
  }

  const entries = [];
  const seen = new Set();
  const addLiteral = (value) => {
    const normalized = normalizeLiteral(value);
    if (!normalized) {
      return;
    }
    const key = normalized.toUpperCase();
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    entries.push({
      text: normalized,
      raw: normalized,
      line: lineNumber,
      matchMode: 'literal',
    });
  };

  if (looksLikeInstructionalOutput(candidate)) {
    PLACEHOLDER_TOKEN_RE.lastIndex = 0;
    if (!PLACEHOLDER_TOKEN_RE.test(candidate)) {
      PLACEHOLDER_TOKEN_RE.lastIndex = 0;
      return [];
    }

    PLACEHOLDER_TOKEN_RE.lastIndex = 0;
    const sourceForFragments = /\bas\s+(.+)$/i.exec(candidate)?.[1] ?? candidate;
    for (const fragment of extractStablePlaceholderFragments(sourceForFragments)) {
      if (/^(and|or)\b/i.test(fragment)) {
        continue;
      }
      addLiteral(fragment);
    }
    return entries;
  }

  if (headerValueMatch) {
    addLiteral(headerValueMatch[1]);
  }

  for (const derived of new Set([
    ...deriveLiteralCandidates(candidate),
    ...extractStablePlaceholderFragments(line),
  ])) {
    addLiteral(derived);
  }

  return entries;
}

function deriveExceptionLiteralEntries(line, lineNumber) {
  const detail = line.includes(':')
    ? line.split(':').slice(1).join(':').trim()
    : line.trim();
  if (!detail) {
    return [];
  }

  const printMatch = /\bthen\s+print\b\s*([\s\S]+)$/i.exec(detail);
  const message = normalizeLiteral(printMatch ? printMatch[1] : detail);
  if (!message) {
    return [];
  }

  PLACEHOLDER_TOKEN_RE.lastIndex = 0;
  if (PLACEHOLDER_TOKEN_RE.test(message)) {
    return extractExceptionPlaceholderFragments(message).map((fragment) => ({
      text: normalizeLiteral(fragment),
      raw: fragment,
      line: lineNumber,
      matchMode: 'string_literal_fragment',
    }));
  }

  PLACEHOLDER_TOKEN_RE.lastIndex = 0;
  return [{
    text: message,
    raw: message,
    line: lineNumber,
    matchMode: 'exact_exception_emission',
  }];
}

function findSectionStop(sections, start, lineCount) {
  return Math.min(...[...sections.values()].filter((value) => value > start), lineCount);
}

function isPromptGroupHeader(line) {
  return /^[A-Za-z][A-Za-z0-9_$#]*\s*:\s*$/.test(line);
}

function looksLikeInstructionalOutput(text) {
  return /^(print|format|sort)\b/i.test(String(text ?? '').trim());
}

function isStrictLiteral(text) {
  const trimmed = String(text ?? '').trim();
  if (!trimmed) {
    return false;
  }
  if (/[\[\]{}<>]/.test(trimmed)) {
    return false;
  }
  if (/^(no sorting required\.?)$/i.test(trimmed)) {
    return false;
  }
  return /[A-Za-z]/.test(trimmed);
}

function deriveLiteralCandidates(text) {
  const literals = [];
  if (isStrictLiteral(text)) {
    literals.push(String(text ?? '').trim());
  }

  for (const fragment of extractStablePlaceholderFragments(text)) {
    literals.push(fragment);
  }

  return [...new Set(literals.map((entry) => normalizeLiteral(entry)).filter(Boolean))];
}

function extractStablePlaceholderFragments(text) {
  const raw = String(text ?? '');
  PLACEHOLDER_TOKEN_RE.lastIndex = 0;
  if (!PLACEHOLDER_TOKEN_RE.test(raw)) {
    return [];
  }

  PLACEHOLDER_TOKEN_RE.lastIndex = 0;
  const fragments = [];

  for (const part of raw.split(PLACEHOLDER_TOKEN_RE)) {
    if (!part || PLACEHOLDER_TOKEN_RE.test(part)) {
      PLACEHOLDER_TOKEN_RE.lastIndex = 0;
      continue;
    }

    PLACEHOLDER_TOKEN_RE.lastIndex = 0;
    const normalized = part
      .replace(/^[|,;/\s]+/, '')
      .replace(/[|,;/\s]+$/, '')
      .replace(/\s+/g, ' ')
      .trim();

    if (!isStrictLiteral(normalized)) {
      continue;
    }

    const alphaLength = normalized.replace(/[^A-Za-z]/g, '').length;
    if (alphaLength < 3) {
      continue;
    }

    fragments.push(normalized);
  }

  return fragments;
}

function dedupeLiterals(values) {
  const seen = new Set();
  const deduped = [];
  for (const entry of values) {
    const key = entry.text.toUpperCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(entry);
  }
  return deduped;
}

function extractExceptionPlaceholderFragments(text) {
  const fragments = [];
  for (const part of String(text ?? '').split(/(\[[^\]]+\]|<[^>]+>)/g)) {
    if (!part || /^(\[[^\]]+\]|<[^>]+>)$/.test(part)) {
      continue;
    }

    const normalized = part
      .replace(/^[|,;/\s]+/, '')
      .replace(/[|,;/\s]+$/, '')
      .replace(/\s+/g, ' ')
      .trim();

    if (!isStrictLiteral(normalized)) {
      continue;
    }

    const alphaLength = normalized.replace(/[^A-Za-z]/g, '').length;
    if (alphaLength < 3) {
      continue;
    }

    fragments.push(normalized);
  }

  return fragments;
}

function parseTestCaseBlocks(testcaseText) {
  const lines = testcaseText.split(/\r?\n/);
  const blocks = [];
  let index = 0;

  while (index < lines.length) {
    const headerMatch = lines[index].match(/^\s*Test Case\s+(\d+)\s*:\s*$/i);
    if (!headerMatch) {
      index += 1;
      continue;
    }

    const number = Number.parseInt(headerMatch[1], 10);
    let instructionsStart = null;
    let resultStart = null;
    let cursor = index + 1;

    while (cursor < lines.length && !/^\s*Test Case\s+\d+\s*:\s*$/i.test(lines[cursor])) {
      const trimmed = lines[cursor].trim().toLowerCase();
      if (trimmed === 'execution_instructions:') {
        instructionsStart = cursor + 1;
      } else if (trimmed === 'execution_result:') {
        resultStart = cursor + 1;
      }
      cursor += 1;
    }

    const blockEnd = cursor;
    const instructionsEnd = resultStart ? resultStart - 1 : blockEnd;
    const instructions = instructionsStart ? lines.slice(instructionsStart, instructionsEnd).join('\n').trim() : '';
    const result = resultStart ? lines.slice(resultStart, blockEnd).join('\n').trim() : '';

    blocks.push({
      number,
      instructions,
      result,
      instructionsLine: instructionsStart ? instructionsStart + 1 : null,
      resultLine: resultStart ? resultStart + 1 : null,
    });

    index = blockEnd;
  }

  return blocks;
}

function extractImplementedUnits(codeText) {
  const units = [];
  const seen = new Set();

  for (const match of codeText.matchAll(TOP_LEVEL_UNIT_RE)) {
    const unit = {
      type: match[1].toUpperCase(),
      name: match[2],
      line: findLineNumber(codeText, match.index ?? 0),
    };
    const key = `${unit.type}:${normalizeIdentifier(unit.name)}`;
    if (!seen.has(key)) {
      seen.add(key);
      units.push(unit);
    }
  }

  for (const match of codeText.matchAll(PACKAGE_ROUTINE_RE)) {
    const unit = {
      type: match[1].toUpperCase(),
      name: match[2],
      line: findLineNumber(codeText, match.index ?? 0),
    };
    const key = `${unit.type}:${normalizeIdentifier(unit.name)}`;
    if (!seen.has(key)) {
      seen.add(key);
      units.push(unit);
    }
  }

  return units;
}

function buildLiteralCoverageResults({
  validatorName,
  taskId,
  turnNumber,
  literal,
  itemPrefix,
  codeText,
  codeFile,
  testcaseFile,
  testcaseBlocks,
  codeRuleId,
  codeMissingRuleId,
  testcaseRuleId,
  testcaseMissingRuleId,
  codeExpectation,
  testcaseExpectation,
}) {
  const results = [];
  const codeSearch = findCodeLiteralCoverage(itemPrefix, codeText, literal);
  const skipTestcaseCoverage = shouldSkipLiteralTestcaseCoverage(itemPrefix, literal.text);
  const testcaseSearch = skipTestcaseCoverage ? { skipped: true } : findLiteralOccurrenceInTestcases(testcaseBlocks, literal.text);
  const codeItem = `${itemPrefix} Code Coverage: ${literal.raw}`;
  const testcaseItem = `${itemPrefix} Test Coverage: ${literal.raw}`;

  if (!codeSearch) {
    results.push(createFail(validatorName, taskId, turnNumber, codeItem, codeMissingRuleId, {
      expected: codeExpectation,
      present: `\`${literal.raw}\` from the prompt was not found in ${codeFile}`,
      update: `emit the exact literal \`${literal.raw}\` from the program logic or exception handler`,
      sourceFile: codeFile,
      line: literal.line,
    }));
  } else {
    results.push(createPass(validatorName, taskId, turnNumber, codeItem, codeRuleId, codeFile));
  }

  if (skipTestcaseCoverage) {
    results.push(createPass(validatorName, taskId, turnNumber, testcaseItem, 'testcase_coverage_not_required', testcaseFile));
  } else if (!testcaseSearch) {
    results.push(createFail(validatorName, taskId, turnNumber, testcaseItem, testcaseMissingRuleId, {
      expected: testcaseExpectation,
      present: `\`${literal.raw}\` from the prompt was not found in any testcase execution result in ${testcaseFile}`,
      update: `add or update a testcase so its execution_result contains the exact literal \`${literal.raw}\``,
      sourceFile: testcaseFile,
      line: literal.line,
    }));
  } else {
    results.push(createPass(validatorName, taskId, turnNumber, testcaseItem, testcaseRuleId, testcaseFile));
  }

  return results;
}

/**
 * Extracts ALL DBMS_OUTPUT.PUT_LINE statements from BEGIN and EXCEPTION blocks in the code.
 * This is MANDATORY per testcase generation rules - every DBMS_OUTPUT must be covered by testcases.
 * @param {string} codeText - The PL/SQL code text to analyze
 * @returns {Array<{text: string, normalizedOutput: string, line: number, blockType: string}>}
 */
function extractAllDbmsOutputStatements(codeText) {
  const statements = [];

  // Match DBMS_OUTPUT.PUT_LINE with string literals (both single and double quotes for robustness)
  // Pattern: DBMS_OUTPUT.PUT_LINE( '...' ) or DBMS_OUTPUT.PUT_LINE( "..." )
  const dbmsOutputRegex = /DBMS_OUTPUT\s*\.\s*PUT_LINE\s*\(\s*(['"])(.*?)\1\s*\)/gi;

  let match;
  while ((match = dbmsOutputRegex.exec(codeText)) !== null) {
    const quoteChar = match[1];
    const outputContent = match[2];
    // Handle escaped quotes within the string
    const unescapedContent = quoteChar === "'"
      ? outputContent.replace(/''/g, "'")
      : outputContent.replace(/""/g, '"');

    statements.push({
      text: unescapedContent,
      normalizedOutput: normalizeLiteral(unescapedContent),
      line: findLineNumber(codeText, match.index ?? 0),
      blockType: 'unknown', // Could be enhanced to detect BEGIN vs EXCEPTION block
    });
  }

  return statements;
}

function shouldSkipLiteralTestcaseCoverage(itemPrefix, literalText) {
  if (itemPrefix !== 'Exception Message') {
    return false;
  }

  return normalizeLiteral(literalText).replace(/[.!]+$/g, '').toUpperCase() === 'UNEXPECTED ERROR OCCURRED';
}

function findCodeLiteralCoverage(itemPrefix, codeText, literal) {
  if (itemPrefix !== 'Exception Message') {
    return findLiteralOccurrence(codeText, literal.text);
  }

  if (literal.matchMode === 'string_literal_fragment') {
    return findStringLiteralFragmentOccurrence(codeText, literal.text);
  }

  return findExactExceptionEmission(codeText, literal.text);
}

function findProgramCoverageInTestcases(testcaseBlocks, programName) {
  const normalizedProgramName = normalizeIdentifier(programName);
  return testcaseBlocks.find((block) => block.instructions.toUpperCase().includes(normalizedProgramName));
}

function findLiteralOccurrence(text, literal) {
  const normalizedText = String(text ?? '').toUpperCase();
  const normalizedLiteral = String(literal ?? '').toUpperCase();
  const index = normalizedText.indexOf(normalizedLiteral);
  return index === -1 ? null : { line: findLineNumber(text, index) };
}

function findStringLiteralFragmentOccurrence(text, literal) {
  const normalizedLiteral = normalizeLiteral(literal).toUpperCase();

  for (const match of String(text ?? '').matchAll(/'(?:[^'\r\n]|'')*'/g)) {
    const content = match[0].slice(1, -1).replace(/''/g, "'");
    if (normalizeLiteral(content).toUpperCase().includes(normalizedLiteral)) {
      return {
        line: findLineNumber(text, match.index ?? 0),
      };
    }
  }

  return null;
}

function findExactExceptionEmission(text, literal) {
  const escapedLiteral = escapeRegex(String(literal ?? '').replace(/'/g, "''"));
  const patterns = [
    new RegExp(`\\bDBMS_OUTPUT\\s*\\.\\s*PUT_LINE\\s*\\(\\s*'${escapedLiteral}'\\s*\\)`, 'i'),
    new RegExp(`\\bRAISE_APPLICATION_ERROR\\s*\\(\\s*-?\\d+\\s*,\\s*'${escapedLiteral}'\\s*\\)`, 'i'),
  ];

  for (const pattern of patterns) {
    const match = pattern.exec(String(text ?? ''));
    if (match?.index !== undefined) {
      return { line: findLineNumber(text, match.index) };
    }
  }

  return null;
}

function findLiteralOccurrenceInTestcases(testcaseBlocks, literal) {
  const normalizedLiteral = normalizeLiteral(literal).toUpperCase();
  for (const block of testcaseBlocks) {
    const normalizedResult = normalizeLiteral(block.result).toUpperCase();
    const index = normalizedResult.indexOf(normalizedLiteral);
    if (index !== -1) {
      return {
        block,
        line: block.resultLine,
      };
    }
  }

  return null;
}

function normalizeIdentifier(value) {
  return String(value ?? '')
    .split('.')
    .pop()
    ?.replaceAll('"', '')
    .trim()
    .toUpperCase() ?? '';
}

function normalizeLiteral(value) {
  return String(value ?? '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\s*:\s*/g, ': ')
    .replace(/\s+\./g, '.');
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

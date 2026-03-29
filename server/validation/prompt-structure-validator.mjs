import {
  VALIDATOR_NAMES,
  createFail,
  createPass,
  loadTurnTextArtifact,
  metadataBool,
} from './common.mjs';

const RETIRED_SECTION_PATTERNS = [
  [/^\s*Requirement\s+Description\s*:/i, 'Requirement Description:'],
  [/^\s*Program\s+Type\s*:/i, 'Program Type:'],
  [/^\s*Program\s+Name\s*:/i, 'Program Name:'],
  [/^\s*Input\s+Contract\s*:/i, 'Input Contract:'],
  [/^\s*Output\s+Contract\s*:/i, 'Output Contract:'],
  [/^\s*Exception\s+Handling\s+Behavior\s*:/i, 'Exception Handling Behavior:'],
];
const REQUIREMENT_NAMING_RULES = {
  PACKAGE: ['pkg_'],
  PROCEDURE: ['sp_'],
  FUNCTION: ['sf_'],
  TRIGGER: ['trg_'],
  OBJECT: ['rec_', 't_'],
};
const PARAMETER_GROUP_RE = /^[A-Za-z][A-Za-z0-9_$#]*\s*:\s*$/;
const PARAMETER_LINE_RE = /^[A-Za-z][A-Za-z0-9_$#]*\s*-\s*(?:IN\s+OUT|INOUT|IN|OUT|LOCAL)\s*-\s*.+?\s*--\s*.+$/i;
const PROMPT_PARAMETER_CAPTURE_RE = /^([A-Za-z][A-Za-z0-9_$#]*)\s*-\s*(IN\s+OUT|INOUT|IN|OUT|LOCAL)\s*-\s*(.+?)\s*--\s*.+$/i;
const OUTPUT_GROUP_RE = /^[A-Za-z][A-Za-z0-9_$#]*\s*:\s*$/;
const EXCEPTION_LINE_RE = /^.+\s:\s+.+$/;
const HTML_TAG_RE = /<\s*\/?\s*(?:a|abbr|article|aside|b|blockquote|body|br|code|div|em|footer|form|h[1-6]|head|header|hr|html|i|img|input|label|li|link|main|meta|nav|ol|p|pre|script|section|small|span|strong|style|sub|sup|table|tbody|td|textarea|th|thead|title|tr|u|ul)\b[^>]*>/i;
const CUSTOM_EXCEPTION_RE = /\bexp_[a-zA-Z0-9_]+\b/gi;
const SELECT_INTO_RE = /\bSELECT\b[\s\S]*?\bINTO\b[\s\S]*?;/gi;
const AGGREGATE_FUNCTION_RE = /\b(COUNT|SUM|AVG|MIN|MAX|LISTAGG)\s*\(/i;
const TOP_LEVEL_NAMED_UNIT_RE = /\bCREATE\s+(?:OR\s+REPLACE\s+)?(PACKAGE(?!\s+BODY\b)|TRIGGER|TYPE(?!\s+BODY\b)|PROCEDURE|FUNCTION)\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)/gi;
const TOP_LEVEL_PROGRAM_SIGNATURE_RE = /\bCREATE\s+(?:OR\s+REPLACE\s+)?(PROCEDURE|FUNCTION)\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)\s*(\(([\s\S]*?)\))?\s*(?:RETURN\s+([\w$#.%()",\s]+?))?\s*(?:IS|AS)\b/gi;
const ROUTINE_IMPLEMENTATION_SIGNATURE_RE = /\b(PROCEDURE|FUNCTION)\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)\s*(\(([\s\S]*?)\))?\s*(?:RETURN\s+([\w$#.%()",\s]+?))?\s*(?:IS|AS)\b/gi;
const ORDER_BY_CLAUSE_RE = /\bORDER\s+BY\s+([A-Za-z0-9_.$#", ]+(?:\s+(?:ASC|DESC))?(?:\s*,\s*[A-Za-z0-9_.$#", ]+(?:\s+(?:ASC|DESC))?)*)/gi;
const SINGLE_ROW_LIMIT_RE = /\bFETCH\s+FIRST\s+1\s+ROW(?:S)?\s+ONLY\b|\bROWNUM\s*(?:<=|=|<)\s*1\b/i;

function indexSectionLines(lines) {
  const index = {};
  lines.forEach((line, lineIndex) => {
    const trimmed = line.trim();
    if (/^Requirements\s*:/i.test(trimmed)) {
      index.requirements = lineIndex;
    } else if (/^Parameters\s*:/i.test(trimmed)) {
      index.parameters = lineIndex;
    } else if (/^Output\s*:/i.test(trimmed)) {
      index.output = lineIndex;
    } else if (/^Sorting\s+Order\s*:/i.test(trimmed)) {
      index.sorting_order = lineIndex;
    } else if (/^Exception\s+Handling\s*:/i.test(trimmed)) {
      index.exception_handling = lineIndex;
    }
  });
  return index;
}

function sectionBody(lines, index, sectionKey) {
  const start = index[sectionKey];
  if (start === undefined) {
    return [];
  }

  const stop = Math.min(...Object.values(index).filter((lineIndex) => lineIndex > start), lines.length);
  return lines.slice(start + 1, stop).map((line) => line.replace(/\r$/, '')).filter((line) => line.trim());
}

function parseRequirementEntries(lines, index) {
  const start = index.requirements;
  if (start === undefined) {
    return { entries: [], error: null };
  }

  const stop = Math.min(...Object.values(index).filter((lineIndex) => lineIndex > start), lines.length);
  const body = lines
    .slice(start + 1, stop)
    .map((line) => line.replace(/\r$/, ''))
    .filter((line) => line.trim());
  const entries = [];

  for (let cursor = 0; cursor < body.length; cursor += 1) {
    const line = body[cursor].trim();

    const isHeader =
      /^Procedure Name:$/i.test(line) ||
      /^Function Name:$/i.test(line) ||
      /^Package Name:$/i.test(line) ||
      /^Trigger Name:$/i.test(line) ||
      /^Object Name:$/i.test(line) ||
      /^Anonymous Block:$/i.test(line);

    if (!isHeader) {
      return { entries, error: { message: `unexpected requirement line \`${line}\``, line: start + cursor + 2 } };
    }

    if (/^Anonymous Block:$/i.test(line)) {
      entries.push(['Anonymous Block:', 'ANONYMOUS BLOCK']);
      continue;
    }

    let foundValue = null;
    while (cursor + 1 < body.length) {
      const value = body[cursor + 1].trim();
      if (/:$/.test(value)) {
        break;
      }
      foundValue = value;
      cursor += 1;
      break;
    }

    if (!foundValue) {
      return { entries, error: { message: `missing requirement value after \`${line}\``, line: start + cursor + 2 } };
    }

    entries.push([line.replace(/\s+/g, ' ').trim(), foundValue.toUpperCase()]);
  }

  return { entries, error: null };
}

function extractRequirementProgramHeaders(lines, index) {
  const requirementsStart = index.requirements;
  if (requirementsStart === undefined) {
    return null;
  }

  const parametersStart = index.parameters;
  const stop = parametersStart !== undefined && parametersStart > requirementsStart
    ? parametersStart
    : Math.min(...Object.values(index).filter((lineIndex) => lineIndex > requirementsStart), lines.length);

  const headers = [];
  let sawNonEmptyLine = false;

  for (let lineIndex = requirementsStart + 1; lineIndex < stop; lineIndex += 1) {
    const rawLine = lines[lineIndex].replace(/\r$/, '');
    const trimmed = rawLine.trim();
    if (!trimmed) {
      continue;
    }
    sawNonEmptyLine = true;
    if (/^\t/.test(rawLine)) {
      continue;
    }
    if (trimmed.endsWith(':')) {
      headers.push(trimmed);
    }
  }

  if (sawNonEmptyLine && !headers.length) {
    return null;
  }

  return headers;
}

function countRequirementPrograms(lines, index) {
  const headers = extractRequirementProgramHeaders(lines, index);
  return headers === null ? null : headers.length;
}

function structuralSectionResult(taskId, turnNumber, lines, index, item, sectionKey, sourceName, optional = false) {
  const body = sectionBody(lines, index, sectionKey);
  const headerMap = {
    requirements: 'Requirements:',
    parameters: 'Parameters:',
    output: 'Output:',
    sorting_order: 'Sorting Order:',
    exception_handling: 'Exception Handling:',
  };
  const header = headerMap[sectionKey];

  if (index[sectionKey] === undefined) {
    if (optional) {
      return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'optional_absent', sourceName);
    }
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'missing_section', {
      expected: `section header \`${header}\` must be present`,
      present: `section not found in ${sourceName}`,
      update: `add \`${header}\` and describe the required ${item.toLowerCase()} details clearly`,
      sourceFile: sourceName,
    });
  }

  if (!body.length) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'missing_content', {
      expected: `\`${header}\` must contain deterministic content`,
      present: `section header found in ${sourceName}, but no content follows it`,
      update: `populate \`${header}\` with specific, deterministic ${item.toLowerCase()} details`,
      sourceFile: sourceName,
      line: index[sectionKey] + 1,
    });
  }

  return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'section_present', sourceName);
}

function validateSectionOrder(taskId, turnNumber, index, sourceName) {
  const required = ['requirements', 'parameters', 'output', 'exception_handling'];
  const missing = required.filter((key) => index[key] === undefined).map((key) => key.replace('_', ' '));
  if (missing.length) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Section Order', 'missing_sections_for_order_check', {
      expected: 'all mandatory sections must be present before order can be validated',
      present: `missing ${missing.join(', ')} in ${sourceName}`,
      update: 'add the missing mandatory sections using the canonical structure',
      sourceFile: sourceName,
    });
  }

  const ordered = [
    ['Requirements', index.requirements],
    ['Parameters', index.parameters],
    ['Output', index.output],
  ];
  if (index.sorting_order !== undefined) {
    ordered.push(['Sorting Order', index.sorting_order]);
  }
  ordered.push(['Exception Handling', index.exception_handling]);

  for (let i = 1; i < ordered.length; i += 1) {
    if (ordered[i][1] <= ordered[i - 1][1]) {
      return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Section Order', 'invalid_section_order', {
        expected: 'sections must appear in the canonical order from prompt/base.md',
        present: `\`${ordered[i][0]}\` appears before \`${ordered[i - 1][0]}\` in ${sourceName}`,
        update: 'reorder the prompt to: Requirements, Parameters, Output, Sorting Order, Exception Handling',
        sourceFile: sourceName,
        line: ordered[i][1] + 1,
      });
    }
  }

  return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Section Order', 'canonical_order', sourceName);
}

function validateParametersShape(taskId, turnNumber, lines, index, sourceName) {
  const body = sectionBody(lines, index, 'parameters');
  if (!body.length) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Parameters Format', 'missing_parameter_contract', {
      expected: '`Parameters:` must contain parameter details',
      present: `\`Parameters:\` is present without parameter content in ${sourceName}`,
      update: 'add parameter lines (with optional program header when multiple programs)',
      sourceFile: sourceName,
      line: (index.parameters ?? 0) + 1,
    });
  }

  const hasGroup = body.some((line) => PARAMETER_GROUP_RE.test(line.trim()));
  const programCount = countRequirementPrograms(lines, index);

  if (hasGroup) {
    let seenGroup = false;
    for (const line of body) {
      const trimmed = line.trim();
      if (PARAMETER_GROUP_RE.test(trimmed)) {
        seenGroup = true;
        continue;
      }
      if (!seenGroup || !PARAMETER_LINE_RE.test(trimmed)) {
        return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Parameters Format', 'invalid_parameter_line', {
          expected: '`Parameters:` must use `<Program Name>:` group headers followed by `<parameter> - <mode> - <datatype> -- <comment>` lines',
          present: `\`${trimmed}\` found in ${sourceName}`,
          update: 'rewrite the section using program headers and parameter lines',
          sourceFile: sourceName,
        });
      }
    }
  } else {
    if (programCount !== 1) {
      return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Parameters Format', 'missing_parameter_group_headers', {
        expected: '`Parameters:` must group entries by program when multiple programs are present',
        present: 'no `<Program Name>:` header found despite multiple programs',
        update: 'add one program header before its parameter lines, or keep a single-program flat list',
        sourceFile: sourceName,
      });
    }
    for (const line of body) {
      if (!PARAMETER_LINE_RE.test(line.trim())) {
        return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Parameters Format', 'invalid_parameter_line', {
          expected: 'single-program `Parameters:` may omit headers but must use `<parameter> - <mode> - <datatype> -- <comment>` lines',
          present: `\`${line.trim()}\` found in ${sourceName}`,
          update: 'rewrite parameter lines in the required format',
          sourceFile: sourceName,
        });
      }
    }
  }

  return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Parameters Format', 'parameter_groups_valid', sourceName);
}

function validateOutputShape(taskId, turnNumber, lines, index, sourceName) {
  const body = sectionBody(lines, index, 'output');
  if (!body.length) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Output Format', 'missing_output_contract', {
      expected: '`Output:` must contain at least one output entry',
      present: `\`Output:\` is empty in ${sourceName}`,
      update: 'add output details (with optional program header when multiple programs)',
      sourceFile: sourceName,
      line: (index.output ?? 0) + 1,
    });
  }

  const hasGroup = body.some((line) => OUTPUT_GROUP_RE.test(line.trim()));
  if (!hasGroup && countRequirementPrograms(lines, index) !== 1) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Output Format', 'missing_output_groups', {
      expected: '`Output:` must group output details under program-name headers when multiple programs exist',
      present: `no \`<Program Name>:\` header found in \`Output:\` of ${sourceName}`,
      update: 'add one program header before its output lines, or keep a flat list for a single program',
      sourceFile: sourceName,
    });
  }

  return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Output Format', 'output_groups_valid', sourceName);
}

function isProgramSubheader(line) {
  const trimmed = line.trim();
  return Boolean(trimmed) && trimmed.endsWith(':') && !trimmed.includes(' : ');
}

function extractRequirementProgramNames(lines, index) {
  const { entries } = parseRequirementEntries(lines, index);
  return [...new Set(entries.map(([label, name]) => (
    /^Anonymous Block:$/i.test(label) ? 'ANONYMOUS BLOCK' : normalizeIdentifier(name)
  )).filter(Boolean))];
}

function isSortingProgramHeader(line, programNames) {
  if (!isProgramSubheader(line)) {
    return false;
  }

  const header = line.trim().replace(/:\s*$/, '');
  if (/^Anonymous Block$/i.test(header)) {
    return programNames.includes('ANONYMOUS BLOCK');
  }

  return programNames.includes(normalizeIdentifier(header));
}

function createPromptSortingGroup() {
  return {
    lines: [],
    queryGroups: [],
    ungroupedLines: [],
  };
}

function parsePromptSortingGroups(lines, index, additionalProgramNames = []) {
  const body = sectionBody(lines, index, 'sorting_order');
  const programNames = [...new Set([
    ...extractRequirementProgramNames(lines, index),
    ...additionalProgramNames.map((name) => normalizeIdentifier(name)).filter(Boolean),
  ])];
  const groups = new Map();
  let currentProgram = programNames.length === 1 ? programNames[0] : null;
  let currentQuery = null;
  let explicitProgramHeaders = 0;

  const ensureGroup = (programName) => {
    if (!groups.has(programName)) {
      groups.set(programName, createPromptSortingGroup());
    }
    return groups.get(programName);
  };

  if (currentProgram) {
    ensureGroup(currentProgram);
  }

  for (const line of body) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }

    if (isSortingProgramHeader(trimmed, programNames)) {
      currentProgram = /^Anonymous Block:$/i.test(trimmed)
        ? 'ANONYMOUS BLOCK'
        : normalizeIdentifier(trimmed.replace(/:\s*$/, ''));
      ensureGroup(currentProgram);
      currentQuery = null;
      explicitProgramHeaders += 1;
      continue;
    }

    if (isProgramSubheader(trimmed)) {
      if (!currentProgram && programNames.length === 1) {
        currentProgram = programNames[0];
        ensureGroup(currentProgram);
      }

      if (currentProgram) {
        currentQuery = {
          label: trimmed.replace(/:\s*$/, ''),
          lines: [],
        };
        ensureGroup(currentProgram).queryGroups.push(currentQuery);
        continue;
      }
    }

    if (!currentProgram && programNames.length === 1) {
      currentProgram = programNames[0];
      ensureGroup(currentProgram);
    }

    if (!currentProgram) {
      continue;
    }

    const group = ensureGroup(currentProgram);
    group.lines.push(trimmed);
    if (currentQuery) {
      currentQuery.lines.push(trimmed);
    } else {
      group.ungroupedLines.push(trimmed);
    }
  }

  return {
    programNames,
    groups,
    explicitProgramHeaders,
  };
}

function validateSortingOrderShape(taskId, turnNumber, lines, index, sourceName) {
  if (index.sorting_order === undefined) {
    return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Format', 'sorting_optional_absent', sourceName);
  }

  const body = sectionBody(lines, index, 'sorting_order');
  if (!body.length) {
    return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Format', 'sorting_content_checked_elsewhere', sourceName);
  }

  const promptSorting = parsePromptSortingGroups(lines, index);
  if (promptSorting.explicitProgramHeaders === 0 && countRequirementPrograms(lines, index) !== 1) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Format', 'missing_sorting_groups', {
      expected: '`Sorting Order:` must group sorting details under program-name headers when multiple programs exist',
      present: `no \`<Program Name>:\` header found in \`Sorting Order:\` of ${sourceName}`,
      update: 'add one program header before its sorting lines, or keep a flat list for a single program',
      sourceFile: sourceName,
    });
  }

  return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Format', 'sorting_groups_valid', sourceName);
}

function validateExceptionHandlingShape(taskId, turnNumber, lines, index, sourceName) {
  const body = sectionBody(lines, index, 'exception_handling');
  if (!body.length) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Exception Handling Format', 'missing_exception_contract', {
      expected: '`Exception Handling:` must list at least one scenario-message pair',
      present: `\`Exception Handling:\` is empty in ${sourceName}`,
      update: 'add indented exception mappings such as `Other Exception : Unexpected error occurred`',
      sourceFile: sourceName,
    });
  }

  const hasGroup = body.some((line) => isProgramSubheader(line));
  if (!hasGroup && countRequirementPrograms(lines, index) !== 1) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Exception Handling Format', 'missing_exception_groups', {
      expected: '`Exception Handling:` must group scenario-message details under program-name headers when multiple programs exist',
      present: `no \`<Program Name>:\` header found in \`Exception Handling:\` of ${sourceName}`,
      update: 'add one program header before its exception lines, or keep a flat list for a single program',
      sourceFile: sourceName,
    });
  }

  for (const line of body) {
    if (isProgramSubheader(line)) {
      continue;
    }
    if (!EXCEPTION_LINE_RE.test(line.trim())) {
      return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Exception Handling Format', 'invalid_exception_line', {
        expected: 'each exception entry must use `<Scenario> : <message>` format',
        present: `\`${line.trim()}\` found in ${sourceName}`,
        update: 'rewrite each exception line as `<Scenario> : <message>`',
        sourceFile: sourceName,
      });
    }
  }

  if (!body.join('\n').toUpperCase().includes('OTHER EXCEPTION : UNEXPECTED ERROR OCCURRED')) {
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Exception Handling Format', 'missing_other_exception_fallback', {
      expected: '`Exception Handling:` must include `Other Exception : Unexpected error occurred`',
      present: `generic fallback line not found in ${sourceName}`,
      update: 'add `Other Exception : Unexpected error occurred` under `Exception Handling:`',
      sourceFile: sourceName,
    });
  }

  return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Exception Handling Format', 'exception_lines_valid', sourceName);
}

function extractTurnProgramUnits(codeText) {
  const units = [];
  for (const match of codeText.matchAll(TOP_LEVEL_NAMED_UNIT_RE)) {
    const type = match[1].replace(/\s+/g, ' ').toUpperCase();
    const objectName = match[2].split('.').pop()?.replaceAll('"', '').toUpperCase() ?? '';
    units.push([type, objectName]);
  }
  return units;
}

function extractSortingClauses(codeText) {
  return [...codeText.matchAll(ORDER_BY_CLAUSE_RE)]
    .map((match) => match[1].trim().replace(/\s+/g, ' ').toUpperCase())
    .filter(Boolean);
}

function extractSortingExpectations(codeText) {
  const expectations = [];
  const routineMatches = [...codeText.matchAll(ROUTINE_IMPLEMENTATION_SIGNATURE_RE)].map((match) => ({
    programName: normalizeIdentifier(match[2]),
    index: match.index ?? 0,
  }));

  if (!routineMatches.length) {
    return extractSortingClauses(codeText).map((clause, clauseIndex) => ({
      programName: 'ANONYMOUS BLOCK',
      queryIndex: clauseIndex + 1,
      clause,
    }));
  }

  for (let index = 0; index < routineMatches.length; index += 1) {
    const current = routineMatches[index];
    const next = routineMatches[index + 1];
    const segment = codeText.slice(current.index, next?.index ?? codeText.length);
    const clauses = extractSortingClauses(segment);
    clauses.forEach((clause, clauseIndex) => {
      expectations.push({
        programName: current.programName,
        queryIndex: clauseIndex + 1,
        clause,
      });
    });
  }

  return expectations;
}

function groupSortingExpectations(expectations) {
  const grouped = new Map();
  for (const expectation of expectations) {
    const existing = grouped.get(expectation.programName) ?? [];
    existing.push(expectation);
    grouped.set(expectation.programName, existing);
  }
  return grouped;
}

function formatSortingExpectation(expectation) {
  return `${expectation.programName} -> Query ${expectation.queryIndex} -> ORDER BY ${expectation.clause}`;
}

function extractCustomExceptions(promptText) {
  return [...new Set([...promptText.matchAll(CUSTOM_EXCEPTION_RE)].map((match) => match[0].toUpperCase()))];
}

function promptRequiresHandler(promptText, handlerName) {
  const upper = promptText.toUpperCase();
  if (handlerName === 'WHEN OTHERS') {
    return upper.includes('WHEN OTHERS');
  }
  const canonical = handlerName.toUpperCase();
  const variants = new Set([canonical, canonical.replaceAll('_', ' ')]);
  return [...variants].some((variant) => upper.includes(variant));
}

function codeHasHandler(codeText, handlerName) {
  if (handlerName === 'WHEN OTHERS') {
    return /\bWHEN\s+OTHERS\b/i.test(codeText);
  }
  return new RegExp(`\\bWHEN\\s+${handlerName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i').test(codeText);
}

function codeDeclaresCustomException(codeText, exceptionName) {
  return new RegExp(`\\b${exceptionName}\\s+EXCEPTION\\s*;`, 'i').test(codeText);
}

function codeRaisesException(codeText, exceptionName) {
  return new RegExp(`\\bRAISE\\s+${exceptionName}\\b`, 'i').test(codeText);
}

function hasPlausibleNoDataFoundSource(codeText) {
  if (/\bRAISE\s+NO_DATA_FOUND\b/i.test(codeText)) {
    return true;
  }
  return [...codeText.matchAll(SELECT_INTO_RE)].some((match) => !AGGREGATE_FUNCTION_RE.test(match[0]));
}

function hasPlausibleTooManyRowsSource(codeText) {
  if (/\bRAISE\s+TOO_MANY_ROWS\b/i.test(codeText)) {
    return true;
  }

  return [...codeText.matchAll(SELECT_INTO_RE)].some((match) =>
    !AGGREGATE_FUNCTION_RE.test(match[0]) && !SINGLE_ROW_LIMIT_RE.test(match[0]),
  );
}

function promptMarkupFailures(promptText) {
  const failures = [];
  if (HTML_TAG_RE.test(promptText)) {
    failures.push('HTML tags found');
  }
  if (/```/.test(promptText)) {
    failures.push('markdown code fences found');
  }
  if (/\*\*|__/.test(promptText)) {
    failures.push('markdown emphasis found');
  }
  return failures;
}

function requirementTypeFromLabel(label) {
  const normalized = label.toUpperCase();
  if (normalized.includes('PACKAGE')) return 'PACKAGE';
  if (normalized.includes('PROCEDURE')) return 'PROCEDURE';
  if (normalized.includes('FUNCTION')) return 'FUNCTION';
  if (normalized.includes('TRIGGER')) return 'TRIGGER';
  if (normalized.includes('OBJECT')) return 'OBJECT';
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

function normalizeMode(value) {
  const normalized = String(value ?? '').trim().replace(/\s+/g, ' ').toUpperCase();
  if (normalized === 'INOUT') {
    return 'IN OUT';
  }
  return normalized || 'IN';
}

function normalizeDatatype(value) {
  return String(value ?? '').trim().replace(/\s+/g, ' ').toUpperCase();
}

function datatypeFamily(value) {
  const normalized = normalizeDatatype(value);
  if (!normalized) {
    return '';
  }
  if (/%ROWTYPE\b/.test(normalized)) {
    return '%ROWTYPE';
  }
  if (/%TYPE\b/.test(normalized)) {
    return '%TYPE';
  }
  return normalized.replace(/\(.*\)$/, '').trim();
}

function datatypesCompatible(promptDatatype, codeDatatype) {
  const normalizedPrompt = normalizeDatatype(promptDatatype);
  const normalizedCode = normalizeDatatype(codeDatatype);
  if (!normalizedPrompt || !normalizedCode) {
    return true;
  }
  if (normalizedPrompt === normalizedCode) {
    return true;
  }

  const promptFamily = datatypeFamily(normalizedPrompt);
  const codeFamily = datatypeFamily(normalizedCode);
  if (!promptFamily || !codeFamily) {
    return true;
  }
  if (['%TYPE', '%ROWTYPE'].includes(promptFamily) || ['%TYPE', '%ROWTYPE'].includes(codeFamily)) {
    return true;
  }
  return promptFamily === codeFamily;
}

function splitTopLevelCommaList(text) {
  const items = [];
  let depth = 0;
  let current = '';

  for (const char of String(text ?? '')) {
    if (char === '(') {
      depth += 1;
    } else if (char === ')' && depth > 0) {
      depth -= 1;
    }

    if (char === ',' && depth === 0) {
      if (current.trim()) {
        items.push(current.trim());
      }
      current = '';
      continue;
    }

    current += char;
  }

  if (current.trim()) {
    items.push(current.trim());
  }

  return items;
}

function parseCodeParameterList(parameterBlob) {
  const parameters = [];

  for (const entry of splitTopLevelCommaList(parameterBlob)) {
    const withoutDefault = entry.split(/\s+(?:DEFAULT|:=)\s+/i)[0]?.trim() ?? '';
    const match = withoutDefault.match(/^("?[\w$#]+"?)\s+([\s\S]+)$/i);
    if (!match) {
      continue;
    }

    let remainder = match[2].trim();
    let mode = 'IN';
    if (/^IN\s+OUT\b/i.test(remainder)) {
      mode = 'IN OUT';
      remainder = remainder.replace(/^IN\s+OUT\b/i, '').trim();
    } else if (/^INOUT\b/i.test(remainder)) {
      mode = 'IN OUT';
      remainder = remainder.replace(/^INOUT\b/i, '').trim();
    } else if (/^OUT\b/i.test(remainder)) {
      mode = 'OUT';
      remainder = remainder.replace(/^OUT\b/i, '').trim();
    } else if (/^IN\b/i.test(remainder)) {
      mode = 'IN';
      remainder = remainder.replace(/^IN\b/i, '').trim();
    }

    if (!remainder) {
      continue;
    }

    parameters.push({
      name: normalizeIdentifier(match[1]),
      mode,
      datatype: normalizeDatatype(remainder),
    });
  }

  return parameters;
}

function extractCodeProgramSignatures(codeText) {
  const signatures = new Map();

  for (const match of codeText.matchAll(TOP_LEVEL_PROGRAM_SIGNATURE_RE)) {
    const objectType = match[1].toUpperCase();
    const objectName = normalizeIdentifier(match[2]);
    const parameterBlob = match[4] ?? '';
    const returnType = objectType === 'FUNCTION' ? normalizeDatatype(match[5]) : null;
    signatures.set(objectName, {
      objectType,
      parameters: parseCodeParameterList(parameterBlob),
      returnType,
    });
  }

  return signatures;
}

function parsePromptParameterGroups(lines, index) {
  const body = sectionBody(lines, index, 'parameters');
  if (!body.length) {
    return new Map();
  }

  const { entries } = parseRequirementEntries(lines, index);
  const namedPrograms = entries
    .map(([label, name]) => ({ label, name }))
    .filter((entry) => requirementTypeFromLabel(entry.label) === 'PROCEDURE' || requirementTypeFromLabel(entry.label) === 'FUNCTION')
    .map((entry) => normalizeIdentifier(entry.name));

  const grouped = body.some((line) => PARAMETER_GROUP_RE.test(line.trim()));
  const groups = new Map();
  let currentProgram = grouped ? null : (namedPrograms.length === 1 ? namedPrograms[0] : null);
  if (currentProgram) {
    groups.set(currentProgram, []);
  }

  for (const line of body) {
    const trimmed = line.trim();
    if (PARAMETER_GROUP_RE.test(trimmed)) {
      currentProgram = normalizeIdentifier(trimmed.replace(/:\s*$/, ''));
      if (!groups.has(currentProgram)) {
        groups.set(currentProgram, []);
      }
      continue;
    }

    const match = trimmed.match(PROMPT_PARAMETER_CAPTURE_RE);
    if (!match || !currentProgram) {
      continue;
    }

    const mode = normalizeMode(match[2]);
    if (mode === 'LOCAL') {
      continue;
    }

    groups.get(currentProgram)?.push({
      name: normalizeIdentifier(match[1]),
      mode,
      datatype: normalizeDatatype(match[3]),
    });
  }

  return groups;
}

function validateRequirementEntries(taskId, turnNumber, lines, index, sourceName, codeText, codeSource) {
  const { entries, error } = parseRequirementEntries(lines, index);
  const results = [];

  if (error) {
    return [createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Requirements Entries', 'invalid_requirement_entries', {
      expected: '`Requirements:` must contain only header-value entries such as `Procedure Name:` followed by the exact implemented name on the next line',
      present: `${error.message} in ${sourceName}`,
      update: 'rewrite `Requirements:` using one entry header per program/object and place the exact name on the next line',
      sourceFile: sourceName,
      line: error.line,
    })];
  }

  results.push(createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Requirements Entries', 'entry_shape_valid', sourceName));

  const newProgramUnits = extractTurnProgramUnits(codeText);
  const requirementsUpper = entries.map(([label, name]) => `${label}\n${name}`).join('\n').toUpperCase();
  const missingUnits = [];
  for (const [kind, name] of newProgramUnits) {
    const expectedLabel =
      kind.startsWith('PROCEDURE') ? 'PROCEDURE NAME:' :
      kind.startsWith('FUNCTION') ? 'FUNCTION NAME:' :
      kind.startsWith('PACKAGE') ? 'PACKAGE NAME:' :
      kind.startsWith('TRIGGER') ? 'TRIGGER NAME:' :
      kind.startsWith('TYPE') ? 'OBJECT NAME:' :
      null;
    if (expectedLabel && (!requirementsUpper.includes(expectedLabel) || !requirementsUpper.includes(name))) {
      missingUnits.push(`${kind} ${name}`);
    }
  }

  if (missingUnits.length) {
    results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Requirements Entries', 'missing_named_units', {
      expected: 'every new program/object in the reference SQL must be declared explicitly in `Requirements:`',
      present: `missing requirement entry or name value for: ${missingUnits.join(', ')}`,
      update: 'add one header-value pair under `Requirements:` for every new package/procedure/function/trigger/object/type name',
      sourceFile: sourceName,
    }));
  } else {
    results.push(createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Requirements Entries', 'all_named_units_declared', sourceName));
  }

  for (const [label, name] of entries) {
    const requirementType = requirementTypeFromLabel(label);
    if (!requirementType || !REQUIREMENT_NAMING_RULES[requirementType]) {
      continue;
    }
    const prefixes = REQUIREMENT_NAMING_RULES[requirementType];
    const valid = prefixes.some((prefix) => name.toLowerCase().startsWith(prefix));
    const item = `Requirement Naming: ${label}`;
    if (valid) {
      results.push(createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'naming_convention_valid', sourceName));
    } else {
      results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'invalid_naming_convention', {
        expected: `${requirementType} names must start with '${prefixes.join("' or '")}'`,
        present: `'${name}' does not start with '${prefixes.join("' or '")}' in ${sourceName}`,
        update: `rename to start with '${prefixes[0]}'`,
        sourceFile: codeSource || sourceName,
      }));
    }
  }

  return results;
}

function validateParameterContract(taskId, turnNumber, lines, index, codeText, codeSource) {
  const promptGroups = parsePromptParameterGroups(lines, index);
  if (!promptGroups.size) {
    return [];
  }

  const codeSignatures = extractCodeProgramSignatures(codeText);
  const results = [];

  for (const [programName, promptParameters] of promptGroups.entries()) {
    const signature = codeSignatures.get(programName);
    if (!signature || !promptParameters.length) {
      continue;
    }

    const codeParameters = new Map(signature.parameters.map((parameter) => [parameter.name, parameter]));
    for (const promptParameter of promptParameters) {
      const item = `Parameter Contract: ${programName}.${promptParameter.name}`;
      const codeParameter = codeParameters.get(promptParameter.name);
      if (!codeParameter) {
        results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'missing_parameter_in_code', {
          expected: `parameter ${promptParameter.name} must exist in the implemented ${signature.objectType.toLowerCase()} signature`,
          present: `${promptParameter.name} was defined in the prompt but not found in ${programName} within ${codeSource}`,
          update: `add parameter ${promptParameter.name} to ${programName} or align the prompt contract`,
          sourceFile: codeSource,
        }));
        continue;
      }

      if (codeParameter.mode !== promptParameter.mode) {
        results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'parameter_mode_mismatch', {
          expected: `parameter mode must match the prompt contract (${promptParameter.mode})`,
          present: `${programName}.${promptParameter.name} uses mode ${codeParameter.mode} in ${codeSource}`,
          update: `change ${promptParameter.name} to mode ${promptParameter.mode} or update the prompt contract`,
          sourceFile: codeSource,
        }));
        continue;
      }

      if (!datatypesCompatible(promptParameter.datatype, codeParameter.datatype)) {
        results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'parameter_datatype_mismatch', {
          expected: `parameter datatype must match the prompt contract (${promptParameter.datatype})`,
          present: `${programName}.${promptParameter.name} uses datatype ${codeParameter.datatype} in ${codeSource}`,
          update: `change ${promptParameter.name} to datatype ${promptParameter.datatype} or update the prompt contract`,
          sourceFile: codeSource,
        }));
        continue;
      }

      results.push(createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'parameter_contract_satisfied', codeSource));
    }
  }

  return results;
}

function validateSortingOrderContract(taskId, turnNumber, lines, index, codeText, sourceName) {
  const sortingExpectations = extractSortingExpectations(codeText);
  const promptSorting = parsePromptSortingGroups(
    lines,
    index,
    sortingExpectations.map((expectation) => expectation.programName),
  );

  if (sortingExpectations.length) {
    const groupedExpectations = groupSortingExpectations(sortingExpectations);
    const failures = [];

    for (const [programName, expectations] of groupedExpectations.entries()) {
      const promptGroup = promptSorting.groups.get(programName);
      const groupText = promptGroup?.lines.join('\n').toUpperCase() ?? '';

      if (expectations.length > 1) {
        const queryGroupCount = promptGroup?.queryGroups.length ?? 0;
        if (queryGroupCount < expectations.length) {
          failures.push(
            `${programName} has ${expectations.length} ordered ${expectations.length === 1 ? 'query' : 'queries'} in SQL, `
            + `but \`Sorting Order:\` has ${queryGroupCount} query/scenario header(s). `
            + `Expected: ${expectations.map(formatSortingExpectation).join('; ')}`,
          );
        }

        for (let queryIndex = 0; queryIndex < Math.min(queryGroupCount, expectations.length); queryIndex += 1) {
          const queryLines = promptGroup?.queryGroups[queryIndex]?.lines.join('\n').toUpperCase() ?? '';
          const expectation = expectations[queryIndex];
          if (!queryLines.includes(expectation.clause)) {
            failures.push(`missing sorting clause for ${formatSortingExpectation(expectation)}`);
          }
        }

        if (queryGroupCount === 0 && promptGroup && expectations.some((expectation) => !groupText.includes(expectation.clause))) {
          for (const expectation of expectations) {
            if (!groupText.includes(expectation.clause)) {
              failures.push(`missing sorting clause for ${formatSortingExpectation(expectation)}`);
            }
          }
        }

        continue;
      }

      const [expectation] = expectations;
      if (!groupText.includes(expectation.clause)) {
        failures.push(`missing sorting clause for ${formatSortingExpectation(expectation)}`);
      }
    }

    if (!failures.length) {
      return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Contract', 'sorting_clause_present', sourceName);
    }

    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Contract', 'missing_sorting_clause', {
      expected: '`Sorting Order:` must list each implemented `ORDER BY` expression from the reference SQL, and when a program has multiple ordered queries it must use query/scenario headers in SQL order',
      present: failures.join(' | '),
      update: 'group sorting details by program, then by query/scenario (for example `Query 1:` / `Query 2:`), and copy the exact ORDER BY columns and directions under each entry',
      sourceFile: sourceName,
    });
  }

  const promptUpper = lines.join('\n').toUpperCase();
  if (!promptUpper.includes('SORTING ORDER:') || promptUpper.includes('NO SORTING REQUIRED')) {
    return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Contract', 'no_sorting_marker_present', sourceName);
  }

  return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Contract', 'unexpected_sorting_content_without_order_by', {
    expected: 'when SQL has no ORDER BY, omit `Sorting Order:` or state `No sorting required.`',
    present: `\`Sorting Order:\` is present with concrete sorting content in ${sourceName}, but the SQL has no ORDER BY`,
    update: 'remove the `Sorting Order:` section or replace its content with `No sorting required.`',
    sourceFile: sourceName,
  });
}

function validatePromptCodeContract(taskId, turnNumber, promptText, codeText, codeSource) {
  const results = [];
  for (const exceptionName of extractCustomExceptions(promptText)) {
    const item = `Custom Exception ${exceptionName}`;
    const declared = codeDeclaresCustomException(codeText, exceptionName);
    const raised = codeRaisesException(codeText, exceptionName);
    const handled = codeHasHandler(codeText, exceptionName);
    if (!new RegExp(`\\b${exceptionName}\\b`, 'i').test(codeText)) {
      results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'missing_exception_reference', {
        expected: `prompt-required custom exception \`${exceptionName}\` must be referenced in PL/SQL`,
        present: `\`${exceptionName}\` is not referenced in ${codeSource}`,
        update: `declare, raise, and handle \`${exceptionName}\` in the PL/SQL program`,
        sourceFile: codeSource,
      }));
      continue;
    }
    if (!declared) {
      results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'missing_exception_declaration', {
        expected: `\`${exceptionName} EXCEPTION;\` must be declared`,
        present: `\`${exceptionName}\` is referenced but not declared in ${codeSource}`,
        update: `declare \`${exceptionName} EXCEPTION;\` in the declarative section`,
        sourceFile: codeSource,
      }));
    }
    if (!raised) {
      results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'missing_exception_raise', {
        expected: `\`${exceptionName}\` must be raised at the required failure point`,
        present: `no \`RAISE ${exceptionName}\` found in ${codeSource}`,
        update: `raise \`${exceptionName}\` in the branch that matches the prompt-defined failure case`,
        sourceFile: codeSource,
      }));
    }
    if (!handled) {
      results.push(createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'missing_exception_handler', {
        expected: `\`WHEN ${exceptionName}\` handler must be present`,
        present: `no handler for \`${exceptionName}\` found in ${codeSource}`,
        update: `add \`WHEN ${exceptionName} THEN ...\` to the exception block`,
        sourceFile: codeSource,
      }));
    }
    if (declared && raised && handled) {
      results.push(createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, item, 'contract_satisfied', codeSource));
    }
  }

  return results;
}

export async function runPromptStructureValidator(taskId, taskDir, metadata) {
  const validatorName = VALIDATOR_NAMES.promptStructure;
  const numTurns = Number.parseInt(String(metadata?.num_turns ?? 0), 10);
  if (!Number.isInteger(numTurns) || numTurns <= 0) {
    return [createFail(validatorName, taskId, null, 'Metadata', 'invalid_num_turns', {
      expected: 'metadata must contain a positive integer `num_turns`',
      present: `found \`num_turns=${metadata?.num_turns ?? null}\``,
      update: 'set `num_turns` to the actual number of turns for the task',
    })];
  }

  const results = [];
  const anonymousBlockPromptTurns = [];

  for (let turnNumber = 1; turnNumber <= numTurns; turnNumber += 1) {
    const promptArtifact = await loadTurnTextArtifact(taskDir, 'turn_user_file', taskId, turnNumber);
    if (!promptArtifact.text) {
      results.push(createFail(validatorName, taskId, turnNumber, 'Prompt Input Artifact', 'missing_artifact', {
        expected: `prompt input file for turn ${turnNumber} must exist`,
        present: `no prompt file found for turn ${turnNumber} in ${taskDir}`,
        update: `create \`${taskId}_turn${turnNumber}_1user.txt\` or the configured per-turn prompt file`,
        sourceFile: promptArtifact.fileName,
      }));
      continue;
    }

    const promptText = promptArtifact.text;
    const lines = promptText.split(/\r?\n/);
    const index = indexSectionLines(lines);
    if (promptText.toUpperCase().includes('ANONYMOUS BLOCK:')) {
      anonymousBlockPromptTurns.push({ turnNumber, sourceName: promptArtifact.fileName });
    }

    const firstSection = Object.values(index).length ? Math.min(...Object.values(index)) : lines.length;
    const preambleLines = lines.slice(0, firstSection).map((line) => line.trim()).filter(Boolean);
    results.push(
      preambleLines.length > 3
        ? createFail(validatorName, taskId, turnNumber, 'Prompt Preamble', 'unexpected_preamble', {
            expected: 'at most 3 lines of business summary may appear immediately before `Requirements:`',
            present: `found ${preambleLines.length} non-empty line(s) before \`Requirements:\` in ${promptArtifact.fileName}`,
            update: 'keep the optional business summary to 3 non-empty lines or fewer before `Requirements:`',
            sourceFile: promptArtifact.fileName,
          })
        : createPass(validatorName, taskId, turnNumber, 'Prompt Preamble', 'preamble_allowed', promptArtifact.fileName),
    );

    const retiredViolations = RETIRED_SECTION_PATTERNS
      .flatMap(([pattern, label]) =>
        lines.map((line, lineIndex) => ({ line, lineIndex, label, pattern })).filter((entry) => entry.pattern.test(entry.line)),
      );
    if (retiredViolations.length) {
      for (const violation of retiredViolations) {
        results.push(createFail(validatorName, taskId, turnNumber, 'Prompt Structure', 'retired_section_header', {
          expected: 'prompt must use the canonical structure from prompt/base.md',
          present: `retired section header \`${violation.label}\` found in ${promptArtifact.fileName}`,
          update: 'replace retired headers with the canonical order: Requirements, Parameters, Output, Sorting Order, Exception Handling',
          sourceFile: promptArtifact.fileName,
          line: violation.lineIndex + 1,
        }));
      }
    } else {
      results.push(createPass(validatorName, taskId, turnNumber, 'Prompt Structure', 'no_retired_sections', promptArtifact.fileName));
    }

    if (index.requirements === undefined) {
      results.push(createFail(validatorName, taskId, turnNumber, 'Requirements', 'missing_section', {
        expected: '`Requirements:` must be present as a standalone header',
        present: `\`Requirements:\` not found in ${promptArtifact.fileName}`,
        update: 'add `Requirements:` as the first section in the prompt',
        sourceFile: promptArtifact.fileName,
      }));
    } else if (lines[index.requirements].trim() !== 'Requirements:') {
      results.push(createFail(validatorName, taskId, turnNumber, 'Requirements', 'invalid_header_content', {
        expected: '`Requirements:` must be a header-only line with no inline content',
        present: `\`${lines[index.requirements].trim()}\` found in ${promptArtifact.fileName}`,
        update: 'rewrite the line as exactly `Requirements:`',
        sourceFile: promptArtifact.fileName,
        line: index.requirements + 1,
      }));
    } else {
      results.push(createPass(validatorName, taskId, turnNumber, 'Requirements', 'header_only_and_first', promptArtifact.fileName));
    }

    results.push(validateSectionOrder(taskId, turnNumber, index, promptArtifact.fileName));
    results.push(structuralSectionResult(taskId, turnNumber, lines, index, 'Parameters', 'parameters', promptArtifact.fileName));
    results.push(validateParametersShape(taskId, turnNumber, lines, index, promptArtifact.fileName));
    results.push(structuralSectionResult(taskId, turnNumber, lines, index, 'Output', 'output', promptArtifact.fileName));
    results.push(validateOutputShape(taskId, turnNumber, lines, index, promptArtifact.fileName));
    results.push(structuralSectionResult(taskId, turnNumber, lines, index, 'Sorting Order', 'sorting_order', promptArtifact.fileName, true));
    results.push(validateSortingOrderShape(taskId, turnNumber, lines, index, promptArtifact.fileName));
    results.push(structuralSectionResult(taskId, turnNumber, lines, index, 'Exception Handling', 'exception_handling', promptArtifact.fileName));
    results.push(
      promptMarkupFailures(promptText).length
        ? createFail(validatorName, taskId, turnNumber, 'Plain Text', 'forbidden_markup', {
            expected: 'prompt must be plain text only with no markdown or HTML styling',
            present: promptMarkupFailures(promptText).join(', '),
            update: 'rewrite the prompt using plain text sections only and remove markdown or HTML formatting',
            sourceFile: promptArtifact.fileName,
          })
        : createPass(validatorName, taskId, turnNumber, 'Plain Text', 'plain_text_only', promptArtifact.fileName),
    );
    results.push(validateExceptionHandlingShape(taskId, turnNumber, lines, index, promptArtifact.fileName));

    const codeArtifact = await loadTurnTextArtifact(taskDir, 'turn_reference_answer_file', taskId, turnNumber);
    if (codeArtifact.text) {
      results.push(...validateRequirementEntries(taskId, turnNumber, lines, index, promptArtifact.fileName, codeArtifact.text, codeArtifact.fileName));
      results.push(...validateParameterContract(taskId, turnNumber, lines, index, codeArtifact.text, codeArtifact.fileName));
      results.push(validateSortingOrderContract(taskId, turnNumber, lines, index, codeArtifact.text, promptArtifact.fileName));
      results.push(...validatePromptCodeContract(taskId, turnNumber, promptText, codeArtifact.text, codeArtifact.fileName));

      if (promptRequiresHandler(promptText, 'NO_DATA_FOUND')) {
        const handlerOk = codeHasHandler(codeArtifact.text, 'NO_DATA_FOUND');
        const sourceOk = hasPlausibleNoDataFoundSource(codeArtifact.text);
        results.push(
          handlerOk && sourceOk
            ? createPass(validatorName, taskId, turnNumber, 'NO_DATA_FOUND Contract', 'contract_satisfied', codeArtifact.fileName)
            : createFail(validatorName, taskId, turnNumber, 'NO_DATA_FOUND Contract', 'contract_mismatch', {
                expected: 'prompt-required NO_DATA_FOUND behavior must have both a handler and a plausible trigger',
                present: [
                  !handlerOk ? '`WHEN NO_DATA_FOUND` handler missing' : null,
                  !sourceOk ? 'no plausible fetch or explicit raise that can trigger NO_DATA_FOUND' : null,
                ].filter(Boolean).join('; '),
                update: [
                  !handlerOk ? 'add `WHEN NO_DATA_FOUND THEN ...`' : null,
                  !sourceOk ? 'add a non-aggregate `SELECT ... INTO` or explicit `RAISE NO_DATA_FOUND`' : null,
                ].filter(Boolean).join('; '),
                sourceFile: codeArtifact.fileName,
              }),
        );
      }

      if (promptRequiresHandler(promptText, 'TOO_MANY_ROWS')) {
        const handlerOk = codeHasHandler(codeArtifact.text, 'TOO_MANY_ROWS');
        const sourceOk = hasPlausibleTooManyRowsSource(codeArtifact.text);
        results.push(
          handlerOk && sourceOk
            ? createPass(validatorName, taskId, turnNumber, 'TOO_MANY_ROWS Contract', 'contract_satisfied', codeArtifact.fileName)
            : createFail(validatorName, taskId, turnNumber, 'TOO_MANY_ROWS Contract', 'contract_mismatch', {
                expected: 'prompt-required TOO_MANY_ROWS behavior must have both a handler and a plausible trigger',
                present: [
                  !handlerOk ? '`WHEN TOO_MANY_ROWS` handler missing' : null,
                  !sourceOk ? 'no non-aggregate SELECT ... INTO or explicit raise that can trigger TOO_MANY_ROWS' : null,
                ].filter(Boolean).join('; '),
                update: [
                  !handlerOk ? 'add `WHEN TOO_MANY_ROWS THEN ...`' : null,
                  !sourceOk ? 'use a non-aggregate SELECT ... INTO without single-row limiting, or explicitly `RAISE TOO_MANY_ROWS`' : null,
                ].filter(Boolean).join('; '),
                sourceFile: codeArtifact.fileName,
              }),
        );
      }

      if (promptRequiresHandler(promptText, 'WHEN OTHERS')) {
        results.push(
          codeHasHandler(codeArtifact.text, 'WHEN OTHERS')
            ? createPass(validatorName, taskId, turnNumber, 'WHEN OTHERS Contract', 'contract_satisfied', codeArtifact.fileName)
            : createFail(validatorName, taskId, turnNumber, 'WHEN OTHERS Contract', 'missing_handler', {
                expected: 'prompt-required `WHEN OTHERS` handler must be present',
                present: `\`WHEN OTHERS\` handler not found in ${codeArtifact.fileName}`,
                update: 'add a `WHEN OTHERS THEN ...` handler that matches the prompt contract',
                sourceFile: codeArtifact.fileName,
              }),
        );
      }
    }
  }

  if (metadataBool(metadata?.required_anonymous_block, false)) {
    if (anonymousBlockPromptTurns.length) {
      const match = anonymousBlockPromptTurns[0];
      results.push(createPass(validatorName, taskId, match.turnNumber, 'Requirements Entries', 'required_anonymous_block_present', match.sourceName));
    } else {
      results.push(createFail(validatorName, taskId, null, 'Requirements Entries', 'missing_required_anonymous_block', {
        expected: '`Anonymous Block:` must appear in at least one prompt turn when required_anonymous_block=true',
        present: `\`Anonymous Block:\` not found in any prompt turn for task ${taskId}`,
        update: 'add `Anonymous Block:` under `Requirements:` in at least one required turn',
      }));
    }
  }

  return results;
}

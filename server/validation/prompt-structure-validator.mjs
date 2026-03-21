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
const OUTPUT_GROUP_RE = /^[A-Za-z][A-Za-z0-9_$#]*\s*:\s*$/;
const EXCEPTION_LINE_RE = /^.+\s:\s+.+$/;
const HTML_TAG_RE = /<\s*\/?\s*(?:a|abbr|article|aside|b|blockquote|body|br|code|div|em|footer|form|h[1-6]|head|header|hr|html|i|img|input|label|li|link|main|meta|nav|ol|p|pre|script|section|small|span|strong|style|sub|sup|table|tbody|td|textarea|th|thead|title|tr|u|ul)\b[^>]*>/i;
const CUSTOM_EXCEPTION_RE = /\bexp_[a-zA-Z0-9_]+\b/gi;
const SELECT_INTO_RE = /\bSELECT\b[\s\S]*?\bINTO\b[\s\S]*?;/gi;
const AGGREGATE_FUNCTION_RE = /\b(COUNT|SUM|AVG|MIN|MAX|LISTAGG)\s*\(/i;
const TOP_LEVEL_NAMED_UNIT_RE = /\bCREATE\s+(?:OR\s+REPLACE\s+)?(PACKAGE(?!\s+BODY\b)|TRIGGER|TYPE(?!\s+BODY\b)|PROCEDURE|FUNCTION)\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)/gi;
const ORDER_BY_CLAUSE_RE = /\bORDER\s+BY\s+([A-Za-z0-9_.$#", ]+(?:\s+(?:ASC|DESC))?(?:\s*,\s*[A-Za-z0-9_.$#", ]+(?:\s+(?:ASC|DESC))?)*)/gi;

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

function countRequirementPrograms(lines, index) {
  const parsed = parseRequirementEntries(lines, index);
  return parsed.entries.filter(([label]) =>
    ['Procedure Name:', 'Function Name:', 'Package Name:', 'Trigger Name:', 'Object Name:', 'Anonymous Block:'].includes(label),
  ).length;
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

  for (const line of body) {
    if (line.trimEnd().endsWith(':') && !line.includes(' : ')) {
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

function extractCustomExceptions(promptText) {
  return [...new Set([...promptText.matchAll(CUSTOM_EXCEPTION_RE)].map((match) => match[0].toUpperCase()))];
}

function promptRequiresHandler(promptText, handlerName) {
  const upper = promptText.toUpperCase();
  if (handlerName === 'WHEN OTHERS') {
    return upper.includes('WHEN OTHERS');
  }
  return upper.includes(handlerName.toUpperCase());
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

function validateSortingOrderContract(taskId, turnNumber, promptText, codeText, sourceName) {
  const promptUpper = promptText.toUpperCase();
  const sortingClauses = extractSortingClauses(codeText);
  if (sortingClauses.length) {
    const missing = sortingClauses.filter((clause) => !promptUpper.includes(clause));
    if (!missing.length) {
      return createPass(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Contract', 'sorting_clause_present', sourceName);
    }
    return createFail(VALIDATOR_NAMES.promptStructure, taskId, turnNumber, 'Sorting Order Contract', 'missing_sorting_clause', {
      expected: '`Sorting Order:` must list the implemented ORDER BY expressions from the reference SQL',
      present: `missing sorting clause(s): ${missing.join(', ')}`,
      update: 'copy the exact ORDER BY columns and directions from the SQL into `Sorting Order:`',
      sourceFile: sourceName,
    });
  }

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
      preambleLines.length > 2
        ? createFail(validatorName, taskId, turnNumber, 'Prompt Preamble', 'unexpected_preamble', {
            expected: 'at most 2 lines of business summary may appear immediately before `Requirements:`',
            present: `found ${preambleLines.length} non-empty line(s) before \`Requirements:\` in ${promptArtifact.fileName}`,
            update: 'keep only an optional 2-line business summary before `Requirements:`',
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
      results.push(validateSortingOrderContract(taskId, turnNumber, promptText, codeArtifact.text, promptArtifact.fileName));
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

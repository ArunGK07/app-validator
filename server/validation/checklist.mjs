const VALIDATOR_NAMES = {
  promptStructure: 'PromptStructureValidator',
  plsqlProgram: 'PLSQLProgramValidator',
  complexityTableCount: 'ComplexityTableCountValidator',
  namingStandard: 'NamingStandardValidator',
};

export const VALIDATION_CHECKLIST_CATALOG = [
  {
    checkId: 'prompt.metadata',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Metadata',
    ruleIds: ['invalid_num_turns', 'invalid_shape', 'invalid_or_missing_metadata'],
    description: 'Metadata must exist, be valid JSON, and define a positive num_turns value.',
  },
  {
    checkId: 'prompt.input-artifact',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Prompt Input Artifact',
    ruleIds: ['missing_artifact'],
    description: 'Each turn must have a prompt input artifact.',
  },
  {
    checkId: 'prompt.preamble',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Prompt Preamble',
    ruleIds: ['preamble_allowed', 'unexpected_preamble'],
    description: 'The prompt may contain at most two preamble lines before Requirements.',
  },
  {
    checkId: 'prompt.structure',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Prompt Structure',
    ruleIds: ['no_retired_sections', 'retired_section_header'],
    description: 'The prompt must use the canonical section structure with no retired headers.',
  },
  {
    checkId: 'prompt.requirements-header',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Requirements',
    ruleIds: ['missing_section', 'invalid_header_content', 'header_only_and_first'],
    description: 'Requirements must be present as the first standalone section header.',
  },
  {
    checkId: 'prompt.section-order',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Section Order',
    ruleIds: ['missing_sections_for_order_check', 'invalid_section_order', 'canonical_order'],
    description: 'Sections must appear in canonical order.',
  },
  {
    checkId: 'prompt.parameters-section',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Parameters',
    ruleIds: ['missing_section', 'missing_content', 'section_present'],
    description: 'Parameters section must exist and contain content.',
  },
  {
    checkId: 'prompt.parameters-format',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Parameters Format',
    ruleIds: ['missing_parameter_contract', 'invalid_parameter_line', 'missing_parameter_group_headers', 'parameter_groups_valid'],
    description: 'Parameters must follow the required line format and grouping rules.',
  },
  {
    checkId: 'prompt.output-section',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Output',
    ruleIds: ['missing_section', 'missing_content', 'section_present'],
    description: 'Output section must exist and contain content.',
  },
  {
    checkId: 'prompt.output-format',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Output Format',
    ruleIds: ['missing_output_contract', 'missing_output_groups', 'output_groups_valid'],
    description: 'Output must follow the expected grouping rules.',
  },
  {
    checkId: 'prompt.sorting-order-section',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Sorting Order',
    ruleIds: ['optional_absent', 'missing_section', 'missing_content', 'section_present'],
    description: 'Sorting Order is optional, but if present it must have valid content.',
  },
  {
    checkId: 'prompt.exception-section',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Exception Handling',
    ruleIds: ['missing_section', 'missing_content', 'section_present'],
    description: 'Exception Handling section must exist and contain content.',
  },
  {
    checkId: 'prompt.plain-text',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Plain Text',
    ruleIds: ['forbidden_markup', 'plain_text_only'],
    description: 'The prompt must be plain text with no HTML or markdown formatting.',
  },
  {
    checkId: 'prompt.exception-format',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Exception Handling Format',
    ruleIds: ['missing_exception_contract', 'invalid_exception_line', 'missing_other_exception_fallback', 'exception_lines_valid'],
    description: 'Exception Handling lines must follow the expected scenario-message format.',
  },
  {
    checkId: 'prompt.requirements-entries-shape',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Requirements Entries',
    ruleIds: ['invalid_requirement_entries', 'entry_shape_valid'],
    description: 'Requirements entries must use header-value pairs in the canonical shape.',
  },
  {
    checkId: 'prompt.requirements-entries-coverage',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Requirements Entries',
    ruleIds: ['missing_named_units', 'all_named_units_declared', 'required_anonymous_block_present', 'missing_required_anonymous_block'],
    description: 'Requirements must declare all implemented program units and required anonymous blocks.',
  },
  {
    checkId: 'prompt.requirement-naming',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    itemPrefix: 'Requirement Naming:',
    dynamic: true,
    ruleIds: ['naming_convention_valid', 'invalid_naming_convention'],
    description: 'Named requirements must follow the required naming conventions.',
  },
  {
    checkId: 'prompt.sorting-order-contract',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'Sorting Order Contract',
    ruleIds: ['sorting_clause_present', 'missing_sorting_clause', 'no_sorting_marker_present', 'unexpected_sorting_content_without_order_by'],
    description: 'Sorting Order must match the implemented SQL ORDER BY behavior.',
  },
  {
    checkId: 'prompt.custom-exception-contract',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    itemPrefix: 'Custom Exception ',
    dynamic: true,
    ruleIds: ['missing_exception_reference', 'missing_exception_declaration', 'missing_exception_raise', 'missing_exception_handler', 'contract_satisfied'],
    description: 'Prompt-required custom exceptions must be declared, raised, and handled in PL/SQL.',
  },
  {
    checkId: 'prompt.no-data-found-contract',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'NO_DATA_FOUND Contract',
    ruleIds: ['contract_satisfied', 'contract_mismatch'],
    description: 'Prompt-required NO_DATA_FOUND handling must exist and be plausibly triggered.',
  },
  {
    checkId: 'prompt.when-others-contract',
    category: 'prompt-structure',
    validator: VALIDATOR_NAMES.promptStructure,
    item: 'WHEN OTHERS Contract',
    ruleIds: ['contract_satisfied', 'missing_handler'],
    description: 'Prompt-required WHEN OTHERS handling must exist.',
  },
  {
    checkId: 'plsql.metadata',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Metadata',
    ruleIds: ['invalid_num_turns'],
    description: 'Metadata must define a positive num_turns value.',
  },
  {
    checkId: 'plsql.reference-artifact',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Reference Answer Artifact',
    ruleIds: ['artifact_present', 'missing_artifact'],
    description: 'Each turn must have a PL/SQL reference answer artifact.',
  },
  {
    checkId: 'plsql.exception-handling',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Exception Handling',
    ruleIds: ['missing_exception_block', 'exception_block_present'],
    description: 'Each PL/SQL turn must contain an EXCEPTION block with handlers.',
  },
  {
    checkId: 'plsql.sqlerrm',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'SQLERRM Usage',
    ruleIds: ['disallowed_sqlerrm', 'not_present'],
    description: 'SQLERRM must not be used in rubric-unsafe ways.',
  },
  {
    checkId: 'plsql.program-unit-creation',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Program Unit Creation',
    ruleIds: ['missing_required_program_unit_creation', 'required_program_unit_present_procedure', 'required_program_unit_present_function', 'required_program_unit_present_package', 'required_program_unit_present_package body'],
    description: 'Required named program units must be created.',
  },
  {
    checkId: 'plsql.anonymous-block',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Anonymous Block',
    ruleIds: ['missing_required_anonymous_block', 'required_anonymous_block_present'],
    description: 'Required anonymous blocks must exist.',
  },
  {
    checkId: 'plsql.root-cause-analysis',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Root Cause Analysis',
    ruleIds: ['not_required', 'missing_evidence', 'multi_turn_evidence_present', 'missing_rca_annotation', 'missing_multi_turn_rca', 'single_turn_evidence_present', 'missing_single_turn_rca'],
    description: 'Root Cause Analysis evidence must satisfy the task metadata requirements.',
  },
  {
    checkId: 'plsql.inline-execution',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Inline Execution',
    ruleIds: ['not_required', 'evidence_present', 'missing_inline_execution'],
    description: 'Inline execution evidence must satisfy the task metadata requirements.',
  },
  {
    checkId: 'plsql.reasoning-type',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    itemPrefix: 'Reasoning Type:',
    dynamic: true,
    ruleIds: ['evidence_present', 'missing_reasoning_type_evidence'],
    description: 'Required reasoning types must be evidenced in the PL/SQL program.',
  },
  {
    checkId: 'plsql.type-usage',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Type Usage',
    ruleIds: ['unused_type_created'],
    description: 'Created types must be used somewhere in the program.',
  },
  {
    checkId: 'plsql.prompt-object-coverage',
    category: 'plsql-program',
    validator: VALIDATOR_NAMES.plsqlProgram,
    item: 'Prompt Object Coverage',
    ruleIds: ['no_named_requirements', 'missing_required_objects', 'all_objects_implemented'],
    description: 'Named prompt requirements must be implemented in the code.',
  },
  {
    checkId: 'complexity.metadata',
    category: 'complexity-table-count',
    validator: VALIDATOR_NAMES.complexityTableCount,
    item: 'Metadata',
    ruleIds: ['invalid_num_turns'],
    description: 'Metadata must define a positive num_turns value.',
  },
  {
    checkId: 'complexity.value',
    category: 'complexity-table-count',
    validator: VALIDATOR_NAMES.complexityTableCount,
    item: 'Complexity',
    ruleIds: ['unsupported_complexity'],
    description: 'Complexity must be supported by the table-count validator.',
  },
  {
    checkId: 'complexity.table-artifact',
    category: 'complexity-table-count',
    validator: VALIDATOR_NAMES.complexityTableCount,
    item: 'Table Artifact',
    ruleIds: ['missing_artifact', 'artifact_present'],
    description: 'Each turn must have a table artifact for complexity checking.',
  },
  {
    checkId: 'complexity.table-count',
    category: 'complexity-table-count',
    validator: VALIDATOR_NAMES.complexityTableCount,
    item: 'Complexity Table Count',
    ruleIds: ['count_aligned', 'count_mismatch'],
    description: 'The table count must align with the declared complexity.',
  },
  {
    checkId: 'naming.metadata',
    category: 'naming-standard',
    validator: VALIDATOR_NAMES.namingStandard,
    item: 'Metadata',
    ruleIds: ['invalid_num_turns'],
    description: 'Metadata must define a positive num_turns value.',
  },
  {
    checkId: 'naming.connection',
    category: 'naming-standard',
    validator: VALIDATOR_NAMES.namingStandard,
    item: 'Connection',
    ruleIds: ['connection_error'],
    description: 'Oracle connection for naming validation must succeed.',
  },
  {
    checkId: 'naming.reference-artifact',
    category: 'naming-standard',
    validator: VALIDATOR_NAMES.namingStandard,
    item: 'Reference Answer Artifact',
    ruleIds: ['missing_artifact'],
    description: 'Each turn must have a PL/SQL reference answer artifact.',
  },
  {
    checkId: 'naming.standard',
    category: 'naming-standard',
    validator: VALIDATOR_NAMES.namingStandard,
    item: 'Naming Standard',
    ruleIds: ['compliant'],
    description: 'The naming standard validator should report compliant code when no violations exist.',
  },
  {
    checkId: 'naming.compilation',
    category: 'naming-standard',
    validator: VALIDATOR_NAMES.namingStandard,
    item: 'Compilation',
    ruleIds: ['compile_or_execution_error'],
    description: 'Compilation and identifier extraction must succeed for naming validation.',
  },
  {
    checkId: 'naming.violations',
    category: 'naming-standard',
    validator: VALIDATOR_NAMES.namingStandard,
    dynamic: true,
    ruleIds: ['naming_violation'],
    description: 'Every naming-standard violation should be listed with PASS or FAIL status.',
  },
];

export function buildValidationChecklist(validatorReports) {
  const allResults = validatorReports.flatMap((report) => report.results);
  const checklist = [];
  const matchedResultIds = new Set();

  for (const definition of VALIDATION_CHECKLIST_CATALOG) {
    if (definition.dynamic) {
      const dynamicMatches = allResults.filter((result, index) => {
        const itemMatches = definition.item
          ? result.item === definition.item
          : definition.itemPrefix
            ? result.item.startsWith(definition.itemPrefix)
            : true;
        return result.validator === definition.validator && definition.ruleIds.includes(result.ruleId) && itemMatches;
      });

      for (const result of dynamicMatches) {
        checklist.push(toChecklistEntry(definition, result));
        matchedResultIds.add(getResultKey(result));
      }
      continue;
    }

    const match = allResults.find((result) => {
      const itemMatches = definition.item ? result.item === definition.item : true;
      return result.validator === definition.validator && definition.ruleIds.includes(result.ruleId) && itemMatches;
    });

    if (match) {
      checklist.push(toChecklistEntry(definition, match));
      matchedResultIds.add(getResultKey(match));
    } else {
      checklist.push({
        checkId: definition.checkId,
        category: definition.category,
        validator: definition.validator,
        item: definition.item ?? definition.itemPrefix ?? definition.checkId,
        ruleId: null,
        description: definition.description,
        status: 'NOT_RUN',
        taskId: null,
        turnId: null,
        expected: null,
        present: null,
        update: null,
        sourceFile: null,
        line: null,
      });
    }
  }

  for (const result of allResults) {
    if (matchedResultIds.has(getResultKey(result))) {
      continue;
    }
    checklist.push({
      checkId: `${result.validator}.${result.ruleId}.${slugify(result.item)}`,
      category: inferCategory(result.validator),
      validator: result.validator,
      item: result.item,
      ruleId: result.ruleId,
      description: null,
      status: result.status,
      taskId: result.taskId,
      turnId: result.turnId,
      expected: result.expected,
      present: result.present,
      update: result.update,
      sourceFile: result.sourceFile,
      line: result.line,
    });
  }

  return checklist;
}

function toChecklistEntry(definition, result) {
  return {
    checkId: definition.dynamic ? `${definition.checkId}.${slugify(result.item)}` : definition.checkId,
    category: definition.category,
    validator: result.validator,
    item: result.item,
    ruleId: result.ruleId,
    description: definition.description,
    status: result.status,
    taskId: result.taskId,
    turnId: result.turnId,
    expected: result.expected,
    present: result.present,
    update: result.update,
    sourceFile: result.sourceFile,
    line: result.line,
  };
}

function getResultKey(result) {
  return `${result.validator}::${result.item}::${result.ruleId}::${result.turnId ?? 'task'}`;
}

function slugify(value) {
  return String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'item';
}

function inferCategory(validator) {
  if (validator === VALIDATOR_NAMES.promptStructure) return 'prompt-structure';
  if (validator === VALIDATOR_NAMES.plsqlProgram || validator === VALIDATOR_NAMES.complexityTableCount) return 'plsql-program';
  if (validator === VALIDATOR_NAMES.namingStandard) return 'naming-standard';
  return 'other';
}

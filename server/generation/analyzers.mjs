import { PLSQL_CONSTRUCT_CATALOG, PLSQL_REASONING_TYPE_CATALOG } from './reference-data.mjs';

export function formatCommaLines(items) {
  const values = items.map((item) => String(item).trim()).filter(Boolean);
  return values.map((value, index) => `${value}${index < values.length - 1 ? ',' : ''}`).join('\n');
}

export function analyzeTables(codeText, schema) {
  const tables = Object.keys(schema?.tables ?? {});
  const tableLookup = Object.fromEntries(tables.map((table) => [normalizeIdentifier(table), table]));
  const matched = new Set();
  const scan = (sqlText) => {
    for (const match of String(sqlText).matchAll(/\b(?:FROM|JOIN|UPDATE|DELETE\s+FROM|INSERT\s+INTO|MERGE\s+INTO)\s+([A-Z0-9_$#".]+)/gi)) {
      const candidate = normalizeIdentifier(match[1]).split('.').at(-1);
      if (tableLookup[candidate]) {
        matched.add(tableLookup[candidate]);
      }
    }
    for (const match of String(sqlText).matchAll(/\b([A-Z0-9_$#"]+)(?:\.([A-Z0-9_$#"]+))?\.[A-Z0-9_$#"]+\s*%\s*(?:TYPE|ROWTYPE)\b/gi)) {
      const candidate = normalizeIdentifier(match[2] || match[1]);
      if (tableLookup[candidate]) {
        matched.add(tableLookup[candidate]);
      }
    }
  };
  scan(removeSqlComments(removeStringLiterals(codeText)).toUpperCase());
  for (const fragment of extractDynamicSqlFragments(codeText)) {
    scan(removeSqlComments(fragment).toUpperCase());
  }
  return [...matched].sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

export function analyzeColumns(codeText, schema) {
  const { database, tableColumns, columnToTables } = loadSchemaColumns(schema);
  const code = removeSqlComments(removeStringLiterals(codeText));
  const aliasMap = extractTableAliases(code, tableColumns);
  const mentionedTables = new Set(Object.values(aliasMap));
  const matched = new Set();

  for (const match of code.matchAll(/\b([A-Z0-9_$#"]+)(?:\.([A-Z0-9_$#"]+))?\.\s*([A-Z0-9_$#"]+)\b/gi)) {
    const owner = normalizeIdentifier(match[2] || match[1]);
    const column = normalizeIdentifier(match[3]);
    const tableName = aliasMap[owner] || Object.keys(tableColumns).find((table) => normalizeIdentifier(table) === owner);
    if (!tableName) continue;
    const original = (tableColumns[tableName] ?? []).find((entry) => normalizeIdentifier(entry) === column);
    if (original) matched.add(`${database.toUpperCase()}.${tableName.toUpperCase()}.${original.toLowerCase()}`);
  }

  for (const [column, tables] of Object.entries(columnToTables)) {
    const pattern = new RegExp(`(?<![.%])\\b${escapeRegex(column)}\\b(?!\\s*%\\s*(?:TYPE|ROWTYPE)\\b)(?!\\s*\\()`, 'i');
    if (!pattern.test(code)) continue;
    const scoped = tables.filter((table) => mentionedTables.has(table));
    if (tables.length === 1) matched.add(`${database.toUpperCase()}.${tables[0].toUpperCase()}.${column.toLowerCase()}`);
    else if (scoped.length === 1) matched.add(`${database.toUpperCase()}.${scoped[0].toUpperCase()}.${column.toLowerCase()}`);
  }

  return [...matched].sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

export function analyzeConstructs(codeText) {
  return [...new Set(evaluatePlsqlConstructs(codeText).filter((entry) => entry.matched).map((entry) => entry.label))]
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

export function evaluatePlsqlConstructs(codeText) {
  const normalized = buildConstructAnalysisText(codeText);
  return PLSQL_CONSTRUCT_CATALOG.map((entry) => {
    const match = entry.pattern.exec(normalized);
    entry.pattern.lastIndex = 0;

    return {
      pdfIndex: entry.pdfIndex,
      id: entry.id,
      label: entry.label,
      considered: true,
      matched: Boolean(match),
      line: match?.index === undefined ? null : findLineNumber(normalized, match.index),
      matchedText: match?.[0] ? collapseWhitespace(match[0]).slice(0, 180) : null,
    };
  });
}

export function analyzeReasoningTypes(codeText) {
  return evaluateReasoningTypes(codeText)
    .filter((entry) => entry.matched)
    .map((entry) => entry.label)
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

export function evaluateReasoningTypes(codeText) {
  const normalized = buildConstructAnalysisText(codeText);
  return PLSQL_REASONING_TYPE_CATALOG.map((entry) => {
    const matches = entry.patterns.map((pattern) => {
      const match = pattern.exec(normalized);
      pattern.lastIndex = 0;
      return match;
    });
    const matched = entry.mode === 'any' ? matches.some(Boolean) : matches.every(Boolean);
    const firstMatch = matches.find(Boolean) ?? null;

    return {
      pdfIndex: entry.pdfIndex,
      id: entry.id,
      label: entry.label,
      considered: true,
      matched,
      line: firstMatch?.index === undefined ? null : findLineNumber(normalized, firstMatch.index),
      matchedText: firstMatch?.[0] ? collapseWhitespace(firstMatch[0]).slice(0, 180) : null,
    };
  });
}

function loadSchemaColumns(schema) {
  const database = String(schema?.database ?? 'UNKNOWN');
  const tables = schema?.tables && typeof schema.tables === 'object' ? schema.tables : {};
  const nameIndex = Array.isArray(schema?._schema_definition?.column_format) ? Math.max(0, schema._schema_definition.column_format.indexOf('name')) : 0;
  const tableColumns = {};
  const columnToTables = {};
  for (const [tableName, tableInfo] of Object.entries(tables)) {
    const columns = (Array.isArray(tableInfo?.columns) ? tableInfo.columns : [])
      .filter((column) => Array.isArray(column) && column.length > nameIndex)
      .map((column) => String(column[nameIndex]));
    tableColumns[tableName] = columns;
    for (const column of columns) {
      if (!columnToTables[column]) columnToTables[column] = [];
      columnToTables[column].push(tableName);
    }
  }
  return { database, tableColumns, columnToTables };
}

function extractTableAliases(code, tableColumns) {
  const aliases = {};
  const tableNames = Object.keys(tableColumns);
  for (const match of code.matchAll(/\b(?:FROM|JOIN|UPDATE|DELETE\s+FROM|INSERT\s+INTO|MERGE\s+INTO)\s+([A-Z0-9_$#".]+)(?:\s+(?:AS\s+)?([A-Z0-9_$#"]+))?/gi)) {
    const tableOnly = normalizeIdentifier(match[1]).split('.').at(-1);
    const tableName = tableNames.find((table) => normalizeIdentifier(table) === tableOnly);
    if (!tableName) continue;
    aliases[tableOnly] = tableName;
    if (match[2]) aliases[normalizeIdentifier(match[2])] = tableName;
  }
  for (const match of code.matchAll(/\bON\s+([A-Z0-9_$#".]+)\s+FOR\s+EACH\s+ROW\b/gi)) {
    const tableOnly = normalizeIdentifier(match[1]).split('.').at(-1);
    const tableName = tableNames.find((table) => normalizeIdentifier(table) === tableOnly);
    if (tableName) {
      aliases.NEW = tableName;
      aliases.OLD = tableName;
    }
  }
  return aliases;
}

function extractDynamicSqlFragments(code) {
  const fragments = [];
  for (const match of String(code).matchAll(/\b(?:EXECUTE\s+IMMEDIATE|OPEN\s+[A-Z0-9_$#"]+\s+FOR)\s+(.*?)(?:\bUSING\b|\bINTO\b|;)/gis)) {
    const joined = [...match[1].matchAll(/'(?:[^']|'')*'/g)]
      .map((entry) => entry[0].slice(1, -1).replace(/''/g, "'").trim())
      .filter(Boolean)
      .join(' ');
    if (/\b(SELECT|INSERT|UPDATE|DELETE|MERGE|FROM|JOIN|WHERE)\b/i.test(joined)) {
      fragments.push(joined);
    }
  }
  return fragments;
}

function buildConstructAnalysisText(codeText) {
  return `${removeSqlComments(removeStringLiterals(codeText))}\n${extractDynamicSqlFragments(codeText).join('\n')}`;
}

function removeSqlComments(code) {
  return String(code).replace(/--[^\n]*(?:\n|$)/g, '\n').replace(/\/\*[\s\S]*?\*\//g, '');
}

function removeStringLiterals(code) {
  return String(code).replace(/'(?:[^']|'')*'/g, "__STR__");
}

function normalizeIdentifier(value) {
  return String(value ?? '').replaceAll('"', '').trim().toUpperCase();
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function findLineNumber(text, index) {
  return String(text).slice(0, index).split(/\r?\n/).length;
}

function collapseWhitespace(value) {
  return String(value).replace(/\s+/g, ' ').trim();
}

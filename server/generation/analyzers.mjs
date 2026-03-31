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
    for (const match of String(sqlText).matchAll(/\b(?:BEFORE|AFTER|INSTEAD\s+OF)\s+(?:INSERT|UPDATE|DELETE)(?:\s+OR\s+(?:INSERT|UPDATE|DELETE))*(?:\s+OF\s+[A-Z0-9_$#".,\s]+)?\s+ON\s+([A-Z0-9_$#".]+)/gi)) {
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
  scan(normalizeCodeForAnalysis(codeText).toUpperCase());
  for (const fragment of extractDynamicSqlFragments(codeText)) {
    scan(removeSqlComments(fragment).toUpperCase());
  }
  return [...matched].sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

export function analyzeColumns(codeText, schema) {
  const { database, tableColumns, columnToTables } = loadSchemaColumns(schema);
  const code = normalizeCodeForAnalysis(codeText);
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

  for (const match of code.matchAll(/:(NEW|OLD)\.\s*([A-Z0-9_$#"]+)\b/gi)) {
    const tableName = aliasMap[normalizeIdentifier(match[1])];
    const column = normalizeIdentifier(match[2]);
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

  for (const entry of collectUpdateTargetColumns(code, tableColumns, database)) {
    matched.add(entry);
  }

  for (const entry of collectStatementScopedColumns(code, tableColumns, columnToTables, database)) {
    matched.add(entry);
  }

  return [...matched].sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

export function analyzeConstructs(codeText) {
  return [...new Set(evaluatePlsqlConstructs(codeText).filter((entry) => entry.matched).map((entry) => entry.label))]
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

function _isHighSignalConstruct(label) {
  if (!label || typeof label !== 'string') return false;
  const explicitHighSignalTokens = new Set(['nvl', 'open', 'with']);
  // Multi-token templates or templates with ellipsis, parentheses, or '%'
  if (label.includes(' ') || label.includes('...') || label.includes('(') || label.includes('%') || label.includes(':') || label.includes('/')) {
    return true;
  }
  // Single-word heuristics: treat short SQL operators as low-signal
  const shortLower = label.trim().toLowerCase();
  if (explicitHighSignalTokens.has(shortLower)) return true;
  const lowSignalTokens = new Set(['in', 'and', 'is', 'as', 'on', 'by', 'to', 'set', 'desc', 'asc', 'or']);
  if (lowSignalTokens.has(shortLower)) return false;
  // Prefer words of length >= 5 as higher signal (COUNT, COMMIT, ROLLBACK etc.)
  return label.trim().length >= 5;
}

export function analyzeConstructsHighSignal(codeText) {
  const all = analyzeConstructs(codeText);
  // Allow verbose override via env var
  if (process.env.PLSQL_CONSTRUCT_VERBOSE === '1') return all;
  return all.filter((label) => _isHighSignalConstruct(label));
}

export function evaluatePlsqlConstructs(codeText) {
  const normalized = buildConstructAnalysisText(codeText);
  const evaluations = PLSQL_CONSTRUCT_CATALOG.map((entry) => {
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

  const nestedBlock = findNestedBeginEndBlock(normalized);
  if (nestedBlock) {
    const nestedEntry = evaluations.find((entry) => entry.label === 'BEGIN ... END (nested block)');
    if (nestedEntry) {
      nestedEntry.matched = true;
      nestedEntry.line = findLineNumber(normalized, nestedBlock.index);
      nestedEntry.matchedText = collapseWhitespace(nestedBlock.text).slice(0, 180);
    }
  }

  const selfJoin = findSelfJoin(normalized);
  const selfJoinEntry = evaluations.find((entry) => entry.label === 'SELF JOIN');
  if (selfJoinEntry) {
    selfJoinEntry.matched = Boolean(selfJoin);
    selfJoinEntry.line = selfJoin ? findLineNumber(normalized, selfJoin.index) : null;
    selfJoinEntry.matchedText = selfJoin ? collapseWhitespace(selfJoin.text).slice(0, 180) : null;
  }

  return evaluations;
}

export function analyzeReasoningTypes(codeText) {
  return evaluateReasoningTypes(codeText)
    .filter((entry) => entry.matched)
    .map((entry) => entry.label)
    .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base' }));
}

export function evaluateReasoningTypes(codeText) {
  const normalized = buildConstructAnalysisText(codeText);
  const original = `${String(codeText)}\n${extractDynamicSqlFragments(codeText).join('\n')}`;
  const evaluations = PLSQL_REASONING_TYPE_CATALOG.map((entry) => {
    const matches = entry.patterns.map((pattern) => {
      const sourceText = entry.label === 'Root Cause Analysis' ? original : normalized;
      const match = pattern.exec(sourceText);
      pattern.lastIndex = 0;
      return match;
    });
    const matched = entry.mode === 'any' ? matches.some(Boolean) : matches.every(Boolean);
    const firstMatch = matches.find(Boolean) ?? null;
    const sourceText = entry.label === 'Root Cause Analysis' ? original : normalized;

    return {
      pdfIndex: entry.pdfIndex,
      id: entry.id,
      label: entry.label,
      considered: true,
      matched,
      line: firstMatch?.index === undefined ? null : findLineNumber(sourceText, firstMatch.index),
      matchedText: firstMatch?.[0] ? collapseWhitespace(firstMatch[0]).slice(0, 180) : null,
    };
  });

  return evaluations;
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

function collectUpdateTargetColumns(code, tableColumns, database) {
  const matched = new Set();
  const tableNames = Object.keys(tableColumns);

  for (const match of String(code).matchAll(/\bUPDATE\s+([A-Z0-9_$#".]+)(?:\s+(?:AS\s+)?([A-Z0-9_$#"]+))?\s+SET\b([\s\S]*?);/gi)) {
    const tableOnly = normalizeIdentifier(match[1]).split('.').at(-1);
    const tableName = tableNames.find((table) => normalizeIdentifier(table) === tableOnly);
    if (!tableName) continue;

    const statementText = String(match[3] ?? '');
    for (const columnName of tableColumns[tableName] ?? []) {
      const pattern = new RegExp(`(?<![.%])\\b${escapeRegex(columnName)}\\b(?!\\s*%\\s*(?:TYPE|ROWTYPE)\\b)(?!\\s*\\()`, 'i');
      if (pattern.test(statementText)) {
        matched.add(`${database.toUpperCase()}.${tableName.toUpperCase()}.${columnName.toLowerCase()}`);
      }
    }
  }

  return matched;
}

function collectStatementScopedColumns(code, tableColumns, columnToTables, database) {
  const matched = new Set();
  const statements = splitSqlStatementsForAnalysis(code);

  for (const statement of statements) {
    if (!/\b(?:SELECT|UPDATE|DELETE|INSERT|MERGE)\b/i.test(statement)) {
      continue;
    }

    const statementAliasMap = extractTableAliases(statement, tableColumns);
    const statementTables = [...new Set(Object.values(statementAliasMap))];
    if (!statementTables.length) {
      continue;
    }

    for (const [column, tables] of Object.entries(columnToTables)) {
      const pattern = new RegExp(`(?<![.%])\\b${escapeRegex(column)}\\b(?!\\s*%\\s*(?:TYPE|ROWTYPE)\\b)(?!\\s*\\()`, 'i');
      if (!pattern.test(statement)) {
        continue;
      }

      const scoped = tables.filter((table) => statementTables.includes(table));
      if (scoped.length === 1) {
        matched.add(`${database.toUpperCase()}.${scoped[0].toUpperCase()}.${column.toLowerCase()}`);
      }
    }
  }

  return matched;
}

function splitSqlStatementsForAnalysis(code) {
  return String(code)
    .split(/;/)
    .map((statement) => statement.trim())
    .filter(Boolean);
}

function buildConstructAnalysisText(codeText) {
  return `${normalizeCodeForAnalysis(codeText)}\n${extractDynamicSqlFragments(codeText).join('\n')}`;
}

function findNestedBeginEndBlock(text) {
  const tokenPattern = /\bBEGIN\b|\bEND\s+IF\b|\bEND\s+LOOP\b|\bEND\s+CASE\b|\bEND\b(?:\s+[A-Z0-9_$#"]+)?\s*;/gi;
  const beginStack = [];

  for (const match of String(text).matchAll(tokenPattern)) {
    const token = match[0].toUpperCase().replace(/\s+/g, ' ').trim();
    if (token === 'BEGIN') {
      beginStack.push(match.index);
      if (beginStack.length >= 2) {
        const blockText = extractNestedBlockText(String(text), match.index);
        return {
          index: match.index,
          text: blockText,
        };
      }
      continue;
    }

    if (token.startsWith('END IF') || token.startsWith('END LOOP') || token.startsWith('END CASE')) {
      continue;
    }

    if (beginStack.length) {
      beginStack.pop();
    }
  }

  return null;
}

function findSelfJoin(text) {
  const statements = String(text).split(/;/);

  for (const statement of statements) {
    const normalizedStatement = collapseWhitespace(statement);
    if (!/\bFROM\b/i.test(normalizedStatement) || !/\bJOIN\b/i.test(normalizedStatement)) {
      continue;
    }

    const fromMatch = /\bFROM\s+([A-Z0-9_$#".]+)/i.exec(normalizedStatement);
    if (!fromMatch) continue;
    const fromTable = normalizeIdentifier(fromMatch[1]);

    for (const joinMatch of normalizedStatement.matchAll(/\bJOIN\s+([A-Z0-9_$#".]+)/gi)) {
      if (normalizeIdentifier(joinMatch[1]) === fromTable) {
        return {
          index: text.indexOf(statement),
          text: statement,
        };
      }
    }
  }

  return null;
}

function extractNestedBlockText(text, beginIndex) {
  const endMatch = /\bEND\b(?:\s+[A-Z0-9_$#"]+)?\s*;/gi;
  endMatch.lastIndex = beginIndex;
  const match = endMatch.exec(text);
  if (!match) {
    return text.slice(beginIndex);
  }
  return text.slice(beginIndex, match.index + match[0].length);
}

function removeSqlComments(code) {
  return String(code).replace(/--[^\n]*(?:\n|$)/g, '\n').replace(/\/\*[\s\S]*?\*\//g, '');
}

function removeStringLiterals(code) {
  return String(code).replace(/'(?:[^']|'')*'/g, "__STR__");
}

function normalizeCodeForAnalysis(code) {
  return scrubSqlForAnalysis(code);
}

function scrubSqlForAnalysis(code) {
  const text = String(code ?? '');
  let index = 0;
  let output = '';
  let pendingStringPlaceholder = false;

  while (index < text.length) {
    const current = text[index];
    const next = text[index + 1];

    if (pendingStringPlaceholder) {
      output += '__STR__';
      pendingStringPlaceholder = false;
    }

    if (current === "'" ) {
      pendingStringPlaceholder = true;
      index += 1;
      while (index < text.length) {
        if (text[index] === "'" && text[index + 1] === "'") {
          index += 2;
          continue;
        }
        if (text[index] === "'") {
          index += 1;
          break;
        }
        if (text[index] === '\r' || text[index] === '\n') {
          output += text[index];
        }
        index += 1;
      }
      continue;
    }

    if (current === '-' && next === '-') {
      index += 2;
      while (index < text.length && text[index] !== '\r' && text[index] !== '\n') {
        index += 1;
      }
      continue;
    }

    if (current === '/' && next === '*') {
      index += 2;
      while (index < text.length) {
        if (text[index] === '*' && text[index + 1] === '/') {
          index += 2;
          break;
        }
        if (text[index] === '\r' || text[index] === '\n') {
          output += text[index];
        }
        index += 1;
      }
      continue;
    }

    output += current;
    index += 1;
  }

  if (pendingStringPlaceholder) {
    output += '__STR__';
  }

  return output;
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

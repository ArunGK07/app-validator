function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function call(name) {
  return new RegExp(`\\b${escapeRegex(name)}\\s*\\(`, 'i');
}

function phrase(text) {
  return new RegExp(
    text
      .trim()
      .split(/\s+/)
      .map((part) => escapeRegex(part))
      .join('\\s+'),
    'i',
  );
}

function entry(label, patterns, mode = 'all') {
  return { label, patterns, mode };
}

const RAW_REASONING_TYPE_CATALOG = [
  entry('Data Synchronization', [/\bMERGE\s+INTO\b[\s\S]{0,400}?\bUSING\b[\s\S]{0,400}?\bON\b/i]),
  entry('Conditional Derivation', [/\bCASE\b[\s\S]{0,300}?\bWHEN\b/i, /\b(?:DECODE|NVL2|COALESCE|NULLIF)\s*\(/i], 'any'),
  entry('Set-based Processing', [/\b(?:UNION(?:\s+ALL)?|MINUS|INTERSECT|MULTISET|EXISTS\s*\(|IN\s*\(\s*SELECT|MERGE\s+INTO)\b/i]),
  entry('Decision Logic', [/\b(?:IF\b[\s\S]{0,2000}?\bTHEN\b|CASE\b[\s\S]{0,2000}?\bWHEN\b)/i]),
  entry('Structural & Scope', [/\b(?:DECLARE\b|CREATE\s+OR\s+REPLACE\s+(?:(?:NON)?EDITIONABLE\s+)?(?:PROCEDURE|FUNCTION|PACKAGE|TYPE|TRIGGER)|SUBTYPE\b)\b/i]),
  entry('Memory & Type', [/\b(?:%TYPE|%ROWTYPE|TYPE\s+\w+\s+IS\s+RECORD|SUBTYPE\b)\b/i]),
  entry('Data Retrieval', [/\bSELECT\b[\s\S]{0,300}?\bFROM\b/i]),
  entry('Iterative', [/\b(?:FOR\b[\s\S]{0,1200}?\bLOOP\b|WHILE\b[\s\S]{0,1200}?\bLOOP\b|(?:^|\s)LOOP\b)/i]),
  entry('Control Flow', [/\b(?:IF\b[\s\S]{0,2000}?\bTHEN\b|CASE\b[\s\S]{0,2000}?\bWHEN\b|EXIT(?:\s+WHEN)?|CONTINUE(?:\s+WHEN)?|GOTO|RETURN\b)\b/i]),
  entry('Data Manipulation', [/\b(?:INSERT\s+INTO|UPDATE\s+[A-Z0-9_$#".]+(?:\s+[A-Z0-9_$#"]+)?\s+SET|DELETE\s+FROM|MERGE\s+INTO)\b/i]),
  entry('Transaction Management', [/\b(?:COMMIT|ROLLBACK|SAVEPOINT|SET\s+TRANSACTION|PRAGMA\s+AUTONOMOUS_TRANSACTION)\b/i]),
  entry('Dynamic SQL', [/\b(?:EXECUTE\s+IMMEDIATE|DBMS_SQL|OPEN\s+\w+\s+FOR)\b/i]),
  entry('Performance & Bulk Reasoning', [/\b(?:BULK\s+COLLECT\s+INTO|FORALL\b|LIMIT\b)\b/i]),
  entry('Exception Handling', [/\b(?:EXCEPTION\b[\s\S]{0,8000}?\bWHEN\b|[A-Z0-9_$#"]+\s+EXCEPTION\b|RAISE(?:_APPLICATION_ERROR)?\b)/i]),
  entry('Event-Driven Logic', [/\b(?:CREATE\s+OR\s+REPLACE\s+TRIGGER|INSERTING\b|UPDATING\b|DELETING\b|COMPOUND\s+TRIGGER)\b/i]),
  entry('Cursors', [/\b(?:CURSOR\s+\w+\s+IS|OPEN\s+\w+\b|FETCH\s+\w+\s+INTO|CLOSE\s+\w+\b|FOR\s+UPDATE\s+OF|WHERE\s+CURRENT\s+OF|SYS_REFCURSOR|REF\s+CURSOR)\b/i]),
  entry('Debugging', [/\b(?:DBMS_OUTPUT\s*\.\s*PUT_LINE|SQLERRM|SQLCODE)\b/i]),
  entry('Object-Oriented Design', [/\b(?:CREATE\s+(?:OR\s+REPLACE\s+)?TYPE\b[\s\S]{0,200}?\bAS\s+OBJECT|MEMBER\s+(?:FUNCTION|PROCEDURE))\b/i]),
  entry('Subprogram Overloading', [/\bCREATE\s+OR\s+REPLACE\s+PACKAGE\b(?!\s+BODY\b)(?:(?!^\s*\/\s*$)[\s\S]){0,4000}?\b(?:FUNCTION|PROCEDURE)\s+([A-Z0-9_$#"]+)\b(?:(?!^\s*\/\s*$)[\s\S]){0,2000}?\b(?:FUNCTION|PROCEDURE)\s+\1\b/im]),
  entry('Function Purity', [/\b(?:DETERMINISTIC|PRAGMA\s+RESTRICT_REFERENCES)\b/i]),
  entry('RNDS', [/\bRNDS\b/i]),
  entry('WNDS', [/\bWNDS\b/i]),
  entry('WNPS', [/\bWNPS\b/i]),
  entry('RNPS', [/\bRNPS\b/i]),
  entry('Encapsulation', [/\bCREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE(?:\s+BODY)?\b/i]),
  entry('Purity Rule Enforcement', [/\bPRAGMA\s+RESTRICT_REFERENCES\b/i, /\b(?:RNDS|WNDS|WNPS|RNPS)\b/i]),
  entry('Compile-Time Checking', [/\b(?:%TYPE|%ROWTYPE|SUBTYPE\b|TYPE\s+\w+\s+IS\s+RECORD)\b/i]),
  entry('Root Cause Analysis', [/(?:--|\/\*)\s*RCA\s*:|\bDBMS_OUTPUT\s*\.\s*PUT_LINE\s*\([\s\S]{0,200}?RCA\s*:/i], 'any'),
  entry('Collections', [/\b(?:TYPE\s+\w+\s+IS\s+(?:TABLE|VARRAY)|INDEX\s+BY\b|BULK\s+COLLECT\s+INTO|FORALL\b)\b|\.\s*(?:EXTEND|TRIM|COUNT|FIRST|LAST|NEXT|PRIOR)\b/i]),
  entry('Validation', [/\b(?:RAISE_APPLICATION_ERROR|REGEXP_LIKE\s*\(|BETWEEN\b|IF\b[\s\S]{0,200}?\bIS\s+NULL\b)\b/i]),
  entry('Aggregation', [/(?:\bCOUNT\s*\(|\bSUM\s*\(|\bAVG\s*\(|\bMIN\s*\(|\bMAX\s*\(|\bLISTAGG\s*\(|\bROLLUP\b|\bCUBE\b)/i]),
  entry('Analytical Modeling', [/\b(?:ROW_NUMBER\s*\(|RANK\s*\(|DENSE_RANK\s*\(|NTILE\s*\(|LAG\s*\(|LEAD\s*\(|FIRST_VALUE\s*\(|PARTITION\s+BY\b)\b/i]),
];

export const PLSQL_REASONING_TYPE_CATALOG = RAW_REASONING_TYPE_CATALOG.map((entryValue, index) => ({
  pdfIndex: index + 1,
  id: `plsql_reasoning_type_${String(index + 1).padStart(3, '0')}`,
  label: entryValue.label,
  patterns: entryValue.patterns,
  mode: entryValue.mode,
}));

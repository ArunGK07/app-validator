import { PLSQL_CONSTRUCT_CATALOG } from './plsql-construct-catalog.mjs';
import { PLSQL_REASONING_TYPE_CATALOG } from './plsql-reasoning-type-catalog.mjs';

export { PLSQL_CONSTRUCT_CATALOG, PLSQL_REASONING_TYPE_CATALOG };

export const PLSQL_CONSTRUCT_PATTERNS = Object.fromEntries(
  PLSQL_CONSTRUCT_CATALOG.map((entry) => [entry.label, entry.pattern]),
);

export const REASONING_TYPE_PATTERNS = Object.fromEntries(
  PLSQL_REASONING_TYPE_CATALOG.map((entry) => [entry.label, entry.patterns]),
);

export const REQUIRED_PUBLISH_FIELDS = [
  'user',
  'tables',
  'columns',
  'referenceAnswer',
  'testCases',
  'reasoningTypes',
  'plSqlConstructs',
];

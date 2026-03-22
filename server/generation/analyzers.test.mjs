import assert from 'node:assert/strict';
import test from 'node:test';

import { analyzeConstructs, analyzeReasoningTypes, evaluatePlsqlConstructs, evaluateReasoningTypes } from './analyzers.mjs';
import { PLSQL_CONSTRUCT_CATALOG, PLSQL_REASONING_TYPE_CATALOG } from './reference-data.mjs';

test('evaluatePlsqlConstructs considers every catalog item', () => {
  const results = evaluatePlsqlConstructs('BEGIN NULL; END; /');

  assert.equal(results.length, PLSQL_CONSTRUCT_CATALOG.length);
  assert.ok(results.every((entry) => entry.considered === true));
  assert.ok(results.every((entry) => typeof entry.matched === 'boolean'));
  assert.ok(results.every((entry) => typeof entry.label === 'string' && entry.label.length > 0));
});

test('evaluateReasoningTypes considers every catalog item', () => {
  const results = evaluateReasoningTypes('BEGIN NULL; END; /');

  assert.equal(results.length, PLSQL_REASONING_TYPE_CATALOG.length);
  assert.ok(results.every((entry) => entry.considered === true));
  assert.ok(results.every((entry) => typeof entry.matched === 'boolean'));
  assert.ok(results.every((entry) => typeof entry.label === 'string' && entry.label.length > 0));
});

test('analyzeConstructs and evaluatePlsqlConstructs detect representative PDF constructs', () => {
  const sql = [
    'CREATE OR REPLACE FUNCTION sf_demo RETURN NUMBER IS',
    '  lv_value NUMBER := 1;',
    '  TYPE t_ids IS TABLE OF NUMBER INDEX BY PLS_INTEGER;',
    '  CURSOR cur_demo IS',
    '    SELECT employee_id',
    '      INTO lv_value',
    '      FROM employees',
    '     WHERE employee_id BETWEEN 1 AND 10',
    '     FOR UPDATE OF employee_id;',
    'BEGIN',
    "  DBMS_OUTPUT.PUT_LINE(TO_CHAR(lv_value));",
    '  IF lv_value = 1 THEN',
    '    EXECUTE IMMEDIATE \'SELECT COUNT(*) FROM employees WHERE employee_id = :1\' INTO lv_value USING lv_value;',
    '  ELSIF lv_value = 2 THEN',
    '    NULL;',
    '  ELSE',
    '    NULL;',
    '  END IF;',
    '  RETURN lv_value;',
    'EXCEPTION',
    '  WHEN NO_DATA_FOUND THEN',
    '    RAISE_APPLICATION_ERROR(-20001, \'missing\');',
    'END;',
    '/',
  ].join('\n');

  const constructs = analyzeConstructs(sql);
  const evaluations = evaluatePlsqlConstructs(sql);
  const matched = new Set(evaluations.filter((entry) => entry.matched).map((entry) => entry.label));

  for (const label of [
    'CREATE OR REPLACE FUNCTION ... RETURN NUMBER',
    'IF ... THEN ... ELSIF ... ELSE ... END IF',
    'EXECUTE IMMEDIATE ... INTO ... USING ...',
    'TYPE ... IS TABLE OF ... INDEX BY ...',
    'CURSOR ... IS ...',
    'FOR UPDATE OF ...',
    'BETWEEN',
    'DBMS_OUTPUT.PUT_LINE',
    'TO_CHAR',
    'COUNT(*)',
    'EXCEPTION ... WHEN NO_DATA_FOUND THEN ...',
    'RAISE_APPLICATION_ERROR ...',
  ]) {
    assert.ok(matched.has(label), `expected ${label} to be matched`);
    assert.ok(constructs.includes(label), `expected ${label} in construct output`);
  }

  const executeImmediateIntoUsing = evaluations.find((entry) => entry.label === 'EXECUTE IMMEDIATE ... INTO ... USING ...');
  assert.ok(executeImmediateIntoUsing?.matched);
  assert.equal(typeof executeImmediateIntoUsing.line, 'number');
  assert.match(String(executeImmediateIntoUsing.matchedText), /EXECUTE IMMEDIATE/i);
});

test('analyzeReasoningTypes and evaluateReasoningTypes detect representative PDF reasoning types', () => {
  const sql = [
    'CREATE OR REPLACE PACKAGE pkg_demo IS',
    '  FUNCTION sf_calc(p_id IN NUMBER DEFAULT 1) RETURN NUMBER DETERMINISTIC;',
    '  FUNCTION sf_calc(p_name IN VARCHAR2 DEFAULT \'x\') RETURN NUMBER DETERMINISTIC;',
    'END;',
    '/',
    'CREATE OR REPLACE PACKAGE BODY pkg_demo IS',
    '  TYPE t_numbers IS TABLE OF NUMBER INDEX BY PLS_INTEGER;',
    '  SUBTYPE short_name_t IS VARCHAR2(30);',
    '  FUNCTION sf_calc(p_id IN NUMBER DEFAULT 1) RETURN NUMBER DETERMINISTIC IS',
    '    lv_total NUMBER := 0;',
    '    lv_label short_name_t := COALESCE(NULLIF(\'A\', \'B\'), \'C\');',
    '    l_values t_numbers;',
    '  BEGIN',
    '    FOR rec IN (SELECT employee_id, salary FROM employees WHERE employee_id BETWEEN 1 AND 10) LOOP',
    '      lv_total := lv_total + rec.salary;',
    '      EXIT WHEN lv_total > 5000;',
    '    END LOOP;',
    "    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM employees WHERE employee_id = :1' INTO lv_total USING p_id;",
    '    DBMS_OUTPUT.PUT_LINE(TO_CHAR(lv_total));',
    '    RETURN lv_total;',
    '  EXCEPTION',
    '    WHEN NO_DATA_FOUND THEN',
    "      -- RCA Note: missing employee row",
    '      RAISE_APPLICATION_ERROR(-20001, \'missing\');',
    '  END;',
    '',
    '  FUNCTION sf_calc(p_name IN VARCHAR2 DEFAULT \'x\') RETURN NUMBER DETERMINISTIC IS',
    '    lv_total NUMBER := 0;',
    '  BEGIN',
    '    SAVEPOINT before_update;',
    '    IF lv_total = 0 THEN',
    '      lv_total := 1;',
    '    END IF;',
    '    UPDATE employees SET salary = salary WHERE employee_id = 1;',
    '    RETURN lv_total;',
    '  END;',
    'END;',
    '/',
  ].join('\n');

  const reasoningTypes = analyzeReasoningTypes(sql);
  const evaluations = evaluateReasoningTypes(sql);
  const matched = new Set(evaluations.filter((entry) => entry.matched).map((entry) => entry.label));

  for (const label of [
    'Conditional Derivation',
    'Decision Logic',
    'Structural & Scope',
    'Memory & Type',
    'Data Retrieval',
    'Iterative',
    'Control Flow',
    'Data Manipulation',
    'Transaction Management',
    'Dynamic SQL',
    'Exception Handling',
    'Debugging',
    'Subprogram Overloading',
    'Function Purity',
    'Encapsulation',
    'Compile-Time Checking',
    'Root Cause Analysis',
    'Collections',
    'Validation',
    'Aggregation',
  ]) {
    assert.ok(matched.has(label), `expected ${label} to be matched`);
    assert.ok(reasoningTypes.includes(label), `expected ${label} in reasoning type output`);
  }

  const overloading = evaluations.find((entry) => entry.label === 'Subprogram Overloading');
  assert.ok(overloading?.matched);
  assert.equal(typeof overloading.line, 'number');
});

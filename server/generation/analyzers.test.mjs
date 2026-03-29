import assert from 'node:assert/strict';
import test from 'node:test';

import { analyzeColumns, analyzeConstructs, analyzeReasoningTypes, analyzeTables, evaluatePlsqlConstructs, evaluateReasoningTypes } from './analyzers.mjs';
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
    "      -- RCA: missing employee row",
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

test('construct analyzer avoids known false positives and captures SQL case/loop constructs', () => {
  const sql = [
    'CREATE OR REPLACE FUNCTION sf_demo RETURN NUMBER IS',
    '  CURSOR cur_demo IS',
    '    SELECT ROUND(CASE WHEN SUM(quantity_sold) = 0 THEN NULL ELSE SUM(amount_sold) / SUM(quantity_sold) END, 2) AS realized_avg_price',
    '      FROM sales s',
    '      INNER JOIN times t ON t.time_id = s.time_id',
    '     WHERE t.calendar_year = 2022',
    '       OR t.calendar_year = 2023;',
    '  lv_rows NUMBER := 0;',
    '  lv_diff_pct NUMBER := ABS(10 - 3);',
    'BEGIN',
    '  LOOP',
    '    FETCH cur_demo INTO lv_rows;',
    '    EXIT WHEN cur_demo%NOTFOUND;',
    '    UPDATE costs',
    '       SET unit_price = unit_price',
    '     WHERE prod_id = 1;',
    '    lv_rows := lv_rows + SQL%ROWCOUNT;',
    '  END LOOP;',
    '  RETURN lv_rows;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(!constructs.has('SELF JOIN'));
  assert.ok(!constructs.has('%ROWCOUNT'));
  assert.ok(constructs.has('SQL%ROWCOUNT'));
  assert.ok(constructs.has('LOOP ... END LOOP'));
  assert.ok(constructs.has('CASE ... WHEN ... END'));
  assert.ok(constructs.has('OR'));
  assert.ok(constructs.has('ROUND()'));
  assert.ok(constructs.has('ABS()'));
  assert.ok(constructs.has('RETURN'));
});

test('construct analyzer detects plain LEFT JOIN separately from LEFT OUTER JOIN', () => {
  const sql = [
    'CREATE OR REPLACE PROCEDURE sp_demo IS',
    '  lv_total NUMBER := 0;',
    'BEGIN',
    '  SELECT NVL(SUM(p.payment_amount), 0)',
    '    INTO lv_total',
    '    FROM delivery_center.payments p',
    '    LEFT JOIN delivery_center.orders o',
    '      ON p.payment_order_id = o.payment_order_id',
    "   WHERE p.payment_method = 'DEBIT';",
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('LEFT JOIN'));
  assert.ok(!constructs.has('LEFT OUTER JOIN'));
});

test('construct analyzer distinguishes ELSIF-with-ELSE from ELSIF-without-ELSE and detects nested blocks', () => {
  const sql = [
    'CREATE OR REPLACE PROCEDURE sp_commit_bowling_summary (',
    '    p_match_id IN NUMBER',
    ')',
    'IS',
    '    lv_role VARCHAR2(30);',
    'BEGIN',
    '    LOOP',
    '        SAVEPOINT sp_row_save;',
    '        BEGIN',
    '            IF p_match_id <= 7 THEN',
    "                lv_role := 'All Rounder';",
    '            ELSIF p_match_id <= 14 THEN',
    "                lv_role := 'Wicket Taker';",
    '            ELSIF p_match_id <= 21 THEN',
    "                lv_role := 'Economy Bowler';",
    '            ELSE',
    "                lv_role := 'Contributor';",
    '            END IF;',
    '        EXCEPTION',
    '            WHEN OTHERS THEN',
    '                ROLLBACK TO SAVEPOINT sp_row_save;',
    '        END;',
    '        EXIT;',
    '    END LOOP;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('IF ... THEN ... ELSIF ... ELSE ... END IF'));
  assert.ok(!constructs.has('IF ... THEN ... ELSIF ... END IF'));
  assert.ok(constructs.has('BEGIN ... END (nested block)'));
});

test('construct analyzer detects IF ... THEN ... ELSE ... END IF and plain JOIN ... ON ...', () => {
  const sql = [
    'CREATE OR REPLACE PACKAGE BODY pkg_demo IS',
    '  PROCEDURE sp_demo IS',
    '    CURSOR c_terms IS',
    '      SELECT t.id_bioguide',
    '        FROM city_legislation.legislators_terms t',
    '        JOIN city_legislation.legislators l ON t.id_bioguide = l.id_bioguide',
    "       WHERE t.state = 'WA';",
    '    v_senior_tag VARCHAR2(30);',
    '  BEGIN',
    '    FOR r_term IN c_terms LOOP',
    '      IF r_term.id_bioguide IS NOT NULL THEN',
    "        v_senior_tag := '[SENIOR MEMBER] ';",
    '      ELSE',
    "        v_senior_tag := '';",
    '      END IF;',
    '    END LOOP;',
    '  END;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('IF ... THEN ... ELSE ... END IF'));
  assert.ok(constructs.has('JOIN ... ON ...'));
});

test('construct analyzer does not treat WHILE loops as generic LOOP constructs', () => {
  const sql = [
    'CREATE OR REPLACE PROCEDURE sp_demo IS',
    '  lv_value NUMBER := 1;',
    'BEGIN',
    '  WHILE lv_value > 0 LOOP',
    '    lv_value := lv_value - 1;',
    '  END LOOP;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('WHILE ... LOOP ... END LOOP'));
  assert.ok(!constructs.has('LOOP ... END LOOP'));
});

test('reasoning analyzer avoids package spec/body overloading false positives and aggregate count collections false positives', () => {
  const sql = [
    'CREATE OR REPLACE PACKAGE pkg_demo IS',
    '  FUNCTION sf_count_suspicious_costs(p_calendar_year IN NUMBER, p_channel_desc IN VARCHAR2) RETURN NUMBER;',
    '  PROCEDURE sp_print_suspicious_sample(p_calendar_year IN NUMBER, p_channel_desc IN VARCHAR2);',
    'END pkg_demo;',
    '/',
    'CREATE OR REPLACE PACKAGE BODY pkg_demo IS',
    '  FUNCTION sf_count_suspicious_costs(p_calendar_year IN NUMBER, p_channel_desc IN VARCHAR2) RETURN NUMBER IS',
    '    lv_cnt NUMBER;',
    '  BEGIN',
    '    SELECT COUNT(*) INTO lv_cnt FROM costs c INNER JOIN times t ON t.time_id = c.time_id WHERE c.unit_cost IS NULL OR c.unit_price IS NULL;',
    '    RETURN lv_cnt;',
    '  END sf_count_suspicious_costs;',
    '',
    '  PROCEDURE sp_print_suspicious_sample(p_calendar_year IN NUMBER, p_channel_desc IN VARCHAR2) IS',
    '  BEGIN',
    '    NULL;',
    '  END sp_print_suspicious_sample;',
    'END pkg_demo;',
    '/',
  ].join('\n');

  const reasoningTypes = new Set(analyzeReasoningTypes(sql));

  assert.ok(!reasoningTypes.has('Collections'));
  assert.ok(!reasoningTypes.has('Subprogram Overloading'));
  assert.ok(reasoningTypes.has('Aggregation'));
  assert.ok(reasoningTypes.has('Encapsulation'));
});

test('column analyzer attributes ambiguous bare WHERE columns in UPDATE statements to the update target table', () => {
  const schema = {
    database: 'IPL',
    _schema_definition: { column_format: ['name'] },
    tables: {
      BALL_BY_BALL: {
        columns: [['MATCH_ID'], ['BOWLER'], ['OVER_ID'], ['BALL_ID'], ['INNINGS_NO']],
      },
      PLAYER: {
        columns: [['PLAYER_ID'], ['PLAYER_NAME']],
      },
      PLAYER_MATCH: {
        columns: [['MATCH_ID'], ['PLAYER_ID'], ['ROLE']],
      },
      WICKET_TAKEN: {
        columns: [['MATCH_ID'], ['OVER_ID'], ['BALL_ID'], ['INNINGS_NO'], ['PLAYER_OUT']],
      },
    },
  };

  const sql = [
    'CREATE OR REPLACE PROCEDURE sp_commit_bowling_summary (',
    '    p_match_id IN IPL.BALL_BY_BALL.match_id%TYPE',
    ')',
    'IS',
    '    CURSOR cur_bowler_stats IS',
    '        SELECT bbb.bowler AS bowler_id,',
    '               pl.player_name AS player_name,',
    '               COUNT(wt.player_out) AS wicket_count',
    '        FROM IPL.BALL_BY_BALL bbb',
    '        INNER JOIN IPL.PLAYER pl ON pl.player_id = bbb.bowler',
    '        LEFT JOIN IPL.WICKET_TAKEN wt ON wt.match_id = bbb.match_id',
    '        GROUP BY bbb.bowler, pl.player_name;',
    'BEGIN',
    '    UPDATE IPL.PLAYER_MATCH',
    '    SET role = lv_role',
    '    WHERE match_id = p_match_id',
    '    AND player_id = rec_stat.bowler_id;',
    'END;',
    '/',
  ].join('\n');

  const columns = new Set(analyzeColumns(sql, schema));

  assert.ok(columns.has('IPL.PLAYER_MATCH.match_id'));
  assert.ok(columns.has('IPL.PLAYER_MATCH.player_id'));
  assert.ok(columns.has('IPL.PLAYER_MATCH.role'));
});

test('trigger analyzers detect trigger table, :NEW columns, reasoning, and trigger-specific constructs', () => {
  const schema = {
    database: 'CITY_LEGISLATION',
    _schema_definition: { column_format: ['name'] },
    tables: {
      LEGISLATORS_TERMS: {
        columns: [['ID_BIOGUIDE'], ['TERM_START'], ['TERM_END'], ['STATE']],
      },
    },
  };

  const sql = [
    'CREATE OR REPLACE TRIGGER trg_term_validate_dates',
    'BEFORE INSERT OR UPDATE ON CITY_LEGISLATION.LEGISLATORS_TERMS',
    'FOR EACH ROW',
    "-- This trigger ensures that a term's start date always precedes its end date.",
    'BEGIN',
    '    IF :NEW.TERM_START IS NULL OR :NEW.TERM_END IS NULL OR :NEW.TERM_START >= :NEW.TERM_END THEN',
    "        RAISE_APPLICATION_ERROR(-20001, 'Invalid Term Timeline: Start must precede End');",
    '    END IF;',
    'EXCEPTION',
    '    WHEN OTHERS THEN',
    "        DBMS_OUTPUT.PUT_LINE('Unexpected error occurred');",
    '        RAISE;',
    'END;',
    '/',
  ].join('\n');

  const tables = new Set(analyzeTables(sql, schema));
  const columns = new Set(analyzeColumns(sql, schema));
  const reasoning = new Set(analyzeReasoningTypes(sql));
  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(tables.has('LEGISLATORS_TERMS'));
  assert.ok(columns.has('CITY_LEGISLATION.LEGISLATORS_TERMS.term_start'));
  assert.ok(columns.has('CITY_LEGISLATION.LEGISLATORS_TERMS.term_end'));
  assert.ok(reasoning.has('Exception Handling'));
  assert.ok(reasoning.has('Validation'));
  assert.ok(reasoning.has('Decision Logic'));
  assert.ok(constructs.has('BEFORE INSERT OR UPDATE ON ...'));
  assert.ok(constructs.has('FOR EACH ROW'));
  assert.ok(constructs.has('IF ... THEN ... END IF'));
  assert.ok(constructs.has('RAISE_APPLICATION_ERROR ...'));
  assert.ok(constructs.has(':NEW'));
  assert.ok(constructs.has('EXCEPTION ... WHEN OTHERS THEN ...'));
});

test('construct analyzer detects combined VALUE_ERROR OR INVALID_NUMBER exception handlers', () => {
  const sql = [
    'CREATE OR REPLACE PROCEDURE sp_analyze_legislator_versatility IS',
    'BEGIN',
    '    NULL;',
    'EXCEPTION',
    '    WHEN VALUE_ERROR OR INVALID_NUMBER THEN',
    "        DBMS_OUTPUT.PUT_LINE('Data Quality Fault occurred');",
    '        RETURN;',
    '    WHEN OTHERS THEN',
    "        DBMS_OUTPUT.PUT_LINE('Unexpected error occurred');",
    '        RETURN;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('EXCEPTION ... WHEN VALUE_ERROR OR INVALID_NUMBER THEN ...'));
});

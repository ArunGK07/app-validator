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

test('construct analyzer detects trigger-specific AFTER UPDATE OF, WHEN (...), and :OLD. ... clause usage', () => {
  const sql = [
    'CREATE OR REPLACE TRIGGER trg_audit_payment_method',
    'AFTER UPDATE OF payment_method ON delivery_center.payments',
    'FOR EACH ROW',
    'WHEN (OLD.payment_method != NEW.payment_method)',
    'BEGIN',
    '  DBMS_OUTPUT.PUT_LINE(:NEW.payment_id);',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('AFTER UPDATE OF'));
  assert.ok(constructs.has('WHEN (...)'));
  assert.ok(constructs.has(':OLD. ...'));
  assert.ok(constructs.has('FOR EACH ROW'));
});

test('construct analyzer does not misclassify BULK COLLECT as SELECT ... INTO ... FROM ...', () => {
  const sql = [
    'CREATE OR REPLACE PROCEDURE analyze_driver_efficiency (',
    '    p_threshold IN NUMBER',
    ') AS',
    '    TYPE rec_driver_perf IS RECORD (driver_id NUMBER, total_distance NUMBER);',
    '    TYPE tbl_driver_perf IS TABLE OF rec_driver_perf;',
    '    lt_driver_data tbl_driver_perf;',
    'BEGIN',
    '    SELECT d.driver_id, SUM(NVL(del.delivery_distance_meters, 0))',
    '    BULK COLLECT',
    '    INTO lt_driver_data',
    '    FROM delivery_center.drivers d',
    '    LEFT JOIN delivery_center.deliveries del ON d.driver_id = del.driver_id',
    '    GROUP BY d.driver_id;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('BULK COLLECT INTO'));
  assert.ok(!constructs.has('SELECT ... INTO ... FROM ...'));
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
  assert.ok(!constructs.has('IF ... THEN ... END IF'));
  assert.ok(constructs.has('JOIN ... ON ...'));
});

test('reasoning analyzer detects Data Manipulation for aliased UPDATE ... SET statements', () => {
  const sql = [
    'DECLARE',
    '  lv_count NUMBER := 0;',
    'BEGIN',
    '  UPDATE delivery_center.orders o',
    "     SET o.order_status = 'DATA_ERROR'",
    '   WHERE o.order_id = 1;',
    '  lv_count := lv_count + 1;',
    'END;',
    '/',
  ].join('\n');

  const reasoning = new Set(analyzeReasoningTypes(sql));

  assert.ok(reasoning.has('Data Manipulation'));
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

test('construct analyzer keeps long anonymous-block matches instead of dropping them on short regex windows', () => {
  const declarePadding = Array.from({ length: 80 }, (_, index) => `  lv_pad_${index} VARCHAR2(20) := 'pad';`).join('\n');
  const ifBody = Array.from({ length: 40 }, (_, index) => `      DBMS_OUTPUT.PUT_LINE('line ${index}');`).join('\n');
  const exceptionPadding = Array.from({ length: 40 }, (_, index) => `    DBMS_OUTPUT.PUT_LINE('other ${index}');`).join('\n');
  const sql = [
    'DECLARE',
    declarePadding,
    '  CURSOR cur_customer_orders IS',
    '    SELECT order_id, order_total',
    '      FROM orders',
    '     WHERE customer_id = 101;',
    '  lv_order_id NUMBER;',
    '  lv_order_total NUMBER;',
    '  lv_found NUMBER := 0;',
    '  exp_no_orders_found EXCEPTION;',
    'BEGIN',
    '  OPEN cur_customer_orders;',
    '  LOOP',
    '    FETCH cur_customer_orders INTO lv_order_id, lv_order_total;',
    '    EXIT WHEN cur_customer_orders%NOTFOUND;',
    '    IF lv_order_total > 1000 THEN',
    ifBody,
    '    END IF;',
    '  END LOOP;',
    '  CLOSE cur_customer_orders;',
    '  IF lv_found = 0 THEN',
    '    RAISE exp_no_orders_found;',
    '  END IF;',
    '  COMMIT;',
    'EXCEPTION',
    '  WHEN exp_no_orders_found THEN',
    "    DBMS_OUTPUT.PUT_LINE('No orders found');",
    exceptionPadding,
    '  WHEN OTHERS THEN',
    "    DBMS_OUTPUT.PUT_LINE('Unexpected');",
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  for (const label of [
    'DECLARE ... BEGIN ... END',
    'EXCEPTION ... WHEN OTHERS THEN ...',
    'IF ... THEN ... END IF',
    'LOOP ... END LOOP',
    'OPEN',
    'FETCH',
    'CLOSE',
    'EXIT WHEN ...',
    '%NOTFOUND',
    'RAISE',
    'COMMIT',
  ]) {
    assert.ok(constructs.has(label), `expected ${label} to survive long-block matching`);
  }
});

test('reasoning analyzer treats conditional flow as control flow even without EXIT or RETURN', () => {
  const sql = [
    'DECLARE',
    '  exp_no_orders_found EXCEPTION;',
    'BEGIN',
    '  IF 1 = 1 THEN',
    '    RAISE exp_no_orders_found;',
    '  END IF;',
    'EXCEPTION',
    '  WHEN exp_no_orders_found THEN',
    '    NULL;',
    'END;',
    '/',
  ].join('\n');

  const reasoningTypes = new Set(analyzeReasoningTypes(sql));

  assert.ok(reasoningTypes.has('Control Flow'));
  assert.ok(reasoningTypes.has('Decision Logic'));
  assert.ok(reasoningTypes.has('Exception Handling'));
});

test('construct analyzer detects TO_TIMESTAMP calls in predicates', () => {
  const sql = [
    'CREATE OR REPLACE PROCEDURE sp_filter_recent_orders IS',
    'BEGIN',
    '  FOR rec IN (',
    '    SELECT o.order_id',
    '      FROM orders o',
    "     WHERE TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF') >= TO_TIMESTAMP('2018-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF')",
    '  ) LOOP',
    '    NULL;',
    '  END LOOP;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));

  assert.ok(constructs.has('TO_TIMESTAMP'));
});

test('analysis normalization does not treat double hyphens inside string literals as SQL comments', () => {
  const sql = [
    'DECLARE',
    '  CURSOR cur_customer_orders IS SELECT 1 AS total_payment FROM dual;',
    '  lv_total_orders NUMBER := 0;',
    '  lv_rec cur_customer_orders%ROWTYPE;',
    '  exp_no_orders_found EXCEPTION;',
    'BEGIN',
    "  DBMS_OUTPUT.PUT_LINE('--- ORDER ANALYSIS STARTED ---');",
    '  OPEN cur_customer_orders;',
    '  LOOP',
    '    FETCH cur_customer_orders INTO lv_rec;',
    '    EXIT WHEN cur_customer_orders%NOTFOUND;',
    '    IF lv_rec.total_payment > 500 THEN',
    '      NULL;',
    '    END IF;',
    '  END LOOP;',
    '  CLOSE cur_customer_orders;',
    "  DBMS_OUTPUT.PUT_LINE('--- ANALYSIS COMPLETED SUCCESSFULLY ---');",
    '  IF lv_total_orders = 0 THEN',
    '    RAISE exp_no_orders_found;',
    '  END IF;',
    '  COMMIT;',
    'EXCEPTION',
    '  WHEN exp_no_orders_found THEN',
    '    ROLLBACK;',
    '  WHEN OTHERS THEN',
    '    ROLLBACK;',
    'END;',
    '/',
  ].join('\n');

  const constructs = new Set(analyzeConstructs(sql));
  const reasoningTypes = new Set(analyzeReasoningTypes(sql));

  for (const label of [
    'COMMIT',
    'LOOP ... END LOOP',
    'OPEN',
    'FETCH',
    'CLOSE',
    '%NOTFOUND',
    'EXIT WHEN ...',
    'IF ... THEN ... END IF',
    'RAISE',
    'DECLARE ... BEGIN ... END',
    'EXCEPTION ... WHEN OTHERS THEN ...',
  ]) {
    assert.ok(constructs.has(label), `expected ${label} to remain visible after normalization`);
  }

  for (const label of ['Exception Handling', 'Control Flow', 'Iterative']) {
    assert.ok(reasoningTypes.has(label), `expected ${label} to remain visible after normalization`);
  }
});

test('trigger analyzers include UPDATE OF target tables, detect :OLD. ... usage, and avoid DML false positives from trigger headers', () => {
  const schema = {
    database: 'DELIVERY_CENTER',
    _schema_definition: { column_format: ['name'] },
    tables: {
      PAYMENTS: {
        columns: [['PAYMENT_ID'], ['PAYMENT_METHOD'], ['PAYMENT_ORDER_ID']],
      },
      ORDERS: {
        columns: [['ORDER_ID'], ['STORE_ID']],
      },
      STORES: {
        columns: [['STORE_ID'], ['HUB_ID']],
      },
      HUBS: {
        columns: [['HUB_ID'], ['HUB_NAME']],
      },
    },
  };

  const sql = [
    'CREATE OR REPLACE TRIGGER trg_audit_payment_method',
    'AFTER UPDATE OF payment_method ON delivery_center.payments',
    'FOR EACH ROW',
    'WHEN (OLD.payment_method != NEW.payment_method)',
    'DECLARE',
    '    lv_hub_name VARCHAR2(255);',
    'BEGIN',
    '    SELECT h.hub_name',
    '      INTO lv_hub_name',
    '      FROM delivery_center.orders o',
    '      JOIN delivery_center.stores s ON o.store_id = s.store_id',
    '      JOIN delivery_center.hubs h ON s.hub_id = h.hub_id',
    '     WHERE o.order_id = :NEW.payment_order_id;',
    'EXCEPTION',
    '    WHEN OTHERS THEN',
    "        DBMS_OUTPUT.PUT_LINE('Unexpected error occurred');",
    'END;',
    '/',
  ].join('\n');

  const tables = new Set(analyzeTables(sql, schema));
  const constructs = new Set(analyzeConstructs(sql));
  const reasoning = new Set(analyzeReasoningTypes(sql));

  assert.ok(tables.has('PAYMENTS'));
  assert.ok(tables.has('ORDERS'));
  assert.ok(tables.has('STORES'));
  assert.ok(tables.has('HUBS'));
  assert.ok(constructs.has(':OLD. ...'));
  assert.ok(constructs.has('WHEN (...)'));
  assert.ok(!reasoning.has('Data Manipulation'));
  assert.ok(reasoning.has('Event-Driven Logic'));
});

test('construct analyzer detects LEAST() and does not emit generic IN for anonymous loop-only usage', () => {
  const procedureSql = [
    'CREATE OR REPLACE PROCEDURE analyze_driver_efficiency (',
    '    p_threshold IN NUMBER',
    ') AS',
    'BEGIN',
    '    FOR i IN 1..LEAST(10, 50) LOOP',
    '        NULL;',
    '    END LOOP;',
    'END;',
    '/',
  ].join('\n');
  const anonymousSql = [
    'DECLARE',
    '  lv_value NUMBER := 1;',
    'BEGIN',
    '  FOR rec_order IN (SELECT 1 AS order_id FROM dual) LOOP',
    '    lv_value := rec_order.order_id;',
    '  END LOOP;',
    'END;',
    '/',
  ].join('\n');

  const procedureConstructs = new Set(analyzeConstructs(procedureSql));
  const anonymousConstructs = new Set(analyzeConstructs(anonymousSql));

  assert.ok(procedureConstructs.has('LEAST()'));
  assert.ok(procedureConstructs.has('IN'));
  assert.ok(!anonymousConstructs.has('IN'));
});

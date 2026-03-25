import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { runPromptStructureValidator } from './prompt-structure-validator.mjs';

test('runPromptStructureValidator allows flat single-program sections for one anonymous block header', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-prompt-structure-'));
  const taskDir = join(root, '24696');
  const metadata = {
    id: 24696,
    num_turns: 1,
    required_anonymous_block: true,
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '24696_turn1_1user.txt'),
      [
        'Retrieve and validate population indicator records.',
        'Requirements:',
        'Anonymous Block:',
        '',
        'Parameters:',
        '\tlv_year - LOCAL - NUMBER -- year used to filter population indicator records',
        '',
        'Output:',
        '\tCountry Code: [country_code]',
        '\tCountry Name: [country_name]',
        '',
        'Sorting Order:',
        '\tcountry_code ASC',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );

    const results = await runPromptStructureValidator('24696', taskDir, metadata);
    const parameterFormatResult = results.find((entry) => entry.item === 'Parameters Format');
    const outputFormatResult = results.find((entry) => entry.item === 'Output Format');
    const sortingFormatResult = results.find((entry) => entry.item === 'Sorting Order Format');
    const exceptionFormatResult = results.find((entry) => entry.item === 'Exception Handling Format');

    assert.ok(parameterFormatResult);
    assert.equal(parameterFormatResult.status, 'PASS');
    assert.notEqual(parameterFormatResult.ruleId, 'missing_parameter_group_headers');
    assert.ok(outputFormatResult);
    assert.equal(outputFormatResult.status, 'PASS');
    assert.notEqual(outputFormatResult.ruleId, 'missing_output_groups');
    assert.ok(sortingFormatResult);
    assert.equal(sortingFormatResult.status, 'PASS');
    assert.notEqual(sortingFormatResult.ruleId, 'missing_sorting_groups');
    assert.ok(exceptionFormatResult);
    assert.equal(exceptionFormatResult.status, 'PASS');
    assert.notEqual(exceptionFormatResult.ruleId, 'missing_exception_groups');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runPromptStructureValidator flags prompt/code parameter datatype drift', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-prompt-structure-'));
  const taskDir = join(root, '24710');
  const metadata = {
    id: 24710,
    num_turns: 1,
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '24710_turn1_1user.txt'),
      [
        'Return one customer code for a given case id.',
        'Requirements:',
        'Function Name:',
        'sf_lookup_customer_code',
        '',
        'Parameters:',
        '\tp_case_id - IN - NUMBER -- case identifier',
        '',
        'Output:',
        '\tCustomer Code: [customer_code]',
        '',
        'Exception Handling:',
        '\tNO_DATA_FOUND : No customer found',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '24710_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE FUNCTION sf_lookup_customer_code(',
        '  p_case_id IN VARCHAR2',
        ') RETURN VARCHAR2 IS',
        '  lv_customer_code VARCHAR2(30);',
        'BEGIN',
        '  SELECT customer_code INTO lv_customer_code FROM demo_customers WHERE case_id = p_case_id;',
        '  RETURN lv_customer_code;',
        'EXCEPTION',
        '  WHEN NO_DATA_FOUND THEN',
        "    RETURN 'No customer found';",
        '  WHEN OTHERS THEN',
        "    RETURN 'Unexpected error occurred';",
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );

    const results = await runPromptStructureValidator('24710', taskDir, metadata);

    assert.ok(results.some((entry) => entry.ruleId === 'parameter_datatype_mismatch' && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runPromptStructureValidator flags prompt-required TOO_MANY_ROWS handlers that cannot fire', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-prompt-structure-'));
  const taskDir = join(root, '24711');
  const metadata = {
    id: 24711,
    num_turns: 1,
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '24711_turn1_1user.txt'),
      [
        'Fetch one order summary by id.',
        'Requirements:',
        'Procedure Name:',
        'sp_emit_order_summary',
        '',
        'Parameters:',
        '\tp_order_id - IN - NUMBER -- order identifier',
        '',
        'Output:',
        '\tOrder ID: [order_id]',
        '',
        'Exception Handling:',
        '\tTOO_MANY_ROWS : Multiple orders found',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '24711_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE PROCEDURE sp_emit_order_summary(',
        '  p_order_id IN NUMBER',
        ') IS',
        '  lv_order_count NUMBER;',
        'BEGIN',
        '  SELECT COUNT(*) INTO lv_order_count FROM demo_orders WHERE order_id = p_order_id;',
        "  DBMS_OUTPUT.PUT_LINE('Order ID: ' || p_order_id);",
        'EXCEPTION',
        '  WHEN TOO_MANY_ROWS THEN',
        "    DBMS_OUTPUT.PUT_LINE('Multiple orders found');",
        '  WHEN OTHERS THEN',
        "    DBMS_OUTPUT.PUT_LINE('Unexpected error occurred');",
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );

    const results = await runPromptStructureValidator('24711', taskDir, metadata);

    assert.ok(results.some((entry) => entry.item === 'TOO_MANY_ROWS Contract' && entry.ruleId === 'contract_mismatch' && entry.status === 'FAIL'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runPromptStructureValidator requires query headers when one program has multiple ordered queries', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-prompt-structure-'));
  const taskDir = join(root, '24712');
  const metadata = {
    id: 24712,
    num_turns: 1,
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '24712_turn1_1user.txt'),
      [
        'Return sorted warehouse details.',
        'Requirements:',
        'Function Name:',
        'sf_fetch_bins',
        '',
        'Parameters:',
        '\tp_warehouse_id - IN - NUMBER -- warehouse identifier',
        '',
        'Output:',
        '\tWarehouse: [warehouse]',
        '\tAisle: [aisle]',
        '',
        'Sorting Order:',
        '\twarehouse ASC',
        '\taisle DESC',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '24712_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE FUNCTION sf_fetch_bins(',
        '  p_warehouse_id IN NUMBER',
        ') RETURN SYS_REFCURSOR IS',
        '  l_cursor SYS_REFCURSOR;',
        'BEGIN',
        '  OPEN l_cursor FOR',
        '    SELECT warehouse, aisle',
        '      FROM inventory_bins',
        '     WHERE warehouse_id = p_warehouse_id',
        '     ORDER BY warehouse ASC;',
        '',
        '  FOR rec IN (',
        '    SELECT warehouse, aisle',
        '      FROM inventory_bins',
        '     WHERE warehouse_id = p_warehouse_id',
        '     ORDER BY aisle DESC',
        '  ) LOOP',
        '    NULL;',
        '  END LOOP;',
        '',
        '  RETURN l_cursor;',
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        '    RAISE;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );

    const results = await runPromptStructureValidator('24712', taskDir, metadata);
    const sortingContract = results.find((entry) => entry.item === 'Sorting Order Contract');

    assert.ok(sortingContract);
    assert.equal(sortingContract.status, 'FAIL');
    assert.equal(sortingContract.ruleId, 'missing_sorting_clause');
    assert.match(sortingContract.present ?? '', /SF_FETCH_BINS has 2 ordered queries in SQL/i);
    assert.match(sortingContract.present ?? '', /SF_FETCH_BINS -> Query 1 -> ORDER BY WAREHOUSE ASC/i);
    assert.match(sortingContract.present ?? '', /SF_FETCH_BINS -> Query 2 -> ORDER BY AISLE DESC/i);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('runPromptStructureValidator accepts query-scoped sorting details for multiple ordered queries in one program', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-prompt-structure-'));
  const taskDir = join(root, '24713');
  const metadata = {
    id: 24713,
    num_turns: 1,
  };

  try {
    await mkdir(taskDir, { recursive: true });
    await writeFile(
      join(taskDir, '24713_turn1_1user.txt'),
      [
        'Return sorted warehouse details.',
        'Requirements:',
        'Function Name:',
        'sf_fetch_bins',
        '',
        'Parameters:',
        '\tp_warehouse_id - IN - NUMBER -- warehouse identifier',
        '',
        'Output:',
        '\tWarehouse: [warehouse]',
        '\tAisle: [aisle]',
        '',
        'Sorting Order:',
        '\tSummary Cursor:',
        '\twarehouse ASC',
        '\tPrinted Loop:',
        '\taisle DESC',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );
    await writeFile(
      join(taskDir, '24713_turn1_4referenceAnswer.sql'),
      [
        'CREATE OR REPLACE FUNCTION sf_fetch_bins(',
        '  p_warehouse_id IN NUMBER',
        ') RETURN SYS_REFCURSOR IS',
        '  l_cursor SYS_REFCURSOR;',
        'BEGIN',
        '  OPEN l_cursor FOR',
        '    SELECT warehouse, aisle',
        '      FROM inventory_bins',
        '     WHERE warehouse_id = p_warehouse_id',
        '     ORDER BY warehouse ASC;',
        '',
        '  FOR rec IN (',
        '    SELECT warehouse, aisle',
        '      FROM inventory_bins',
        '     WHERE warehouse_id = p_warehouse_id',
        '     ORDER BY aisle DESC',
        '  ) LOOP',
        '    NULL;',
        '  END LOOP;',
        '',
        '  RETURN l_cursor;',
        'EXCEPTION',
        '  WHEN OTHERS THEN',
        '    RAISE;',
        'END;',
        '/',
      ].join('\n'),
      'utf8',
    );

    const results = await runPromptStructureValidator('24713', taskDir, metadata);
    const sortingContract = results.find((entry) => entry.item === 'Sorting Order Contract');

    assert.ok(sortingContract);
    assert.equal(sortingContract.status, 'PASS');
    assert.equal(sortingContract.ruleId, 'sorting_clause_present');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

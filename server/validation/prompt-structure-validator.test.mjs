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

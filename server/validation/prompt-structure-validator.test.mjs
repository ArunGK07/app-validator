import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { runPromptStructureValidator } from './prompt-structure-validator.mjs';

test('runPromptStructureValidator allows flat Parameters for a single anonymous block', async () => {
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
        'Requirements:',
        '\tAnonymous Block:',
        '',
        'Parameters:',
        '\tlv_year - LOCAL - NUMBER -- year used to filter population indicator records',
        '',
        'Output:',
        '\tCountry Code: [country_code]',
        '',
        'Exception Handling:',
        '\tOther Exception : Unexpected error occurred',
      ].join('\n'),
      'utf8',
    );

    const results = await runPromptStructureValidator('24696', taskDir, metadata);
    const parameterFormatResult = results.find((entry) => entry.item === 'Parameters Format');

    assert.ok(parameterFormatResult);
    assert.equal(parameterFormatResult.status, 'PASS');
    assert.notEqual(parameterFormatResult.ruleId, 'missing_parameter_group_headers');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

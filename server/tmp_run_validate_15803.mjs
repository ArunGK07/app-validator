import { runNativeValidation } from './validation/engine.mjs';
import { join } from 'node:path';

(async function(){
  const taskId = '15803';
  const taskDir = join('..','task-output', taskId);
  const logFile = join(taskDir, '_logs', 'tmp_validate_15803.log');
  try {
    const result = await runNativeValidation(taskId, taskDir, logFile);
    console.log('Validation result:', JSON.stringify(result, null, 2));
  } catch (err) {
    console.error('ERROR running validator', err);
    process.exit(2);
  }
})();

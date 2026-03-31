import { runNativeGenerateOutputs } from './generation/engine.mjs';
import { join } from 'node:path';

(async function(){
  const taskId = '15803';
  const taskDir = join('..','task-output', taskId);
  const logFile = join(taskDir, '_logs', 'tmp_generate_15803.log');
  try {
    const result = await runNativeGenerateOutputs(taskId, taskDir, logFile, {});
    console.log('Generate result:', JSON.stringify(result, null, 2));
  } catch (err) {
    console.error('ERROR running generator', err);
    process.exit(2);
  }
})();

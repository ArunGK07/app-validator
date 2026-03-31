import { readFile } from 'node:fs/promises';
import { analyzeConstructsHighSignal, analyzeReasoningTypes } from './generation/analyzers.mjs';

(async function(){
  try {
    const sql = await readFile('../task-output/15803/15803_turn2_4referenceAnswer.sql', 'utf8');
    const constructs = analyzeConstructsHighSignal(sql);
    const reasoning = analyzeReasoningTypes(sql);
    console.log(JSON.stringify({ constructs, reasoning }, null, 2));
  } catch (err) {
    console.error('ERROR', err);
    process.exit(2);
  }
})();

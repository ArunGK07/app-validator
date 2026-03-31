import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { readRuntimeConfig } from './turing-api.mjs';
import { runTaskWorkflowAction } from './task-workflows.mjs';

function loadLocalEnvFile(fileName) {
  const baseDir = dirname(fileURLToPath(import.meta.url));
  const envPath = join(baseDir, '..', fileName);
  if (!existsSync(envPath)) {
    return;
  }
  const content = readFileSync(envPath, 'utf8');
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) {
      continue;
    }
    const separatorIndex = line.indexOf('=');
    if (separatorIndex <= 0) {
      continue;
    }
    const key = line.slice(0, separatorIndex).trim();
    let value = line.slice(separatorIndex + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (!(key in process.env)) {
      process.env[key] = value;
    }
  }
}

(async function main() {
  loadLocalEnvFile('.env.local');
  const config = readRuntimeConfig(process.env);
  const result = await runTaskWorkflowAction('validate', '15793', config);
  console.log(JSON.stringify(result, null, 2));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});

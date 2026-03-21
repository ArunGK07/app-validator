import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';

import { fetchTaskOutputArtifacts } from './task-output-fetcher.mjs';

test('fetchTaskOutputArtifacts writes existing_output and extracted turn files', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-fetch-output-'));
  const originalFetch = global.fetch;

  global.fetch = async (input, init) => {
    assert.equal(String(input), 'https://rlhf-api.turing.com/graphql');
    assert.equal(init?.method, 'POST');
    assert.equal(init?.headers?.Cookie, 'cookie=value');

    return new Response(
      JSON.stringify({
        data: {
          prompt: {
            id: 'prompt-123',
            type: 'PROMPT',
            formData: {
              feedback: null,
            },
            promptTurns: [
              {
                promptIndex: 0,
                promptEvaluationFeedback: {
                  promptTurnEvaluation: [
                    { name: 'User', value: 'Prompt turn 1' },
                    { name: 'Reference Answer', value: 'select 1 from dual;' },
                  ],
                },
              },
              {
                promptIndex: 1,
                promptEvaluationFeedback: {
                  promptTurnEvaluation: [
                    { name: 'Tables', value: 'EMP, DEPT' },
                    { name: 'Test Cases', value: 'begin null; end;' },
                  ],
                },
              },
            ],
          },
        },
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } },
    );
  };

  try {
    const result = await fetchTaskOutputArtifacts(
      {
        taskId: '24696',
        collabLink: 'https://rlhf-v3.turing.com/prompt/prompt-123',
      },
      {
        cookie: 'cookie=value',
        taskOutputDir: root,
        rlhfGraphqlUrl: 'https://rlhf-api.turing.com/graphql',
      },
    );

    assert.equal(result.taskId, '24696');
    assert.equal(result.promptId, 'prompt-123');
    assert.equal(result.metadataFile, null);
    assert.deepEqual(result.generatedFiles, [
      '24696_turn1_1user.txt',
      '24696_turn1_4referenceAnswer.sql',
      '24696_turn2_2tables.txt',
      '24696_turn2_5testCases.sql',
    ]);

    const existingOutput = JSON.parse(await readFile(join(root, '24696', '24696_existing_output.json'), 'utf8'));
    assert.equal(existingOutput.task_id, 24696);
    assert.equal(existingOutput.prompt_id, 'prompt-123');

    assert.equal(await readFile(join(root, '24696', '24696_turn1_1user.txt'), 'utf8'), 'Prompt turn 1\n');
    assert.equal(await readFile(join(root, '24696', '24696_turn1_4referenceAnswer.sql'), 'utf8'), 'select 1 from dual;\n');
  } finally {
    global.fetch = originalFetch;
    await rm(root, { recursive: true, force: true });
  }
});

test('fetchTaskOutputArtifacts falls back to reviews API when prompt metadata is missing', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-fetch-output-'));
  const originalFetch = global.fetch;

  global.fetch = async (input, init) => {
    const url = String(input);

    if (url === 'https://labeling-o.turing.com/api/reviews?conversationId=9462&join%5B0%5D=conversation') {
      assert.equal(init?.headers?.Cookie, 'cookie=value');

      return new Response(
        JSON.stringify({
          data: [
            {
              conversation: {
                id: 9462,
                colabLink: 'https://rlhf-v3.turing.com/prompt/recovered-prompt-9462',
              },
            },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    if (url === 'https://rlhf-api.turing.com/graphql') {
      return new Response(
        JSON.stringify({
          data: {
            prompt: {
              id: 'recovered-prompt-9462',
              type: 'PROMPT',
              formData: {
                feedback: null,
              },
              promptTurns: [
                {
                  promptIndex: 0,
                  promptEvaluationFeedback: {
                    promptTurnEvaluation: [{ name: 'User', value: 'Recovered prompt body' }],
                  },
                },
              ],
            },
          },
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    throw new Error(`Unexpected URL ${url}`);
  };

  try {
    const result = await fetchTaskOutputArtifacts(
      {
        taskId: '9462',
      },
      {
        cookie: 'cookie=value',
        labelingBaseUrl: 'https://labeling-o.turing.com',
        taskOutputDir: root,
        rlhfGraphqlUrl: 'https://rlhf-api.turing.com/graphql',
      },
    );

    assert.equal(result.taskId, '9462');
    assert.equal(result.promptId, 'recovered-prompt-9462');
    assert.equal(result.collabLink, 'https://rlhf-v3.turing.com/prompt/recovered-prompt-9462');
    assert.equal(result.metadataFile, null);
    assert.deepEqual(result.generatedFiles, ['9462_turn1_1user.txt']);
    assert.equal(await readFile(join(root, '9462', '9462_turn1_1user.txt'), 'utf8'), 'Recovered prompt body\n');
  } finally {
    global.fetch = originalFetch;
    await rm(root, { recursive: true, force: true });
  }
});

test('fetchTaskOutputArtifacts writes the metadata file and includes the schema artifact when task metadata is available', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-fetch-output-'));
  const originalFetch = global.fetch;

  global.fetch = async () =>
    new Response(
      JSON.stringify({
        data: {
          prompt: {
            id: 'prompt-777',
            type: 'PROMPT',
            formData: {
              feedback: null,
            },
            promptTurns: [
              {
                promptIndex: 0,
                promptEvaluationFeedback: {
                  promptTurnEvaluation: [{ name: 'User', value: 'Schema task prompt' }],
                },
              },
            ],
          },
        },
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } },
    );

  try {
    const result = await fetchTaskOutputArtifacts(
      {
        taskId: '777',
        collabLink: 'https://rlhf-v3.turing.com/prompt/prompt-777',
        metadata: {
          dataset: 'Spider 2.0-Lite',
          database: 'GNOMAD',
        },
      },
      {
        cookie: 'cookie=value',
        taskOutputDir: root,
        rlhfGraphqlUrl: 'https://rlhf-api.turing.com/graphql',
      },
      {
        generateTaskSchemaArtifact: async (task) => {
          assert.equal(task.taskId, '777');
          assert.deepEqual(task.metadata, {
            dataset: 'Spider 2.0-Lite',
            database: 'GNOMAD',
          });
          return {
            schemaFile: 'spider_2_lite/GNOMAD.json',
          };
        },
      },
    );

    assert.equal(result.metadataFile, '777_1metadata.json');
    assert.equal(result.schemaFile, 'spider_2_lite/GNOMAD.json');
    assert.equal(result.schemaError, null);

    const metadataFile = JSON.parse(await readFile(join(root, '777', '777_1metadata.json'), 'utf8'));
    assert.deepEqual(metadataFile, {
      dataset: 'Spider 2.0-Lite',
      database: 'GNOMAD',
      id: 777,
      colabLink: 'https://rlhf-v3.turing.com/prompt/prompt-777',
    });
  } finally {
    global.fetch = originalFetch;
    await rm(root, { recursive: true, force: true });
  }
});

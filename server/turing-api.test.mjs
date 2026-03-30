import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildBatchesUrl,
  buildConversationDetailUrl,
  buildConversationEditUrl,
  buildConversationsUrl,
  buildConversationsUrls,
  buildPassthroughUrl,
  buildReviewsUrl,
  buildSchemaWarmupConversationsUrl,
  buildCurrentUserUrl,
  editConversation,
  fetchBatches,
  fetchConversation,
  fetchConversations,
  fetchTeamMembers,
  fetchRawConversations,
  findPromptIdentifier,
  getProxyHealth,
  inferBusinessStatus,
  inferCurrentStatus,
  inferComplexity,
  inferTurnCount,
  listTaskOutputFiles,
  normalizeConversation,
  readRuntimeConfig,
  readTaskOutputFile,
  resolveTaskSchemaInfo,
  summarizeMetadata,
  writeTaskOutputFile,
} from './turing-api.mjs';

const config = {
  cookie: 'cookie=value',
  labelingBaseUrl: 'https://labeling-o.turing.com',
  projectId: '57',
};

test('getProxyHealth reports missing cookie clearly', () => {
  assert.deepEqual(getProxyHealth({ cookie: '' }), {
    configured: false,
    message: 'Set TURING_COOKIE in .env.local before fetching live data.',
  });
});

test('all status uses a single broad query for the selected user', () => {
  const urls = buildConversationsUrls({ userId: '463', status: 'all', taskId: '' }, config);
  const url = new URL(urls[0]);

  assert.equal(urls.length, 1);
  assert.equal(url.searchParams.get('sort[0]'), 'updatedAt,DESC');
  assert.equal(url.searchParams.get('filter[0]'), 'batch.status||$ne||draft');
  assert.equal(url.searchParams.get('filter[1]'), 'project.status||$ne||archived');
  assert.equal(url.searchParams.get('filter[5]'), '$claimedBy||$in||463');
  assert.equal(url.searchParams.get('filter[6]'), null);
});

test('in progress uses the provided task list query and adds user and task filters', () => {
  const url = new URL(
    buildConversationsUrl({ userId: '463', status: 'in-progress', taskId: '42' }, config),
  );

  assert.equal(url.searchParams.get('limit'), '10');
  assert.equal(url.searchParams.get('filter[0]'), 'status||$eq||labeling');
  assert.equal(url.searchParams.get('filter[1]'), 'status||$in||labeling,validating');
  assert.equal(url.searchParams.get('filter[7]'), '$claimedBy||$in||463');
  assert.equal(url.searchParams.get('filter[8]'), 'id||$eq||42');
});

test('rework uses the rework query shape and sorting', () => {
  const url = new URL(buildConversationsUrl({ userId: '', status: 'rework', taskId: '' }, config));

  assert.equal(url.searchParams.get('sort[0]'), 'updatedAt,DESC');
  assert.equal(url.searchParams.get('filter[0]'), 'status||$eq||rework');
  assert.equal(url.searchParams.get('filter[1]'), 'batch.status||$ne||draft');
});

test('completed uses followup filter', () => {
  const url = new URL(buildConversationsUrl({ userId: '', status: 'completed', taskId: '' }, config));

  assert.equal(url.searchParams.get('limit'), '50');
  assert.equal(url.searchParams.get('filter[0]'), 'status||$eq||completed');
  assert.equal(url.searchParams.get('filter[1]'), '$needFollowup||$eq||true');
});

test('reviewed once uses the manual review followup false filter', () => {
  const url = new URL(buildConversationsUrl({ userId: '', status: 'reviewed-once', taskId: '' }, config));

  assert.equal(url.searchParams.get('limit'), '30');
  assert.equal(url.searchParams.get('filter[0]'), 'status||$eq||completed');
  assert.equal(url.searchParams.get('filter[2]'), 'manualReview.followupRequired||$eq||false');
});

test('selected batch adds a batch filter to the conversations query', () => {
  const url = new URL(buildConversationsUrl({ userId: '', status: 'all', taskId: '', batchId: '172,201' }, config));

  assert.equal(url.searchParams.get('filter[5]'), 'batchId||$in||172,201');
});

test('buildCurrentUserUrl points to auth me endpoint', () => {
  assert.equal(buildCurrentUserUrl(config), 'https://labeling-o.turing.com/api/auth/me');
});

test('buildBatchesUrl points to the labeling batches endpoint with project filters', () => {
  const url = new URL(buildBatchesUrl(config));

  assert.equal(url.origin + url.pathname, 'https://labeling-o.turing.com/api/batches');
  assert.equal(url.searchParams.get('sort[0]'), 'projectId,DESC');
  assert.equal(url.searchParams.get('sort[1]'), 'id,DESC');
  assert.equal(url.searchParams.get('fields'), 'id,name,status,projectId,jibbleActivity');
  assert.equal(url.searchParams.get('filter[0]'), 'status||$notin||draft');
  assert.equal(url.searchParams.get('filter[1]'), 'projectId||$eq||57');
  assert.equal(url.searchParams.get('filter[2]'), 'status||ne||archived');
  assert.equal(url.searchParams.get('join[0]'), 'batchStats');
});

test('buildConversationDetailUrl points to the single conversation endpoint with the required joins', () => {
  const url = new URL(buildConversationDetailUrl('9419', config));

  assert.equal(url.origin + url.pathname, 'https://labeling-o.turing.com/api/conversations/9419');
  assert.equal(url.searchParams.get('join[0]'), 'project||id,name,status,projectType,supportsFunctionCalling,supportsWorkflows,supportsMultipleFilesPerTask,jibbleActivity,instructionsLink,readonly,averageHandleTimeMinutes');
  assert.equal(url.searchParams.get('join[1]'), 'batch||id,name,status,projectId,jibbleActivity,maxClaimGoldenTaskAllowed,averageHandleTimeMinutes');
  assert.equal(url.searchParams.get('join[2]'), 'currentUser||id,name,turingEmail,profilePicture,isBlocked');
  assert.equal(url.searchParams.get('join[3]'), 'currentUser.teamLead||id,name,turingEmail,profilePicture,isBlocked');
  assert.equal(url.searchParams.get('join[4]'), 'seed||metadata,turingMetadata');
  assert.equal(url.searchParams.get('join[5]'), 'labels||id,labelId');
  assert.equal(url.searchParams.get('join[6]'), 'labels.label');
  assert.equal(url.searchParams.get('join[7]'), 'variations||id');
  assert.equal(url.searchParams.get('join[8]'), 'project.projectFormStages');
});


test('buildConversationEditUrl points to the conversation edit endpoint', () => {
  assert.equal(buildConversationEditUrl('15708', config), 'https://labeling-o.turing.com/api/conversations/15708/edit');
});

test('editConversation posts to the upstream edit endpoint and forwards the bearer token when available', async () => {
  const originalFetch = global.fetch;
  const requests = [];

  global.fetch = async (input, init) => {
    requests.push({ url: String(input), init });
    return new Response(null, { status: 204 });
  };

  try {
    const result = await editConversation('15708', {
      ...config,
      cookie: 'cookie=value; oracle_access_token=test-token',
    });

    assert.deepEqual(result, {});
    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, 'https://labeling-o.turing.com/api/conversations/15708/edit');
    assert.equal(requests[0].init?.method, 'POST');
    assert.equal(requests[0].init?.headers?.Cookie, 'cookie=value; oracle_access_token=test-token');
    assert.equal(requests[0].init?.headers?.Authorization, 'Bearer test-token');
  } finally {
    global.fetch = originalFetch;
  }
});

test('readRuntimeConfig picks up AUTHORIZATION from the environment', () => {
  const runtime = readRuntimeConfig({
    AUTHORIZATION: 'Bearer explicit-token',
    TURING_COOKIE: 'cookie=value',
    LABELING_API_BASE_URL: 'https://labeling-o.turing.com',
    LABELING_PROJECT_ID: '57',
  });

  assert.equal(runtime.authorizationHeader, 'Bearer explicit-token');
});

test('fetchRawConversations forwards Authorization when configured', async () => {
  const originalFetch = global.fetch;

  global.fetch = async (_input, init) => {
    assert.equal(init?.headers?.Cookie, config.cookie);
    assert.equal(init?.headers?.Authorization, 'Bearer explicit-token');

    return new Response(
      JSON.stringify({
        data: [],
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } },
    );
  };

  try {
    await fetchRawConversations(
      {
        limit: '1',
      },
      {
        ...config,
        authorizationHeader: 'Bearer explicit-token',
      },
    );
  } finally {
    global.fetch = originalFetch;
  }
});
test('buildPassthroughUrl preserves the provided query without reshaping it', () => {
  const url = new URL(
    buildPassthroughUrl(
      '/api/conversations',
      {
        'sort[0]': 'updatedAt,DESC',
        limit: '10',
        page: '1',
        'join[0]': 'seed||metadata,turingMetadata',
        'filter[0]': 'status||$eq||rework',
        'filter[1]': 'batch.status||$ne||draft',
      },
      config.labelingBaseUrl,
    ),
  );

  assert.equal(url.origin + url.pathname, 'https://labeling-o.turing.com/api/conversations');
  assert.equal(url.searchParams.get('sort[0]'), 'updatedAt,DESC');
  assert.equal(url.searchParams.get('limit'), '10');
  assert.equal(url.searchParams.get('join[0]'), 'seed||metadata,turingMetadata');
  assert.equal(url.searchParams.get('filter[0]'), 'status||$eq||rework');
  assert.equal(url.searchParams.get('filter[1]'), 'batch.status||$ne||draft');
});

test('buildReviewsUrl points to reviews endpoint for a conversation', () => {
  const url = new URL(buildReviewsUrl('9418', config));

  assert.equal(url.origin + url.pathname, 'https://labeling-o.turing.com/api/reviews');
  assert.equal(url.searchParams.get('conversationId'), '9418');
  assert.equal(url.searchParams.get('join[0]'), 'conversation');
});

test('buildSchemaWarmupConversationsUrl includes joins needed by relational filters', () => {
  const url = new URL(buildSchemaWarmupConversationsUrl(config));

  assert.equal(url.origin + url.pathname, 'https://labeling-o.turing.com/api/conversations');
  assert.equal(url.searchParams.get('join[0]'), 'seed||metadata,turingMetadata');
  assert.equal(url.searchParams.get('join[1]'), 'project||id,status');
  assert.equal(url.searchParams.get('join[2]'), 'batch||id,status,projectId');
  assert.equal(url.searchParams.get('filter[0]'), 'batch.status||$ne||draft');
  assert.equal(url.searchParams.get('filter[1]'), 'project.status||$ne||archived');
});

test('fetchTeamMembers includes the current user from auth me, dedupes by id, and keeps them first', async () => {
  const originalFetch = global.fetch;
  const requestedUrls = [];

  global.fetch = async (input, init) => {
    const url = String(input);
    requestedUrls.push(url);

    assert.equal(init?.headers?.Cookie, config.cookie);

    if (url === 'https://labeling-o.turing.com/api/contributors/my-team') {
      return new Response(
        JSON.stringify({
          contributors: [
            {
              id: '050',
              name: 'Aaron Stone',
              turingEmail: 'aaron@turing.com',
            },
            {
              id: '200',
              name: 'Grace Hopper',
              turingEmail: 'grace@turing.com',
            },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    if (url === 'https://labeling-o.turing.com/api/auth/me') {
      return new Response(
        JSON.stringify({
          data: {
            id: '100',
            firstName: 'Ada',
            lastName: 'Lovelace',
            email: 'ada@turing.com',
          },
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    throw new Error('Unexpected URL ' + url);
  };

  try {
    const members = await fetchTeamMembers(config);

    assert.deepEqual(requestedUrls.sort(), [
      'https://labeling-o.turing.com/api/auth/me',
      'https://labeling-o.turing.com/api/contributors/my-team',
    ]);
    assert.deepEqual(members, [
      { id: '100', name: 'Ada Lovelace', email: 'ada@turing.com' },
      { id: '050', name: 'Aaron Stone', email: 'aaron@turing.com' },
      { id: '200', name: 'Grace Hopper', email: 'grace@turing.com' },
    ]);
  } finally {
    global.fetch = originalFetch;
  }
});

test('fetchBatches loads, dedupes, and sorts batch options', async () => {
  const originalFetch = global.fetch;
  const requestedUrls = [];

  global.fetch = async (input, init) => {
    const url = String(input);
    requestedUrls.push(url);

    assert.equal(init?.headers?.Cookie, config.cookie);

    const parsed = new URL(url);

    if (parsed.pathname === '/api/batches' && parsed.searchParams.get('page') === '1') {
      return new Response(
        JSON.stringify({
          data: [
            { id: '172', name: 'Practice' },
            { id: '201', name: 'Production A' },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    throw new Error('Unexpected URL ' + url);
  };

  try {
    const batches = await fetchBatches(config);

    assert.equal(requestedUrls.length, 1);
    assert.deepEqual(batches, [
      { id: '172', name: 'Practice' },
      { id: '201', name: 'Production A' },
    ]);
  } finally {
    global.fetch = originalFetch;
  }
});

test('fetchRawConversations returns the upstream payload unchanged', async () => {
  const originalFetch = global.fetch;

  global.fetch = async (input, init) => {
    const url = String(input);

    assert.equal(
      url,
      'https://labeling-o.turing.com/api/conversations?sort%5B0%5D=updatedAt%2CDESC&limit=10&page=1&filter%5B0%5D=status%7C%7C%24eq%7C%7Crework',
    );
    assert.equal(init?.headers?.Cookie, config.cookie);

    return new Response(
      JSON.stringify({
        data: [{ id: 9462, conversation: { colabLink: 'https://rlhf-v3.turing.com/prompt/c1a69f1e-9b9b-4b9b-8a29-e67bad592a60' } }],
        meta: { page: 1 },
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } },
    );
  };

  try {
    const payload = await fetchRawConversations(
      {
        'sort[0]': 'updatedAt,DESC',
        limit: '10',
        page: '1',
        'filter[0]': 'status||$eq||rework',
      },
      config,
    );

    assert.deepEqual(payload, {
      data: [{ id: 9462, conversation: { colabLink: 'https://rlhf-v3.turing.com/prompt/c1a69f1e-9b9b-4b9b-8a29-e67bad592a60' } }],
      meta: { page: 1 },
    });
  } finally {
    global.fetch = originalFetch;
  }
});

test('fetchConversation loads and normalizes a single task record from the detail endpoint', async () => {
  const originalFetch = global.fetch;

  global.fetch = async (input, init) => {
    const url = String(input);
    assert.equal(init?.headers?.Cookie, config.cookie);

    if (url.startsWith('https://labeling-o.turing.com/api/conversations/9419?')) {
      return new Response(
        JSON.stringify({
          id: 9419,
          status: 'labeling',
          batch: { id: 311, name: 'Batch 311' },
          currentUser: { name: 'Ada Lovelace' },
          seed: {
            metadata: {
              num_turns: '1',
              complexity: 'intermediate',
            },
          },
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    if (url === 'https://labeling-o.turing.com/api/reviews?conversationId=9419&join%5B0%5D=conversation') {
      return new Response(
        JSON.stringify({
          data: [
            {
              conversation: {
                colabLink: 'https://rlhf-v3.turing.com/prompt/single-task-link',
              },
            },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    throw new Error('Unexpected URL ' + url);
  };

  try {
    const row = await fetchConversation('9419', config);

    assert.equal(row?.taskId, '9419');
    assert.equal(row?.batch, 'Batch 311');
    assert.equal(row?.assignedUser, 'Ada Lovelace');
    assert.equal(row?.turnCount, '1');
    assert.equal(row?.complexity, 'Intermediate');
    assert.equal(row?.collabLink, 'https://rlhf-v3.turing.com/prompt/single-task-link');
  } finally {
    global.fetch = originalFetch;
  }
});

test('fetchConversation prefers newer turingMetadata values when metadata and turingMetadata disagree', async () => {
  const originalFetch = global.fetch;

  global.fetch = async (input, init) => {
    const url = String(input);
    assert.equal(init?.headers?.Cookie, config.cookie);

    if (url.startsWith('https://labeling-o.turing.com/api/conversations/15761?')) {
      return new Response(
        JSON.stringify({
          id: 15761,
          status: 'labeling',
          batch: { id: 214, name: 'production_batch_0' },
          currentUser: { name: 'Aniket Saxena' },
          seed: {
            metadata: {
              num_turns: '4',
              required_debugging_task: false,
              required_cursors: true,
            },
            turingMetadata: {
              required_debugging_task: true,
            },
          },
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    if (url === 'https://labeling-o.turing.com/api/reviews?conversationId=15761&join%5B0%5D=conversation') {
      return new Response(
        JSON.stringify({
          data: [
            {
              conversation: {
                colabLink: 'https://rlhf-v3.turing.com/prompt/9b5feb50-d795-4e27-abba-2a8a927030f3',
              },
            },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    throw new Error('Unexpected URL ' + url);
  };

  try {
    const row = await fetchConversation('15761', config);

    assert.equal(row?.taskId, '15761');
    assert.equal(row?.metadata.required_cursors, true);
    assert.equal(row?.metadata.required_debugging_task, true);
  } finally {
    global.fetch = originalFetch;
  }
});

test('fetchConversations enriches missing collabLink values from reviews api', async () => {
  const originalFetch = global.fetch;

  global.fetch = async (input, init) => {
    const url = String(input);
    assert.equal(init?.headers?.Cookie, config.cookie);

    if (url.startsWith('https://labeling-o.turing.com/api/conversations?')) {
      return new Response(
        JSON.stringify({
          data: [
            {
              id: 9418,
              status: 'labeling',
              batch: { id: 172, name: 'practice_batch' },
              currentUser: { name: 'Shivani s' },
              seed: {
                metadata: {
                  batchId: 172,
                  num_turns: '1',
                  complexity: 'intermediate',
                },
              },
            },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    if (url === 'https://labeling-o.turing.com/api/reviews?conversationId=9418&join%5B0%5D=conversation') {
      return new Response(
        JSON.stringify({
          data: [
            {
              conversation: {
                colabLink: 'https://rlhf-v3.turing.com/prompt/c1a69f1e-9b9b-4b9b-8a29-e67bad592a60',
              },
            },
          ],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } },
      );
    }

    throw new Error('Unexpected URL ' + url);
  };

  try {
    const rows = await fetchConversations({ userId: '', status: 'in-progress', taskId: '', batchId: '' }, config);

    assert.equal(rows.length, 1);
    assert.equal(rows[0].taskId, '9418');
    assert.equal(rows[0].collabLink, 'https://rlhf-v3.turing.com/prompt/c1a69f1e-9b9b-4b9b-8a29-e67bad592a60');
  } finally {
    global.fetch = originalFetch;
  }
});

test('resolveTaskSchemaInfo follows spider lite routing rules', () => {
  assert.deepEqual(resolveTaskSchemaInfo({ dataset: 'Spider 2.0-Lite', database: 'GNOMAD' }), {
    dataset: 'Spider 2.0-Lite',
    database: 'GNOMAD',
    schemaName: 'GNOMAD',
    profile: 'spider_2_lite',
  });
});

test('resolveTaskSchemaInfo follows spider snow routing rules', () => {
  assert.deepEqual(resolveTaskSchemaInfo({ dataset: 'SPIDER 2.0-SNOW', database: 'SNOWDB' }), {
    dataset: 'SPIDER 2.0-SNOW',
    database: 'SNOWDB',
    schemaName: 'SNOWDB',
    profile: 'spider_2_snow',
  });
});

test('resolveTaskSchemaInfo follows bigquery routing rules', () => {
  assert.deepEqual(resolveTaskSchemaInfo({ dataset: 'CALIFORNIA_SCHOOLS', database: 'bigquery-public-data' }), {
    dataset: 'CALIFORNIA_SCHOOLS',
    database: 'bigquery-public-data',
    schemaName: 'CALIFORNIA_SCHOOLS',
    profile: 'bigquery_public_data',
  });
});

test('findPromptIdentifier scans nested metadata', () => {
  const promptId = findPromptIdentifier({
    nested: {
      deeper: {
        promptUuid: 'd84082c2-b5f8-4870-938a-11ca6f78a2e6',
      },
    },
  });

  assert.equal(promptId, 'd84082c2-b5f8-4870-938a-11ca6f78a2e6');
});

test('inferBusinessStatus derives reviewed once from completed tasks with followup false', () => {
  const status = inferBusinessStatus({
    status: 'completed',
    latestManualReview: { review: { followupRequired: false } },
  });

  assert.equal(status, 'Reviewed Once');
});

test('inferCurrentStatus prefers workflow state over top-level status', () => {
  const status = inferCurrentStatus({
    status: 'completed',
    latestLabelingWorkflow: {
      workflow: {
        currentWorkflowStatus: 'waiting_for_reviewer',
      },
    },
  });

  assert.equal(status, 'Waiting For Reviewer');
});

test('inferTurnCount reads nested metadata values only', () => {
  assert.equal(inferTurnCount({ task: { numTurns: 3 } }), '3');
  assert.equal(inferTurnCount({ payload: { promptTurns: ['a', 'b'] } }), '2');
  assert.equal(inferTurnCount('{}'), 'Unknown');
});

test('inferComplexity reads nested metadata values only', () => {
  assert.equal(inferComplexity({ details: { difficultyLevel: { levelInfo: { name: 'hard' } } } }), 'Hard');
  assert.equal(inferComplexity({ complexity: 'medium' }), 'Medium');
  assert.equal(inferComplexity('{}'), 'Unknown');
});

test('summarizeMetadata keeps the full payload for the metadata drawer', () => {
  const summary = summarizeMetadata({ data: 'x'.repeat(5000) });

  assert.match(summary, /x{100}/);
  assert.equal(summary.endsWith('...'), false);
});

test('normalizeConversation maps task fields for the dashboard and preserves business label', () => {
  const row = normalizeConversation(
    {
      id: 99,
      status: 'labeling',
      batchId: '172',
      batch: { name: 'Batch 172' },
      currentUser: { name: 'Ada Lovelace' },
      colabLink: '2677bee9-5ce0-4e1b-8f84-4e03588947cb',
      seed: {
        metadata: {
          dataset: 'Spider 2.0-Lite',
          database: 'GNOMAD',
          promptId: 'prompt-123',
          turns: [1, 2],
          difficultyLevel: {
            levelInfo: {
              name: 'hard',
            },
          },
        },
      },
      latestLabelingWorkflow: {
        workflow: {
          currentWorkflowStatus: 'waiting_for_reviewer',
        },
      },
    },
    'In Progress',
  );

  assert.equal(row.taskId, '99');
  assert.equal(row.status, 'Waiting For Reviewer');
  assert.equal(row.businessStatus, 'In Progress');
  assert.equal(row.turnCount, '2');
  assert.equal(row.complexity, 'Hard');
  assert.equal(row.batchId, '172');
  assert.equal(row.batch, 'Batch 172');
  assert.equal(row.schemaName, 'GNOMAD');
  assert.equal(row.assignedUser, 'Ada Lovelace');
  assert.equal(row.promptId, 'prompt-123');
  assert.equal(
    row.collabLink,
    'https://rlhf-v3.turing.com/prompt/2677bee9-5ce0-4e1b-8f84-4e03588947cb?origin=https%3A%2F%2Flabeling-o.turing.com&redirect_url=https%3A%2F%2Flabeling-o.turing.com%2Fconversations%2F99%2Fview',
  );
  assert.match(row.metadataPreview, /prompt-123/);
});

test('listTaskOutputFiles returns sorted files from the configured task folder including nested logs', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-report-'));
  const taskDir = join(root, '24696');
  const logsDir = join(taskDir, '_logs');

  try {
    await mkdir(taskDir);
    await mkdir(logsDir);
    await writeFile(join(taskDir, '24696_turn2.txt'), 'turn 2');
    await writeFile(join(taskDir, '24696_turn1.txt'), 'turn 1');
    await writeFile(join(logsDir, 'validate-2026-03-21.log'), 'log output');

    const report = await listTaskOutputFiles('24696', { taskOutputDir: root });

    assert.equal(report.taskId, '24696');
    assert.equal(report.folderPath, taskDir);
    assert.deepEqual(
      report.files.map((file) => file.name),
      ['_logs/validate-2026-03-21.log', '24696_turn1.txt', '24696_turn2.txt'],
    );
    assert.equal(report.files[0].extension, 'log');
    assert.equal(report.files[1].extension, 'txt');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('listTaskOutputFiles repairs broken prompt artifacts from existing_output before returning files', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-report-'));
  const taskDir = join(root, '15835');

  try {
    await mkdir(taskDir);
    await writeFile(
      join(taskDir, '15835_turn1_1user.txt'),
      [
        'Write an anonymous block that analyzes fatal traffic collisions for a specific county.',
        '',
        'Requirements:',
        'Anonymous Block Name: Anonymous PL/SQL block.',
        '',
        'Parameters:',
        '',
        'Output:',
        '',
        'Exception Handling:',
        '',
      ].join('\n'),
    );
    await writeFile(
      join(taskDir, '15835_existing_output.json'),
      JSON.stringify(
        {
          response: {
            data: {
              prompt: {
                promptTurns: [
                  {
                    promptIndex: 0,
                    promptEvaluationFeedback: {
                      promptTurnEvaluation: [
                        {
                          name: 'user',
                          value: [
                            'Write an anonymous block that analyzes fatal traffic collisions for a specific county.',
                            '',
                            'Requirements:',
                            'Anonymous Block Name: Anonymous PL/SQL block.',
                            '',
                            "Parameters: Declare gc_county_name as CONSTANT VARCHAR2(100) := 'los angeles'. Declare gc_severity_fatal as CONSTANT VARCHAR2(20) := 'fatal'.",
                            '',
                            "Output: Print exactly: '=== Fatal Collision Analysis ===' then 'County: <value>'.",
                            '',
                            "Exception Handling: If gc_county_name is NULL print exactly: 'ERROR: County name cannot be NULL.' and terminate.",
                          ].join('\n'),
                        },
                      ],
                    },
                  },
                ],
              },
            },
          },
        },
        null,
        2,
      ),
    );

    const report = await listTaskOutputFiles('15835', { taskOutputDir: root });

    assert.equal(report.taskId, '15835');
    assert.equal(
      await readFile(join(taskDir, '15835_turn1_1user.txt'), 'utf8'),
      [
        'Write an anonymous block that analyzes fatal traffic collisions for a specific county.',
        '',
        'Requirements:',
        'Anonymous Block:',
        '',
        'Parameters:',
        "  Declare gc_county_name as CONSTANT VARCHAR2(100) := 'los angeles'. Declare gc_severity_fatal as CONSTANT VARCHAR2(20) := 'fatal'.",
        '',
        'Output:',
        "  Print exactly: '=== Fatal Collision Analysis ===' then 'County: <value>'.",
        '',
        'Exception Handling:',
        "  If gc_county_name is NULL print exactly : 'ERROR: County name cannot be NULL.' and terminate.",
        '',
      ].join('\n'),
    );
    assert.ok(report.files.some((file) => file.name === '15835_turn1_1user.txt'));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('readTaskOutputFile returns file contents from the configured task folder', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-report-'));
  const taskDir = join(root, '9479');
  const filePath = join(taskDir, '9479_existing_output.json');

  try {
    await mkdir(taskDir);
    await writeFile(filePath, '{\"ok\":true}');

    const file = await readTaskOutputFile('9479', '9479_existing_output.json', { taskOutputDir: root });

    assert.equal(file.taskId, '9479');
    assert.equal(file.name, '9479_existing_output.json');
    assert.equal(file.extension, 'json');
    assert.equal(file.content, '{"ok":true}');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('readTaskOutputFile returns nested log file contents from the configured task folder', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-report-'));
  const taskDir = join(root, '9479');
  const logsDir = join(taskDir, '_logs');
  const filePath = join(logsDir, 'validate-2026-03-21.log');

  try {
    await mkdir(logsDir, { recursive: true });
    await writeFile(filePath, 'validation started');

    const file = await readTaskOutputFile('9479', '_logs/validate-2026-03-21.log', { taskOutputDir: root });

    assert.equal(file.taskId, '9479');
    assert.equal(file.name, '_logs/validate-2026-03-21.log');
    assert.equal(file.extension, 'log');
    assert.equal(file.content, 'validation started');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('writeTaskOutputFile updates editable txt files in the configured task folder', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-report-'));
  const taskDir = join(root, '9479');
  const filePath = join(taskDir, '9479_turn1_1user.txt');

  try {
    await mkdir(taskDir);
    await writeFile(filePath, 'before');

    const file = await writeTaskOutputFile('9479', '9479_turn1_1user.txt', 'after', { taskOutputDir: root });

    assert.equal(file.extension, 'txt');
    assert.equal(file.content, 'after');
    assert.equal(await readTaskOutputFile('9479', '9479_turn1_1user.txt', { taskOutputDir: root }).then((entry) => entry.content), 'after');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('writeTaskOutputFile rejects non-editable file extensions', async () => {
  const root = await mkdtemp(join(os.tmpdir(), 'app-validator-report-'));
  const taskDir = join(root, '9479');
  const filePath = join(taskDir, '9479_existing_output.json');

  try {
    await mkdir(taskDir);
    await writeFile(filePath, '{"ok":true}');

    await assert.rejects(
      writeTaskOutputFile('9479', '9479_existing_output.json', '{"ok":false}', { taskOutputDir: root }),
      /Only \.txt and \.sql files can be edited/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


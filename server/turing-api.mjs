import { readdir, readFile, stat, writeFile } from 'node:fs/promises';
import { join, resolve as pathResolve, relative as pathRelative } from 'node:path';
import { URL, URLSearchParams } from 'node:url';

import { resolveAuthorizationHeader } from './auth-header.mjs';
import { createLogger, isBackendDebugEnabled, sanitizeHeaders } from './logger.mjs';
import { extendRuntimeConfigWithWorkflowDefaults } from './task-workflows.mjs';
import { getSchemaCacheDir, getTaskOutputDir } from './workspace-config.mjs';
import { resolveTaskRouting } from './schema-db-config.mjs';

const DEFAULT_LABELING_BASE_URL = 'https://labeling-o.turing.com';
const DEFAULT_RLHF_BASE_URL = 'https://rlhf-v3.turing.com';
const DEFAULT_PROJECT_ID = '57';
const DEFAULT_PAGE = '1';
const DEFAULT_BATCH_LIMIT = 100;
const ALL_STATUS = 'all';
const IN_PROGRESS_STATUS = 'in-progress';
const REWORK_STATUS = 'rework';
const COMPLETED_STATUS = 'completed';
const REVIEWED_ONCE_STATUS = 'reviewed-once';
const logger = createLogger('turing-api');

const SINGLE_CONVERSATION_JOINS = [
  'project||id,name,status,projectType,supportsFunctionCalling,supportsWorkflows,supportsMultipleFilesPerTask,jibbleActivity,instructionsLink,readonly,averageHandleTimeMinutes',
  'batch||id,name,status,projectId,jibbleActivity,maxClaimGoldenTaskAllowed,averageHandleTimeMinutes',
  'currentUser||id,name,turingEmail,profilePicture,isBlocked',
  'currentUser.teamLead||id,name,turingEmail,profilePicture,isBlocked',
  'seed||metadata,turingMetadata',
  'labels||id,labelId',
  'labels.label',
  'variations||id',
  'project.projectFormStages',
  'latestManualReview',
  'latestManualReview.review||id,score,feedback,status,audit,conversationId,reviewerId,reviewType,followupRequired',
  'latestAutoReview',
  'latestAutoReview.review||id,score,conversationId,status,reviewType,followupRequired',
  'latestLabelingWorkflow',
  'latestLabelingWorkflow.workflow||status,createdAt,currentWorkflowStatus',
];

const statusQueryPresets = {
  [ALL_STATUS]: {
    label: 'All',
    sort: ['updatedAt,DESC'],
    limit: '100',
    joins: [
      'project||id,name,status,projectType,supportsFunctionCalling,supportsWorkflows,supportsMultipleFilesPerTask,jibbleActivity,instructionsLink,readonly,averageHandleTimeMinutes',
      'batch||id,name,status,projectId,jibbleActivity,maxClaimGoldenTaskAllowed,averageHandleTimeMinutes',
      'currentUser||id,name,turingEmail,profilePicture,isBlocked',
      'currentUser.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'seed||metadata,turingMetadata',
      'labels||id,labelId',
      'labels.label',
      'latestManualReview',
      'latestManualReview.review||id,score,feedback,status,audit,conversationId,reviewerId,reviewType,followupRequired',
      'latestManualReview.review.reviewer||id,name,turingEmail,profilePicture,isBlocked',
      'latestAutoReview',
      'latestAutoReview.review||id,score,conversationId,status,reviewType,followupRequired',
      'difficultyLevel',
      'difficultyLevel.levelInfo||name',
      'latestLabelingWorkflow',
      'latestLabelingWorkflow.workflow||status,createdAt,currentWorkflowStatus',
      'latestLabelingWorkflow.workflow.currentCollaborator||id',
      'latestLabelingWorkflow.workflow.currentCollaborator.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators||role',
      'latestLabelingWorkflow.workflow.collaborators.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators.collaborator.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'latestDeliveryBatch',
      'latestDeliveryBatch.deliveryBatch||id,name,status',
      'reviews||id,status,audit,conversationId,reviewerId,reviewType,updatedAt',
      'reviews.reviewer||id,name,turingEmail,profilePicture,isBlocked',
    ],
    buildFilters: (config) => [
      'batch.status||$ne||draft',
      'project.status||$ne||archived',
      `projectId||$eq||${config.projectId}`,
      'batch.status||$eq||ongoing',
      'batch.status||$ne||archived',
    ],
  },
  [IN_PROGRESS_STATUS]: {
    label: 'In Progress',
    limit: '10',
    joins: [
      'project||id,name,status,projectType,supportsFunctionCalling,supportsWorkflows,supportsMultipleFilesPerTask,jibbleActivity,instructionsLink,readonly,averageHandleTimeMinutes',
      'batch||id,name,status,projectId,jibbleActivity,maxClaimGoldenTaskAllowed,averageHandleTimeMinutes',
      'currentUser||id,name,turingEmail,profilePicture,isBlocked',
      'currentUser.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'seed||metadata,turingMetadata',
      'labels||id,labelId',
      'labels.label',
      'difficultyLevel',
      'difficultyLevel.levelInfo||name',
      'latestLabelingWorkflow',
      'latestLabelingWorkflow.workflow||status,createdAt,currentWorkflowStatus',
      'latestLabelingWorkflow.workflow.currentCollaborator||id',
      'latestLabelingWorkflow.workflow.currentCollaborator.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators||role',
      'latestLabelingWorkflow.workflow.collaborators.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators.collaborator.teamLead||id,name,turingEmail,profilePicture,isBlocked',
    ],
    buildFilters: (config) => [
      'status||$eq||labeling',
      'status||$in||labeling,validating',
      'batch.status||$ne||draft',
      'project.status||$ne||archived',
      `projectId||$eq||${config.projectId}`,
      'batch.status||$eq||ongoing',
      'batch.status||$ne||archived',
    ],
  },
  [REWORK_STATUS]: {
    label: 'Rework',
    sort: ['updatedAt,DESC'],
    limit: '10',
    joins: [
      'project||id,name,status,projectType,supportsFunctionCalling,supportsWorkflows,supportsMultipleFilesPerTask,jibbleActivity,instructionsLink,readonly,averageHandleTimeMinutes',
      'batch||id,name,status,projectId,jibbleActivity,maxClaimGoldenTaskAllowed,averageHandleTimeMinutes',
      'currentUser||id,name,turingEmail,profilePicture,isBlocked',
      'currentUser.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'seed||metadata,turingMetadata',
      'labels||id,labelId',
      'labels.label',
      'difficultyLevel',
      'difficultyLevel.levelInfo||name',
      'latestManualReview',
      'latestManualReview.review||id,score,feedback,status,audit,conversationId,reviewerId,reviewType,followupRequired',
      'latestManualReview.review.reviewer||id,name,turingEmail,profilePicture,isBlocked',
      'latestManualReview.review.qualityDimensionValues||id,score,qualityDimensionId,weight,scoreText',
      'latestManualReview.review.qualityDimensionValues.qualityDimension||id,name',
      'latestAutoReview',
      'latestAutoReview.review||id,score,conversationId,status,reviewType,followupRequired',
      'latestAutoReview.review.qualityDimensionValues||id,score,qualityDimensionId,weight,scoreText',
      'latestAutoReview.review.qualityDimensionValues.qualityDimension||id,name',
      'latestLabelingWorkflow',
      'latestLabelingWorkflow.workflow||status,createdAt,currentWorkflowStatus',
      'latestLabelingWorkflow.workflow.currentCollaborator||id',
      'latestLabelingWorkflow.workflow.currentCollaborator.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators||role',
      'latestLabelingWorkflow.workflow.collaborators.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators.collaborator.teamLead||id,name,turingEmail,profilePicture,isBlocked',
    ],
    buildFilters: (config) => [
      'status||$eq||rework',
      'batch.status||$ne||draft',
      'project.status||$ne||archived',
      `projectId||$eq||${config.projectId}`,
      'batch.status||$eq||ongoing',
      'batch.status||$ne||archived',
    ],
  },
  [COMPLETED_STATUS]: {
    label: 'Completed',
    limit: '50',
    joins: [
      'project||id,name,status,projectType,supportsFunctionCalling,supportsWorkflows,supportsMultipleFilesPerTask,jibbleActivity,instructionsLink,readonly,averageHandleTimeMinutes',
      'batch||id,name,status,projectId,jibbleActivity,maxClaimGoldenTaskAllowed,averageHandleTimeMinutes',
      'currentUser||id,name,turingEmail,profilePicture,isBlocked',
      'currentUser.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'seed||metadata,turingMetadata',
      'labels||id,labelId',
      'labels.label',
      'latestManualReview',
      'latestManualReview.review||id,score,feedback,status,audit,conversationId,reviewerId,reviewType,followupRequired',
      'latestManualReview.review.reviewer||id,name,turingEmail,profilePicture,isBlocked',
      'latestManualReview.review.qualityDimensionValues||id,score,qualityDimensionId,weight,scoreText',
      'latestManualReview.review.qualityDimensionValues.qualityDimension||id,name',
      'latestAutoReview',
      'latestAutoReview.review||id,score,conversationId,status,reviewType,followupRequired',
      'latestAutoReview.review.qualityDimensionValues||id,score,qualityDimensionId,weight,scoreText',
      'latestAutoReview.review.qualityDimensionValues.qualityDimension||id,name',
      'difficultyLevel',
      'difficultyLevel.levelInfo||name',
      'latestLabelingWorkflow',
      'latestLabelingWorkflow.workflow||status,createdAt,currentWorkflowStatus',
      'latestLabelingWorkflow.workflow.currentCollaborator||id',
      'latestLabelingWorkflow.workflow.currentCollaborator.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators||role',
      'latestLabelingWorkflow.workflow.collaborators.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators.collaborator.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'latestDeliveryBatch',
      'latestDeliveryBatch.deliveryBatch||id,name,status',
      'reviews||id,status,audit,conversationId,reviewerId,reviewType,updatedAt',
      'reviews.reviewer||id,name,turingEmail,profilePicture,isBlocked',
    ],
    buildFilters: (config) => [
      'status||$eq||completed',
      '$needFollowup||$eq||true',
      'project.status||$ne||archived',
      `projectId||$eq||${config.projectId}`,
      'batch.status||$eq||ongoing',
      'batch.status||$ne||archived',
    ],
  },
  [REVIEWED_ONCE_STATUS]: {
    label: 'Reviewed Once',
    limit: '30',
    joins: [
      'project||id,name,status,projectType,supportsFunctionCalling,supportsWorkflows,supportsMultipleFilesPerTask,jibbleActivity,instructionsLink,readonly,averageHandleTimeMinutes',
      'batch||id,name,status,projectId,jibbleActivity,maxClaimGoldenTaskAllowed,averageHandleTimeMinutes',
      'currentUser||id,name,turingEmail,profilePicture,isBlocked',
      'currentUser.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'seed||metadata,turingMetadata',
      'labels||id,labelId',
      'labels.label',
      'difficultyLevel',
      'difficultyLevel.levelInfo||name',
      'latestManualReview',
      'latestManualReview.review||id,score,feedback,status,audit,conversationId,reviewerId,reviewType,followupRequired',
      'latestManualReview.review.reviewer||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow',
      'latestLabelingWorkflow.workflow||status,createdAt,currentWorkflowStatus',
      'latestLabelingWorkflow.workflow.currentCollaborator||id',
      'latestLabelingWorkflow.workflow.currentCollaborator.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators||role',
      'latestLabelingWorkflow.workflow.collaborators.collaborator||id,name,turingEmail,profilePicture,isBlocked',
      'latestLabelingWorkflow.workflow.collaborators.collaborator.teamLead||id,name,turingEmail,profilePicture,isBlocked',
      'latestAutoReview',
      'latestAutoReview.review||id,score,conversationId,status,reviewType,followupRequired',
      'reviews||id,submittedAt,feedback,status,audit,score,conversationId,reviewerId,conversationVersionId,reviewType',
      'reviews.qualityDimensionValues||id,score,qualityDimensionId,weight,scoreText',
      'reviews.reviewer||id,name,turingEmail,profilePicture,isBlocked',
      'latestDeliveryBatch',
      'latestDeliveryBatch.deliveryBatch||id,name,status',
    ],
    buildFilters: (config) => [
      'status||$eq||completed',
      'batch.status||$ne||draft',
      'manualReview.followupRequired||$eq||false',
      'project.status||$ne||archived',
      `projectId||$eq||${config.projectId}`,
      'batch.status||$eq||ongoing',
      'batch.status||$ne||archived',
    ],
  },
};

export function readRuntimeConfig(env = process.env) {
  return extendRuntimeConfigWithWorkflowDefaults({
    authorizationHeader: resolveAuthorizationHeader(env),
    cookie: env.TURING_COOKIE ?? '',
    labelingBaseUrl: env.LABELING_API_BASE_URL ?? DEFAULT_LABELING_BASE_URL,
    projectId: env.LABELING_PROJECT_ID ?? DEFAULT_PROJECT_ID,
    taskOutputDir: getTaskOutputDir(env),
    schemaCacheDir: getSchemaCacheDir(env),
  }, env);
}

export function getProxyHealth(config) {
  return {
    configured: Boolean(config.cookie),
    message: config.cookie
      ? 'Cookie found. Proxy can call labeling-o.turing.com.'
      : 'Set TURING_COOKIE in .env.local before fetching live data.',
  };
}

export async function fetchTeamMembers(config) {
  const [teamResponse, currentUserResponse] = await Promise.all([
    callJsonApi(buildTeamMembersUrl(config), config),
    callJsonApi(buildCurrentUserUrl(config), config),
  ]);

  const teamMembers = coerceArray(teamResponse).map((record) => normalizeTeamMember(record));
  const currentUser = normalizeTeamMember(coerceRecord(currentUserResponse));
  const records = currentUser.id ? [...teamMembers, currentUser] : teamMembers;

  return dedupeTeamMembers(records)
    .filter((record) => record.id && record.name)
    .sort((left, right) => {
      if (currentUser.id) {
        if (left.id === currentUser.id) {
          return -1;
        }

        if (right.id === currentUser.id) {
          return 1;
        }
      }

      return left.name.localeCompare(right.name);
    });
}

export async function fetchBatches(config) {
  const batches = [];

  for (let page = 1; ; page += 1) {
    const response = await callJsonApi(buildBatchesUrl(config, page), config);
    const records = coerceArray(response).map((record) => normalizeBatch(record)).filter((batch) => batch.id && batch.name);

    batches.push(...records);

    if (records.length < DEFAULT_BATCH_LIMIT) {
      break;
    }
  }

  return dedupeBatches(batches).sort((left, right) =>
    left.name.localeCompare(right.name, undefined, { numeric: true, sensitivity: 'base' }),
  );
}

export async function fetchConversations(filters, config) {
  logger.debug('Fetching conversations', { filters });
  const requests = buildConversationsRequests(filters, config);
  const responses = await Promise.all(requests.map((request) => callJsonApi(request.url, config)));

  const normalizedRows = responses.flatMap((response) =>
    coerceArray(response).map((record) => normalizeConversation(record)),
  );

  const rows = await enrichConversationRowsWithCollabLinks(dedupeConversationRows(normalizedRows), config);
  logger.info('Fetched conversations', { filters, rowCount: rows.length });
  return rows;
}

export async function fetchConversation(taskId, config) {
  logger.debug('Fetching single conversation', { taskId });
  const response = await callJsonApi(buildConversationDetailUrl(taskId, config), config);
  const record = coerceRecord(response);

  if (!Object.keys(record).length) {
    logger.info('Single conversation payload was empty', { taskId });
    return null;
  }

  const normalized = normalizeConversation(record);
  const rows = await enrichConversationRowsWithCollabLinks([normalized], config);
  logger.info('Fetched single conversation', { taskId, found: Boolean(rows[0]) });
  return rows[0] ?? normalized;
}

export async function editConversation(taskId, config, payload = {}) {
  const authorizationHeader = resolveAuthorizationHeader(config);
  logger.debug('Editing conversation', {
    taskId,
    payload,
    hasAuthorizationToken: Boolean(authorizationHeader),
  });
  const response = await callJsonApi(buildConversationEditUrl(taskId, config), config, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  logger.info('Edited conversation', { taskId });
  return response;
}

export async function fetchSchemaWarmupCandidates(config) {
  const candidates = [];
  const seen = new Set();

  for (let page = 1; ; page += 1) {
    const response = await callJsonApi(buildSchemaWarmupConversationsUrl(config, page), config);
    const records = coerceArray(response);

    for (const record of records) {
      const metadata = extractConversationMetadata(record);

      if (!isRecord(metadata)) {
        continue;
      }

      const schema = resolveTaskSchemaInfo(metadata);

      if (!schema.schemaName) {
        continue;
      }

      if (
        schema.profile === 'bigquery_public_data' &&
        /^bigquery-public-data$/i.test(String(schema.schemaName).trim())
      ) {
        continue;
      }

      const key = `${schema.profile}::${schema.schemaName}`;

      if (seen.has(key)) {
        continue;
      }

      seen.add(key);
      candidates.push({
        metadata,
        schemaName: schema.schemaName,
        profile: schema.profile,
      });
    }

    if (records.length < DEFAULT_BATCH_LIMIT) {
      break;
    }
  }

  logger.info('Resolved schema warmup candidates', { candidateCount: candidates.length });
  return candidates;
}

export async function fetchRawConversations(query, config) {
  return callJsonApi(buildPassthroughUrl('/api/conversations', query, config.labelingBaseUrl), config);
}

export async function fetchPromptById(id, env = process.env) {
  const endpoint = env.RLHF_GRAPHQL_URL ?? 'https://rlhf-api.turing.com/graphql';
  const body = {
    operationName: 'GetPrompt',
    variables: { id },
    query: 'query GetPrompt($id: ID!) { prompt(idOrUuid: $id) { id uuid status metadata turingMetadata __typename } }',
  };

  return callJsonApi(endpoint, readRuntimeConfig(env), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
}

export async function listTaskOutputFiles(taskId, config) {
  try {
    const folderPath = resolveTaskOutputFolder(taskId, config);
    const files = await collectTaskOutputFiles(taskId, folderPath, '', config);

    return {
      taskId,
      folderPath,
      files: files.sort((left, right) => left.name.localeCompare(right.name, undefined, { numeric: true })),
    };
  } catch (error) {
    throw asTaskOutputError(error, `Task folder ${taskId} was not found in the configured output directory.`);
  }
}

const NON_TASK_FOLDER_NAMES = new Set(['schema', 'tmp', 'tmp_labeling_assets']);

export async function listTaskOutputTasks(config, filters = {}) {
  const requestedTaskId = asString(filters.taskId).trim();

  try {
    const rootPath = pathResolve(config.taskOutputDir);
    const entries = await readdir(rootPath, { withFileTypes: true });
    const taskDirectories = entries.filter(
      (entry) =>
        entry.isDirectory() &&
        isExactTaskId(entry.name) &&
        !NON_TASK_FOLDER_NAMES.has(entry.name.toLowerCase()) &&
        (!requestedTaskId || entry.name === requestedTaskId),
    );
    const rows = await Promise.all(
      taskDirectories.map((entry) => buildTaskOutputTaskRow(entry.name, join(rootPath, entry.name), config)),
    );

    return rows
      .filter((row) => row !== null)
      .sort((left, right) => right.taskId.localeCompare(left.taskId, undefined, { numeric: true, sensitivity: 'base' }));
  } catch (error) {
    throw asTaskOutputError(error, 'Task output directory was not found in the configured workspace.');
  }
}

export async function fetchConversationReviewDetail(taskId, config) {
  const response = await callJsonApi(buildConversationReviewsUrl(taskId, config), config);
  const data = coerceArray(coerceRecord(response).data ?? response);
  const review = data[0] ?? null;
  return normalizeReviewFromReviewsApi(taskId, review);
}

export async function readTaskOutputFile(taskId, name, config) {
  if (!name) {
    throw notFoundError('Select a file to view its content.');
  }

  try {
    const filePath = resolveTaskOutputFilePath(taskId, name, config);
    const [metadata, content] = await Promise.all([stat(filePath), readFile(filePath, 'utf8')]);

    if (!metadata.isFile()) {
      throw notFoundError(`File ${name} was not found for task ${taskId}.`);
    }

    return {
      taskId,
      name,
      size: metadata.size,
      modifiedAt: metadata.mtime.toISOString(),
      extension: readFileExtension(name),
      content,
    };
  } catch (error) {
    throw asTaskOutputError(error, `File ${name} was not found for task ${taskId}.`);
  }
}

export async function writeTaskOutputFile(taskId, name, content, config) {
  if (!name) {
    throw notFoundError('Select a file to save.');
  }

  const extension = readFileExtension(name);
  if (!['txt', 'sql'].includes(extension)) {
    throw Object.assign(new Error(`Only .txt and .sql files can be edited. Received: ${name}`), { statusCode: 400 });
  }

  try {
    const filePath = resolveTaskOutputFilePath(taskId, name, config);
    const metadata = await stat(filePath);

    if (!metadata.isFile()) {
      throw notFoundError(`File ${name} was not found for task ${taskId}.`);
    }

    await writeFile(filePath, typeof content === 'string' ? content : String(content ?? ''), 'utf8');
    const updated = await stat(filePath);

    return {
      taskId,
      name,
      size: updated.size,
      modifiedAt: updated.mtime.toISOString(),
      extension,
      content: typeof content === 'string' ? content : String(content ?? ''),
    };
  } catch (error) {
    throw asTaskOutputError(error, `File ${name} was not found for task ${taskId}.`);
  }
}

export function buildTeamMembersUrl(config) {
  return new URL('/api/contributors/my-team', config.labelingBaseUrl).toString();
}

export function buildCurrentUserUrl(config) {
  return new URL('/api/auth/me', config.labelingBaseUrl).toString();
}

export function buildBatchesUrl(config, page = 1) {
  const url = new URL('/api/batches', config.labelingBaseUrl);
  const params = new URLSearchParams();

  params.set('sort[0]', 'projectId,DESC');
  params.set('sort[1]', 'id,DESC');
  params.set('limit', String(DEFAULT_BATCH_LIMIT));
  params.set('page', String(page));
  params.set('fields', 'id,name,status,projectId,jibbleActivity');
  params.set('filter[0]', 'status||$notin||draft');
  params.set('filter[1]', `projectId||$eq||${config.projectId}`);
  params.set('filter[2]', 'status||ne||archived');
  params.set('join[0]', 'batchStats');

  url.search = params.toString();
  return url.toString();
}

export function buildConversationsRequests(filters, config) {
  const selectedStatus = filters.status || ALL_STATUS;
  const statuses = [selectedStatus];

  return statuses.map((status) => {
    const preset = statusQueryPresets[status] ?? statusQueryPresets[ALL_STATUS];

    return {
      status,
      label: preset.label,
      url: buildConversationsUrl({ ...filters, status }, config),
    };
  });
}

export function buildConversationsUrls(filters, config) {
  return buildConversationsRequests(filters, config).map((request) => request.url);
}

export function buildConversationsUrl(filters, config) {
  const preset = statusQueryPresets[filters.status] ?? statusQueryPresets[ALL_STATUS];
  const url = new URL('/api/conversations', config.labelingBaseUrl);
  const params = new URLSearchParams();

  params.set('limit', preset.limit);
  params.set('page', DEFAULT_PAGE);

  preset.sort?.forEach((sortValue, index) => {
    params.set(`sort[${index}]`, sortValue);
  });

  preset.joins.forEach((joinValue, index) => {
    params.set(`join[${index}]`, joinValue);
  });

  const filterValues = [...preset.buildFilters(config)];

  if (filters.userId) {
    filterValues.push(`$claimedBy||$in||${filters.userId}`);
  }

  if (filters.batchId) {
    filterValues.push(`batchId||$in||${filters.batchId}`);
  }

  if (filters.taskId && isExactTaskId(filters.taskId)) {
    filterValues.push(`id||$eq||${filters.taskId}`);
  }

  filterValues.forEach((filterValue, index) => {
    params.set(`filter[${index}]`, filterValue);
  });

  url.search = params.toString();
  return url.toString();
}

export function buildConversationDetailUrl(taskId, config) {
  const url = new URL(`/api/conversations/${encodeURIComponent(taskId)}`, config.labelingBaseUrl);

  SINGLE_CONVERSATION_JOINS.forEach((joinValue, index) => {
    url.searchParams.set(`join[${index}]`, joinValue);
  });

  return url.toString();
}

export function buildConversationEditUrl(taskId, config) {
  return new URL(`/api/conversations/${encodeURIComponent(taskId)}/edit`, config.labelingBaseUrl).toString();
}

export function buildPassthroughUrl(pathname, query, baseUrl) {
  const url = new URL(pathname, baseUrl);

  for (const [key, rawValue] of Object.entries(query ?? {})) {
    if (Array.isArray(rawValue)) {
      for (const value of rawValue) {
        if (value !== undefined && value !== null) {
          url.searchParams.append(key, String(value));
        }
      }
      continue;
    }

    if (rawValue !== undefined && rawValue !== null) {
      url.searchParams.append(key, String(rawValue));
    }
  }

  return url.toString();
}

export function buildReviewsUrl(taskId, config) {
  const url = new URL('/api/reviews', config.labelingBaseUrl);
  url.searchParams.set('conversationId', taskId);
  url.searchParams.set('join[0]', 'conversation');
  return url.toString();
}

export function buildConversationReviewsUrl(taskId, config) {
  const url = new URL('/api/reviews', config.labelingBaseUrl);
  const params = url.searchParams;
  params.set('limit', '10');
  params.set('page', '1');
  params.set('filter[0]', `conversationId||$eq||${taskId}`);
  params.set('filter[1]', 'conversation.batch.status||$ne||archived');
  params.set('join[0]', 'reviewer||id,name,turingEmail,profilePicture,isBlocked');
  params.set('join[1]', 'conversationVersion||id,colabRevisionId,formStage');
  params.set('join[2]', 'conversation');
  params.set('join[3]', 'conversation.project||id,name,status');
  params.set('join[4]', 'conversation.batch||id,name,status');
  params.set('join[5]', 'qualityDimensionValues||id,feedback,score,weight,scoreText,negativeReviewThreshold,trainerFeedback');
  params.set('join[6]', 'qualityDimensionValues.qualityDimension||id,name,description,reviewDisplay');
  params.set('join[7]', 'qualityDimensionValues.projectQualityDimension||id,name,reviewDisplay,sortOrder,negativeReviewThreshold');
  return url.toString();
}

export function buildSchemaWarmupConversationsUrl(config, page = 1) {
  const preset = statusQueryPresets[ALL_STATUS];
  const url = new URL('/api/conversations', config.labelingBaseUrl);
  const params = new URLSearchParams();

  params.set('limit', String(DEFAULT_BATCH_LIMIT));
  params.set('page', String(page));
  params.set('join[0]', 'seed||metadata,turingMetadata');
  params.set('join[1]', 'project||id,status');
  params.set('join[2]', 'batch||id,status,projectId');

  preset.buildFilters(config).forEach((filterValue, index) => {
    params.set(`filter[${index}]`, filterValue);
  });

  url.search = params.toString();
  return url.toString();
}

export function normalizeConversation(record, businessStatus) {
  const metadata = extractConversationMetadata(record) ?? {};
  const taskId = asString(record.id ?? record.uuid);
  const batchId = resolveBatchId(record);
  const batch = resolveBatchName(record);
  const schema = resolveTaskSchemaInfo(metadata);
  const assignedUser =
    asString(record.currentUser?.name) ||
    asString(record.latestLabelingWorkflow?.workflow?.currentCollaborator?.collaborator?.name) ||
    asString(record.latestLabelingWorkflow?.workflow?.collaborators?.[0]?.collaborator?.name) ||
    'Unassigned';
  const promptId = findPromptIdentifier(metadata) ?? findPromptIdentifier(record.seed?.turingMetadata) ?? undefined;
  const reviewFields = extractReviewFields(record);

  return {
    taskId,
    metadata,
    metadataPreview: summarizeMetadata(metadata),
    status: inferCurrentStatus(record),
    businessStatus: inferBusinessStatus(record),
    turnCount: inferTurnCount(metadata),
    complexity: inferComplexity(metadata),
    batchId,
    batch,
    schemaName: schema.schemaName || 'Unknown',
    assignedUser,
    promptId,
    collabLink: resolveCollabLink(record, taskId),
    source: 'conversation',
    updatedAt: asString(record.updatedAt) || null,
    ...reviewFields,
  };
}

export function extractReviewFields(record) {
  const manualReview = record.latestManualReview?.review ?? null;
  const autoReview = record.latestAutoReview?.review ?? null;
  const review = manualReview ?? autoReview;

  if (!review) {
    return {
      lastReviewScore: null,
      lastReviewFeedback: null,
      lastReviewerName: null,
      lastReviewStatus: null,
      lastReviewType: null,
      lastReviewFollowup: null,
    };
  }

  return {
    lastReviewScore: typeof review.score === 'number' ? review.score : null,
    lastReviewFeedback: asString(review.feedback) || null,
    lastReviewerName: asString(review.reviewer?.name) || null,
    lastReviewStatus: asString(review.status) || null,
    lastReviewType: asString(review.reviewType) || null,
    lastReviewFollowup: typeof review.followupRequired === 'boolean' ? review.followupRequired : null,
  };
}

export function normalizeReviewDetail(taskId, record) {
  const manualReview = record.latestManualReview?.review ?? null;
  const autoReview = record.latestAutoReview?.review ?? null;
  const review = manualReview ?? autoReview;

  if (!review) {
    return {
      taskId,
      reviewId: null,
      score: null,
      feedback: null,
      status: null,
      reviewType: null,
      followupRequired: null,
      reviewerName: null,
      reviewerEmail: null,
      audit: null,
      qualityDimensions: [],
    };
  }

  const qualityDimensions = coerceArray(review.qualityDimensionValues).map((qd) => ({
    name: asString(qd.qualityDimension?.name) || asString(qd.qualityDimensionId) || 'Unknown',
    score: typeof qd.score === 'number' ? qd.score : null,
    weight: typeof qd.weight === 'number' ? qd.weight : null,
    scoreText: asString(qd.scoreText) || null,
  }));

  return {
    taskId,
    reviewId: asString(review.id) || null,
    score: typeof review.score === 'number' ? review.score : null,
    feedback: asString(review.feedback) || null,
    status: asString(review.status) || null,
    reviewType: asString(review.reviewType) || null,
    followupRequired: typeof review.followupRequired === 'boolean' ? review.followupRequired : null,
    reviewerName: asString(review.reviewer?.name) || null,
    reviewerEmail: asString(review.reviewer?.turingEmail) || null,
    audit: isRecord(review.audit) ? review.audit : null,
    qualityDimensions,
  };
}

export function normalizeReviewFromReviewsApi(taskId, review) {
  if (!review) {
    return {
      taskId,
      reviewId: null,
      score: null,
      feedback: null,
      status: null,
      reviewType: null,
      followupRequired: null,
      reviewerName: null,
      reviewerEmail: null,
      audit: null,
      qualityDimensions: [],
    };
  }

  const rawQds = coerceArray(review.qualityDimensionValues);
  const sortedQds = rawQds.slice().sort((a, b) => {
    const aOrder = typeof a.projectQualityDimension?.sortOrder === 'number' ? a.projectQualityDimension.sortOrder : 999;
    const bOrder = typeof b.projectQualityDimension?.sortOrder === 'number' ? b.projectQualityDimension.sortOrder : 999;
    return aOrder - bOrder;
  });

  const qualityDimensions = sortedQds.map((qd) => ({
    name:
      asString(qd.projectQualityDimension?.name) ||
      asString(qd.qualityDimension?.name) ||
      'Unknown',
    score: typeof qd.score === 'number' ? qd.score : null,
    weight: typeof qd.weight === 'number' ? qd.weight : null,
    scoreText: asString(qd.scoreText) || null,
    feedback: asString(qd.feedback) || null,
    trainerFeedback: asString(qd.trainerFeedback) || null,
  }));

  return {
    taskId,
    reviewId: asString(review.id) || null,
    score: typeof review.score === 'number' ? review.score : null,
    feedback: asString(review.feedback) || null,
    status: asString(review.status) || null,
    reviewType: asString(review.reviewType) || null,
    followupRequired: typeof review.followupRequired === 'boolean' ? review.followupRequired : null,
    reviewerName: asString(review.reviewer?.name) || null,
    reviewerEmail: asString(review.reviewer?.turingEmail) || null,
    audit: isRecord(review.audit) ? review.audit : null,
    qualityDimensions,
  };
}

export function resolveTaskSchemaInfo(metadata) {
  return resolveTaskRouting(metadata);
}

function extractConversationMetadata(record) {
  return mergeMetadataValues(
    record?.metadata,
    record?.turingMetadata,
    record?.seed?.metadata,
    record?.seed?.turingMetadata,
  );
}

function mergeMetadataValues(...values) {
  const normalizedValues = values
    .map((value) => coerceStructuredValue(value))
    .filter((value) => value !== null && value !== undefined);

  const recordValues = normalizedValues.filter((value) => isRecord(value));

  if (recordValues.length) {
    return Object.assign({}, ...recordValues);
  }

  return normalizedValues.at(-1) ?? null;
}

function resolveBatchId(record) {
  return asString(record.batch?.id ?? record.latestDeliveryBatch?.deliveryBatch?.id ?? record.batchId);
}

function resolveBatchName(record) {
  return (
    asString(record.batch?.name) ||
    asString(record.latestDeliveryBatch?.deliveryBatch?.name) ||
    asString(record.batchName) ||
    'Unknown'
  );
}

function resolveCollabLink(record, taskId) {
  const rawValue = asString(
    record.colabLink ?? readNestedValue(record, ['task', 'colabLink']),
  ).trim();

  if (!rawValue) {
    return undefined;
  }

  if (/^https?:\/\//i.test(rawValue)) {
    return rawValue;
  }

  const promptPath = rawValue.startsWith('prompt/') ? rawValue : `prompt/${rawValue.replace(/^\/+/, '')}`;
  const url = new URL(promptPath, `${DEFAULT_RLHF_BASE_URL}/`);

  if (!url.searchParams.get('origin')) {
    url.searchParams.set('origin', DEFAULT_LABELING_BASE_URL);
  }

  if (!url.searchParams.get('redirect_url')) {
    url.searchParams.set('redirect_url', `${DEFAULT_LABELING_BASE_URL}/conversations/${taskId}/view`);
  }

  return url.toString();
}

export function summarizeMetadata(metadata) {
  if (metadata === null || metadata === undefined) {
    return 'No metadata';
  }

  if (typeof metadata === 'string') {
    return metadata;
  }

  try {
    return JSON.stringify(metadata, null, 2);
  } catch {
    return 'Metadata could not be rendered';
  }
}

export function findPromptIdentifier(value) {
  if (!value || typeof value !== 'object') {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const nested = findPromptIdentifier(item);
      if (nested) {
        return nested;
      }
    }
    return null;
  }

  const record = value;
  const candidateKeys = ['promptId', 'promptID', 'promptUuid', 'promptUUID', 'prompt_id', 'prompt_uuid'];

  for (const key of candidateKeys) {
    if (record[key]) {
      return asString(record[key]);
    }
  }

  for (const nestedValue of Object.values(record)) {
    const nested = findPromptIdentifier(nestedValue);
    if (nested) {
      return nested;
    }
  }

  return null;
}

export function inferBusinessStatus(record) {
  const rawStatus = asString(record.status).toLowerCase();
  const followupRequired =
    record.latestManualReview?.review?.followupRequired ??
    record.latestAutoReview?.review?.followupRequired ??
    record.needFollowup ??
    record.followupRequired;

  if (rawStatus === 'rework') {
    return 'Rework';
  }

  if (rawStatus === 'labeling' || rawStatus === 'validating') {
    return 'In Progress';
  }

  if (rawStatus === 'completed') {
    return 'Completed';
  }

  return toDisplayLabel(rawStatus || 'unknown');
}

export function inferCurrentStatus(record) {
  const status =
    asString(record.latestLabelingWorkflow?.workflow?.currentWorkflowStatus) ||
    asString(record.latestLabelingWorkflow?.workflow?.status) ||
    asString(record.latestManualReview?.review?.status) ||
    asString(record.latestAutoReview?.review?.status) ||
    asString(record.status);

  return status ? toDisplayLabel(status) : 'Unknown';
}

export function inferTurnCount(metadata) {
  const source = coerceStructuredValue(metadata);
  const directValue = findCountValue(source);

  if (directValue !== null) {
    return String(directValue);
  }

  const arrayValue = findTurnArrayLength(source);
  return arrayValue !== null ? String(arrayValue) : 'Unknown';
}

export function inferComplexity(metadata) {
  const source = coerceStructuredValue(metadata);
  const complexity = findComplexityValue(source);

  return complexity ? toDisplayLabel(complexity) : 'Unknown';
}

function dedupeConversationRows(rows) {
  const byTaskId = new Map();

  for (const row of rows) {
    if (!byTaskId.has(row.taskId)) {
      byTaskId.set(row.taskId, row);
    }
  }

  return [...byTaskId.values()];
}

async function enrichConversationRowsWithCollabLinks(rows, config) {
  const missingRows = rows.filter((row) => !row.collabLink);

  if (!missingRows.length) {
    return rows;
  }

  logger.debug('Resolving missing colabLink values', {
    missingTaskIds: missingRows.map((row) => row.taskId),
  });

  const resolvedEntries = await Promise.all(
    missingRows.map(async (row) => [row.taskId, await fetchConversationCollabLink(row.taskId, config)]),
  );
  const resolvedLinks = new Map(resolvedEntries.filter((entry) => entry[1]));

  if (!resolvedLinks.size) {
    return rows;
  }

  return rows.map((row) =>
    resolvedLinks.has(row.taskId)
      ? {
          ...row,
          collabLink: resolvedLinks.get(row.taskId),
        }
      : row,
  );
}

async function fetchConversationCollabLink(taskId, config) {
  try {
    const payload = await callJsonApi(buildReviewsUrl(taskId, config), config);
    const review = pickReviewRecord(payload);

    if (!review) {
      logger.debug('No review record found for colabLink lookup', { taskId });
      return undefined;
    }

    const collabLink = resolveCollabLink(review.conversation ?? review, taskId);
    logger.debug('Resolved colabLink from reviews api', { taskId, found: Boolean(collabLink) });
    return collabLink;
  } catch {
    logger.warn('Failed to resolve colabLink from reviews api', { taskId });
    return undefined;
  }
}

async function collectTaskOutputFiles(taskId, folderPath, relativePrefix, config) {
  const entries = await readdir(folderPath, { withFileTypes: true });
  const files = [];

  for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name, undefined, { numeric: true }))) {
    const relativeName = relativePrefix ? `${relativePrefix}/${entry.name}` : entry.name;

    if (entry.isDirectory()) {
      if (entry.name.toLowerCase() === '_internal') {
        continue;
      }

      files.push(...(await collectTaskOutputFiles(taskId, join(folderPath, entry.name), relativeName, config)));
      continue;
    }

    if (!entry.isFile()) {
      continue;
    }

    const filePath = resolveTaskOutputFilePath(taskId, relativeName, config);
    const metadata = await stat(filePath);

    files.push({
      name: relativeName,
      size: metadata.size,
      modifiedAt: metadata.mtime.toISOString(),
      extension: readFileExtension(entry.name),
    });
  }

  return files;
}

async function buildTaskOutputTaskRow(taskId, folderPath, config) {
  try {
    const response = await callJsonApi(buildConversationDetailUrl(taskId, config), config);
    const record = coerceRecord(response);

    if (!Object.keys(record).length) {
      return null;
    }

    const liveRow = normalizeConversation(record);
    return {
      ...liveRow,
      source: 'task-output',
    };
  } catch {
    // Fall back to local metadata when API is unreachable (no cookie, network error, etc.)
    const metadata = await readTaskOutputMetadata(taskId, folderPath);
    const schema = resolveTaskSchemaInfo(metadata);
    return {
      taskId,
      metadata,
      metadataPreview: summarizeMetadata(metadata),
      status: 'Available Locally',
      businessStatus: 'Available Locally',
      turnCount: inferTurnCount(metadata),
      complexity: inferComplexity(metadata),
      batchId: '',
      batch: '',
      schemaName: schema.schemaName || 'Unknown',
      assignedUser: inferAssignedUserFromMetadata(metadata) || 'Unassigned',
      promptId: findPromptIdentifier(metadata) ?? undefined,
      collabLink: resolveCollabLink(isRecord(metadata) ? metadata : {}, taskId),
      source: 'task-output',
      updatedAt: null,
      lastReviewScore: null,
    };
  }
}

async function readTaskOutputValidationStatus(taskId, folderPath) {
  try {
    const validationDir = join(folderPath, '_validation');
    const entries = await readdir(validationDir, { withFileTypes: true });
    const masterFile = entries.find(
      (e) => e.isFile() && e.name.toLowerCase().startsWith('master_validator_'),
    );

    if (!masterFile) {
      return 'Not Validated';
    }

    const content = await readFile(join(validationDir, masterFile.name), 'utf8');
    const parsed = coerceStructuredValue(content);
    const summary = isRecord(parsed) ? parsed.summary : null;

    if (!isRecord(summary)) {
      return 'Not Validated';
    }

    const failed = typeof summary.tasksFailed === 'number' ? summary.tasksFailed : typeof summary.validatorsFailed === 'number' ? summary.validatorsFailed : null;

    if (failed === null) {
      return 'Validated';
    }

    return failed === 0 ? 'Passed' : 'Failed';
  } catch {
    return 'Not Validated';
  }
}

function inferAssignedUserFromMetadata(metadata) {
  if (!isRecord(metadata)) {
    return '';
  }

  const candidate =
    asString(metadata.currentUserName) ||
    asString(metadata.assignedUser) ||
    asString(metadata.user) ||
    asString(metadata.userName) ||
    asString(metadata.annotator) ||
    asString(metadata.labeler);

  return candidate || '';
}

async function readTaskOutputMetadata(taskId, folderPath) {
  const entries = await readdir(folderPath, { withFileTypes: true });
  const metadataEntry = entries.find((entry) => entry.isFile() && isTaskMetadataFile(entry.name, taskId));

  if (!metadataEntry) {
    return null;
  }

  try {
    const content = await readFile(join(folderPath, metadataEntry.name), 'utf8');
    return coerceStructuredValue(content);
  } catch {
    return null;
  }
}

function isTaskMetadataFile(name, taskId) {
  const normalizedName = name.trim().toLowerCase();
  const normalizedTaskId = taskId.trim().toLowerCase();

  return normalizedName === `${normalizedTaskId}_1metadata.json` || normalizedName.endsWith('metadata.json');
}

function pickReviewRecord(payload) {
  if (Array.isArray(payload) && payload.length) {
    return isRecord(payload[0]) ? payload[0] : null;
  }

  if (!isRecord(payload)) {
    return null;
  }

  if (isRecord(payload.conversation)) {
    return payload;
  }

  for (const key of ['data', 'items', 'rows', 'results']) {
    const candidate = payload[key];

    if (Array.isArray(candidate) && candidate.length && isRecord(candidate[0])) {
      return candidate[0];
    }
  }

  return null;
}

function readCountCandidate(source, keys) {
  for (const key of keys) {
    const value = source?.[key];

    if (typeof value === 'number' && Number.isFinite(value)) {
      return value;
    }

    if (typeof value === 'string' && /^\d+$/.test(value.trim())) {
      return Number(value.trim());
    }
  }

  return null;
}

function readArrayCountCandidate(source, keys) {
  for (const key of keys) {
    const value = source?.[key];

    if (Array.isArray(value)) {
      return value.length;
    }
  }

  return null;
}

function findCountValue(source) {
  if (!source || typeof source !== 'object') {
    return null;
  }

  if (Array.isArray(source)) {
    for (const item of source) {
      const nested = findCountValue(item);
      if (nested !== null) {
        return nested;
      }
    }

    return null;
  }

  const countKeys = new Set([
    'numturns',
    'turncount',
    'numberofturns',
    'turnscount',
    'num_turns',
    'turn_count',
    'number_of_turns',
    'turns_count',
  ]);

  for (const [key, value] of Object.entries(source)) {
    if (countKeys.has(normalizeKey(key))) {
      const direct = readCountCandidate({ [key]: value }, [key]);
      if (direct !== null) {
        return direct;
      }
    }
  }

  for (const value of Object.values(source)) {
    const nested = findCountValue(value);
    if (nested !== null) {
      return nested;
    }
  }

  return null;
}

function findTurnArrayLength(source) {
  if (!source || typeof source !== 'object') {
    return null;
  }

  if (Array.isArray(source)) {
    for (const item of source) {
      const nested = findTurnArrayLength(item);
      if (nested !== null) {
        return nested;
      }
    }

    return null;
  }

  const arrayKeys = new Set(['turns', 'conversationturns', 'messages', 'promptturns']);

  for (const [key, value] of Object.entries(source)) {
    if (arrayKeys.has(normalizeKey(key)) && Array.isArray(value)) {
      return value.length;
    }
  }

  for (const value of Object.values(source)) {
    const nested = findTurnArrayLength(value);
    if (nested !== null) {
      return nested;
    }
  }

  return null;
}

function findComplexityValue(source) {
  if (source === null || source === undefined) {
    return null;
  }

  if (typeof source === 'string') {
    return source.trim() || null;
  }

  if (Array.isArray(source)) {
    for (const item of source) {
      const nested = findComplexityValue(item);
      if (nested) {
        return nested;
      }
    }

    return null;
  }

  if (typeof source !== 'object') {
    return null;
  }

  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeKey(key);

    if ((normalizedKey === 'complexity' || normalizedKey === 'difficulty') && typeof value === 'string') {
      return value;
    }

    if ((normalizedKey === 'difficultylevel' || normalizedKey === 'difficulty_level') && value) {
      const fromLevel =
        asString(value.levelInfo?.name) || asString(value.level_info?.name) || asString(value.name);

      if (fromLevel) {
        return fromLevel;
      }
    }

    if ((normalizedKey === 'levelinfo' || normalizedKey === 'level_info') && value && typeof value === 'object') {
      const fromInfo = asString(value.name);
      if (fromInfo) {
        return fromInfo;
      }
    }
  }

  for (const value of Object.values(source)) {
    const nested = findComplexityValue(value);
    if (nested) {
      return nested;
    }
  }

  return null;
}

function coerceStructuredValue(value) {
  if (typeof value !== 'string') {
    return value;
  }

  const trimmed = value.trim();
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) {
    return value;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
}

function normalizeKey(value) {
  return value.toLowerCase().replace(/[\s-]+/g, '_');
}

function readNestedValue(source, path) {
  let current = source;

  for (const key of path) {
    if (!current || typeof current !== 'object' || !(key in current)) {
      return null;
    }

    current = current[key];
  }

  return current;
}

async function callJsonApi(url, config, init = {}) {
  if (!config.cookie) {
    throw Object.assign(new Error('Missing TURING_COOKIE. Add it to .env.local before calling the proxy.'), {
      statusCode: 500,
    });
  }

  const startedAt = Date.now();
  const authorizationHeader = resolveAuthorizationHeader(config);
  const headers = {
    Accept: 'application/json',
    Cookie: config.cookie,
    ...(authorizationHeader ? { Authorization: authorizationHeader } : {}),
    ...(init.headers ?? {}),
  };

  logger.debug('Calling upstream API', {
    url,
    method: init.method ?? 'GET',
    headers: sanitizeHeaders(headers),
  });

  const response = await fetch(url, {
    ...init,
    headers,
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    logger.warn('Upstream API failed', {
      url,
      method: init.method ?? 'GET',
      statusCode: response.status,
      durationMs: Date.now() - startedAt,
      body,
    });
    throw Object.assign(new Error(`Upstream request failed with ${response.status}: ${body || response.statusText}`), {
      statusCode: response.status,
    });
  }

  if (isBackendDebugEnabled()) {
    logger.debug('Upstream API succeeded', {
      url,
      method: init.method ?? 'GET',
      statusCode: response.status,
      durationMs: Date.now() - startedAt,
    });
  }

  return safeReadJson(response);
}

function coerceArray(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }

  if (payload && typeof payload === 'object') {
    const candidates = [payload.data, payload.items, payload.rows, payload.results, payload.contributors];

    for (const candidate of candidates) {
      if (Array.isArray(candidate)) {
        return candidate;
      }
    }
  }

  return [];
}

function coerceRecord(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return {};
  }

  const candidates = [payload.data, payload.user, payload.me, payload.profile];

  for (const candidate of candidates) {
    if (candidate && typeof candidate === 'object' && !Array.isArray(candidate)) {
      return candidate;
    }
  }

  return payload;
}

function normalizeTeamMember(record) {
  const firstName = asString(record.firstName ?? record.first_name).trim();
  const lastName = asString(record.lastName ?? record.last_name).trim();
  const combinedName = [firstName, lastName].filter(Boolean).join(' ');

  return {
    id: asString(record.id ?? record.userId ?? record.user_id ?? record.contributorId),
    name: asString(record.name ?? record.fullName ?? record.full_name ?? record.turingName ?? combinedName),
    email: asString(record.turingEmail ?? record.turing_email ?? record.email ?? record.username),
  };
}

function dedupeTeamMembers(records) {
  const byId = new Map();

  for (const record of records) {
    if (!record?.id || byId.has(record.id)) {
      continue;
    }

    byId.set(record.id, record);
  }

  return [...byId.values()];
}

function normalizeBatch(record) {
  return {
    id: asString(record.id),
    name: asString(record.name),
  };
}

function dedupeBatches(records) {
  const byId = new Map();

  for (const record of records) {
    if (!record?.id || byId.has(record.id)) {
      continue;
    }

    byId.set(record.id, record);
  }

  return [...byId.values()];
}

function resolveTaskOutputFolder(taskId, config) {
  if (!/^[a-z0-9_-]+$/i.test(taskId)) {
    throw notFoundError(`Task ${taskId} is not a valid task folder name.`);
  }

  const rootPath = pathResolve(config.taskOutputDir);
  const folderPath = pathResolve(rootPath, taskId);

  ensurePathWithin(rootPath, folderPath, `Task ${taskId} is outside the configured output folder.`);
  return folderPath;
}

function resolveTaskOutputFilePath(taskId, name, config) {
  const folderPath = resolveTaskOutputFolder(taskId, config);
  const filePath = pathResolve(folderPath, name);

  ensurePathWithin(folderPath, filePath, `File ${name} is outside task ${taskId}.`);
  return filePath;
}

function ensurePathWithin(rootPath, targetPath, message) {
  const relativePath = pathRelative(rootPath, targetPath);

  if (!relativePath || relativePath === '.') {
    return;
  }

  if (relativePath.startsWith('..') || pathResolve(rootPath, relativePath) !== targetPath) {
    throw notFoundError(message);
  }
}

function readFileExtension(name) {
  const dotIndex = name.lastIndexOf('.');
  return dotIndex === -1 ? '' : name.slice(dotIndex + 1).toLowerCase();
}

function notFoundError(message) {
  return Object.assign(new Error(message), { statusCode: 404 });
}

function asTaskOutputError(error, fallbackMessage) {
  if (typeof error === 'object' && error !== null && 'statusCode' in error) {
    return error;
  }

  if (typeof error === 'object' && error !== null && 'code' in error) {
    const code = error.code;

    if (code === 'ENOENT' || code === 'ENOTDIR') {
      return notFoundError(fallbackMessage);
    }
  }

  return error;
}

function isExactTaskId(taskId) {
  return /^[a-z0-9-]+$/i.test(taskId.trim());
}

function asString(value) {
  if (value === null || value === undefined) {
    return '';
  }

  return String(value);
}

function isRecord(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function toDisplayLabel(value) {
  return value
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

async function safeReadJson(response) {
  const body = await safeReadText(response);

  if (!body.trim()) {
    return {};
  }

  try {
    return JSON.parse(body);
  } catch {
    return {};
  }
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch {
    return '';
  }
}



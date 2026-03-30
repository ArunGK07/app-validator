import { mkdir, readdir, unlink, writeFile } from 'node:fs/promises';
import { resolve as pathResolve, relative as pathRelative } from 'node:path';

import { resolveAuthorizationHeader } from './auth-header.mjs';
import { createLogger, sanitizeHeaders } from './logger.mjs';
import { generateTaskSchemaArtifact } from './schema-extractor.mjs';
import { formatTaskArtifactName } from './workspace-config.mjs';

const DEFAULT_GRAPHQL_URL = 'https://rlhf-api.turing.com/graphql';
const DEFAULT_LABELING_BASE_URL = 'https://labeling-o.turing.com';
const logger = createLogger('task-output');
const EXTRACTED_NAME_TEMPLATE_KEY_BY_NORMALIZED_NAME = {
  user: 'turn_user_file',
  tables: 'turn_tables_file',
  tablesrequired: 'turn_tables_file',
  columns: 'turn_columns_file',
  columnsrequired: 'turn_columns_file',
  referenceanswer: 'turn_reference_answer_file',
  testcase: 'turn_test_cases_file',
  testcases: 'turn_test_cases_file',
  reasoningtypes: 'turn_reasoning_types_file',
  plsqlconstructs: 'turn_plsql_constructs_file',
};

const GRAPHQL_QUERY = `query GetPrompt($id: ID!) {  prompt(idOrUuid: $id) {    ...PromptBaseFields    modelModalities    feedback    genericTextLLMChecks {      ...GenericTextLLMCheckBaseFields      __typename    }    promptTurns {      ...PromptTurnBaseFields      rlhfCopilot      promptResponses {        ...PromptResponseBaseFields        failedRequests {          id          failureReason          requestId          requestGroupId          __typename        }        __typename      }      __typename    }    llmChecks {      ...LLMCheckBaseFields      feedbacks {        ...LLMCheckFeedbackBaseFields        __typename      }      __typename    }    formData {      ...FormDataBaseFields      __typename    }    __typename  }}fragment ReviewCriteriaBaseFields on ReviewCriteria {  key  name  description  type  displayCondition  addToMetadata  freeTextType  disableCodeEditor  uploadToGCS  customGcsUpload  gcsBucketPath  jsonTemplate  collapsible  allowCopy  blockCopyPaste  autoExpandDescription  maxDynamicGroups  inputFieldsGroup {    title    type    enableWordCount    maxWordCount    options {      name      value      __typename    }    __typename  }  readonlyInputGroups {    title    type    options {      name      value    __typename    }    __typename  }  autoDefault  autoDefaultKey  autoFill  autoFillKey  autoFillFromQualityDimensionAgents  autoFillProjectQualityDimension  options {    name    value    displayCondition    __typename  }  llmCheckEnabled  disableSapling  allowCopy  allowOptional  enableTabularView  collapsible  visibility {    id    key    name    order    label    value    __typename  }  editability {    id    key    name    order    label    value    __typename  }  weight  allowCountToOverallRating  copilot {    rubricCriteria    systemPrompt    isSystemPromptValidated    models {      model      provider      temperature      __typename    }    __typename  }  __typename}fragment PromptTurnUploadedFilesBaseFields on PromptTurnUploadedFile {  id  filename  gcsPath  mimeType  createdAt  updatedAt  signedUrl  transcriptText  __typename}fragment PromptResponseClaimBaseFields on PromptResponseClaim {  id  claim  createdAt  updatedAt  feedback  claimIndex  __typename}fragment PromptResponseStepBaseFields on PromptResponseStep {  id  step  createdAt  updatedAt  suggestion  stepIndex  feedbackType  rawPromptToLLM  rawLLMResponse  type  humanFeedback  promptResponseId  stepHistory {    normal    reasoning    __typename  }  lineContent  lineFeedbackHistory {    lines    feedbackType    feedback    timestamp    selectedRange {      start      end      __typename    }    __typename  }  selectedLineRange {    start    end    __typename  }  rewrittenLines  streamingMetadata  __typename}fragment PromptResponseSearchContextBaseFields on PromptResponseSearchContext {  id  promptResponseId  type  feedback  article  searchContextUUID  createdAt  updatedAt  __typename}fragment PromptResponsesUploadedFilesBaseFields on PromptResponsesUploadedFile {  id  filename  gcsPath  mimeType  createdAt  updatedAt  signedUrl  __typename}fragment PromptBaseFields on Prompt {  id  uuid  status  modelConfig  promptState  conversationHistory  qualityDimensions {    systemName    qualityGuidelines    qualityEvaluationRules    checkedPart    __typename  }  reviewCriteria {    ...ReviewCriteriaBaseFields    __typename  }  createdAt  updatedAt  metadata  turingMetadata  config  type  lastSupervisionOperation  __typename}fragment GenericTextLLMCheckBaseFields on GenericTextLLMCheck {  id  status  text  runId  result  createdAt  updatedAt  __typename}fragment PromptTurnBaseFields on PromptTurn {  id  prompt  createdAt  updatedAt  promptIndex  groupIndex  tags  parentId  feedback  feedbackLoop  promptEvaluationFeedback  preferenceJustification  preferenceSignal  customTitle  uploadedFiles {    ...PromptTurnUploadedFilesBaseFields    __typename  }  idealResponse  idealResponseAsPreferred  idealResponseLLMReviewStatus  idealResponseLLMReviewPayload  unratable  isToolTurn  hint  hintHistory  historyLine  groundTruth  initialSteps  selectedModel  selectedParsingSystemPrompt  timingEvents  taskStage  systemPrompt  __typename}fragment PromptResponseBaseFields on PromptResponse {  id  claims {    ...PromptResponseClaimBaseFields    __typename  }  steps {    ...PromptResponseStepBaseFields    __typename  }  searchContexts {    ...PromptResponseSearchContextBaseFields    __typename  }  searchContextPayload {    id    searchResult    searchQuery    type    __typename  }  uploadedFiles {    ...PromptResponsesUploadedFilesBaseFields    __typename  }  response  toolCalls  toolOutputs  stepDetails  model  temperature  feedback  failureReason  chosenToContinue  createdAt  updatedAt  promptTurnId  llmReviewPayload  llmReviewStatus  tags  overallWebRagFeedback  overallXRagFeedback  requestId  requestGroupId  rawPromptToLLM  rawLLMResponse  supervisionStatus  reasoningSummary  reasoningSummaryModel  reasoningSummaryPrompt  rubricEvaluation {    id    totalPossibleScore    netScore    passRate    complexityLevel    evaluationDetails    evaluationModel    evaluationTemperature    createdAt    updatedAt    __typename  }  __typename}fragment LLMCheckBaseFields on LLMCheck {  id  checkPartType  status  runId  resultJson  checkPartId  createdAt  updatedAt  promptId  __typename}fragment LLMCheckFeedbackBaseFields on LLMChecksFeedback {  id  llmCheckId  qualityDimension  thumbScore  feedback  isIncorrect  createdAt  updatedAt  __typename}fragment FormDataBaseFields on FormData {  id  promptId  feedback  input  formStage  copilotSuggestions  agenticReviewQDFeedback  customAPIChecks  __typename}`;

export async function fetchTaskOutputArtifacts(task, config, dependencies = {}) {
  const taskId = asTaskId(task?.taskId);
  logger.debug('Starting task output fetch', { taskId, hasPromptId: Boolean(task?.promptId), hasCollabLink: Boolean(task?.collabLink) });

  if (!config.cookie) {
    throw withStatus(new Error('Missing TURING_COOKIE. Add it to .env.local before fetching task output.'), 500);
  }

  let collabLink = asOptionalString(task?.collabLink);
  let promptId = extractPromptId(asOptionalString(task?.promptId) || collabLink);

  if (!promptId) {
    collabLink = collabLink || (await fetchCollabLinkFromReviews(taskId, config));
    promptId = extractPromptId(collabLink);
  }

  if (!promptId) {
    throw withStatus(
      new Error(`Task ${taskId} is missing both promptId and collabLink, and no colabLink was found in the reviews API.`),
      400,
    );
  }

  const responsePayload = await fetchPromptPayload(promptId, collabLink, config);
  const normalizedMetadata = isRecord(task?.metadata) ? buildTaskMetadataPayload(taskId, task.metadata, collabLink) : null;
  const hydratedResponsePayload = applyTaskMetadataToPromptPayload(responsePayload, normalizedMetadata);
  const metadataFile = await writeTaskMetadataFile(taskId, task?.metadata, collabLink, config);
  const outputPath = await writeExistingOutputFile(taskId, collabLink, promptId, hydratedResponsePayload, config);
  const generatedFiles = await extractPromptTurnEvaluations(taskId, hydratedResponsePayload, config);
  const schemaResult = await tryGenerateSchemaArtifact(
    {
      taskId,
      metadata: isRecord(task?.metadata) ? task.metadata : null,
      taskDir: resolveTaskOutputFolder(taskId, config),
    },
    config,
    dependencies,
  );
  logger.info('Completed task output fetch', {
    taskId,
    promptId,
    generatedFileCount: generatedFiles.length,
    existingOutputFile: outputPath.name,
    metadataFile,
    schemaFile: schemaResult.schemaFile,
  });

  return {
    taskId,
    promptId,
    collabLink,
    graphqlUrl: config.rlhfGraphqlUrl ?? DEFAULT_GRAPHQL_URL,
    folderPath: resolveTaskOutputFolder(taskId, config),
    existingOutputFile: outputPath.name,
    metadataFile,
    generatedFiles,
    schemaFile: schemaResult.schemaFile,
    schemaError: schemaResult.schemaError,
    graphqlErrors: Array.isArray(hydratedResponsePayload.errors) ? hydratedResponsePayload.errors.length : 0,
  };
}

async function fetchCollabLinkFromReviews(taskId, config) {
  logger.debug('Looking up colabLink from reviews api', { taskId });
  const authorizationHeader = resolveAuthorizationHeader(config);
  const headers = {
    Accept: 'application/json',
    Cookie: config.cookie,
    ...(authorizationHeader ? { Authorization: authorizationHeader } : {}),
  };
  const response = await fetch(buildReviewListUrl(taskId, config), {
    method: 'GET',
    headers,
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    logger.warn('Reviews api lookup failed', { taskId, statusCode: response.status, body });
    throw withStatus(
      new Error(`Task ${taskId} review lookup failed with ${response.status}: ${body || response.statusText}`),
      response.status,
    );
  }

  const payload = await response.json();
  const review = pickReviewFromListing(payload);
  const collabLink = asOptionalString(review?.conversation?.colabLink);
  logger.debug('Reviews api lookup completed', { taskId, found: Boolean(collabLink) });

  return collabLink;
}

async function fetchPromptPayload(promptId, collabLink, config) {
  const startedAt = Date.now();
  const authorizationHeader = resolveAuthorizationHeader(config);
  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    Cookie: config.cookie,
    ...(authorizationHeader ? { Authorization: authorizationHeader } : {}),
    ...(collabLink ? { referer: collabLink } : {}),
  };
  const response = await fetch(config.rlhfGraphqlUrl ?? DEFAULT_GRAPHQL_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      operationName: 'GetPrompt',
      variables: { id: promptId },
      query: GRAPHQL_QUERY,
    }),
  });

  if (!response.ok) {
    const body = await safeReadText(response);
    logger.warn('RLHF GraphQL fetch failed', {
      promptId,
      statusCode: response.status,
      durationMs: Date.now() - startedAt,
      body,
    });
    throw withStatus(
      new Error(`Task output fetch failed with ${response.status}: ${body || response.statusText}`),
      response.status,
    );
  }

  logger.debug('RLHF GraphQL fetch succeeded', {
    promptId,
    statusCode: response.status,
    durationMs: Date.now() - startedAt,
    headers: sanitizeHeaders(headers),
  });

  return response.json();
}

async function writeTaskMetadataFile(taskId, metadata, collabLink, config) {
  if (!isRecord(metadata)) {
    return null;
  }

  const taskDir = await ensureTaskOutputFolder(taskId, config);
  const fileName = formatTaskOutputName('metadata_file', { taskId });
  const filePath = pathResolve(taskDir, fileName);
  const payload = buildTaskMetadataPayload(taskId, metadata, collabLink);

  await writeFile(filePath, JSON.stringify(payload, null, 2), 'utf8');
  return fileName;
}

function buildTaskMetadataPayload(taskId, metadata, collabLink) {
  const payload = {
    ...metadata,
    id: normalizeTaskIdForMetadata(taskId),
  };

  if (collabLink) {
    payload.colabLink = collabLink;
  }

  return payload;
}

function applyTaskMetadataToPromptPayload(responsePayload, metadata) {
  if (!isRecord(metadata)) {
    return responsePayload;
  }

  if (!isRecord(responsePayload) || !isRecord(responsePayload.data) || !isRecord(responsePayload.data.prompt)) {
    return responsePayload;
  }

  const prompt = responsePayload.data.prompt;
  const nextPrompt = {
    ...prompt,
    ...metadata,
    metadata: {
      ...(isRecord(prompt.metadata) ? prompt.metadata : {}),
      ...metadata,
    },
    turingMetadata: {
      ...(isRecord(prompt.turingMetadata) ? prompt.turingMetadata : {}),
      ...metadata,
    },
  };

  return {
    ...responsePayload,
    data: {
      ...responsePayload.data,
      prompt: nextPrompt,
    },
  };
}

function normalizeTaskIdForMetadata(taskId) {
  return /^\d+$/.test(taskId) ? Number(taskId) : taskId;
}

async function writeExistingOutputFile(taskId, collabLink, promptId, responsePayload, config) {
  const taskDir = await ensureTaskOutputFolder(taskId, config);
  const outputPath = pathResolve(taskDir, formatTaskOutputName('existing_output_file', { taskId }));
  const payload = {
    task_id: Number(taskId),
    colabLink: collabLink || null,
    prompt_id: promptId,
    graphql_url: config.rlhfGraphqlUrl ?? DEFAULT_GRAPHQL_URL,
    fetched_at_utc: new Date().toISOString(),
    response: responsePayload,
  };

  await writeFile(outputPath, JSON.stringify(payload, null, 2), 'utf8');
  return { name: formatTaskOutputName('existing_output_file', { taskId }), path: outputPath };
}

async function extractPromptTurnEvaluations(taskId, responsePayload, config) {
  const prompt = responsePayload?.data?.prompt;

  if (!prompt || typeof prompt !== 'object') {
    throw withStatus(new Error(`Task ${taskId} returned no prompt payload from GraphQL.`), 502);
  }

  const promptTurns = Array.isArray(prompt.promptTurns) ? prompt.promptTurns : null;

  if (!promptTurns || !promptTurns.length) {
    const customCriteria = prompt?.formData?.feedback?.customModelReviewCriteria;

    if (Array.isArray(customCriteria) && customCriteria.length) {
      throw withStatus(
        new Error(
          'Prompt uses formData.customModelReviewCriteria with no promptTurns. This extractor only supports promptTurnEvaluation payloads.',
        ),
        422,
      );
    }

    throw withStatus(new Error(`Prompt for task ${taskId} does not contain any promptTurns to extract.`), 422);
  }

  const extracted = new Map();

  for (const [index, promptTurn] of promptTurns.entries()) {
    if (!promptTurn || typeof promptTurn !== 'object') {
      continue;
    }

    const turnNumber = Number.isInteger(promptTurn.promptIndex) ? promptTurn.promptIndex + 1 : index + 1;
    const evaluationItems = promptTurn?.promptEvaluationFeedback?.promptTurnEvaluation;

    if (!Array.isArray(evaluationItems)) {
      continue;
    }

    for (const item of evaluationItems) {
      if (!item || typeof item !== 'object') {
        continue;
      }

      const name = asOptionalString(item.name);
      const value = typeof item.value === 'string' ? item.value : '';

      if (!name || !value) {
        continue;
      }

      const key = `${turnNumber}::${name}`;
      const existing = extracted.get(key) ?? { turnNumber, name, values: [] };
      existing.values.push(value);
      extracted.set(key, existing);
    }
  }

  const taskDir = await ensureTaskOutputFolder(taskId, config);
  await clearExistingExtractedFiles(taskDir, taskId);

  const writtenFiles = [];

  for (const entry of extracted.values()) {
    const fileName = resolveExtractedOutputName(taskId, entry.turnNumber, entry.name);
    const filePath = pathResolve(taskDir, fileName);
    const content = normalizeExtractedContent(fileName, `${entry.values.join('\n\n').replace(/\s+$/u, '')}\n`);

    await writeFile(filePath, content, 'utf8');
    writtenFiles.push(fileName);
  }

  logger.debug('Extracted prompt turn evaluation files', { taskId, writtenFiles });
  return writtenFiles.sort((left, right) => left.localeCompare(right, undefined, { numeric: true }));
}

function normalizeExtractedContent(fileName, content) {
  if (!/_1user\.txt$/i.test(String(fileName))) {
    return content;
  }

  return normalizeLegacyUserPrompt(content);
}

function normalizeLegacyUserPrompt(content) {
  const lines = String(content).replace(/\r/g, '').split('\n');
  const normalized = [];
  let section = null;

  for (const line of lines) {
    const trimmed = line.trim();

    if (/^Requirements\s*:/i.test(trimmed)) {
      section = 'requirements';
      normalized.push('Requirements:');
      continue;
    }

    if (/^Parameters\s*:/i.test(trimmed)) {
      section = 'parameters';
      normalized.push('Parameters:');
      continue;
    }

    if (/^Output\s*:/i.test(trimmed)) {
      section = 'output';
      normalized.push('Output:');
      continue;
    }

    if (/^Sorting\s+Order\s*:/i.test(trimmed)) {
      section = 'sorting_order';
      normalized.push('Sorting Order:');
      continue;
    }

    if (/^Exception\s+Handling\s*:/i.test(trimmed)) {
      section = 'exception_handling';
      normalized.push('Exception Handling:');
      continue;
    }

    if (!trimmed) {
      normalized.push('');
      continue;
    }

    if (section === 'requirements') {
      const inlineRequirementMatch = trimmed.match(/^(Public Procedure Name|Procedure Name|Function Name|Package Name|Trigger Name|Object Name)\s*:\s*(.+)$/i);
      if (inlineRequirementMatch) {
        normalized.push(`  ${normalizeRequirementLabel(inlineRequirementMatch[1])}:`);
        normalized.push(`  ${inlineRequirementMatch[2].trim()}`);
        continue;
      }
    }

    if (section === 'parameters') {
      const parameterLine = normalizeLegacyParameterLine(trimmed);
      if (parameterLine) {
        normalized.push(`  ${parameterLine}`);
        continue;
      }
    }

    if (section === 'exception_handling') {
      const exceptionLine = normalizeLegacyExceptionLine(trimmed);
      if (exceptionLine) {
        normalized.push(`  ${exceptionLine}`);
        continue;
      }
    }

    normalized.push(line.replace(/\s+$/u, ''));
  }

  return `${normalized.join('\n').replace(/\n{3,}/g, '\n\n').replace(/\s+$/u, '')}\n`;
}

function normalizeRequirementLabel(label) {
  const normalized = String(label).trim().toLowerCase();
  if (normalized === 'public procedure name') {
    return 'Procedure Name';
  }

  return normalized
    .split(/\s+/)
    .map((part) => (part ? `${part[0].toUpperCase()}${part.slice(1)}` : part))
    .join(' ');
}

function normalizeLegacyParameterLine(line) {
  if (/^[A-Za-z][A-Za-z0-9_$#]*\s*:\s*$/i.test(line)) {
    return line;
  }

  if (/^(Public Procedure Name|Procedure Name|Function Name|Package Name|Trigger Name|Object Name)\s*:/i.test(line)) {
    return null;
  }

  const match = line.match(/^(?:\d+\.\s*)?([A-Za-z][A-Za-z0-9_$#]*)\s+(?:(IN\s+OUT|INOUT|IN|OUT|LOCAL)\s+)?(.+?)(?:\s*--\s*(.+))?$/i);
  if (!match) {
    return null;
  }

  const name = match[1].trim();
  const mode = (match[2] ?? 'IN').replace(/\s+/g, ' ').toUpperCase();
  const datatype = match[3].trim();
  const comment = match[4]?.trim() || 'parameter value';

  return `${name} - ${mode} - ${datatype} -- ${comment}`;
}
function normalizeLegacyExceptionLine(line) {
  const whenOthersMatch = line.match(/^WHEN\s+OTHERS\s*:\s*(.+)$/i);
  if (whenOthersMatch) {
    return `Other Exception : ${whenOthersMatch[1].trim().replace(/[.]+$/u, '')}`;
  }

  const genericMatch = line.match(/^([^:]+)\s*:\s*(.+)$/);
  if (!genericMatch) {
    return null;
  }

  return `${genericMatch[1].trim()} : ${genericMatch[2].trim().replace(/\s+/g, ' ')}`;
}
async function clearExistingExtractedFiles(taskDir, taskId) {
  const entries = await readdir(taskDir, { withFileTypes: true });
  const taskPattern = new RegExp(`^${escapeForRegex(taskId)}_turn\\d+_.+\\.(?:txt|sql)$`, 'i');
  const legacyPattern = new RegExp(`^existing_${escapeForRegex(taskId)}_.+\\.txt$`, 'i');

  await Promise.all(
    entries
      .filter((entry) => entry.isFile() && (taskPattern.test(entry.name) || legacyPattern.test(entry.name)))
      .map((entry) => unlink(pathResolve(taskDir, entry.name))),
  );
}

function resolveExtractedOutputName(taskId, turnNumber, rawName) {
  const normalized = [...rawName.toLowerCase()].filter((character) => /[a-z0-9]/.test(character)).join('');
  const templateKey = EXTRACTED_NAME_TEMPLATE_KEY_BY_NORMALIZED_NAME[normalized];

  if (templateKey) {
    return formatTaskOutputName(templateKey, { taskId, turnNumber });
  }

  const sanitized = rawName
    .trim()
    .replace(/[^a-z0-9_-]+/gi, '_')
    .replace(/^_+|_+$/g, '') || 'unnamed';

  return `${formatTaskOutputName('extracted_value_file_prefix', { taskId, turnNumber })}${sanitized}${formatTaskOutputName('extracted_value_file_suffix')}`;
}

function formatTaskOutputName(templateKey, values = {}) {
  return formatTaskArtifactName(templateKey, values);
}

function buildReviewListUrl(taskId, config) {
  const url = new URL('/api/reviews', config.labelingBaseUrl ?? DEFAULT_LABELING_BASE_URL);
  url.searchParams.set('conversationId', taskId);
  url.searchParams.set('join[0]', 'conversation');
  return url.toString();
}

function pickReviewFromListing(payload) {
  if (Array.isArray(payload) && payload.length) {
    return isRecord(payload[0]) ? payload[0] : null;
  }

  if (isRecord(payload)) {
    if (isRecord(payload.conversation)) {
      return payload;
    }

    for (const key of ['data', 'items', 'rows', 'results']) {
      const candidate = payload[key];

      if (Array.isArray(candidate) && candidate.length && isRecord(candidate[0])) {
        return candidate[0];
      }
    }
  }

  return null;
}

async function ensureTaskOutputFolder(taskId, config) {
  const folderPath = resolveTaskOutputFolder(taskId, config);
  await mkdir(folderPath, { recursive: true });
  return folderPath;
}

function resolveTaskOutputFolder(taskId, config) {
  if (!/^[a-z0-9_-]+$/i.test(taskId)) {
    throw withStatus(new Error(`Task ${taskId} is not a valid task folder name.`), 400);
  }

  const rootPath = pathResolve(config.taskOutputDir);
  const folderPath = pathResolve(rootPath, taskId);
  const relativePath = pathRelative(rootPath, folderPath);

  if (relativePath.startsWith('..') || pathResolve(rootPath, relativePath) !== folderPath) {
    throw withStatus(new Error(`Task ${taskId} is outside the configured output directory.`), 400);
  }

  return folderPath;
}

function extractPromptId(rawValue) {
  const value = asOptionalString(rawValue);

  if (!value) {
    return '';
  }

  try {
    const parsed = new URL(value);
    const parts = parsed.pathname.split('/').filter(Boolean);
    return parts.at(-1) ?? '';
  } catch {
    return value.trim();
  }
}

function asTaskId(value) {
  const taskId = asOptionalString(value);

  if (!taskId) {
    throw withStatus(new Error('Task id is required.'), 400);
  }

  return taskId;
}

function asOptionalString(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function isRecord(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

async function tryGenerateSchemaArtifact(task, config, dependencies) {
  if (!isRecord(task.metadata)) {
    return {
      schemaFile: null,
      schemaError: null,
    };
  }

  try {
    const generator = dependencies.generateTaskSchemaArtifact ?? generateTaskSchemaArtifact;
    const result = await generator(task, config, dependencies.schemaOptions);

    return {
      schemaFile: result.schemaFile ?? null,
      schemaError: null,
    };
  } catch (error) {
    const schemaError = error instanceof Error ? error.message : 'Schema generation failed.';
    logger.warn('Schema generation failed', { taskId: task.taskId, schemaError });
    return {
      schemaFile: null,
      schemaError,
    };
  }
}

function escapeForRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function withStatus(error, statusCode) {
  return Object.assign(error, { statusCode });
}

async function safeReadText(response) {
  try {
    return await response.text();
  } catch {
    return '';
  }
}

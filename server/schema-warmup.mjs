import { createLogger } from './logger.mjs';
import { generateSharedSchemaArtifact } from './schema-extractor.mjs';
import { resolveTaskRouting, supportsOracleSchemaExtraction } from './schema-db-config.mjs';
import { fetchSchemaWarmupCandidates } from './turing-api.mjs';

const logger = createLogger('schema-warmup');

export async function warmSchemaCache(config, dependencies = {}) {
  if (!config?.cookie) {
    logger.info('Skipping schema warmup because TURING_COOKIE is not configured.');
    return {
      status: 'skipped',
      candidateCount: 0,
      generatedCount: 0,
      reusedCount: 0,
      failedCount: 0,
      failures: [],
    };
  }

  const fetchCandidates = dependencies.fetchSchemaCandidates ?? fetchSchemaWarmupCandidates;
  const generateSchema = dependencies.generateSharedSchemaArtifact ?? generateSharedSchemaArtifact;
  let candidates = [];

  try {
    candidates = dedupeSchemaCandidates(await fetchCandidates(config));
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Schema warmup failed while fetching candidates.';

    logger.error('Schema warmup could not fetch candidates', { message });
    return {
      status: 'completed_with_errors',
      candidateCount: 0,
      generatedCount: 0,
      reusedCount: 0,
      failedCount: 1,
      failures: [
        {
          schemaName: null,
          profile: null,
          message,
        },
      ],
    };
  }

  if (!candidates.length) {
    logger.info('No eligible schemas found for startup warmup.');
    return {
      status: 'completed',
      candidateCount: 0,
      generatedCount: 0,
      reusedCount: 0,
      failedCount: 0,
      failures: [],
    };
  }

  logger.info('Starting schema warmup', { candidateCount: candidates.length });

  let generatedCount = 0;
  let reusedCount = 0;
  let failedCount = 0;
  const failures = [];

  for (const candidate of candidates) {
    try {
      const result = await generateSchema(candidate.metadata, config, dependencies.schemaOptions);

      if (result.source === 'database') {
        generatedCount += 1;
      } else if (result.source === 'cache') {
        reusedCount += 1;
      }
    } catch (error) {
      failedCount += 1;
      const message = error instanceof Error ? error.message : 'Schema warmup failed.';

      failures.push({
        schemaName: candidate.schemaName,
        profile: candidate.profile,
        message,
      });

      logger.warn('Schema warmup candidate failed', {
        schemaName: candidate.schemaName,
        profile: candidate.profile,
        message,
      });
    }
  }

  const status = failedCount ? 'completed_with_errors' : 'completed';

  logger.info('Completed schema warmup', {
    status,
    candidateCount: candidates.length,
    generatedCount,
    reusedCount,
    failedCount,
  });

  return {
    status,
    candidateCount: candidates.length,
    generatedCount,
    reusedCount,
    failedCount,
    failures,
  };
}

function dedupeSchemaCandidates(candidates) {
  const uniqueCandidates = [];
  const seen = new Set();

  for (const candidate of candidates ?? []) {
    const metadata = isRecord(candidate?.metadata) ? candidate.metadata : null;

    if (!metadata) {
      continue;
    }

    const routing = resolveTaskRouting(metadata);

    if (!routing.schemaName) {
      continue;
    }

    if (
      routing.profile === 'bigquery_public_data' &&
      /^bigquery-public-data$/i.test(String(routing.schemaName).trim())
    ) {
      continue;
    }

    if (!supportsOracleSchemaExtraction(routing)) {
      continue;
    }

    const key = `${routing.profile}::${routing.schemaName}`;

    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    uniqueCandidates.push({
      metadata,
      schemaName: routing.schemaName,
      profile: routing.profile,
    });
  }

  return uniqueCandidates;
}

function isRecord(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

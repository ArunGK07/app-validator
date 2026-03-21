import assert from 'node:assert/strict';
import test from 'node:test';

import { warmSchemaCache } from './schema-warmup.mjs';

test('warmSchemaCache skips startup work when the proxy is not configured', async () => {
  const result = await warmSchemaCache(
    {
      cookie: '',
    },
    {
      fetchSchemaCandidates: async () => {
        throw new Error('fetchSchemaCandidates should not be called without a cookie.');
      },
    },
  );

  assert.equal(result.status, 'skipped');
  assert.equal(result.candidateCount, 0);
  assert.equal(result.generatedCount, 0);
  assert.equal(result.reusedCount, 0);
  assert.equal(result.failedCount, 0);
});

test('warmSchemaCache generates each eligible schema once during startup', async () => {
  const generated = [];

  const result = await warmSchemaCache(
    {
      cookie: 'cookie=value',
    },
    {
      fetchSchemaCandidates: async () => [
        {
          metadata: {
            dataset: 'Spider 2.0-Lite',
            database: 'GNOMAD',
          },
          schemaName: 'GNOMAD',
          profile: 'spider_2_lite',
        },
        {
          metadata: {
            dataset: 'Spider 2.0-Lite',
            database: 'GNOMAD',
          },
          schemaName: 'GNOMAD',
          profile: 'spider_2_lite',
        },
        {
          metadata: {
            dataset: 'cms_medicare',
            database: 'bigquery-public-data',
          },
          schemaName: 'cms_medicare',
          profile: 'bigquery_public_data',
        },
      ],
      generateSharedSchemaArtifact: async (metadata) => {
        generated.push(`${metadata.dataset}::${metadata.database}`);
        return {
          source: generated.length === 1 ? 'database' : 'cache',
        };
      },
    },
  );

  assert.deepEqual(generated, [
    'Spider 2.0-Lite::GNOMAD',
    'cms_medicare::bigquery-public-data',
  ]);
  assert.equal(result.status, 'completed');
  assert.equal(result.candidateCount, 2);
  assert.equal(result.generatedCount, 1);
  assert.equal(result.reusedCount, 1);
  assert.equal(result.failedCount, 0);
});

test('warmSchemaCache reports candidate fetch failures without throwing', async () => {
  const result = await warmSchemaCache(
    {
      cookie: 'cookie=value',
    },
    {
      fetchSchemaCandidates: async () => {
        throw Object.assign(new Error('Upstream request failed with 400: Unknown column'), {
          statusCode: 400,
        });
      },
    },
  );

  assert.equal(result.status, 'completed_with_errors');
  assert.equal(result.candidateCount, 0);
  assert.equal(result.generatedCount, 0);
  assert.equal(result.reusedCount, 0);
  assert.equal(result.failedCount, 1);
  assert.deepEqual(result.failures, [
    {
      schemaName: null,
      profile: null,
      message: 'Upstream request failed with 400: Unknown column',
    },
  ]);
});

test('warmSchemaCache skips invalid bigquery placeholder candidates', async () => {
  const generated = [];

  const result = await warmSchemaCache(
    {
      cookie: 'cookie=value',
    },
    {
      fetchSchemaCandidates: async () => [
        {
          metadata: {
            database: 'bigquery-public-data',
          },
          schemaName: 'bigquery-public-data',
          profile: 'bigquery_public_data',
        },
        {
          metadata: {
            dataset: 'cms_medicare',
            database: 'bigquery-public-data',
          },
          schemaName: 'cms_medicare',
          profile: 'bigquery_public_data',
        },
      ],
      generateSharedSchemaArtifact: async (metadata) => {
        generated.push(${metadata.dataset ?? ''}::);
        return {
          source: 'database',
        };
      },
    },
  );

  assert.deepEqual(generated, ['cms_medicare::bigquery-public-data']);
  assert.equal(result.status, 'completed');
  assert.equal(result.candidateCount, 1);
  assert.equal(result.generatedCount, 1);
  assert.equal(result.failedCount, 0);
});


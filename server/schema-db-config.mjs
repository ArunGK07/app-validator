const DEFAULT_DB_PROFILES = {
  spider_2_lite: {
    user: 'sys',
    password: 'secret',
    host: 'localhost',
    port: 21521,
    service: 'XEPDB1',
    mode: 'SYSDBA',
  },
  spider_2_snow: {
    user: 'sys',
    password: 'secret',
    host: 'localhost',
    port: 21521,
    service: 'XEPDB1',
    mode: 'SYSDBA',
  },
  bigquery_public_data: {
    user: 'sys',
    password: 'secret',
    host: 'localhost',
    port: 21522,
    service: 'XEPDB1',
    mode: 'SYSDBA',
  },
};

export const DEFAULT_PROFILE = 'bigquery_public_data';

export function resolveTaskRouting(metadata) {
  const dataset = asString(metadata?.dataset).trim();
  const database = asString(metadata?.database).trim();
  const datasetKey = normalizeRoutingValue(dataset);
  const databaseKey = normalizeRoutingValue(database);

  if (datasetKey === 'spider20lite' || datasetKey === 'spider20snow') {
    return {
      dataset,
      database,
      schemaName: database,
      profile: datasetKey === 'spider20snow' ? 'spider_2_snow' : 'spider_2_lite',
    };
  }

  if (databaseKey === 'bigquerypublicdata') {
    return {
      dataset,
      database,
      schemaName: dataset,
      profile: 'bigquery_public_data',
    };
  }

  return {
    dataset,
    database,
    schemaName: dataset || database,
    profile: DEFAULT_PROFILE,
  };
}

export function getConnectionParamsForProfile(profile, env = process.env) {
  const normalizedProfile = DEFAULT_DB_PROFILES[profile] ? profile : DEFAULT_PROFILE;
  const defaults = DEFAULT_DB_PROFILES[normalizedProfile];
  const prefix = profileEnvPrefix(normalizedProfile);

  return {
    user: env[`${prefix}_USER`] ?? defaults.user,
    password: env[`${prefix}_PASSWORD`] ?? defaults.password,
    host: env[`${prefix}_HOST`] ?? defaults.host,
    port: Number(env[`${prefix}_PORT`] ?? defaults.port),
    service: env[`${prefix}_SERVICE`] ?? defaults.service,
    mode: env[`${prefix}_MODE`] ?? defaults.mode,
    profile: normalizedProfile,
  };
}

export function getConnectionParamsForTask(metadata, env = process.env) {
  const routing = resolveTaskRouting(metadata);
  return {
    ...getConnectionParamsForProfile(routing.profile, env),
    profile: routing.profile,
  };
}

function profileEnvPrefix(profile) {
  return `ORACLE_${profile.toUpperCase()}`;
}

function normalizeRoutingValue(value) {
  return asString(value).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function asString(value) {
  if (value === null || value === undefined) {
    return '';
  }

  return String(value);
}

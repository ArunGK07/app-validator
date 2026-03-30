export function resolveAuthorizationHeader(source = process.env) {
  const explicitHeader = normalizeAuthorizationHeader(
    readSourceValue(source, ['authorizationHeader', 'authorization', 'AUTHORIZATION']),
  );

  if (explicitHeader) {
    return explicitHeader;
  }

  const token = readSourceValue(source, ['oracleAccessToken'])
    || extractOracleAccessToken(readSourceValue(source, ['cookie', 'TURING_COOKIE']));

  return token ? `Bearer ${token}` : '';
}

export function extractOracleAccessToken(cookieHeader) {
  if (!cookieHeader || typeof cookieHeader !== 'string') {
    return '';
  }

  const parts = cookieHeader.split(';');

  for (const part of parts) {
    const trimmed = part.trim();

    if (!trimmed.toLowerCase().startsWith('oracle_access_token=')) {
      continue;
    }

    return trimmed.slice('oracle_access_token='.length).trim();
  }

  return '';
}

function readSourceValue(source, keys) {
  if (!source || typeof source !== 'object') {
    return '';
  }

  for (const key of keys) {
    const value = source[key];

    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }

  return '';
}

function normalizeAuthorizationHeader(value) {
  if (!value) {
    return '';
  }

  return /^bearer\s+/i.test(value) ? value : `Bearer ${value}`;
}

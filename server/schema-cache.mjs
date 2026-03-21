import { resolve as pathResolve, relative as pathRelative } from 'node:path';

import { resolveTaskRouting } from './schema-db-config.mjs';
import { getSchemaCacheDir } from './workspace-config.mjs';

export function sanitizePathComponent(name) {
  const sanitized = [...String(name ?? '').trim()]
    .map((character) => (/[\p{L}\p{N}_-]/u.test(character) ? character : '_'))
    .join('')
    .replace(/^_+|_+$/g, '');

  return sanitized || 'unknown';
}

export function getSharedSchemaPath(metadata, schemaRoot = getSchemaCacheDir()) {
  const relativePath = getSharedSchemaRelativePath(metadata);

  if (!relativePath) {
    return null;
  }

  return pathResolve(schemaRoot, relativePath);
}

export function getSharedSchemaRelativePath(metadata) {
  const routing = resolveTaskRouting(metadata);

  if (!routing.schemaName) {
    return null;
  }

  const datasource = sanitizePathComponent(routing.profile || routing.database || routing.dataset || 'unknown');
  const schemaName = sanitizePathComponent(routing.schemaName);
  return `${datasource}/${schemaName}.json`;
}

export function getSharedSchemaDisplayPath(path, schemaRoot = getSchemaCacheDir()) {
  if (!path) {
    return null;
  }

  return pathRelative(pathResolve(schemaRoot), pathResolve(path)).replace(/\\/g, '/');
}

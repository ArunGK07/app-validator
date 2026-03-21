import { access, mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

import { createLogger } from './logger.mjs';
import { getSharedSchemaDisplayPath, getSharedSchemaPath } from './schema-cache.mjs';
import { getConnectionParamsForTask, resolveTaskRouting } from './schema-db-config.mjs';

const logger = createLogger('schema-extractor');
let cachedOracleDbModulePromise;

export class SchemaExtractor {
  constructor(options) {
    this.schemaName = asOracleIdentifier(options.schemaName);
    this.sourceDataset = asString(options.sourceDataset).trim();
    this.sourceDatabase = asString(options.sourceDatabase).trim();
    this.env = options.env ?? process.env;
    this.connectionFactory = options.connectionFactory;
    this.oracledbModule = options.oracledbModule;
    this.connection = null;
  }

  async connect() {
    if (this.connection) {
      return this.connection;
    }

    if (this.connectionFactory) {
      this.connection = await this.connectionFactory({
        schemaName: this.schemaName,
        sourceDataset: this.sourceDataset,
        sourceDatabase: this.sourceDatabase,
      });
      return this.connection;
    }

    const oracledb = this.oracledbModule ?? (await loadOracleDbModule());
    const params =
      this.sourceDataset || this.sourceDatabase
        ? getConnectionParamsForTask(
            {
              dataset: this.sourceDataset,
              database: this.sourceDatabase,
            },
            this.env,
          )
        : getConnectionParamsForTask({}, this.env);

    const connectOptions = {
      user: params.user,
      password: params.password,
      connectString: `${params.host}:${params.port}/${params.service}`,
    };
    const privilege = resolveOraclePrivilege(oracledb, params.mode);

    if (privilege !== undefined) {
      connectOptions.privilege = privilege;
    }

    logger.debug('Opening Oracle schema connection', {
      schemaName: this.schemaName,
      profile: params.profile,
      host: params.host,
      port: params.port,
      service: params.service,
      privilege: params.mode || undefined,
    });

    this.connection = await oracledb.getConnection(connectOptions);
    return this.connection;
  }

  async disconnect() {
    if (!this.connection || typeof this.connection.close !== 'function') {
      this.connection = null;
      return;
    }

    await this.connection.close();
    this.connection = null;
  }

  async getTables() {
    const result = await this.execute(
      `
        SELECT TABLE_NAME
        FROM ALL_TABLES
        WHERE OWNER = :owner
        ORDER BY TABLE_NAME
      `,
      { owner: this.schemaName },
    );

    return result.rows.map((row) => row[0]);
  }

  async getColumns(tableName) {
    const result = await this.execute(
      `
        SELECT
          COLUMN_NAME,
          DATA_TYPE,
          NULLABLE,
          DATA_LENGTH,
          DATA_PRECISION,
          DATA_SCALE,
          COLUMN_ID
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :owner
          AND TABLE_NAME = :table_name
        ORDER BY COLUMN_ID
      `,
      {
        owner: this.schemaName,
        table_name: asOracleIdentifier(tableName),
      },
    );

    return result.rows.map(([name, dataType, nullable, dataLength, dataPrecision, dataScale, columnId]) => ({
      name,
      data_type: formatColumnDataType(dataType, dataLength, dataPrecision, dataScale),
      nullable: nullable === 'Y',
      column_id: columnId,
      data_length: dataLength,
      data_precision: dataPrecision,
      data_scale: dataScale,
    }));
  }

  async getPrimaryKeys(tableName) {
    const constraintResult = await this.execute(
      `
        SELECT CONSTRAINT_NAME
        FROM ALL_CONSTRAINTS
        WHERE OWNER = :owner
          AND TABLE_NAME = :table_name
          AND CONSTRAINT_TYPE = 'P'
      `,
      {
        owner: this.schemaName,
        table_name: asOracleIdentifier(tableName),
      },
    );

    const primaryKeyName = constraintResult.rows[0]?.[0];

    if (!primaryKeyName) {
      return null;
    }

    const columnsResult = await this.execute(
      `
        SELECT COLUMN_NAME
        FROM ALL_CONS_COLUMNS
        WHERE OWNER = :owner
          AND CONSTRAINT_NAME = :constraint_name
        ORDER BY POSITION
      `,
      {
        owner: this.schemaName,
        constraint_name: primaryKeyName,
      },
    );

    return {
      constraint_name: primaryKeyName,
      columns: columnsResult.rows.map((row) => row[0]),
    };
  }

  async getForeignKeys(tableName) {
    const result = await this.execute(
      `
        SELECT
          c.CONSTRAINT_NAME,
          cc.COLUMN_NAME,
          c.R_OWNER,
          c.R_CONSTRAINT_NAME,
          rcc.TABLE_NAME,
          rcc.COLUMN_NAME
        FROM ALL_CONSTRAINTS c
        JOIN ALL_CONS_COLUMNS cc
          ON c.OWNER = cc.OWNER
          AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
        JOIN ALL_CONS_COLUMNS rcc
          ON c.R_OWNER = rcc.OWNER
          AND c.R_CONSTRAINT_NAME = rcc.CONSTRAINT_NAME
        WHERE c.OWNER = :owner
          AND c.TABLE_NAME = :table_name
          AND c.CONSTRAINT_TYPE = 'R'
        ORDER BY c.CONSTRAINT_NAME, cc.POSITION
      `,
      {
        owner: this.schemaName,
        table_name: asOracleIdentifier(tableName),
      },
    );

    const grouped = new Map();

    for (const [constraintName, columnName, referencedOwner, _referencedConstraint, referencedTable, referencedColumn] of result.rows) {
      const foreignKey = grouped.get(constraintName) ?? {
        constraint_name: constraintName,
        columns: [],
        referenced_owner: referencedOwner,
        referenced_table: referencedTable,
        referenced_columns: [],
      };

      foreignKey.columns.push(columnName);

      if (!foreignKey.referenced_columns.includes(referencedColumn)) {
        foreignKey.referenced_columns.push(referencedColumn);
      }

      grouped.set(constraintName, foreignKey);
    }

    return [...grouped.values()];
  }

  async getTableRecordCount(tableName) {
    const qualifiedName = `${this.schemaName}.${asOracleIdentifier(tableName)}`;
    const result = await this.execute(`SELECT COUNT(*) FROM ${qualifiedName}`);
    return Number(result.rows[0]?.[0] ?? 0);
  }

  async extractSchema() {
    await this.connect();

    try {
      const tables = await this.getTables();
      const schema = {
        database: this.schemaName,
        timestamp: new Date().toISOString(),
        _schema_definition: {
          column_format: ['name', 'data_type', 'nullable', 'column_id', 'data_length', 'data_precision', 'data_scale'],
          note: 'Each column is an array following the column_format order. nullable: 0=not null, 1=nullable',
        },
        table_count: tables.length,
        tables: {},
      };

      for (const tableName of tables) {
        const [columns, primaryKey, foreignKeys, recordCount] = await Promise.all([
          this.getColumns(tableName),
          this.getPrimaryKeys(tableName),
          this.getForeignKeys(tableName),
          this.getTableRecordCount(tableName),
        ]);

        schema.tables[tableName] = {
          record_count: recordCount,
          column_count: columns.length,
          columns: columns.map((column) => [
            column.name,
            column.data_type,
            column.nullable ? 1 : 0,
            column.column_id,
            column.data_length,
            column.data_precision,
            column.data_scale,
          ]),
          primary_key: primaryKey,
          foreign_keys: foreignKeys,
        };
      }

      return schema;
    } finally {
      await this.disconnect();
    }
  }

  async saveSchemaJson(outputFile) {
    const schema = await this.extractSchema();
    await mkdir(dirname(outputFile), { recursive: true });
    await writeFile(outputFile, JSON.stringify(schema), 'utf8');
    return schema;
  }

  async execute(sql, binds = {}) {
    const connection = await this.connect();
    return connection.execute(sql, binds);
  }
}

export async function getCachedSchemaArtifact(metadata, config = {}) {
  const { routing, sharedSchemaPath, sharedSchemaFile } = resolveSharedSchemaTarget(metadata, config);

  if (!routing.schemaName || !sharedSchemaPath || !sharedSchemaFile) {
    return null;
  }

  if (!(await fileExists(sharedSchemaPath))) {
    return null;
  }

  return {
    schemaFile: sharedSchemaFile,
    sharedSchemaFile,
    schemaName: routing.schemaName,
    profile: routing.profile,
    source: 'cache',
  };
}

export async function generateSharedSchemaArtifact(metadata, config = {}, options = {}) {
  const { routing, sharedSchemaPath, sharedSchemaFile } = resolveSharedSchemaTarget(metadata, config);

  if (!routing.schemaName || !sharedSchemaPath || !sharedSchemaFile) {
    return buildSkippedSchemaResult(routing);
  }

  if (await fileExists(sharedSchemaPath)) {
    logger.debug('Reused cached schema artifact', {
      sharedSchemaPath,
      schemaName: routing.schemaName,
      profile: routing.profile,
    });

    return {
      schemaFile: sharedSchemaFile,
      sharedSchemaFile,
      schemaName: routing.schemaName,
      profile: routing.profile,
      source: 'cache',
    };
  }

  const extractor = new SchemaExtractor({
    schemaName: routing.schemaName,
    sourceDataset: routing.dataset,
    sourceDatabase: routing.database,
    env: options.env ?? process.env,
    connectionFactory: options.connectionFactory,
    oracledbModule: options.oracledbModule,
  });

  const schema = await extractor.extractSchema();
  const payload = JSON.stringify(schema);

  await mkdir(dirname(sharedSchemaPath), { recursive: true });
  await writeFile(sharedSchemaPath, payload, 'utf8');

  logger.info('Generated schema artifact from Oracle', {
    schemaName: routing.schemaName,
    profile: routing.profile,
    tableCount: schema.table_count,
    sharedSchemaPath,
  });

  return {
    schemaFile: sharedSchemaFile,
    sharedSchemaFile,
    schemaName: routing.schemaName,
    profile: routing.profile,
    source: 'database',
  };
}

export async function generateTaskSchemaArtifact(task, config = {}, options = {}) {
  const taskId = asString(task?.taskId).trim();

  if (!taskId) {
    throw withStatus(new Error('Task id is required to generate the schema artifact.'), 400);
  }

  return {
    taskId,
    ...(await generateSharedSchemaArtifact(task?.metadata, config, options)),
  };
}

function resolveSharedSchemaTarget(metadata, config = {}) {
  if (!isRecord(metadata)) {
    return {
      routing: {
        dataset: '',
        database: '',
        schemaName: '',
        profile: '',
      },
      sharedSchemaPath: null,
      sharedSchemaFile: null,
    };
  }

  const routing = resolveTaskRouting(metadata);
  const sharedSchemaPath = getSharedSchemaPath(metadata, config.schemaCacheDir);
  const sharedSchemaFile = getSharedSchemaDisplayPath(sharedSchemaPath, config.schemaCacheDir);

  return {
    routing,
    sharedSchemaPath,
    sharedSchemaFile,
  };
}

function buildSkippedSchemaResult(routing) {
  return {
    schemaFile: null,
    sharedSchemaFile: null,
    schemaName: routing?.schemaName ?? '',
    profile: routing?.profile ?? '',
    source: 'skipped',
  };
}

async function loadOracleDbModule() {
  if (!cachedOracleDbModulePromise) {
    cachedOracleDbModulePromise = import('oracledb')
      .then((module) => module.default ?? module)
      .catch((error) => {
        cachedOracleDbModulePromise = undefined;
        throw withStatus(
          new Error(`Failed to load the oracledb package. Run npm install and confirm Oracle connectivity. ${error.message}`),
          500,
        );
      });
  }

  return cachedOracleDbModulePromise;
}

function resolveOraclePrivilege(oracledb, mode) {
  const normalizedMode = asString(mode).trim().toUpperCase();

  if (!normalizedMode) {
    return undefined;
  }

  return oracledb[normalizedMode] ?? oracledb[`AUTH_MODE_${normalizedMode}`];
}

function formatColumnDataType(dataType, dataLength, dataPrecision, dataScale) {
  if (!dataType) {
    return '';
  }

  if ((dataType === 'NUMBER' || dataType === 'NUMERIC') && dataPrecision) {
    return dataScale ? `${dataType}(${dataPrecision},${dataScale})` : `${dataType}(${dataPrecision})`;
  }

  if ((dataType === 'VARCHAR2' || dataType === 'VARCHAR' || dataType === 'CHAR') && dataLength) {
    return `${dataType}(${dataLength})`;
  }

  return dataType;
}

async function fileExists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function asOracleIdentifier(value) {
  const identifier = asString(value).trim().toUpperCase();

  if (!identifier) {
    throw new Error('Schema name is required.');
  }

  if (!/^[A-Z][A-Z0-9_$#]*$/u.test(identifier)) {
    throw new Error(`Oracle identifier ${identifier} is not supported by the extractor.`);
  }

  return identifier;
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

function withStatus(error, statusCode) {
  return Object.assign(error, { statusCode });
}

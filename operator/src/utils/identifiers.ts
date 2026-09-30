/**
 * Firebird identifiers as users write them in specs, following SQL: a regular identifier
 * (letters, digits, "_" and "$", starting with a letter) is stored in upper case; a name in double
 * quotes ("Sales Team") is a delimited identifier, stored exactly as written, with "" standing for
 * a double quote.
 */

/** Regular (unquoted) Firebird identifier */
export const REGULAR_IDENTIFIER = /^[A-Za-z][A-Za-z0-9_$]{0,62}$/;

/** Delimited identifier as written: double quotes around it, embedded quotes doubled */
const DELIMITED_IDENTIFIER = /^"((?:[^"]|"")+)"$/;

/** Longest Firebird identifier (characters) */
const MAX_IDENTIFIER_LENGTH = 63;

/** Characters a delimited identifier must not contain */
const CONTROL = /[\x00-\x1f\x7f]/;

export interface Identifier {
  /** The name as Firebird stores it (RDB$ROLE_NAME, RDB$USER_PRIVILEGES) */
  name: string;
  /** The name as written in SQL statements */
  sql: string;
}

/**
 * Parses a spec identifier; undefined when it is neither a regular identifier nor a valid
 * delimited one (1 to 63 characters, no control characters, no leading or trailing spaces, which
 * Firebird would trim).
 */
export function parseIdentifier(value: string): Identifier | undefined {
  if (REGULAR_IDENTIFIER.test(value)) {
    const name = value.toUpperCase();
    return { name, sql: name };
  }
  const m = DELIMITED_IDENTIFIER.exec(value);
  if (!m) return undefined;
  const name = m[1].replace(/""/g, '"');
  if ([...name].length > MAX_IDENTIFIER_LENGTH || CONTROL.test(name) || name.trim() !== name) return undefined;
  return { name, sql: delimited(name) };
}

/** A stored name as a delimited identifier */
export function delimited(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** A value as an SQL string literal */
export function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

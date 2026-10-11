/**
 * Standard 5-field cron expressions (minute hour day-of-month month day-of-week), evaluated in
 * UTC like a CronJob without timeZone: for the schedules the operator runs itself (volume snapshot
 * backups, utils/volume-snapshot.ts). Fields take "*", numbers, ranges "a-b", steps "*\/n" and
 * "a-b/n", lists "a,b", and month or weekday names (JAN, MON); weekday 7 is Sunday. As in cron,
 * when both day fields are restricted a day matches either.
 */

interface Field {
  values: Set<number>;
  /** "*" (or "*\/n"): unrestricted for the day-of-month / day-of-week rule */
  star: boolean;
}

export interface CronSchedule {
  minute: Field;
  hour: Field;
  dayOfMonth: Field;
  month: Field;
  dayOfWeek: Field;
}

const MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
const WEEKDAYS = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];

function parseField(text: string, min: number, max: number, names?: string[], nameBase = 0): Field {
  const values = new Set<number>();
  const value = (s: string): number => {
    const named = names?.indexOf(s.toUpperCase()) ?? -1;
    if (named >= 0) return named + nameBase;
    if (!/^\d+$/.test(s)) throw new Error(`invalid value "${s}"`);
    return Number(s);
  };
  for (const part of text.split(',')) {
    const [range, stepText] = part.split('/');
    const step = stepText === undefined ? 1 : Number(stepText);
    if (!Number.isInteger(step) || step < 1 || (stepText !== undefined && !/^\d+$/.test(stepText))) {
      throw new Error(`invalid step in "${part}"`);
    }
    let from: number;
    let to: number;
    if (range === '*') {
      [from, to] = [min, max];
    } else if (range.includes('-')) {
      const [a, b] = range.split('-');
      [from, to] = [value(a), value(b)];
    } else {
      from = value(range);
      // "a/n" runs from a to the end of the range
      to = stepText === undefined ? from : max;
    }
    if (from < min || to > max || from > to) throw new Error(`"${part}" is out of range ${min}-${max}`);
    for (let v = from; v <= to; v += step) values.add(v);
  }
  return { values, star: text.startsWith('*') };
}

/** Parses a 5-field cron expression; throws with the reason when it is invalid */
export function parseCron(expression: string): CronSchedule {
  const fields = expression.trim().split(/\s+/);
  if (fields.length !== 5) throw new Error(`"${expression}" is not a 5-field cron expression`);
  const dayOfWeek = parseField(fields[4], 0, 7, WEEKDAYS);
  // 7 is Sunday too
  if (dayOfWeek.values.delete(7)) dayOfWeek.values.add(0);
  return {
    minute: parseField(fields[0], 0, 59),
    hour: parseField(fields[1], 0, 23),
    dayOfMonth: parseField(fields[2], 1, 31),
    month: parseField(fields[3], 1, 12, MONTHS, 1),
    dayOfWeek,
  };
}

/** Whether the schedule fires at this minute (UTC) */
export function cronMatches(schedule: CronSchedule, time: Date): boolean {
  if (!schedule.minute.values.has(time.getUTCMinutes())) return false;
  if (!schedule.hour.values.has(time.getUTCHours())) return false;
  if (!schedule.month.values.has(time.getUTCMonth() + 1)) return false;
  const dom = schedule.dayOfMonth.values.has(time.getUTCDate());
  const dow = schedule.dayOfWeek.values.has(time.getUTCDay());
  if (schedule.dayOfMonth.star || schedule.dayOfWeek.star) {
    return (schedule.dayOfMonth.star || dom) && (schedule.dayOfWeek.star || dow);
  }
  return dom || dow;
}

const MINUTE = 60_000;

/**
 * The latest time the schedule fired after `after` and at or before `now` (whole minutes, UTC), or
 * undefined when it did not. Times missed while the operator was down collapse into the latest
 * one, like a CronJob's missed runs. At most a year is looked back.
 */
export function latestCronTime(expression: string, after: Date, now: Date): Date | undefined {
  const schedule = parseCron(expression);
  const floor = Math.max(Math.floor(after.getTime() / MINUTE) * MINUTE, now.getTime() - 366 * 24 * 60 * MINUTE);
  for (let t = Math.floor(now.getTime() / MINUTE) * MINUTE; t > floor; t -= MINUTE) {
    const time = new Date(t);
    if (cronMatches(schedule, time)) return time;
  }
  return undefined;
}

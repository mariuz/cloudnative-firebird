import { describe, it, expect } from 'vitest';
import { spawnSync } from 'child_process';
import { readFileSync } from 'fs';
import { join } from 'path';

/**
 * The in-place promotion's full shutdown (segment-server.pl promote_here) is retried before the
 * operator falls back to the offline promotion. The retry block runs here as it is in the script,
 * against a stub of the service manager call and of the database state.
 */
const script = readFileSync(join(__dirname, '..', 'src', 'replication', 'segment-server.pl'), 'utf8');
const start = script.indexOf('    # the full shutdown can fail while the server is busy');
const end = script.indexOf('\n', script.indexOf("    $shut or die 'full shutdown failed after", start));
const block = script.slice(start, end + 1);

// outcomes: one per attempt, "ok" or an error message; state: what database_state returns after a failure
const run = (outcomes: string[], state = 'online', attempts = 5) =>
  spawnSync(
    'perl',
    [
      '-e',
      `use strict; use warnings;
       my @outcomes = @ARGV; my $calls = 0; my $fb_out = '';
       my $fb = sub { $calls++; my $o = shift(@outcomes) // 'ok'; if ($o eq 'ok') { $fb_out = ''; return 1 } $fb_out = $o; return 0 };
       sub database_state { return '${state}' }
       my ($promote_shutdown_attempts, $promote_shutdown_pause) = (${attempts}, 0);
       my $shut = 0;
       my $ok = eval {
${block}
         1;
       };
       print "calls=$calls shut=$shut\\n";
       print "error=$@" unless $ok;`,
      ...outcomes,
    ],
    { encoding: 'utf8' },
  );

describe('the in-place promotion full shutdown', () => {
  it('is the script block', () => {
    expect(start).toBeGreaterThan(0);
    expect(block).toContain("$fb->('prp_shutdown_mode', 'prp_sm_full', 'prp_force_shutdown', '0')");
  });

  it('succeeds at once when the server shuts the database down', () => {
    const r = run(['ok']);
    expect(r.stdout).toContain('calls=1 shut=1');
    expect(r.stdout).not.toContain('error=');
  });

  it('is retried while it fails, and goes on once it works', () => {
    const r = run(['lock time-out on wait transaction', 'object in use', 'ok']);
    expect(r.stdout).toContain('calls=3 shut=1');
    expect(r.stdout).toContain('full shutdown attempt 1 of 5 failed: lock time-out on wait transaction');
    expect(r.stdout).toContain('full shutdown attempt 2 of 5 failed: object in use');
    expect(r.stdout).not.toContain('error=');
  });

  it('counts a database left in full shutdown despite the error as shut down', () => {
    const r = run(['connection lost'], 'shutdown');
    expect(r.stdout).toContain('calls=1 shut=1');
    expect(r.stdout).not.toContain('error=');
  });

  it('gives up after the last attempt with every message, for the offline promotion', () => {
    const r = run(['busy', 'busy', 'still busy'], 'online', 3);
    expect(r.stdout).toContain('calls=3 shut=0');
    expect(r.stdout).toContain('error=full shutdown failed after 3 attempts: busy; busy; still busy');
  });

  it('defaults to five attempts three seconds apart', () => {
    expect(script).toMatch(/\$promote_shutdown_attempts = .* : 5;/);
    expect(script).toMatch(/\$promote_shutdown_pause = .* : 3;/);
  });
});

"""Unit test for GUI saved-view export/import helpers.

    python3 scripts/gui_views_test.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODULE = (ROOT / "gui/src/savedViews.ts").as_uri()


def main() -> int:
    module = MODULE.replace("'", "\\'")
    tests = f"""
import assert from 'node:assert/strict';
import {{
  SAVED_VIEWS_FORMAT,
  SAVED_VIEWS_VERSION,
  normalizeViewList,
  parseSavedViewsFile,
  savedViewsFileName,
  serializeSavedViews,
}} from '{module}';

const view = {{
  name: 'Active',
  filter: '{{"status":"active"}}',
  sortField: 'created_at',
  sortDir: 'desc',
  limit: 25,
  projection: '{{"name":1}}',
}};
const exported = serializeSavedViews('orders', [{{...view, token: 'secret', endpoint: 'http://x'}}]);
const parsed = JSON.parse(exported);
assert.equal(parsed.format, SAVED_VIEWS_FORMAT);
assert.equal(parsed.version, SAVED_VIEWS_VERSION);
assert.equal(parsed.collection, 'orders');
assert.deepEqual(parsed.views, [view]);
assert.ok(!exported.includes('secret'));
assert.ok(!exported.includes('http://x'));
assert.ok(!exported.includes('token'));
assert.ok(!exported.includes('endpoint'));
assert.deepEqual(parseSavedViewsFile(exported), [view]);

const defaults = normalizeViewList([
  {{name: ' zeta '}},
  {{name: 'alpha', filter: '{{}}', sortField: '_id', sortDir: 'asc', limit: 0, projection: ''}},
  {{name: 'zeta', filter: '{{"n":1}}'}},
]);
assert.deepEqual(defaults.map((item) => item.name), ['alpha', 'zeta']);
assert.equal(defaults[1].filter, '{{"n":1}}');
assert.equal(defaults[1].limit, 50);
assert.equal(defaults[0].limit, 0);

const withCredentials = JSON.stringify({{
  format: SAVED_VIEWS_FORMAT,
  version: SAVED_VIEWS_VERSION,
  endpoint: 'https://example.invalid',
  token: 'do-not-import',
  views: [{{name: 'shared', filter: '{{}}', token: 'nested-secret'}}],
}});
const cleaned = serializeSavedViews('orders', parseSavedViewsFile(withCredentials));
assert.ok(!cleaned.includes('endpoint'));
assert.ok(!cleaned.includes('token'));
assert.ok(!cleaned.includes('do-not-import'));
assert.ok(!cleaned.includes('nested-secret'));

for (const bad of [
  'not json',
  '{{"format":"other","version":1,"views":[]}}',
  '{{"format":"nonodb.saved-views","version":2,"views":[]}}',
  '{{"format":"nonodb.saved-views","version":1,"views":{{}}}}',
  '{{"format":"nonodb.saved-views","version":1,"views":[{{"name":""}}]}}',
  '{{"format":"nonodb.saved-views","version":1,"views":[{{"name":"x","sortDir":"sideways"}}]}}',
  '{{"format":"nonodb.saved-views","version":1,"views":[{{"name":"x","limit":10001}}]}}',
]) {{
  assert.throws(() => parseSavedViewsFile(bad));
}}

assert.equal(savedViewsFileName('orders'), 'nonodb-views-orders.json');
assert.equal(savedViewsFileName('a b/c'), 'nonodb-views-a_b_c.json');
assert.equal(savedViewsFileName('***'), 'nonodb-views-collection.json');
console.log('gui saved views: ok');
"""
    result = subprocess.run(
        ["node", "--no-warnings", "--experimental-strip-types", "--input-type=module", "-e", tests],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        return result.returncode
    print(result.stdout.strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

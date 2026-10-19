import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ── Config (override with env vars) ───────────────────────────────────────────
var BASE_URL       = __ENV.BASE_URL || 'http://localhost:8080';
var RUN_NAME       = __ENV.RUN_NAME || 'run';
var ABANDON_RATE   = parseFloat(__ENV.ABANDON_RATE || '0');   // 0 = all queued users attempt payment
var TTL_WAIT_SEC   = parseInt(__ENV.TTL_WAIT_SEC || '0', 10); // teardown wait: slot TTL + reaper interval + a few sec
var STOCK_URL      = __ENV.STOCK_URL || '';                   // optional, e.g. http://localhost:8080/debug/stock

// 200 / 409 / 429 are valid business outcomes, not HTTP failures
http.setResponseCallback(http.expectedStatuses(200, 409, 429));

// ── Custom metrics ────────────────────────────────────────────────────────────
var queued      = new Counter('orders_queued');
var soldOut     = new Counter('orders_sold_out');
var rateLimited = new Counter('orders_rate_limited');
var errors      = new Counter('orders_errors');       // /buy 5xx or network failure
var abandoned   = new Counter('orders_abandoned');    // queued but never paid (tests expiry leak)

var paySuccess  = new Counter('payments_confirmed');
var payFailed   = new Counter('payments_failed');
var payExpired  = new Counter('payments_slot_expired');
var payErrors   = new Counter('payments_errors');     // /pay 5xx or network failure

var slotRate    = new Rate('slot_acquired_rate');     // got slot / (got slot + sold out)
var errorRate   = new Rate('error_rate');             // 5xx or network failure / all requests
var paySuccessRate = new Rate('pay_success_rate');

var buyLat200   = new Trend('buy_latency_200_ms', true);
var buyLat409   = new Trend('buy_latency_409_ms', true);
var buyLat5xx   = new Trend('buy_latency_5xx_ms', true);
var payLatency  = new Trend('pay_latency_ms', true);

// ── Load shape ────────────────────────────────────────────────────────────────
export var options = {
  vus: 500,
  duration: '10s',
  thresholds: {
    'orders_errors':      ['count==0'],
    'payments_errors':    ['count==0'],
    'error_rate':         ['rate<0.001'],
    'buy_latency_200_ms': ['p(95)<500'],
    'buy_latency_409_ms': ['p(95)<100'],
  },
};

// ── Main test function ────────────────────────────────────────────────────────
export default function () {
  var userId    = 'user_' + __VU + '_' + __ITER;
  var productId = 'iphone15';
  var params    = { headers: { 'Content-Type': 'application/json' } };

  // Step 1: Buy request
  var buyRes = http.post(
    BASE_URL + '/buy',
    JSON.stringify({ user_id: userId, product_id: productId }),
    params
  );

  var buyIsError = buyRes.status >= 500 || buyRes.status === 0;
  errorRate.add(buyIsError);

  if (buyRes.status === 200) {
    buyLat200.add(buyRes.timings.duration);
    queued.add(1);
    slotRate.add(true);
  } else if (buyRes.status === 409) {
    buyLat409.add(buyRes.timings.duration);
    soldOut.add(1);
    slotRate.add(false);
  } else if (buyRes.status === 429) {
    rateLimited.add(1);
  } else {
    buyLat5xx.add(buyRes.timings.duration);
    errors.add(1);
  }

  check(buyRes, {
    'buy: not a 5xx': function (r) { return r.status < 500 && r.status !== 0; },
  });

  // Only continue if we got a slot
  if (buyRes.status !== 200) return;

  // Some users abandon and never pay -> slot must expire and stock must return
  if (Math.random() < ABANDON_RATE) {
    abandoned.add(1);
    return;
  }

  sleep(0.1);

  // Step 2: Payment
  var payRes = http.post(
    BASE_URL + '/pay',
    JSON.stringify({ user_id: userId, product_id: productId }),
    params
  );

  payLatency.add(payRes.timings.duration);
  paySuccessRate.add(payRes.status === 200);

  var payIsError = payRes.status >= 500 || payRes.status === 0;
  errorRate.add(payIsError);
  if (payIsError) payErrors.add(1);

  check(payRes, {
    'pay: not a 5xx': function (r) { return r.status < 500 && r.status !== 0; },
  });

  try {
    var body = JSON.parse(payRes.body);
    if (body.status === 'ORDER_CONFIRMED')     paySuccess.add(1);
    else if (body.status === 'PAYMENT_FAILED') payFailed.add(1);
    else if (body.status === 'SLOT_EXPIRED')   payExpired.add(1);
  } catch (e) {}
}

// ── Teardown: wait for expiry, then check stock invariant ─────────────────────
export function teardown() {
  if (TTL_WAIT_SEC > 0) {
    console.log('Teardown: waiting ' + TTL_WAIT_SEC + 's for slots to expire...');
    sleep(TTL_WAIT_SEC);
  }
  if (STOCK_URL) {
    var res = http.get(STOCK_URL);
    console.log('STOCK STATE: ' + res.body);
    console.log('Check: stock_left + units_held + units_sold must equal initial_stock');
  } else {
    console.log('No STOCK_URL set. Run your invariant check script now.');
  }
}

// ── Summary report ────────────────────────────────────────────────────────────
export function handleSummary(data) {
  var m = data.metrics;

  function count(name) {
    if (!m[name]) return 0;
    return m[name].values['count'] || 0;
  }

  function trend(name, stat) {
    if (!m[name]) return 'N/A';
    var v = m[name].values[stat];
    return v !== undefined ? v.toFixed(2) + ' ms' : 'N/A';
  }

  function rate(name) {
    if (!m[name]) return 'N/A';
    var v = m[name].values['rate'];
    return v !== undefined ? (v * 100).toFixed(2) + '%' : 'N/A';
  }

  function reqRate() {
    if (!m['http_reqs']) return 'N/A';
    var v = m['http_reqs'].values['rate'];
    return v !== undefined ? v.toFixed(0) + ' req/s' : 'N/A';
  }

  function row(label, value) {
    return '║    ' + label.padEnd(19) + ': ' + String(value).padEnd(32) + '║';
  }

  var report = [
    '',
    '╔══════════════════════════════════════════════════════════╗',
    '║        LIVE SALE PLATFORM — LOAD TEST REPORT             ║',
    '║        run: ' + RUN_NAME.padEnd(45) + '║',
    '╠══════════════════════════════════════════════════════════╣',
    '║  THROUGHPUT                                              ║',
    row('Total requests', count('http_reqs')),
    row('Req/s', reqRate()),
    row('Error rate (5xx)', rate('error_rate')),
    '╠══════════════════════════════════════════════════════════╣',
    '║  BUY ENDPOINT (/buy)                                     ║',
    row('Queued (200)', count('orders_queued')),
    row('Sold out (409)', count('orders_sold_out')),
    row('Rate limited (429)', count('orders_rate_limited')),
    row('Errors (5xx)', count('orders_errors')),
    row('Abandoned', count('orders_abandoned')),
    row('Slot acquired rate', rate('slot_acquired_rate')),
    row('200 p50 / p95', trend('buy_latency_200_ms', 'p(50)') + ' / ' + trend('buy_latency_200_ms', 'p(95)')),
    row('409 p50 / p95', trend('buy_latency_409_ms', 'p(50)') + ' / ' + trend('buy_latency_409_ms', 'p(95)')),
    row('5xx p50 / p95', trend('buy_latency_5xx_ms', 'p(50)') + ' / ' + trend('buy_latency_5xx_ms', 'p(95)')),
    '╠══════════════════════════════════════════════════════════╣',
    '║  PAYMENT ENDPOINT (/pay)                                 ║',
    row('Confirmed orders', count('payments_confirmed')),
    row('Payment failed', count('payments_failed')),
    row('Slot expired', count('payments_slot_expired')),
    row('Errors (5xx)', count('payments_errors')),
    row('Pay success rate', rate('pay_success_rate')),
    row('p50 / p99', trend('pay_latency_ms', 'p(50)') + ' / ' + trend('pay_latency_ms', 'p(99)')),
    '╚══════════════════════════════════════════════════════════╝',
    '',
  ].join('\n');

  console.log(report);

  var out = { stdout: report };
  out['results/' + RUN_NAME + '.json'] = JSON.stringify(data, null, 2);
  out['results/' + RUN_NAME + '.txt']  = report;
  return out;
}
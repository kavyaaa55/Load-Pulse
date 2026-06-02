import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ── Custom metrics ────────────────────────────────────────────────────────────
var queued       = new Counter('orders_queued');
var soldOut      = new Counter('orders_sold_out');
var rateLimited  = new Counter('orders_rate_limited');
var errors       = new Counter('orders_errors');
var paySuccess   = new Counter('payments_confirmed');
var payFailed    = new Counter('payments_failed');
var payExpired   = new Counter('payments_slot_expired');

var buySuccessRate = new Rate('buy_success_rate');
var paySuccessRate = new Rate('pay_success_rate');

var buyLatency   = new Trend('buy_latency_ms', true);
var payLatency   = new Trend('pay_latency_ms', true);

// ── Load shape ────────────────────────────────────────────────────────────────
export var options = {
  vus: 10000,
  duration: '10s',
  thresholds: {
    'buy_latency_ms':   ['p(99)<5000'],
    'buy_success_rate': ['rate>0.01'],
    'http_req_failed':  ['rate<0.99'],
  },
};

// ── Main test function ────────────────────────────────────────────────────────
export default function () {
  var userId    = 'user_' + __VU + '_' + __ITER;
  var productId = 'iphone15';

  // Step 1: Buy request
  var buyRes = http.post(
    'http://localhost:8080/buy',
    JSON.stringify({ user_id: userId, product_id: productId }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  buyLatency.add(buyRes.timings.duration);
  buySuccessRate.add(buyRes.status === 200 || buyRes.status === 409 || buyRes.status === 429);

  check(buyRes, {
    'buy: not a 5xx': function(r) { return r.status < 500; },
  });

  if (buyRes.status === 200)      queued.add(1);
  else if (buyRes.status === 409) soldOut.add(1);
  else if (buyRes.status === 429) rateLimited.add(1);
  else                            errors.add(1);

  // Only attempt payment if queued
  if (buyRes.status !== 200) return;

  sleep(0.1);

  // Step 2: Payment
  var payRes = http.post(
    'http://localhost:8080/pay',
    JSON.stringify({ user_id: userId, product_id: productId }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  payLatency.add(payRes.timings.duration);
  paySuccessRate.add(payRes.status === 200);

  check(payRes, {
    'pay: got response': function(r) { return r.status !== 0; },
  });

  try {
    var body = JSON.parse(payRes.body);
    if (body.status === 'ORDER_CONFIRMED')  paySuccess.add(1);
    else if (body.status === 'PAYMENT_FAILED') payFailed.add(1);
    else if (body.status === 'SLOT_EXPIRED')   payExpired.add(1);
  } catch (e) {}
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
    return v !== undefined ? (v * 100).toFixed(1) + '%' : 'N/A';
  }

  function reqRate() {
    if (!m['http_reqs']) return 'N/A';
    var v = m['http_reqs'].values['rate'];
    return v !== undefined ? v.toFixed(0) + ' req/s' : 'N/A';
  }

  var report = [
    '',
    '╔══════════════════════════════════════════════════════════╗',
    '║        LIVE SALE PLATFORM — LOAD TEST REPORT             ║',
    '╠══════════════════════════════════════════════════════════╣',
    '║  THROUGHPUT                                              ║',
    '║    Total requests    : ' + String(count('http_reqs')).padEnd(34) + '║',
    '║    Req/s             : ' + String(reqRate()).padEnd(34) + '║',
    '╠══════════════════════════════════════════════════════════╣',
    '║  BUY ENDPOINT (/buy)                                     ║',
    '║    Queued            : ' + String(count('orders_queued')).padEnd(34) + '║',
    '║    Sold out (409)    : ' + String(count('orders_sold_out')).padEnd(34) + '║',
    '║    Rate limited(429) : ' + String(count('orders_rate_limited')).padEnd(34) + '║',
    '║    Errors (5xx)      : ' + String(count('orders_errors')).padEnd(34) + '║',
    '║    p50 latency       : ' + String(trend('buy_latency_ms','p(50)')).padEnd(34) + '║',
    '║    p95 latency       : ' + String(trend('buy_latency_ms','p(95)')).padEnd(34) + '║',
    '║    p99 latency       : ' + String(trend('buy_latency_ms','p(99)')).padEnd(34) + '║',
    '╠══════════════════════════════════════════════════════════╣',
    '║  PAYMENT ENDPOINT (/pay)                                 ║',
    '║    Confirmed orders  : ' + String(count('payments_confirmed')).padEnd(34) + '║',
    '║    Payment failed    : ' + String(count('payments_failed')).padEnd(34) + '║',
    '║    Slot expired      : ' + String(count('payments_slot_expired')).padEnd(34) + '║',
    '║    p50 latency       : ' + String(trend('pay_latency_ms','p(50)')).padEnd(34) + '║',
    '║    p99 latency       : ' + String(trend('pay_latency_ms','p(99)')).padEnd(34) + '║',
    '╠══════════════════════════════════════════════════════════╣',
    '║  CORRECTNESS                                             ║',
    '║    Buy success rate  : ' + String(rate('buy_success_rate')).padEnd(34) + '║',
    '║    Pay success rate  : ' + String(rate('pay_success_rate')).padEnd(34) + '║',
    '╚══════════════════════════════════════════════════════════╝',
    '',
  ].join('\n');

  console.log(report);
  return { stdout: report };
}

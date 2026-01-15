// =============================================================================
// k6 Spike Test - POST /api/events
// =============================================================================
// Spike test pattern to stress test the API under sudden load bursts
// Pattern: baseline -> spike -> return to baseline
// =============================================================================

import http from 'k6/http';
import { check, sleep } from 'k6';

// Spike test configuration
export const options = {
  stages: [
    // Baseline phase (2 minutes at 10 VUs)
    { duration: '2m', target: 10 },

    // Ramp up to spike (30 seconds to 100 VUs)
    { duration: '30s', target: 100 },

    // Hold spike (1 minute at 100 VUs)
    { duration: '1m', target: 100 },

    // Ramp down from spike (30 seconds back to 10 VUs)
    { duration: '30s', target: 10 },

    // Return to baseline (2 minutes at 10 VUs)
    { duration: '2m', target: 10 },

    // Ramp down to zero
    { duration: '30s', target: 0 },
  ],

  thresholds: {
    // 95% of requests should complete under 500ms
    'http_req_duration': ['p(95)<500'],
    // Error rate should be less than 1%
    'http_req_failed': ['rate<0.01'],
    // Checks should pass 99% of the time
    'checks': ['rate>0.99'],
  },

  // Tags for Prometheus metrics
  tags: {
    scenario: 'spike-test',
  },
};

// Target URL - use internal Docker network DNS
const API_URL = __ENV.API_URL || 'http://go-api:8080';

// Generate test event data
function generateEvent() {
  const endpoints = ['/api/users', '/api/products', '/api/orders', '/api/search'];
  const methods = ['GET', 'POST', 'PUT', 'DELETE'];
  const statusCodes = [200, 201, 204, 400, 404, 500];

  return {
    event_id: `evt_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    timestamp: new Date().toISOString(),
    endpoint: endpoints[Math.floor(Math.random() * endpoints.length)],
    method: methods[Math.floor(Math.random() * methods.length)],
    status_code: statusCodes[Math.floor(Math.random() * statusCodes.length)],
    response_time_ms: Math.floor(Math.random() * 500) + 10,
    user_agent: `k6-load-test/vu-${__VU}`,
    ip_address: `10.0.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`,
  };
}

export default function () {
  const payload = JSON.stringify(generateEvent());

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: {
      name: 'POST /api/events',
    },
  };

  const response = http.post(`${API_URL}/api/events`, payload, params);

  // Verify response
  check(response, {
    'status is 202': (r) => r.status === 202,
    'response has status field': (r) => {
      try {
        return JSON.parse(r.body).status === 'accepted';
      } catch (e) {
        return false;
      }
    },
    'response time under 500ms': (r) => r.timings.duration < 500,
  });

  // Small sleep between requests
  sleep(0.1);
}

// Summary output
export function handleSummary(data) {
  return {
    'stdout': textSummary(data, { indent: ' ', enableColors: true }),
  };
}

// Text summary helper
function textSummary(data, options) {
  const lines = [];
  lines.push('\n==================== Test Summary ====================\n');

  if (data.metrics) {
    if (data.metrics.http_reqs) {
      lines.push(`Total Requests: ${data.metrics.http_reqs.values.count}`);
      lines.push(`Requests/sec: ${data.metrics.http_reqs.values.rate.toFixed(2)}`);
    }
    if (data.metrics.http_req_duration) {
      lines.push(`Avg Response Time: ${data.metrics.http_req_duration.values.avg.toFixed(2)}ms`);
      lines.push(`p95 Response Time: ${data.metrics.http_req_duration.values['p(95)'].toFixed(2)}ms`);
    }
    if (data.metrics.http_req_failed) {
      lines.push(`Error Rate: ${(data.metrics.http_req_failed.values.rate * 100).toFixed(2)}%`);
    }
  }

  lines.push('\n======================================================\n');
  return lines.join('\n');
}

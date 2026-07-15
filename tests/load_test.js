import http from "k6/http";
import { check } from "k6";
import { Counter, Rate } from "k6/metrics";


const upstream1Requests = new Counter("upstream_1_requests");
const upstream2Requests = new Counter("upstream_2_requests");
const connectionReused = new Rate("connection_reused");
const timeoutsObserved = new Counter("timeouts_observed");

const baseUrl = __ENV.BASE_URL || "http://127.0.0.1:8090";
const testType = __ENV.TEST_TYPE || "load";


const profiles = {
  smoke: {
    scenarios: {
      smoke: {
        executor: "shared-iterations",
        exec: "regularTraffic",
        vus: 1,
        iterations: 10,
        maxDuration: "30s",
      },
    },
    thresholds: {
      http_req_failed: ["rate<0.01"],
      http_req_duration: ["p(95)<500"],
    },
  },

  load: {
    scenarios: {
      load: {
        executor: "ramping-vus",
        exec: "regularTraffic",
        stages: [
          { duration: "5s", target: 5 },
          { duration: "15s", target: 25 },
          { duration: "15s", target: 50 },
          { duration: "5s", target: 0 },
        ],
      },
    },
    thresholds: {
      http_req_failed: ["rate<0.01"],
      http_req_duration: ["p(95)<500", "p(99)<1000"],
    },
  },

  stress: {
    scenarios: {
      stress: {
        executor: "ramping-vus",
        exec: "regularTraffic",
        stages: [
          { duration: "10s", target: 50 },
          { duration: "15s", target: 100 },
          { duration: "15s", target: 200 },
          { duration: "15s", target: 200 },
          { duration: "5s", target: 0 },
        ],
      },
    },
    thresholds: {
      http_req_failed: ["rate<0.05"],
      http_req_duration: ["p(95)<1500"],
    },
  },

  keepalive: {
    scenarios: {
      keepalive: {
        executor: "per-vu-iterations",
        exec: "keepAliveTraffic",
        vus: 10,
        iterations: 20,
        maxDuration: "1m",
      },
    },
    thresholds: {
      http_req_failed: ["rate<0.01"],
      http_req_duration: ["p(95)<500"],
      connection_reused: ["rate>0.90"],
    },
  },

  timeout: {
    scenarios: {
      timeout: {
        executor: "shared-iterations",
        exec: "timeoutTraffic",
        vus: 2,
        iterations: 4,
        maxDuration: "1m",
      },
    },
    thresholds: {
      timeouts_observed: ["count>0"],
    },
  },
};


if (!(testType in profiles)) {
  throw new Error(
    `Unknown TEST_TYPE=${testType}. `
    + `Expected one of: ${Object.keys(profiles).join(", ")}`,
  );
}


export const options = profiles[testType];


function recordUpstream(response) {
  try {
    const body = response.json();

    if (body.upstream === "upstream-1") {
      upstream1Requests.add(1);
    }

    if (body.upstream === "upstream-2") {
      upstream2Requests.add(1);
    }
  } catch {
  }
}


function checkResponse(response) {
  check(response, {
    "status is 200": (result) => result.status === 200,
    "response came from upstream": (result) => (
      result.body.includes("upstream-1")
      || result.body.includes("upstream-2")
    ),
  });

  recordUpstream(response);
}


function sendRegularRequest() {
  if (__ITER % 5 === 0) {
    return http.post(
      `${baseUrl}/echo`,
      "hello world",
      {
        headers: {
          "Content-Type": "text/plain",
        },
        tags: {
          request_type: "post",
        },
      },
    );
  }

  return http.get(
    `${baseUrl}/`,
    {
      tags: {
        request_type: "get",
      },
    },
  );
}


export function regularTraffic() {
  checkResponse(sendRegularRequest());
}


export function keepAliveTraffic() {
  const first = http.get(`${baseUrl}/`);
  checkResponse(first);

  const second = http.get(`${baseUrl}/echo`);
  checkResponse(second);
  connectionReused.add(second.timings.connecting === 0);

  const third = http.get(`${baseUrl}/`);
  checkResponse(third);
  connectionReused.add(third.timings.connecting === 0);
}


export function timeoutTraffic() {
  const delaySeconds = __ENV.DELAY_SECONDS || "20";
  const response = http.get(`${baseUrl}/delay/${delaySeconds}`);

  const timeoutWasObserved = (
    response.status === 0
    || response.status === 504
  );

  timeoutsObserved.add(timeoutWasObserved ? 1 : 0);

  check(response, {
    "proxy stopped waiting for slow upstream": () => timeoutWasObserved,
  });
}

# Metrics

Resonate emits the following Prometheus metrics.

---

## aio_submissions_total

**Type:** Counter
**Help:** Total number of AIO submissions.

**Dimensions:**
- `type`
- `status` (success/failure)

---

## aio_submissions_in_flight

**Type:** Gauge
**Help:** Number of in-flight AIO submissions.

**Dimensions:**
- `type`

---

## aio_duration_seconds

**Type:** Histogram
**Help:** Duration of AIO submissions in seconds.

**Dimensions:**
- `type`

---

## aio_worker_count

**Type:** Gauge
**Help:** Number of AIO subsystem workers.

**Dimensions:**
- `type`

---

## aio_worker_submissions_in_flight

**Type:** Gauge
**Help:** Number of in-flight AIO submissions per worker.

**Dimensions:**
- `type`
- `worker`

---

## aio_connection

**Type:** Gauge
**Help:** Number of AIO subsystem connections.

**Dimensions:**
- `type`

---

## api_requests_total

**Type:** Counter
**Help:** Total number of API requests.

**Dimensions:**
- `type`
- `protocol`
- `status` (analogous to HTTP status code)

---

## api_requests_in_flight

**Type:** Gauge
**Help:** Number of in-flight API requests.

**Dimensions:**
- `type`
- `protocol`

---

## api_duration_seconds

**Type:** Histogram
**Help:** Duration of API requests in seconds.

**Dimensions:**
- `type`
- `protocol`

---

## coroutines_total

**Type:** Counter
**Help:** Total number of coroutines.

**Dimensions:**
- `type`

---

## coroutines_in_flight

**Type:** Gauge
**Help:** Number of in-flight coroutines.

**Dimensions:**
- `type`

---

## coroutines_seconds

**Type:** Histogram
**Help:** Duration of coroutines in seconds.

**Dimensions:**
- `type`

---

## promises_total

**Type:** Counter
**Help:** Count of promises.

**Dimensions:**
- `state`

---

## schedules_total

**Type:** Counter
**Help:** Count of schedules.

**Dimensions:**
- `state`

---

## tasks_total

**Type:** Counter
**Help:** Count of tasks.

**Dimensions:**
- `state`

# Metrics

Resonate emits the following Prometheus metrics.

## aio_total_submissions

The total number of AIO submissions.

Dimensions:
- type
- status (success/failure)

## aio_in_flight_submissions

The current number of in flight AIO submissions.

Dimensions:
- type

## api_total_requests

The total number of API requests.

Dimensions:
- type
- status (analagous to http status code)

## api_in_flight_requests

The current number of in flight API requests.

Dimensions:
- type

## coroutines_total

The total number of coroutines.

Dimensions
- type

## coroutines_in_flight

The current number of in flight coroutines.

Dimensions
- type

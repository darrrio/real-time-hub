# Implementation Plan: real-time-hub

IoT/environmental data simulators → Redpanda Cloud → Redpanda Connect pipelines → AI-generated comfort-regulation suggestions.

This plan is phased so each phase can be executed in a fresh chat context (via the `do` skill or manually). Each phase is self-contained: it restates the relevant facts from Phase 0 rather than assuming they're remembered.

---

## Phase 0: Documentation Discovery (findings — read before any phase)

Sources consulted: docs.redpanda.com/redpanda-connect (configuration/about, components/outputs/redpanda, components/inputs/redpanda, components/processors/openai_chat_completion, components/processors/http, guides/getting_started), docs.redpanda.com/redpanda-cloud (get-started/cloud-overview, get-started/cluster-types/serverless, develop/connect/connect-quickstart), redpanda.com/blog (benthos rename, python-redpanda-kafka-api-tutorial, analyzing-iot-telemetry-data-apache-spark).

### Allowed APIs / exact names to use

- **Redpanda Connect** = renamed Benthos. Docker image: `docker.redpanda.com/redpandadata/connect`. Run: `docker run --rm -v ./config.yaml:/config.yaml docker.redpanda.com/redpandadata/connect run /config.yaml`.
- Config top-level keys: `input`, `pipeline.processors[]`, `output`.
- **Kafka/Redpanda connectivity component: `redpanda` (input and output)** — NOT `kafka_franz` (deprecated) and NOT `kafka` (deprecated). Confirmed fields for `redpanda` output (input is symmetric plus `topics`/`consumer_group`):
  ```yaml
  output:
    redpanda:
      seed_brokers: []
      topic: ""
      key: ""
      max_in_flight: 256
      tls:
        enabled: false
        root_cas_file: ""
      sasl:
        - mechanism: SCRAM-SHA-256
          username: ""
          password: ""
      compression: ""      # e.g. snappy
      acks: all
  ```
- Redpanda Cloud auth pattern (from the official Connect Quickstart for Redpanda Cloud) — use `${secrets.NAME}` / env var interpolation, never hardcode:
  ```yaml
  tls:
    enabled: true
  sasl:
    - mechanism: SCRAM-SHA-256
      username: ${secrets.KAFKA_USER_CONNECT}
      password: ${secrets.KAFKA_PASSWORD_CONNECT}
  ```
- **Built-in AI processor exists**: `openai_chat_completion`. Fields: `server_address` (default `https://api.openai.com/v1`), `api_key` (required), `model` (required), `prompt` (bloblang mapping), `system_prompt`, `temperature`, `max_tokens`, `tools` (required, `[]` if unused), `response_format`.
- **Generic `http` processor** (for calling a non-OpenAI-shaped LLM API, e.g. Anthropic's Messages API): fields `url` (required), `verb` (default POST), `headers` (map, supports interpolation — needed for `x-api-key` / `anthropic-version`), `timeout` (default 5s), `retries` (default 3), no native Anthropic auth helper — pass API key via `headers`.
- Redpanda Cloud: **Serverless** cluster type has a free trial ($100 credit / 14 days, no card) — suitable for this project. Connection details (bootstrap host, SASL user/password) come from the cluster's Overview → "How to connect → Kafka API" tab and the Security tab (create a SCRAM-SHA-256 user).
- Python producer client: Redpanda docs present both `kafka-python` and `confluent-kafka-python`; no single "official" pick, but `kafka-python` is the simpler one shown in Redpanda's own tutorial:
  ```python
  from kafka import KafkaProducer
  import json
  producer = KafkaProducer(
      bootstrap_servers="<bootstrap-host>:9092",
      security_protocol="SASL_SSL",
      sasl_mechanism="SCRAM-SHA-256",
      sasl_plain_username="<username>",
      sasl_plain_password="<password>",
      value_serializer=lambda m: json.dumps(m).encode("utf-8"),
  )
  producer.send(topic, value=payload_dict)
  ```

### Anti-patterns to avoid

- Do NOT use `kafka_franz` or `kafka` components in new config — deprecated in favor of unified `redpanda` component.
- Do NOT hardcode Redpanda Cloud credentials in committed YAML — use env-var/secrets interpolation (`${VAR}` / `${secrets.NAME}`) and `.env` (gitignored).
- Do NOT invent an "Anthropic" native processor in Redpanda Connect — it does not exist. Use the generic `http` processor with manually-set headers, or the `openai_chat_completion` processor if using OpenAI directly.
- Do NOT assume a single official end-to-end example exists combining all of Connect + AI processor + Cloud + IoT sim — this plan assembles three separate confirmed official patterns.

### Known gaps (confirm during Phase 3, don't block on them now)

- Exact field list for `redpanda` **input** component (topics/consumer_group naming) wasn't independently re-verified beyond the output schema above — verify with `docs.redpanda.com/redpanda-connect/components/inputs/redpanda/` before writing the ingest-side config.
- Which LLM provider/key the user will actually use for inference is still open — Phase 3 includes a checkpoint to confirm this before writing the processor config (default assumption below: generic `http` processor calling Anthropic's Messages API, since that's this project's existing ecosystem; OpenAI via `openai_chat_completion` is the fallback if an OpenAI key is preferred).

---

## Phase 1: Project scaffolding & local orchestration skeleton

**What to implement:**
- Create directory layout:
  ```
  generators/            # Python simulators, one module per data domain
  connect/                # Redpanda Connect YAML pipeline configs
  docker-compose.yml
  .env.example
  ```
- `docker-compose.yml` defines services for: each generator (or one generator service parameterized by a `SIM_TYPE` env var), and one `redpanda-connect` service using image `docker.redpanda.com/redpandadata/connect`, mounting `./connect` to `/connect`, command `run /connect/<pipeline>.yaml`.
- `.env.example` lists placeholders: `REDPANDA_BOOTSTRAP_SERVERS`, `REDPANDA_SASL_USERNAME`, `REDPANDA_SASL_PASSWORD`, and an LLM key placeholder (`ANTHROPIC_API_KEY` or `OPENAI_API_KEY`, pending Phase 3 checkpoint).
- Add `.env` to `.gitignore` (check it isn't already covered).
- `generators/requirements.txt`: `kafka-python`.

**Documentation references:** Docker image name/run form and env-var interpolation pattern — Phase 0 findings above.

**Verification checklist:**
- `docker compose config` parses without error.
- `.env` is listed in `.gitignore`; `git status` shows no secrets tracked.
- Directory structure matches the layout above.

**Anti-pattern guards:** no credentials committed; no use of `kafka_franz`/`kafka` component names anywhere yet (none expected this phase).

---

## Phase 2: Data generator simulators (Python)

**What to implement:** one simulator module per domain, each a small loop that builds a JSON payload and calls `producer.send(topic, value=payload)` on an interval (configurable via env var, e.g. `EMIT_INTERVAL_SECONDS`). Copy the `KafkaProducer` construction exactly from the Phase 0 snippet (SASL_SSL/SCRAM-SHA-256), reading connection values from env vars set in `.env`.

Domains/topics (one topic per domain, prefixed `sensors.`):
- `sensors.indoor_temperature` — indoor temp per room/zone id, degrees C, timestamp.
- `sensors.outdoor_temperature` — outdoor temp, timestamp.
- `sensors.thermal_energy` — energy produced by heating unit (kWh or W), unit id, timestamp.
- `sensors.flow_temperature` — flow/return temperature regulating heating units, unit id, timestamp.
- `weather.forecast` — simulated forecast payload (temp, condition, wind) for a lookahead window; simulate locally (no need for a real external weather API unless the user wants one — flag as a decision point, don't silently call a live API).
- `comfort.setpoints` — desired comfort temperature per zone/schedule slot.
- `schedule.hours` — the active hour-scheduling program (which hours a zone is in "comfort" vs "eco" mode).

Each payload should include a `zone_id`/`unit_id`, ISO8601 `timestamp`, and the domain-specific numeric fields — keep the schema flat and simple; do not add fields not listed here.

**Documentation references:** `kafka-python` producer construction (Phase 0 snippet).

**Verification checklist:**
- Run one generator locally against `docker compose up`, confirm messages land using `rpk topic consume <topic>` (or Redpanda Console) against the real Redpanda Cloud cluster, or a local Redpanda container for dry-run testing.
- JSON payloads are valid JSON and match the flat schema above (spot-check a few messages).

**Anti-pattern guards:** don't use `confluent-kafka-python` unless explicitly switching (Phase 0 found no single official recommendation — stick with `kafka-python` for consistency unless the user asks to change); don't add retry/backoff logic beyond what `kafka-python`'s producer already provides.

---

## Phase 3: Redpanda Connect pipeline — ingest, unify, AI inference

**Checkpoint before writing config:** confirm with the user which LLM the inference step should call (Anthropic via generic `http` processor, or OpenAI via native `openai_chat_completion`). Default to the `http` + Anthropic Messages API approach unless told otherwise, since this project already operates in an Anthropic/Claude ecosystem.

**What to implement:**
- `connect/pipeline.yaml`: `input.redpanda` reading all `sensors.*`, `weather.forecast`, `comfort.setpoints`, `schedule.hours` topics (verify exact multi-topic field name against `docs.redpanda.com/redpanda-connect/components/inputs/redpanda/` — Phase 0 flagged this as unconfirmed).
- A `pipeline.processors` chain:
  1. `mapping` (bloblang) processor to tag each message with its source topic/domain and normalize into one common envelope, e.g. `{domain, zone_id, timestamp, payload}`.
  2. Either:
     - `http` processor calling Anthropic's Messages API (`https://api.anthropic.com/v1/messages`), with `headers` including `x-api-key: ${ANTHROPIC_API_KEY}` and `anthropic-version`, `verb: POST`, a JSON body built via bloblang `mapping` beforehand (a preceding `mapping` processor should construct the exact request body, since `http` processor sends the message body as-is), OR
     - `openai_chat_completion` processor per the Phase 0 config shape, if the user picked OpenAI.
  3. A `mapping` processor to extract the suggestion text/JSON from the LLM response into a clean output envelope (`{zone_id, timestamp, suggestion, raw_model_output}`).
- `output.redpanda` writing to a new topic `comfort.suggestions`, using the same `tls`/`sasl` block pattern from Phase 0 (`${secrets.*}` or env interpolation — match whatever `.env` variable names Phase 1 established).

**Documentation references:** `redpanda` output config fields, `sasl`/`tls` blocks, `openai_chat_completion` fields, `http` processor fields — all quoted in Phase 0.

**Verification checklist:**
- `docker run ... connect lint /connect/pipeline.yaml` (or `rpk connect lint` if using rpk) passes.
- Run the pipeline against the real generators + Redpanda Cloud cluster; confirm messages appear on `comfort.suggestions` with a non-empty `suggestion` field.
- Confirm no plaintext credentials appear in `connect/pipeline.yaml` (only `${VAR}` interpolations).

**Anti-pattern guards:** grep the config for `kafka_franz:` and `kafka:` (should be zero matches — only `redpanda:`); grep for any literal API key or password string (should be zero matches).

---

## Phase 4: Verification & wrap-up

1. Bring the full stack up: `docker compose up`, confirm all generator services + `redpanda-connect` service report healthy/running with no crash loops.
2. End-to-end check: watch `comfort.suggestions` topic (via `rpk topic consume` or Redpanda Console) and confirm a full cycle — synthetic sensor data in, LLM suggestion out — happens within one `EMIT_INTERVAL_SECONDS` window.
3. Re-grep the whole repo for deprecated component names (`kafka_franz`, `kafka:` as a Connect component) and for hardcoded secrets — both should be clean.
4. Confirm `.env` (with real values) is untracked by git; only `.env.example` is committed.

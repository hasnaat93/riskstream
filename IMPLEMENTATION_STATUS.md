# Implementation status

An honest record of what was built, what was run, and what was not. Last updated 2026-11-15.

## Verified locally (Windows 11, Temurin JDK 21)

| Check | Result |
|---|---|
| `./mvnw verify` | 28 tests pass: 20 topology, 3 simulation, 5 application |
| Simulation scoring | 88,406 payments, 160 injected incidents: 159 caught by the intended rule. 378 of 87,648 honest payments (0.43%) raised an alert |
| Out-of-order delivery | Same incidents caught with every payment delivered up to 20 s early or late |
| Local run, embedded mode | Started, ran for about 2.5 minutes at 12 payments a second, every injected incident caught (11 of 11), no errors apart from one noisy log line, now fixed |
| Browser walkthrough (Microsoft Edge, scripted) | 13 of 13 checks pass: live traffic, each of the four scenarios injected from the console produces its alert in the feed, scoreboard, alert evidence, OpenAPI, no horizontal overflow on a phone, no console errors. The README screenshot is from this run |

## Verified in GitHub Actions (Ubuntu, Java 21)

| Job | Result |
|---|---|
| Tests | 28 pass |
| Real Kafka broker | Riskstream started in Kafka mode against an `apache/kafka` container, state `RUNNING`, exactly-once. All four patterns detected through the broker. Killed with `kill -9` and restarted: it resumed and kept raising alerts. The committed alerts topic then held 36 alerts with 36 distinct ids: none written twice. Topics created: `payments`, `risk-alerts`, the repartition topic and the card-state changelog |
| Docker image | Booted under `--memory=512m`, served the console, detected an injected pattern, used about 179 MiB |

## Found while building

- **Window stores were far too slow.** The first state design used Kafka Streams window stores. Rule tests passed, but the 87,000-payment simulation did not finish in ten minutes, because the in-memory window store walks every timestamp in a range. Replaced with one record per card. Detailed in docs/design.md.
- **A junk record could move stream time.** A record with no payment let the broker's current time into the stream and made genuine, older-dated payments look late. A test caught it; such a record no longer moves time.
- **A payment dated in the future could silence the stream.** Its event time is now capped at arrival plus 5 minutes.
- **Simulator ids repeated after a restart**, and the stream rightly discarded them as duplicates. Ids now carry a per-run prefix.

## Not verified

- **A public deployment.** The image and `render.yaml` are ready and the image is tested in CI, but nothing is hosted.
- **Multi-broker Kafka**, partition rebalancing between several running instances, and consumer lag under load. CI uses one broker and one instance at a time.
- **Throughput limits.** The simulation shows the topology keeps up with 88,000 payments on a laptop, and nothing more precise was measured.
- **Long-running behaviour.** The longest continuous run was a few minutes.
- **Recovery with large state.** State here is a few thousand cards. Restore time from the changelog at realistic volumes was not measured.
- **Keyboard-only and screen-reader use of the console.**

## Known limitations

Listed in the [README](README.md#limitations).

# V4 RL Improvement Report (2026-04-05T02:45:23.884Z)

- Campaign: `V4 RL Improvement Program 2026-04-04 Relaunch 2`
- Campaign ID: `38e3ab18-0eab-4882-9ef0-8b3a84945cb2`
- Active policy version: `adaptive-v1`
- Active policy snapshot: `0`

## Baseline
```json
null
```

## Final Snapshot
```json
{
  "capturedAt": "2026-04-05T02:45:23.863Z",
  "queueQueued": 12714,
  "oldestQueuedAgeMinutes": 5879,
  "placeholderReviewCount": 0,
  "overlongReviewCount": 0,
  "delayedRewardEventCount": 3,
  "delayedRewardTotal": 2.0824999999999996,
  "guardrailHitRate": 0,
  "validMissingCount": 0,
  "verifiedWebsiteCount": 0,
  "topReviewReasons": [
    {
      "reason": "Search enrichment found only a low-confidence candidate for contact_email",
      "count": 162
    },
    {
      "reason": "Search enrichment found only a low-confidence candidate for website_url",
      "count": 147
    },
    {
      "reason": "Chapter record appears to be navigation or placeholder text",
      "count": 61
    },
    {
      "reason": "Chapter record name exceeded max supported length",
      "count": 31
    },
    {
      "reason": "Chapter record slug exceeded max supported length",
      "count": 31
    },
    {
      "reason": "Strategy repeated_block returned no chapter records",
      "count": 17
    },
    {
      "reason": "Strategy script_json returned no chapter records",
      "count": 15
    },
    {
      "reason": "Unable to determine a supported extraction strategy for this source page",
      "count": 9
    },
    {
      "reason": "Chapter record university exceeded max supported length",
      "count": 7
    },
    {
      "reason": "403 Client Error: Forbidden for url: https://dke.org/chapter-experience/chapters/",
      "count": 3
    },
    {
      "reason": "429 Client Error: Too Many Requests for url: https://tke.org/about-us/tke-today/notable-tekes",
      "count": 1
    },
    {
      "reason": "429 Client Error: Too Many Requests for url: https://tke.org/careers",
      "count": 1
    },
    {
      "reason": "429 Client Error: Too Many Requests for url: https://tke.org/careers/",
      "count": 1
    },
    {
      "reason": "429 Client Error: Too Many Requests for url: https://tke.org/join-tke/find-a-chapter/start-a-chapter",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='chipsi.org', port=443): Max retries exceeded with url: /where-we-are/ (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000001AC7F93FF20>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='deltasig.org', port=443): Max retries exceeded with url: /groups?type=collegiate_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x0000013C6E905940>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='deltasig.org', port=443): Max retries exceeded with url: /groups?type=collegiate_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000001B12B7CFA40>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='deltasig.org', port=443): Max retries exceeded with url: /groups?type=collegiate_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000001B12B85AC90>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='deltasig.org', port=443): Max retries exceeded with url: /groups?type=collegiate_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000001EF6B6CFDA0>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='deltasig.org', port=443): Max retries exceeded with url: /groups?type=collegiate_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000001EF6B6E47A0>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='deltasig.org', port=443): Max retries exceeded with url: /groups?type=collegiate_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x0000025068913E60>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='deltasig.org', port=443): Max retries exceeded with url: /groups?type=collegiate_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000002506891AF30>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='en.wikipedia.org', port=443): Max retries exceeded with url: /wiki/List_of_Pi_Kappa_Alpha_chapters (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x0000027518D7B080>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='sigmachi.org', port=443): Max retries exceeded with url: /chapters/ (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x00000121B00A9370>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    },
    {
      "reason": "HTTPSConnectionPool(host='sigmachi.org', port=443): Max retries exceeded with url: /chapters/ (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x00000121B0480230>: Failed to establish a new connection: [WinError 10013] An attempt was made to access a socket in a way forbidden by its access permissions'))",
      "count": 1
    }
  ]
}
```

## Promotion Decisions
```json
[]
```

## Remaining Failure Modes
```json
[]
```

# Product screenshots

These PNG/JPG files are referenced from the pitch deck (`/pitch`), the demo
deck (`/demo-deck`), and a few of the operator-manual pages. Drop the files
here with the exact filenames below — the templates already point at them.

Recommended capture: Edge / Chrome at 1920×1080, light theme unless the
filename says `-dark`. PNG preferred (transparency + sharp text); JPG is
fine for hero shots if file size matters.

## Expected filenames

| File | What it shows | Used in |
|---|---|---|
| `dashboard-light.png`           | Dashboard with KPI tiles + charts (light theme) | Pitch slide 5 ("What it is"), Demo step 1, Manual overview hero |
| `dashboard-dark.png`            | Same dashboard in dark theme | Pitch slide 10 (UX) |
| `cash-accounts-area-picker.png` | Cash accounts page with the Active-area picker open | Pitch slide 11c (Multi-stream / scoping), Demo step 7 |
| `sessions-reconcile.png`        | Inside a session — Tier-1 strict-match SWIFT vs Flexcube | Pitch slide 9 (Engine), Demo step 3 |
| `cards.png`                     | Cards page with the PCI-DSS posture banner | Pitch slide 11d (Cards module), Demo step 5c |
| `mobile-money.png`              | Mobile money page (provider tiles or empty state) | Pitch slide 11c, Demo step 5b |
| `reports.png`                   | Reports — Archived sessions table | Demo step 8 |
| `scheduler.png`                 | Scheduler with daemon RUNNING + active jobs | Pitch slide 7 (It runs itself), Demo step 7 |

## Re-shooting rules of thumb

- **Empty states are fine** — a "No mobile-money wallets yet" screenshot
  reads honestly to prospects, especially when paired with the talk track
  about pre-seeded operator profiles.
- **Avoid customer / pilot data** — if a screenshot shows real account
  numbers, BICs, or balances, retake against demo data first. Demo seed
  fixtures live in `demo_data/`.
- **Same browser chrome** — keep zoom at 100% and the sidebar fully
  expanded so all shots feel like one product, not seven.
- **PNG, lossless** — slide compression is fine, but don't pre-compress
  to JPG with visible blocking; the deck zooms these on hover.

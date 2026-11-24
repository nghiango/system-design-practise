# Ticket Booking plan (2026-11-24)

- booking-api: events/seats schema via Flyway (both services per flash-sale convention).
  `events(id bigserial, name, price)`; `seats(event_id, seat, status FREE|HELD|BOOKED, hold_owner, hold_expires_at, booked_by, pk(event_id,seat))`; `payments(order...)`.
  Endpoints:
  - POST /api/events {name, price, rows, cols} → create event + generate seats (e.g. 10x10, transactional).
  - GET /api/events/{id}/seats → seat map with status (expired holds shown as FREE — filter on read).
  - POST /api/events/{id}/hold {seat, owner} → atomic `UPDATE seats SET status='HELD', hold_owner=?, hold_expires_at=now()+5min WHERE event_id=? AND seat=? AND (status='FREE' OR hold_expires_at < now())` → 1 row = 200 {holdId expiresAt}; 0 rows = 409. Publish ticket-booking.booking.events {type:HELD, seat, owner}.
  - POST /api/holds/{seat? or hold}/confirm {owner} → "payment": simulated (deterministic, e.g. owner 'decline' fails → release) → UPDATE ... WHERE status='HELD' AND hold_owner=? AND hold_expires_at>now() → BOOKED + payment row; expired → 410 gone and free seat. Publish CONFIRMED/RELEASED.
  - DELETE /api/holds → owner releases explicitly.
- booking-worker: @Scheduled sweeper every 10s: `UPDATE seats SET status='FREE', hold_owner=null WHERE status='HELD' AND hold_expires_at<now()` + publishes EXPIRED events; Kafka consumer on booking.events → booking_history append-only audit table. (Documents: sweeper vs Redis TTL vs delayed queue.)
- Race evidence: N threads hold same seat → exactly 1 succeeds; hold expires → another owner can take it; confirm after expiry → 410 and seat free.
- k6 load/book.js: create event 10x10, 500 users hold distinct seats, confirm 60%, sweeper frees the rest — assert booked count.
- Docs README + decisions/001-postgres-locking.md, 002-expiry-sweeper.md.

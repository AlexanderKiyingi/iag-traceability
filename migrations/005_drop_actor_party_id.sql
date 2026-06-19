-- Drop the never-populated actor_party_id column. The auth token (authclient.Claims)
-- carries no party/supplier identity, so there was no source to populate it, and
-- "actor party" conflated the acting user with the event's subject party. The
-- acting user is already captured by actor_user_id.

ALTER TABLE trace_events DROP COLUMN IF EXISTS actor_party_id;

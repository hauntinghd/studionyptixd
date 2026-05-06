-- Catalyst Reference Videos: per-user library of inspiration / "winning"
-- YouTube videos. Studio's generators thread these into Grok system prompts
-- so output mimics the patterns that already work in the wild.
--
-- Phase 1 (this migration): metadata only — yt-dlp pulls title/desc/tags/views.
-- Phase 2 (later): transcript (Whisper) + keyframe vision analysis +
-- Grok-decoded pattern_summary jsonb.

CREATE TABLE IF NOT EXISTS catalyst_reference_videos (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid NOT NULL,

    -- Which Studio channel this video inspires.
    -- Matches long_form/prompts/channels.py keys + skeleton_ai categories.
    -- e.g. 'empire_magnates', 'lacuna', 'zerotier', 'cryptic_science', etc.
    -- '' (empty) = applies to all generations for the user.
    channel_key     text NOT NULL DEFAULT '',

    -- yt-dlp metadata.
    yt_video_id     text NOT NULL,
    yt_url          text NOT NULL,
    title           text,
    description     text,
    tags            text[],
    yt_channel_id   text,
    channel_title   text,
    duration_sec    integer,
    view_count      bigint,
    like_count      bigint,
    comment_count   bigint,
    thumbnail_url   text,
    upload_date     date,                  -- YYYYMMDD from yt-dlp

    -- Phase 2 / deferred fields.
    transcript      text,                   -- Whisper output
    keyframe_count  integer DEFAULT 0,      -- # of analyzed keyframes
    pattern_summary jsonb,                  -- Grok-decoded patterns
    analyzed_at     timestamptz,

    -- User-supplied notes (why this video matters).
    user_notes      text DEFAULT '',

    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS catalyst_reference_videos_user_idx
    ON catalyst_reference_videos (user_id);
CREATE INDEX IF NOT EXISTS catalyst_reference_videos_user_channel_idx
    ON catalyst_reference_videos (user_id, channel_key);
CREATE UNIQUE INDEX IF NOT EXISTS catalyst_reference_videos_user_video_idx
    ON catalyst_reference_videos (user_id, yt_video_id);

-- Row-level security: user can only see their own references.
ALTER TABLE catalyst_reference_videos ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "user can read own references" ON catalyst_reference_videos;
CREATE POLICY "user can read own references" ON catalyst_reference_videos
    FOR SELECT USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "user can insert own references" ON catalyst_reference_videos;
CREATE POLICY "user can insert own references" ON catalyst_reference_videos
    FOR INSERT WITH CHECK (auth.uid() = user_id);

DROP POLICY IF EXISTS "user can update own references" ON catalyst_reference_videos;
CREATE POLICY "user can update own references" ON catalyst_reference_videos
    FOR UPDATE USING (auth.uid() = user_id);

DROP POLICY IF EXISTS "user can delete own references" ON catalyst_reference_videos;
CREATE POLICY "user can delete own references" ON catalyst_reference_videos
    FOR DELETE USING (auth.uid() = user_id);
